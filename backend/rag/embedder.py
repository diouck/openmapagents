"""
rag/embedder.py — Embedder pgvector pour OpenMapAgents
=======================================================
Adapté au .env :
  PG_HOST=geoafrica.fr / PG_PORT=5435 / PG_DB=openmapagents
  EMBED_MODEL=sentence-transformers/paraphrase-multilingual-mpnet-base-v2
  EMBED_DIM=768

Crée la table rag_tools avec index HNSW si elle n'existe pas.
Embedde chaque tool du registry au démarrage si vecteur absent.
Supporte aussi les embeddings via LiteLLM (OpenAI / OpenRouter).
"""

import os
import json
import logging
import hashlib
from typing import Optional

log = logging.getLogger("embedder")

# ── Config — depuis le .env ───────────────────────────────────
PG_HOST     = os.getenv("PG_HOST",     "geoafrica.fr")
PG_PORT     = int(os.getenv("PG_PORT", "5435"))
PG_DB       = os.getenv("PG_DB",       "openmapagents")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "Postgres2024!")

EMBED_MODEL = os.getenv(
    "EMBED_MODEL",
    "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
)
EMBED_DIM   = int(os.getenv("EMBED_DIM", "768"))

# Fallback OpenAI embeddings si sentence-transformers non dispo
OPENAI_EMBED_MODEL = "text-embedding-3-small"
OPENAI_EMBED_DIM   = 1536


def _get_conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
        connect_timeout=5,
    )


def _ensure_schema(conn):
    """Crée la table rag_tools et l'index HNSW si absents."""
    with conn.cursor() as cur:
        # Extension pgvector
        #cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

        # Table principale
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS rag_tools (
                id           SERIAL PRIMARY KEY,
                tool_id      VARCHAR(100) UNIQUE NOT NULL,
                server       VARCHAR(50),
                tool         VARCHAR(100),
                description  TEXT,
                triggers     JSONB,
                output_action VARCHAR(50),
                few_shot     JSONB,
                embedding    vector({EMBED_DIM}),
                embed_model  VARCHAR(200),
                embed_hash   VARCHAR(64),
                created_at   TIMESTAMP DEFAULT NOW(),
                updated_at   TIMESTAMP DEFAULT NOW()
            )
        """)

        # Index HNSW pour la similarité cosine (rapide)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS rag_tools_hnsw_idx
            ON rag_tools
            USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
        """)

        # Index sur server pour les requêtes filtrées
        cur.execute("""
            CREATE INDEX IF NOT EXISTS rag_tools_server_idx
            ON rag_tools (server)
        """)

    conn.commit()
    log.info("✓ Schema pgvector rag_tools prêt")


def _text_for_embedding(entry: dict) -> str:
    """
    Construit le texte à embedder pour un tool.
    Combine description + triggers + few_shot pour un vecteur riche.
    """
    parts = [entry["description"]]

    # Ajouter les triggers comme phrases naturelles
    triggers = entry.get("triggers", [])
    if triggers:
        parts.append("Mots-clés : " + ", ".join(triggers))

    # Ajouter les exemples few-shot
    few_shot = entry.get("few_shot", [])
    for ex in few_shot[:2]:  # max 2 exemples
        parts.append(f"Exemple : {ex.get('user', '')}")

    return " | ".join(parts)


def _embed_sentence_transformers(text: str) -> list[float]:
    """Embedding local via sentence-transformers."""
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(EMBED_MODEL)
    vec   = model.encode(text, normalize_embeddings=True)
    return vec.tolist()


def _embed_litellm(text: str) -> list[float]:
    """Embedding via LiteLLM (OpenAI text-embedding-3-small)."""
    from litellm import embedding
    resp = embedding(
        model=OPENAI_EMBED_MODEL,
        input=[text],
    )
    return resp.data[0]["embedding"]


def embed_text(text: str) -> tuple[list[float], str, int]:
    """
    Embedde un texte. Retourne (vecteur, modèle_utilisé, dimension).
    Priorité : sentence-transformers → LiteLLM OpenAI
    """
    # Essayer sentence-transformers en premier
    try:
        vec = _embed_sentence_transformers(text)
        return vec, EMBED_MODEL, len(vec)
    except ImportError:
        log.debug("sentence-transformers non installé → LiteLLM")
    except Exception as e:
        log.warning(f"sentence-transformers error: {e} → LiteLLM")

    # Fallback LiteLLM
    try:
        vec = _embed_litellm(text)
        return vec, OPENAI_EMBED_MODEL, len(vec)
    except Exception as e:
        raise RuntimeError(f"Aucun backend d'embedding disponible: {e}")


def _content_hash(entry: dict) -> str:
    """Hash du contenu pour détecter les changements."""
    content = {
        "description": entry.get("description",""),
        "triggers":    entry.get("triggers",[]),
        "few_shot":    entry.get("few_shot",[]),
    }
    return hashlib.md5(
        json.dumps(content, sort_keys=True).encode()
    ).hexdigest()


# ═══════════════════════════════════════════════════════════════
# EMBEDDER PRINCIPAL
# ═══════════════════════════════════════════════════════════════

class Embedder:
    """
    Gère l'embedding et le stockage des tools dans pgvector.
    Appelé au démarrage de l'application (warmup).
    """

    def __init__(self):
        self._conn = None
        self._ready = False

    def _get_conn(self):
        if not self._conn or self._conn.closed:
            self._conn = _get_conn()
        return self._conn

    def setup(self):
        """Initialise le schema pgvector. Appelé dans lifespan()."""
        try:
            conn = self._get_conn()
            _ensure_schema(conn)
            self._ready = True
        except Exception as e:
            log.error(f"Embedder setup failed: {e}")
            raise

    def warmup(self, force: bool = False) -> dict:
        """
        Embedde tous les tools du registry manquants ou modifiés.
        force=True : re-embedde tout même si déjà présent.
        Retourne les stats de l'opération.
        """
        from rag.tool_registry import TOOL_REGISTRY

        if not self._ready:
            self.setup()

        conn    = self._get_conn()
        stats   = {"total": len(TOOL_REGISTRY),
                   "embedded": 0, "skipped": 0, "errors": 0}

        for entry in TOOL_REGISTRY:
            tool_id = entry["id"]
            h       = _content_hash(entry)

            # Vérifier si déjà embedé et à jour
            if not force:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT embed_hash FROM rag_tools WHERE tool_id=%s",
                        (tool_id,)
                    )
                    row = cur.fetchone()
                    if row and row[0] == h:
                        stats["skipped"] += 1
                        continue

            # Calculer l'embedding
            try:
                text    = _text_for_embedding(entry)
                vec, model_used, dim = embed_text(text)

                # Adapter la dimension si nécessaire
                if dim != EMBED_DIM:
                    log.warning(
                        f"Dimension {dim} ≠ {EMBED_DIM} pour {model_used}. "
                        f"Mise à jour de la table."
                    )
                    with conn.cursor() as cur:
                        cur.execute(f"""
                            ALTER TABLE rag_tools
                            ALTER COLUMN embedding TYPE vector({dim})
                        """)
                    conn.commit()

                # Upsert dans pgvector
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO rag_tools
                            (tool_id, server, tool, description, triggers,
                             output_action, few_shot, embedding,
                             embed_model, embed_hash, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s::vector,%s,%s,NOW())
                        ON CONFLICT (tool_id) DO UPDATE SET
                            description   = EXCLUDED.description,
                            triggers      = EXCLUDED.triggers,
                            few_shot      = EXCLUDED.few_shot,
                            embedding     = EXCLUDED.embedding,
                            embed_model   = EXCLUDED.embed_model,
                            embed_hash    = EXCLUDED.embed_hash,
                            updated_at    = NOW()
                    """, (
                        tool_id,
                        entry["server"],
                        entry["tool"],
                        entry["description"],
                        json.dumps(entry.get("triggers",[])),
                        entry.get("output_action",""),
                        json.dumps(entry.get("few_shot",[])),
                        str(vec),
                        model_used,
                        h,
                    ))
                conn.commit()
                stats["embedded"] += 1
                log.debug(f"Embedé: {tool_id} ({model_used[:30]})")

            except Exception as e:
                stats["errors"] += 1
                log.error(f"Embed error {tool_id}: {e}")

        log.info(
            f"Embedder warmup: {stats['embedded']} embedés, "
            f"{stats['skipped']} inchangés, {stats['errors']} erreurs"
        )
        return stats

    def embed_query(self, query: str) -> Optional[list[float]]:
        """
        Embedde une query utilisateur pour la recherche.
        Retourne le vecteur ou None si échec.
        """
        try:
            vec, _, _ = embed_text(query)
            return vec
        except Exception as e:
            log.warning(f"embed_query failed: {e}")
            return None

    def stats(self) -> dict:
        """Stats de la table rag_tools."""
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT server), "
                    "MIN(updated_at), MAX(updated_at) FROM rag_tools"
                )
                row = cur.fetchone()
                cur.execute(
                    "SELECT server, COUNT(*) FROM rag_tools "
                    "GROUP BY server ORDER BY server"
                )
                by_server = dict(cur.fetchall())
            return {
                "total_tools": row[0] or 0,
                "servers":     row[1] or 0,
                "last_update": str(row[3]) if row[3] else None,
                "by_server":   by_server,
                "embed_model": EMBED_MODEL,
                "embed_dim":   EMBED_DIM,
            }
        except Exception as e:
            return {"error": str(e)}


# Singleton
_embedder: Optional[Embedder] = None

def get_embedder() -> Embedder:
    global _embedder
    if _embedder is None:
        _embedder = Embedder()
    return _embedder
