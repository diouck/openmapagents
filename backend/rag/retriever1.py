"""
retriever.py — Retrieval sémantique des outils via pgvector
Prend une query utilisateur → retourne top-k tool docs pertinents

Stratégie hybride :
  1. Embedding sémantique (pgvector cosine similarity) — poids principal
  2. Filtre domaine optionnel (si le router a déjà classifié)
  3. Boost lexical sur les triggers (tiebreaker)
  4. Fallback lexical pur si pgvector indisponible

Usage :
    from rag.retriever import Retriever
    retriever = Retriever()
    tools = retriever.retrieve("restaurants à Nantes", top_k=4)
"""

import os
import json
import logging
from typing import Optional
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("retriever")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB",       "openmap")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
TABLE_NAME  = "rag_tools"

# Seuil minimum de similarité cosine (0-1)
# En dessous → l'outil est considéré non pertinent
MIN_SCORE   = float(os.getenv("RAG_MIN_SCORE", "0.30"))

# Top-k par défaut
DEFAULT_TOP_K = int(os.getenv("RAG_TOP_K", "5"))


# ─── CONNEXION POOLÉE ────────────────────────────────────────────────────────
class PGPool:
    """Pool de connexions minimal pour FastAPI (thread-safe)."""

    def __init__(self):
        self._conn = None

    def get(self):
        try:
            if self._conn and not self._conn.closed:
                # Test que la connexion est vivante
                self._conn.cursor().execute("SELECT 1")
                return self._conn
        except Exception:
            self._conn = None

        import psycopg2
        self._conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
        )
        log.info("✓ Nouvelle connexion PG retriever")
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None


_pool = PGPool()


# ─── RETRIEVER PRINCIPAL ──────────────────────────────────────────────────────
class Retriever:
    """
    Retriever hybride : embedding pgvector + boost lexical triggers.

    Architecture :
      query → embed → pgvector cosine search → boost triggers → top-k tools
                           ↓ si PG indisponible
                      fallback lexical pur (triggers)
    """

    def __init__(self):
        self._embed_fn = None  # lazy load

    def _get_embed_fn(self):
        """Charge le modèle d'embedding (lazy, une seule fois)."""
        if self._embed_fn is None:
            from rag.embedder import embed_query
            self._embed_fn = embed_query
        return self._embed_fn

    def retrieve(
        self,
        query: str,
        top_k: int = DEFAULT_TOP_K,
        domain: Optional[str] = None,
        min_score: float = MIN_SCORE,
    ) -> list[dict]:
        """
        Recherche les top_k outils les plus pertinents pour une query.

        Args:
            query      : question utilisateur (fr ou en)
            top_k      : nombre d'outils à retourner
            domain     : filtre optionnel sur le domaine (ex: "satellite")
            min_score  : seuil minimum de similarité (0-1)

        Returns:
            Liste de tool docs enrichis avec un champ 'score' (0-1)
        """
        try:
            return self._retrieve_pgvector(query, top_k, domain, min_score)
        except Exception as e:
            log.warning(f"pgvector indisponible ({e}), fallback lexical")
            return self._retrieve_lexical(query, top_k, domain)

    def _retrieve_pgvector(
        self,
        query: str,
        top_k: int,
        domain: Optional[str],
        min_score: float,
    ) -> list[dict]:
        """Recherche via pgvector (cosine similarity) + boost triggers."""

        # 1. Embed la query
        embed_fn = self._get_embed_fn()
        vec = embed_fn(query)
        vec_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"

        # 2. Requête pgvector
        # On récupère top_k * 2 pour pouvoir re-ranker avec le boost triggers
        fetch_k = min(top_k * 2, 20)

        domain_filter = "AND domain = %(domain)s" if domain else ""

        sql = f"""
        SELECT DISTINCT ON (tool_id)
            tool_id AS id,
            domain,
            title,
            description,
            triggers,
            examples,
            schema,
            endpoint,
            -- Meilleur score parmi tous les vecteurs du tool (main + examples)
            1 - MIN(embedding <=> %(vec)s::vector) OVER (PARTITION BY tool_id) AS cosine_score
        FROM {TABLE_NAME}
        WHERE 1=1
          {domain_filter}
          AND (embedding <=> %(vec)s::vector) < %(max_dist)s
        ORDER BY tool_id, embedding <=> %(vec)s::vector
        LIMIT %(fetch_k)s
        """

        params = {
            "vec":      vec_str,
            "max_dist": 1.0 - min_score,
            "fetch_k":  fetch_k,
            "domain":   domain,
        }

        conn = _pool.get()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

        if not rows:
            log.info(f"pgvector: 0 résultats pour '{query}'")
            return []

        # 3. Construire les résultats avec boost lexical
        query_lower = query.lower()
        results = []

        for row in rows:
            doc = dict(zip(cols, row))
            cosine = float(doc["cosine_score"])

            # Boost triggers : +0.02 par trigger matché (max +0.15)
            trigger_boost = min(
                0.15,
                sum(0.02 for t in doc["triggers"] if t.lower() in query_lower),
            )

            final_score = cosine + trigger_boost

            # Parser les champs JSONB
            if isinstance(doc["examples"], str):
                doc["examples"] = json.loads(doc["examples"])
            if isinstance(doc["schema"], str):
                doc["schema"] = json.loads(doc["schema"])

            doc["score"]         = round(final_score, 4)
            doc["cosine_score"]  = round(cosine, 4)
            doc["trigger_boost"] = round(trigger_boost, 4)
            results.append(doc)

        # 4. Re-trier par score final, prendre top_k
        results.sort(key=lambda x: -x["score"])
        results = results[:top_k]

        log.info(
            f"RAG '{query}' → {len(results)} tools : "
            + ", ".join(f"{r['id']}({r['score']:.2f})" for r in results)
        )
        return results

    def _retrieve_lexical(
        self,
        query: str,
        top_k: int,
        domain: Optional[str],
    ) -> list[dict]:
        """
        Fallback lexical pur sur les triggers.
        Utilisé si pgvector est indisponible (pas d'extension, PG down, etc.)
        """
        from rag.tool_registry import TOOL_REGISTRY

        query_lower = query.lower()
        scored = []

        for tool in TOOL_REGISTRY:
            if domain and tool["domain"] != domain:
                continue
            score = sum(1 for t in tool["triggers"] if t.lower() in query_lower)
            if score > 0:
                result = dict(tool)
                result["score"] = score / max(len(tool["triggers"]), 1)
                result["cosine_score"] = 0.0
                result["trigger_boost"] = result["score"]
                scored.append(result)

        scored.sort(key=lambda x: -x["score"])
        log.info(f"Lexical fallback '{query}' → {len(scored[:top_k])} tools")
        return scored[:top_k]

    def retrieve_for_domains(
        self,
        query: str,
        domains: list[str],
        top_k_per_domain: int = 3,
    ) -> dict[str, list[dict]]:
        """
        Recherche par domaine — utile quand le router identifie plusieurs domaines candidats.
        Retourne un dict {domain: [tools]}
        """
        result = {}
        for domain in domains:
            result[domain] = self.retrieve(query, top_k=top_k_per_domain, domain=domain)
        return result

    def retrieve_and_merge(
        self,
        query: str,
        domains: list[str],
        top_k: int = 5,
    ) -> list[dict]:
        """
        Recherche sur plusieurs domaines, fusionne et déduplique par score.
        Utilisé quand le router est incertain entre 2 domaines.
        """
        all_tools = {}
        for domain in domains:
            tools = self.retrieve(query, top_k=top_k, domain=domain, min_score=0.20)
            for t in tools:
                tid = t["id"]
                if tid not in all_tools or t["score"] > all_tools[tid]["score"]:
                    all_tools[tid] = t

        merged = sorted(all_tools.values(), key=lambda x: -x["score"])
        return merged[:top_k]


# ─── SINGLETON ────────────────────────────────────────────────────────────────
_retriever: Optional[Retriever] = None


def get_retriever() -> Retriever:
    """Retourne le singleton Retriever (lazy init)."""
    global _retriever
    if _retriever is None:
        _retriever = Retriever()
    return _retriever


# ─── FONCTIONS SHORTCUT ───────────────────────────────────────────────────────
def retrieve_tools(
    query: str,
    top_k: int = DEFAULT_TOP_K,
    domain: Optional[str] = None,
    conn=None,  # conservé pour compatibilité mais non utilisé (pool interne)
) -> list[dict]:
    """
    Shortcut principale.
    Retourne top_k tool docs pour une query utilisateur.
    """
    return get_retriever().retrieve(query, top_k=top_k, domain=domain)


def retrieve_tool_ids(query: str, top_k: int = DEFAULT_TOP_K) -> list[str]:
    """Retourne uniquement les IDs des outils pertinents."""
    return [t["id"] for t in retrieve_tools(query, top_k=top_k)]


def retrieve_schemas_for_llm(query: str, top_k: int = DEFAULT_TOP_K) -> list[dict]:
    """
    Retourne les schemas LiteLLM des outils pertinents.
    Prêt à passer dans le paramètre `tools` de LiteLLM.
    """
    from rag.tool_registry import get_schemas_for_llm
    tool_ids = retrieve_tool_ids(query, top_k=top_k)
    return get_schemas_for_llm(tool_ids)


# ─── TEST ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    TEST_QUERIES = [
        ("restaurants à Nantes", None),
        ("Nantes restaurants",   None),          # même résultat en inversé
        ("NDVI Dakar Sentinel-2", None),
        ("satellite imagery Dakar", None),        # en anglais
        ("itinéraire vélo gare château", None),
        ("zone accessible 15 minutes voiture", None),
        ("ma base PostGIS parcelles cadastre", None),
        ("buffer 500m autour des commerces", None),
        ("temperature surface landsat", "satellite"),  # avec filtre domaine
        ("buildings around castle", None),        # en anglais
        ("occupation du sol worldcover", None),
        ("divisons administratives communes", None),
    ]

    print(f"\n{'='*60}")
    print("TEST RETRIEVER OpenMapAgents")
    print(f"{'='*60}")

    # Mode fallback lexical (si pas de PG)
    use_fallback = "--fallback" in sys.argv

    retriever = Retriever()

    for query, domain in TEST_QUERIES:
        if use_fallback:
            results = retriever._retrieve_lexical(query, top_k=3, domain=domain)
        else:
            results = retriever.retrieve(query, top_k=3, domain=domain)

        domain_str = f" [domain={domain}]" if domain else ""
        print(f"\n📝 '{query}'{domain_str}")
        for r in results:
            print(f"   [{r['score']:.3f}] {r['id']:35s} {r['title']}")

    print(f"\n{'='*60}")
    print("✓ Tests terminés")
