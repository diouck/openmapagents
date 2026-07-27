"""
rag/retriever.py — Retriever pgvector pour OpenMapAgents
=========================================================
Adapté au .env :
  PG_HOST=geoafrica.fr / PG_PORT=5435 / PG_DB=openmapagents
  EMBED_DIM=768
  ROUTER_CONFIDENCE_HIGH=0.85
  ROUTER_CONFIDENCE_LOW=0.55

Stratégie de recherche :
  1. Trigger exact match (boost +0.2) — très rapide
  2. Cosine similarity pgvector HNSW — sémantique
  3. Combine les deux scores → résultat final
  4. Fallback keyword si pgvector indispo

Utilisé par :
  - orchestrator.py : identifier les MCP servers à appeler
  - debug_route endpoint de agent.py (retrieve_tools)
  - debate_layer.py : RAG → system prompts des agents de débat
"""

import os
import json
import logging
from typing import Optional

log = logging.getLogger("retriever")

# ── Config ────────────────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST",     "geoafrica.fr")
PG_PORT     = int(os.getenv("PG_PORT", "5435"))
PG_DB       = os.getenv("PG_DB",       "openmapagents")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "Postgres2024!")

CONFIDENCE_HIGH = float(os.getenv("ROUTER_CONFIDENCE_HIGH", "0.85"))
CONFIDENCE_LOW  = float(os.getenv("ROUTER_CONFIDENCE_LOW",  "0.55"))
TRIGGER_BOOST   = float(os.getenv("RAG_TRIGGER_BOOST",      "0.20"))
DEFAULT_TOP_K   = int(os.getenv("RAG_TOP_K",                "5"))
MIN_SCORE       = float(os.getenv("RAG_MIN_SCORE",          "0.40"))


def _get_conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
        connect_timeout=3,
        options="-c statement_timeout=5000",
    )


def _keyword_fallback(query: str, top_k: int) -> list[dict]:
    """
    Fallback keyword si pgvector indispo.
    Recherche les triggers exacts dans le registre.
    """
    from rag.tool_registry import TOOL_REGISTRY, find_by_trigger

    q          = query.lower()
    matched_ids = find_by_trigger(q)
    results    = []

    for entry in TOOL_REGISTRY:
        if entry["id"] not in matched_ids:
            # Fallback : chercher dans la description
            desc = entry["description"].lower()
            if not any(word in desc for word in q.split() if len(word) > 3):
                continue

        # Score basé sur le nombre de triggers matchés
        trigger_matches = sum(
            1 for t in entry["triggers"] if t in q
        )
        score = min(0.5 + trigger_matches * 0.1, 0.85)

        results.append({
            "id":            entry["id"],
            "server":        entry["server"],
            "tool":          entry["tool"],
            "score":         round(score, 3),
            "output_action": entry.get("output_action",""),
            "few_shot":      entry.get("few_shot",[]),
            "method":        "keyword",
        })

    # Trier par score
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]


# ═══════════════════════════════════════════════════════════════
# RETRIEVER PRINCIPAL
# ═══════════════════════════════════════════════════════════════

class Retriever:
    """
    Retriever RAG pour OpenMapAgents.
    Compatible avec l'interface attendue par agent.py :
        from rag.retriever import retrieve_tools
        tools = retrieve_tools(query, top_k=5)
    """

    def __init__(self):
        self._pool = None
        self._pg_ok = None

    def _check_pg(self) -> bool:
        if self._pg_ok is None:
            try:
                conn = _get_conn()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM rag_tools "
                        "WHERE embedding IS NOT NULL"
                    )
                    count = cur.fetchone()[0]
                conn.close()
                self._pg_ok = count > 0
                if self._pg_ok:
                    log.info(f"✓ pgvector retriever prêt ({count} vecteurs)")
                else:
                    log.warning("pgvector vide → fallback keyword")
            except Exception as e:
                log.warning(f"pgvector indispo ({e}) → keyword fallback")
                self._pg_ok = False
        return self._pg_ok

    def retrieve(
        self,
        query:     str,
        top_k:     int            = DEFAULT_TOP_K,
        server:    Optional[str]  = None,
        min_score: float          = MIN_SCORE,
    ) -> list[dict]:
        """
        Recherche les tools les plus pertinents pour une query.

        Args:
            query:     requête utilisateur en langage naturel
            top_k:     nombre max de résultats
            server:    filtrer sur un server MCP spécifique
            min_score: score minimum pour inclure un résultat

        Returns:
            liste de dicts avec id, server, tool, score, few_shot, output_action
        """
        if not self._check_pg():
            return _keyword_fallback(query, top_k)

        # 1. Trigger exact match (instantané)
        from rag.tool_registry import find_by_trigger
        trigger_matches = set(find_by_trigger(query))

        # 2. Embedding de la query
        from rag.embedder import get_embedder
        embedder = get_embedder()
        vec      = embedder.embed_query(query)

        if vec is None:
            log.warning("embed_query failed → keyword fallback")
            return _keyword_fallback(query, top_k)

        # 3. Requête pgvector cosine similarity
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                # Filtre server optionnel
                server_clause = ""
                params: list = [str(vec), top_k * 3]  # on prend plus pour le reranking
                if server:
                    server_clause = "AND server = %s"
                    params.insert(1, server)

                cur.execute(f"""
                    SELECT
                        tool_id,
                        server,
                        tool,
                        output_action,
                        few_shot,
                        1 - (embedding <=> %s::vector) AS cosine_score
                    FROM rag_tools
                    WHERE embedding IS NOT NULL
                    {server_clause}
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, [str(vec)] + ([server] if server else []) + [str(vec), top_k * 3])

                rows = cur.fetchall()
            conn.close()

        except Exception as e:
            log.warning(f"pgvector query failed ({e}) → keyword fallback")
            self._pg_ok = False
            return _keyword_fallback(query, top_k)

        # 4. Reranking : cosine + boost triggers
        results = []
        for row in rows:
            tool_id, srv, tool, output_action, few_shot_raw, cosine = row

            # Boost si trigger exact match
            boost = TRIGGER_BOOST if tool_id in trigger_matches else 0.0
            final_score = min(float(cosine) + boost, 1.0)

            if final_score < min_score:
                continue

            # Parser few_shot
            few_shot = []
            if few_shot_raw:
                try:
                    few_shot = json.loads(few_shot_raw) \
                               if isinstance(few_shot_raw, str) \
                               else few_shot_raw
                except Exception:
                    pass

            results.append({
                "id":            tool_id,
                "server":        srv,
                "tool":          tool,
                "score":         round(final_score, 3),
                "cosine_score":  round(float(cosine), 3),
                "boost":         boost,
                "output_action": output_action or "",
                "few_shot":      few_shot,
                "method":        "pgvector" + ("+trigger" if boost > 0 else ""),
            })

        # Trier par score final décroissant et limiter à top_k
        results.sort(key=lambda x: x["score"], reverse=True)
        results = results[:top_k]

        # Ajouter les tools avec trigger match qui ne seraient pas dans top_k
        existing_ids = {r["id"] for r in results}
        for tid in trigger_matches:
            if tid not in existing_ids and len(results) < top_k:
                entry = self._get_entry_by_id(tid)
                if entry:
                    results.append({
                        **entry,
                        "score":        round(TRIGGER_BOOST + 0.5, 3),
                        "cosine_score": 0.0,
                        "boost":        TRIGGER_BOOST,
                        "method":       "trigger_only",
                    })

        log.debug(
            f"RAG '{query[:40]}' → {len(results)} outils "
            f"(top: {results[0]['tool'] if results else 'none'} "
            f"score={results[0]['score'] if results else 0})"
        )
        return results

    def _get_entry_by_id(self, tool_id: str) -> Optional[dict]:
        """Récupère un entry du registry par son ID."""
        from rag.tool_registry import TOOL_REGISTRY
        for entry in TOOL_REGISTRY:
            if entry["id"] == tool_id:
                return {
                    "id":            entry["id"],
                    "server":        entry["server"],
                    "tool":          entry["tool"],
                    "output_action": entry.get("output_action",""),
                    "few_shot":      entry.get("few_shot",[]),
                }
        return None

    def build_few_shot_prompt(
        self, tools: list[dict], max_examples: int = 3
    ) -> str:
        """
        Construit le bloc few-shot à injecter dans un system prompt.
        Appelé par les agents de débat.
        """
        if not tools:
            return ""

        lines = ["\nEXEMPLES D'UTILISATION DES TOOLS DISPONIBLES:"]
        count = 0

        for tool in tools:
            for ex in tool.get("few_shot", []):
                if count >= max_examples:
                    break
                user   = ex.get("user","")
                t_name = tool.get("tool","")
                params = ex.get("params",{})
                lines.append(
                    f'- User: "{user}"\n'
                    f'  → Appeler: {t_name}({json.dumps(params, ensure_ascii=False)})'
                )
                count += 1
            if count >= max_examples:
                break

        lines.append(
            "\nRÈGLE: Tu DOIS appeler le tool approprié. "
            "Ne jamais répondre en texte sans appel tool si un tool correspond."
        )
        return "\n".join(lines)

    def get_tools_context(self, query: str, top_k: int = 3) -> dict:
        """
        Retourne le contexte complet des tools pour une query.
        Utilisé par l'orchestrateur et les agents de débat.
        """
        tools = self.retrieve(query, top_k=top_k)

        if not tools:
            return {
                "tools":       [],
                "servers":     [],
                "few_shot_prompt": "",
                "confidence":  0.0,
                "method":      "none",
            }

        top_score  = tools[0]["score"]
        servers    = list(dict.fromkeys(t["server"] for t in tools))
        confidence = (
            "high"   if top_score >= CONFIDENCE_HIGH else
            "medium" if top_score >= CONFIDENCE_LOW  else
            "low"
        )

        return {
            "tools":           tools,
            "servers":         servers,
            "primary_tool":    tools[0]["tool"],
            "primary_server":  tools[0]["server"],
            "output_action":   tools[0]["output_action"],
            "few_shot_prompt": self.build_few_shot_prompt(tools),
            "confidence":      confidence,
            "top_score":       round(top_score, 3),
            "method":          tools[0].get("method",""),
        }


# ═══════════════════════════════════════════════════════════════
# INTERFACE PUBLIQUE — compatible avec agent.py existant
# ═══════════════════════════════════════════════════════════════

_retriever: Optional[Retriever] = None

def _get_retriever() -> Retriever:
    global _retriever
    if _retriever is None:
        _retriever = Retriever()
    return _retriever


def retrieve_tools(query: str, top_k: int = 5) -> list[dict]:
    """
    Interface publique — appelée par agent.py debug_route.
    Compatible avec l'appel existant : retrieve_tools(query, top_k=5)
    """
    return _get_retriever().retrieve(query, top_k=top_k)


def get_tools_context(query: str, top_k: int = 3) -> dict:
    """Interface pour l'orchestrateur et les agents de débat."""
    return _get_retriever().get_tools_context(query, top_k=top_k)


def build_few_shot_prompt(tools: list[dict], max_examples: int = 3) -> str:
    """Interface pour les system prompts des agents."""
    return _get_retriever().build_few_shot_prompt(tools, max_examples)
