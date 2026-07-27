"""
debate/debate_layer.py — Couche débat multi-agents pour OpenMapAgents
======================================================================
5 agents légers qui analysent la requête en parallèle avant exécution.
Chaque agent a un rôle précis et complémentaire.

Adapté au .env :
  LLM_PROVIDER=openrouter
  OPENROUTER_MODEL=openrouter/openrouter/free
  PG_HOST=geoafrica.fr / EMBED_MODEL=...

Architecture :
  AnalysteAgent    → extrait bbox, dates, paramètres, résout ambiguïtés
  StrategeAgent    → choisit server MCP + tool optimal depuis RAG
  CritiqueAgent    → détecte risques, hallucinations, paramètres invalides
  OperationnelAgent→ vérifie faisabilité réelle (quota GEE, API dispo...)
  SynthetiseurAgent→ produit le plan JSON final propre

Tous s'exécutent en parallèle via asyncio.gather().
Résultat transmis au Modérateur.
"""

import os
import json
import logging
import asyncio
from typing import Optional

log = logging.getLogger("debate_layer")

# ── Config LLM — depuis le .env ───────────────────────────────
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openrouter")
_MODEL_MAP   = {
    "claude":     os.getenv("CLAUDE_MODEL",    "claude-sonnet-4-20250514"),
    "openai":     os.getenv("OPENAI_MODEL",    "gpt-4o"),
    "openrouter": os.getenv("OPENROUTER_MODEL","openrouter/openrouter/free"),
    "deepseek":   os.getenv("DEEPSEEK_MODEL",  "deepseek/deepseek-chat"),
    "mistral":    os.getenv("MISTRAL_MODEL",   "mistral/mistral-large-latest"),
    "ollama":     os.getenv("OLLAMA_MODEL",    "ollama/llama3.1"),
}
DEBATE_MODEL = _MODEL_MAP.get(LLM_PROVIDER, "openrouter/openrouter/free")

# Les agents de débat utilisent des prompts courts → max_tokens petit
DEBATE_MAX_TOKENS = int(os.getenv("DEBATE_MAX_TOKENS", "600"))
DEBATE_TIMEOUT    = int(os.getenv("DEBATE_TIMEOUT_S",  "15"))


def _ollama_kwargs() -> dict:
    if LLM_PROVIDER == "ollama":
        return {"api_base": os.getenv("OLLAMA_API_BASE","http://localhost:11434")}
    return {}


async def _llm_json(system: str, user: str) -> Optional[dict]:
    """
    Appel LLM minimal pour les agents de débat.
    Retourne un dict JSON ou None si échec.
    Utilise le client résilient pour le fallback automatique.
    """
    try:
        from resilience.llm_resilience import get_resilient_client
        client = get_resilient_client()
        resp   = await asyncio.wait_for(
            client.acomplete(
                domain="default",
                model=DEBATE_MODEL,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user},
                ],
                max_tokens=DEBATE_MAX_TOKENS,
                temperature=0.1,
                response_format={"type": "json_object"},
                **_ollama_kwargs(),
            ),
            timeout=DEBATE_TIMEOUT,
        )
        content = resp.choices[0].message.content or "{}"
        # Nettoyer éventuels backticks
        content = content.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        return json.loads(content)
    except asyncio.TimeoutError:
        log.warning(f"Debate LLM timeout ({DEBATE_TIMEOUT}s)")
        return None
    except Exception as e:
        log.warning(f"Debate LLM error: {e!s:.80}")
        return None


# ═══════════════════════════════════════════════════════════════
# AGENT 1 — ANALYSTE
# ═══════════════════════════════════════════════════════════════

async def run_analyste(
    query:   str,
    context: dict,
) -> dict:
    """
    Extrait et structure les paramètres cartographiques de la requête.
    Résout les ambiguïtés géographiques.
    """
    system = """Tu es l'Analyste d'un système cartographique IA.
Extrait les paramètres de la requête et retourne UNIQUEMENT ce JSON :
{
  "lieu": "nom du lieu ou null",
  "bbox": [xmin,ymin,xmax,ymax] ou null,
  "start_date": "YYYY-MM-DD" ou null,
  "end_date": "YYYY-MM-DD" ou null,
  "year": 2024 ou null,
  "collection": "sentinel2|landsat8|landsat9" ou null,
  "indicator": "code WB" ou null,
  "profile": "foot|bike|car" ou null,
  "duration_min": int ou null,
  "manquant": ["liste des paramètres manquants critiques"],
  "ambiguites": ["liste des ambiguïtés détectées"],
  "resume": "résumé 1 phrase de ce que l'utilisateur veut"
}"""

    context_str = ""
    if context.get("current_bbox_str"):
        context_str += f"\nZone courante: {context['current_bbox_str']}"
    if context.get("current_dates"):
        context_str += f"\nDates courantes: {context['current_dates']}"
    if context.get("current_collection"):
        context_str += f"\nCollection: {context['current_collection']}"
    if context.get("current_transport"):
        context_str += f"\nTransport: {context['current_transport']}"

    user = f"Requête: \"{query}\"\nContexte session:{context_str or ' aucun'}"

    result = await _llm_json(system, user)
    if not result:
        # Fallback déterministe
        return {
            "lieu":      None, "bbox": context.get("current_bbox"),
            "start_date":None, "end_date": None, "year": None,
            "collection": context.get("current_collection","sentinel2"),
            "indicator": context.get("current_indicator"),
            "profile":   context.get("current_transport","foot"),
            "duration_min": None,
            "manquant":  ["bbox"] if not context.get("current_bbox") else [],
            "ambiguites":[], "resume": query,
            "_fallback": True,
        }
    result["_agent"] = "analyste"
    return result


# ═══════════════════════════════════════════════════════════════
# AGENT 2 — STRATÈGE
# ═══════════════════════════════════════════════════════════════

async def run_stratege(
    query:    str,
    rag_tools: list[dict],
    context:  dict,
) -> dict:
    """
    Choisit le ou les MCP servers + tools optimaux.
    S'appuie sur les résultats RAG.
    """
    tools_summary = "\n".join(
        f"- {t['tool']} (server:{t['server']}, score:{t['score']}, "
        f"action:{t['output_action']})"
        for t in rag_tools[:5]
    ) or "Aucun tool RAG disponible"

    system = """Tu es le Stratège d'un système cartographique IA.
Choisis le(s) tool(s) optimal(aux) pour répondre à la requête.
Retourne UNIQUEMENT ce JSON :
{
  "primary_server": "nom du server MCP principal",
  "primary_tool": "nom du tool principal",
  "secondary_steps": [
    {"server": "...", "tool": "...", "raison": "..."}
  ],
  "output_action": "add_layer|add_isochrone|add_choropleth|add_timelapse|add_markers|add_route",
  "justification": "pourquoi ce choix en 1 phrase",
  "multi_agent_needed": true/false
}"""

    user = (
        f"Requête: \"{query}\"\n"
        f"Tools RAG disponibles:\n{tools_summary}\n"
        f"Dernier domaine: {context.get('last_domain','inconnu')}"
    )

    result = await _llm_json(system, user)
    if not result and rag_tools:
        top = rag_tools[0]
        result = {
            "primary_server":   top["server"],
            "primary_tool":     top["tool"],
            "secondary_steps":  [],
            "output_action":    top["output_action"],
            "justification":    f"Tool RAG score={top['score']}",
            "multi_agent_needed": False,
            "_fallback": True,
        }
    elif not result:
        result = {
            "primary_server": "overture", "primary_tool": "query_places",
            "secondary_steps": [], "output_action": "add_markers",
            "justification": "fallback défaut", "_fallback": True,
        }
    result["_agent"] = "stratege"
    return result


# ═══════════════════════════════════════════════════════════════
# AGENT 3 — CRITIQUE
# ═══════════════════════════════════════════════════════════════

async def run_critique(
    query:     str,
    analyste:  dict,
    stratege:  dict,
    rag_tools: list[dict],
) -> dict:
    """
    Identifie les risques, hallucinations et problèmes potentiels.
    Vérifie la cohérence des paramètres.
    """
    system = """Tu es le Critique d'un système cartographique IA.
Identifie les risques et problèmes. Retourne UNIQUEMENT ce JSON :
{
  "risques": ["liste des risques détectés"],
  "hallucinations": ["paramètres potentiellement hallusinés"],
  "bbox_valide": true/false,
  "dates_valides": true/false,
  "tool_adapte": true/false,
  "corrections": {
    "bbox": [xmin,ymin,xmax,ymax] ou null,
    "start_date": "..." ou null,
    "end_date": "..." ou null
  },
  "bloquant": true/false,
  "severite": "low|medium|high"
}"""

    bbox = analyste.get("bbox")
    bbox_info = f"{bbox}" if bbox else "non fournie"

    user = (
        f"Requête: \"{query}\"\n"
        f"Analyste: lieu={analyste.get('lieu')}, "
        f"bbox={bbox_info}, "
        f"dates={analyste.get('start_date')}→{analyste.get('end_date')}\n"
        f"Stratège: tool={stratege.get('primary_tool')}, "
        f"server={stratege.get('primary_server')}\n"
        f"Tools RAG top score: {rag_tools[0]['score'] if rag_tools else 0}"
    )

    result = await _llm_json(system, user)
    if not result:
        # Vérifications déterministes minimales
        bbox_ok  = bbox is not None and len(bbox) == 4
        dates_ok = True
        if analyste.get("start_date") and analyste.get("end_date"):
            dates_ok = analyste["start_date"] < analyste["end_date"]
        result = {
            "risques":       [] if (bbox_ok and dates_ok) else
                             ["bbox manquante" if not bbox_ok else "dates invalides"],
            "hallucinations": [],
            "bbox_valide":   bbox_ok,
            "dates_valides": dates_ok,
            "tool_adapte":   len(rag_tools) > 0,
            "corrections":   {},
            "bloquant":      not bbox_ok,
            "severite":      "low",
            "_fallback": True,
        }
    result["_agent"] = "critique"
    return result


# ═══════════════════════════════════════════════════════════════
# AGENT 4 — OPÉRATIONNEL
# ═══════════════════════════════════════════════════════════════

async def run_operationnel(
    query:   str,
    stratege: dict,
    analyste: dict,
) -> dict:
    """
    Vérifie la faisabilité opérationnelle.
    Estime la latence et les paramètres réels.
    """
    system = """Tu es l'agent Opérationnel d'un système cartographique IA.
Évalue la faisabilité technique. Retourne UNIQUEMENT ce JSON :
{
  "faisable": true/false,
  "latence_estimee_s": 5,
  "params_finaux": {
    "collection": "sentinel2",
    "cloud_cover": 30,
    "limit": 500
  },
  "avertissements": ["liste d'avertissements techniques"],
  "optimisations": ["suggestions d'optimisation"]
}"""

    server = stratege.get("primary_server","")
    tool   = stratege.get("primary_tool","")
    bbox   = analyste.get("bbox")

    # Estimation de latence basée sur le server
    latency_map = {
        "gee":       20, "worldbank": 8, "overture":  6,
        "ors":       4,  "osm":       8, "postgis":   3,
        "nominatim": 2,  "stac":      5, "maptiler":  3,
    }
    estimated = latency_map.get(server, 5)

    # Vérification bbox superficie — GEE TEMPOREL uniquement
    # Les tools statiques (WorldCover, Canopy, SRTM, Hansen...) sont mondiaux → pas de limite
    _STATIC_GEE_TOOLS = {
        "compute_worldcover", "compute_esa_worldcover",
        "compute_canopy_height", "compute_canopy",
        "compute_forest_watch", "compute_forest_cover",
        "compute_srtm", "compute_dem", "compute_elevation",
        "compute_hansen", "compute_global_surface_water",
    }
    _STATIC_KEYWORDS = ("worldcover","canopy","forest_watch","srtm","dem","hansen","surface_water")

    bbox_warning = ""
    is_gee_temporal = (
        server == "gee"
        and tool not in _STATIC_GEE_TOOLS
        and not any(kw in tool for kw in _STATIC_KEYWORDS)
    )
    if bbox and len(bbox) == 4 and is_gee_temporal:
        area = (bbox[2]-bbox[0]) * (bbox[3]-bbox[1]) * 111 * 111
        if area > 500:
            bbox_warning = f"bbox trop grande ({area:.0f}km²) pour GEE (max 500km²)"

    user = (
        f"Requête: \"{query}\"\n"
        f"Tool: {server}.{tool}\n"
        f"bbox: {bbox}\n"
        f"Dates: {analyste.get('start_date')}→{analyste.get('end_date')}\n"
        f"Avertissement bbox: {bbox_warning or 'aucun'}"
    )

    result = await _llm_json(system, user)
    if not result:
        params = {
            "collection":  analyste.get("collection","sentinel2"),
            "cloud_cover": 30,
            "limit":       500,
        }
        warnings = [bbox_warning] if bbox_warning else []
        result = {
            "faisable":          True,
            "latence_estimee_s": estimated,
            "params_finaux":     params,
            "avertissements":    warnings,
            "optimisations":     [],
            "_fallback": True,
        }
    result["_agent"] = "operationnel"
    return result


# ═══════════════════════════════════════════════════════════════
# AGENT 5 — SYNTHÉTISEUR
# ═══════════════════════════════════════════════════════════════

async def run_synthetiseur(
    query:        str,
    analyste:     dict,
    stratege:     dict,
    critique:     dict,
    operationnel: dict,
    rag_tools:    list[dict],
) -> dict:
    """
    Fusionne les outputs des 4 agents en un plan JSON final.
    Intègre les corrections du Critique.
    """
    system = """Tu es le Synthétiseur d'un système cartographique IA.
Produis le plan d'exécution final en JSON. Retourne UNIQUEMENT ce JSON :
{
  "plan": {
    "server": "nom du server MCP",
    "tool": "nom du tool",
    "args": {
      "bbox": [...] ou null,
      "start_date": "...",
      "end_date": "...",
      "collection": "...",
      "cloud_cover": 30
    },
    "output_action": "add_layer|add_isochrone|...",
    "secondary_steps": []
  },
  "message_utilisateur": "description courte pour l'utilisateur (fr)",
  "confidence": 0.85
}"""

    # Intégrer les corrections du critique
    bbox = (critique.get("corrections",{}).get("bbox")
            or analyste.get("bbox"))
    start = (critique.get("corrections",{}).get("start_date")
             or analyste.get("start_date","2024-01-01"))
    end   = (critique.get("corrections",{}).get("end_date")
             or analyste.get("end_date","2024-12-31"))

    params_finaux = operationnel.get("params_finaux", {})

    user = (
        f"Requête: \"{query}\"\n"
        f"Server: {stratege.get('primary_server')}\n"
        f"Tool: {stratege.get('primary_tool')}\n"
        f"bbox: {bbox}\ndates: {start}→{end}\n"
        f"Params finaux: {json.dumps(params_finaux)}\n"
        f"Avertissements: {critique.get('risques',[])}\n"
        f"Action: {stratege.get('output_action','add_layer')}"
    )

    result = await _llm_json(system, user)
    if not result:
        # Plan déterministe de fallback
        args = {
            **({"bbox": bbox} if bbox else {}),
            **({"start_date": start, "end_date": end}
               if start and end else {}),
            **params_finaux,
        }
        result = {
            "plan": {
                "server":         stratege.get("primary_server","overture"),
                "tool":           stratege.get("primary_tool","query_places"),
                "args":           args,
                "output_action":  stratege.get("output_action","add_markers"),
                "secondary_steps":stratege.get("secondary_steps",[]),
            },
            "message_utilisateur": analyste.get("resume", query),
            "confidence": 0.6 if rag_tools else 0.4,
            "_fallback": True,
        }
    result["_agent"] = "synthetiseur"
    return result


# ═══════════════════════════════════════════════════════════════
# ORCHESTRATION — DEBATE LAYER
# ═══════════════════════════════════════════════════════════════

async def run_debate(
    query:     str,
    context:   dict,
    rag_tools: list[dict],
) -> dict:
    """
    Lance les 5 agents en parallèle.
    Retourne les outputs structurés pour le Modérateur.

    Args:
        query:     requête utilisateur
        context:   dict session memory (bbox, dates, layers, etc.)
        rag_tools: tools RAG récupérés par le retriever

    Returns:
        dict avec outputs des 5 agents
    """
    log.info(f"[Debate] Lancement 5 agents pour: {query[:60]}")
    t0 = __import__("time").time()

    # Phase 1 : Analyste et Stratège en parallèle (indépendants)
    analyste_task = asyncio.create_task(
        run_analyste(query, context)
    )
    stratege_task = asyncio.create_task(
        run_stratege(query, rag_tools, context)
    )

    analyste, stratege = await asyncio.gather(
        analyste_task, stratege_task,
        return_exceptions=True
    )

    # Gérer les exceptions
    if isinstance(analyste, Exception):
        log.warning(f"Analyste exception: {analyste}")
        analyste = {"lieu": None, "bbox": context.get("current_bbox"),
                    "resume": query, "_error": str(analyste)}
    if isinstance(stratege, Exception):
        log.warning(f"Stratège exception: {stratege}")
        top = rag_tools[0] if rag_tools else {}
        stratege = {
            "primary_server": top.get("server","overture"),
            "primary_tool":   top.get("tool","query_places"),
            "output_action":  top.get("output_action","add_markers"),
            "_error": str(stratege),
        }

    # Phase 2 : Critique, Opérationnel et Synthétiseur en parallèle
    critique_task = asyncio.create_task(
        run_critique(query, analyste, stratege, rag_tools)
    )
    operationnel_task = asyncio.create_task(
        run_operationnel(query, stratege, analyste)
    )

    critique, operationnel = await asyncio.gather(
        critique_task, operationnel_task,
        return_exceptions=True
    )

    if isinstance(critique, Exception):
        log.warning(f"Critique exception: {critique}")
        critique = {"risques":[], "bloquant":False, "severite":"low",
                    "_error": str(critique)}
    if isinstance(operationnel, Exception):
        log.warning(f"Opérationnel exception: {operationnel}")
        operationnel = {"faisable":True, "latence_estimee_s":5,
                        "params_finaux":{}, "_error": str(operationnel)}

    # Phase 3 : Synthétiseur (dépend de tous les autres)
    synthetiseur = await run_synthetiseur(
        query, analyste, stratege, critique, operationnel, rag_tools
    )
    if isinstance(synthetiseur, Exception):
        log.warning(f"Synthétiseur exception: {synthetiseur}")
        synthetiseur = {
            "plan": {
                "server": stratege.get("primary_server","overture"),
                "tool":   stratege.get("primary_tool","query_places"),
                "args":   {},
                "output_action": stratege.get("output_action","add_markers"),
            },
            "message_utilisateur": query,
            "confidence": 0.4,
            "_error": str(synthetiseur),
        }

    latency = int((__import__("time").time() - t0) * 1000)
    log.info(f"[Debate] Terminé en {latency}ms")

    return {
        "analyste":     analyste,
        "stratege":     stratege,
        "critique":     critique,
        "operationnel": operationnel,
        "synthetiseur": synthetiseur,
        "query":        query,
        "rag_tools":    rag_tools,
        "latency_ms":   latency,
    }
