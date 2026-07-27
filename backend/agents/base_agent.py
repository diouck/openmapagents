"""
agents/base_agent.py — Agent de base universel pour OpenMapAgents
=================================================================
Adapté au .env existant :
  LLM_PROVIDER=openrouter / OPENROUTER_MODEL=openrouter/openrouter/free
  OPENROUTER_API_KEY=sk-or-v1-...

Fonctionnalités :
  - Injecte ResilientLLMClient (retry+fallback+circuit breaker)
  - build_system_prompt() → ROLE + TOOLS MCP + FEW-SHOT + CONTRAT JSON
  - tool_choice="required" quand domaine connu
  - response_format={"type":"json_object"} sur tous les agents
  - Contexte mémoire session injecté dans chaque appel
  - Vérification post-appel : LLM a-t-il appelé un tool ?
    Si non → retry avec prompt renforcé (max 2 retries)
  - Logger structuré : agent, model, tool, latence, tokens, cache_hit

Usage :
    class SatelliteAgent(BaseAgent):
        def __init__(self):
            super().__init__(
                name="satellite_agent",
                domain="gee",
                server="gee",
            )

    agent = SatelliteAgent()
    result = await agent.run(query, context, rag_tools)
"""

import os
import json
import time
import logging
from typing import Optional, Any

log = logging.getLogger("base_agent")

# ── Config depuis .env ────────────────────────────────────────
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openrouter")
_MODEL_MAP   = {
    "claude":     os.getenv("CLAUDE_MODEL",    "claude-sonnet-4-20250514"),
    "openai":     os.getenv("OPENAI_MODEL",    "gpt-4o"),
    "openrouter": os.getenv("OPENROUTER_MODEL","openrouter/openrouter/free"),
    "deepseek":   os.getenv("DEEPSEEK_MODEL",  "deepseek/deepseek-chat"),
    "mistral":    os.getenv("MISTRAL_MODEL",   "mistral/mistral-large-latest"),
    "ollama":     os.getenv("OLLAMA_MODEL",    "ollama/llama3.1"),
}
DEFAULT_MODEL     = _MODEL_MAP.get(LLM_PROVIDER, "openrouter/openrouter/free")
MAX_TOOL_RETRIES  = int(os.getenv("AGENT_MAX_TOOL_RETRIES", "2"))
AGENT_MAX_TOKENS  = int(os.getenv("AGENT_MAX_TOKENS",       "1500"))
AGENT_TEMPERATURE = float(os.getenv("AGENT_TEMPERATURE",    "0.1"))

# ── Contrat de sortie par action ──────────────────────────────
OUTPUT_CONTRACTS = {
    "add_layer": """{
  "action": "add_layer",
  "tile_url": "https://.../{z}/{x}/{y}",
  "layer_name": "...",
  "min": float, "max": float,
  "palette": ["hex1","hex2",...],
  "message": "description courte"
}""",
    "add_isochrone": """{
  "action": "add_isochrone",
  "geojson": {GeoJSON FeatureCollection},
  "layer_name": "...",
  "center": [lng, lat],
  "duration": int,
  "profile": "foot|bike|car",
  "message": "..."
}""",
    "add_choropleth": """{
  "action": "add_choropleth",
  "geojson": {GeoJSON FeatureCollection},
  "layer_name": "...",
  "property_name": "value",
  "palette": ["hex1","hex2",...],
  "min_value": float, "max_value": float,
  "message": "..."
}""",
    "add_timelapse": """{
  "action": "add_timelapse",
  "frames": ["url1","url2",...],
  "dates": ["2024-01","2024-02",...],
  "layer_name": "...",
  "message": "..."
}""",
    "add_markers": """{
  "action": "add_markers",
  "geojson": {GeoJSON FeatureCollection},
  "layer_name": "...",
  "message": "..."
}""",
    "add_route": """{
  "action": "add_route",
  "geojson": {GeoJSON LineString},
  "layer_name": "...",
  "distance_km": float,
  "duration_min": float,
  "message": "..."
}""",
}

# ── System prompts par domaine ────────────────────────────────
DOMAIN_ROLE = {
    "gee": (
        "Tu es un expert en télédétection et analyse satellite (GEE). "
        "Tu utilises Google Earth Engine pour calculer des indices "
        "(NDVI, EVI, SAR, LST, ERA5...) et des timelapse."
    ),
    "routing": (
        "Tu es un expert en mobilité urbaine et accessibilité. "
        "Tu calcules des isochrones, itinéraires et matrices de distance "
        "via OpenRouteService."
    ),
    "worldbank": (
        "Tu es un expert en données économiques mondiales. "
        "Tu affiches des indicateurs WorldBank (PIB, population, "
        "espérance de vie...) sur des cartes choroplèthes mondiales."
    ),
    "overture": (
        "Tu es un expert en données géographiques Overture Maps. "
        "Tu requêtes des POI, bâtiments, routes et divisions "
        "administratives via DuckDB."
    ),
    "osm": (
        "Tu es un expert en données OpenStreetMap. "
        "Tu requêtes des équipements, réseaux de transport, "
        "cours d'eau et espaces verts via l'API Overpass."
    ),
    "spatial": (
        "Tu es un expert en analyse spatiale. "
        "Tu réalises des opérations spatiales (buffer, intersection, "
        "clip, jointure) via PostGIS."
    ),
    "elevation": (
        "Tu es un expert en topographie et relief. "
        "Tu calcules des profils altimétriques, pentes et courbes "
        "de niveau via IGN/MapTiler/SRTM."
    ),
    "default": (
        "Tu es un assistant cartographique expert. "
        "Tu utilises les tools MCP disponibles pour répondre "
        "aux demandes de l'utilisateur."
    ),
}


# ═══════════════════════════════════════════════════════════════
# BASE AGENT
# ═══════════════════════════════════════════════════════════════

class BaseAgent:
    """
    Agent de base universel pour OpenMapAgents.
    Hérité par tous les agents spécialisés.
    """

    def __init__(
        self,
        name:     str,
        domain:   str = "default",
        server:   str = "",
        model:    Optional[str] = None,
    ):
        self.name   = name
        self.domain = domain
        self.server = server
        self.model  = model or DEFAULT_MODEL
        self._llm   = None

        log.debug(
            f"[{self.name}] init | domain={domain} | "
            f"server={server} | model={self.model[:40]}"
        )

    @property
    def llm(self):
        """Lazy init du client résilient."""
        if self._llm is None:
            from resilience.llm_resilience import get_resilient_client
            self._llm = get_resilient_client()
        return self._llm

    # ─── SYSTEM PROMPT ────────────────────────────────────────

    def build_system_prompt(
        self,
        agent_config: dict,
        context:      dict,
        rag_tools:    list,
    ) -> str:
        """
        Construit le system prompt complet :
          ROLE + TOOLS MCP + FEW-SHOT + CONTRAT JSON + CONTEXTE SESSION

        Args:
            agent_config: config spécifique de l'agent
                          {"tools": [tool_defs], "output_action": str, ...}
            context:      dict session memory (bbox, dates, layers...)
            rag_tools:    tools RAG récupérés par le retriever
        """
        parts = []

        # ── RÔLE ─────────────────────────────────────────────
        role = agent_config.get("role") or DOMAIN_ROLE.get(
            self.domain, DOMAIN_ROLE["default"]
        )
        parts.append(role)

        # ── TOOLS MCP DISPONIBLES ────────────────────────────
        tools_def = agent_config.get("tools", [])
        if tools_def:
            parts.append("\nTOOLS MCP DISPONIBLES:")
            for t in tools_def:
                parts.append(
                    f"  - {t['name']}: {t.get('description','')}"
                )

        # ── FEW-SHOT depuis RAG ───────────────────────────────
        few_shots = []
        for rt in rag_tools[:3]:
            for ex in rt.get("few_shot",[])[:1]:
                few_shots.append(ex)

        # Few-shots spécifiques de l'agent
        few_shots += agent_config.get("few_shot",[])[:2]

        if few_shots:
            parts.append("\nEXEMPLES D'UTILISATION (OBLIGATOIRES):")
            for ex in few_shots[:3]:
                user   = ex.get("user","")
                t_name = ex.get("tool","")
                params = ex.get("params",{})
                parts.append(
                    f'  User: "{user}"\n'
                    f'  → Appeler: {t_name}('
                    f'{json.dumps(params, ensure_ascii=False, separators=(",",":"))})'
                )

        # ── CONTRAT DE SORTIE JSON ────────────────────────────
        output_action = agent_config.get("output_action","add_markers")
        contract      = OUTPUT_CONTRACTS.get(output_action,"")
        if contract:
            parts.append(
                f"\nCONTRAT DE SORTIE OBLIGATOIRE ({output_action}):\n"
                f"{contract}"
            )

        # ── RÈGLES ABSOLUES ───────────────────────────────────
        parts.append(
            "\nRÈGLES ABSOLUES:\n"
            "1. Tu DOIS TOUJOURS appeler un tool — jamais répondre en texte pur.\n"
            "2. Ne jamais dire 'je n'ai pas accès' si un tool existe.\n"
            "3. Retourner UNIQUEMENT du JSON valide, sans markdown ni backticks.\n"
            "4. Si un paramètre manque, utiliser les valeurs du CONTEXTE SESSION."
        )

        # ── CONTEXTE SESSION ──────────────────────────────────
        context_block = self._build_context_block(context)
        if context_block:
            parts.append(context_block)

        return "\n".join(parts)

    def _build_context_block(self, context: dict) -> str:
        """Construit le bloc contexte session pour le system prompt."""
        if not context:
            return ""

        lines = ["\nCONTEXTE SESSION (utiliser en priorité):"]

        if context.get("current_bbox_str"):
            lines.append(f"  bbox courante: {context['current_bbox_str']}")

        if context.get("current_dates"):
            d = context["current_dates"]
            if "year" in d:
                lines.append(f"  année courante: {d['year']}")
            else:
                lines.append(
                    f"  période: {d.get('start_date','')} → {d.get('end_date','')}"
                )

        if context.get("current_collection"):
            lines.append(f"  collection: {context['current_collection']}")

        if context.get("current_indicator"):
            lines.append(f"  indicateur: {context['current_indicator']}")

        if context.get("current_transport"):
            lines.append(f"  transport: {context['current_transport']}")

        if context.get("layers_summary"):
            lines.append(f"  couches actives: {context['layers_summary']}")

        if len(lines) == 1:
            return ""
        return "\n".join(lines)

    # ─── TOOL DEFINITIONS ─────────────────────────────────────

    def _build_litellm_tools(self, agent_config: dict) -> list:
        """
        Convertit les tool defs de l'agent en format LiteLLM/OpenAI.
        """
        tools_def = agent_config.get("tools", [])
        litellm_tools = []
        for t in tools_def:
            litellm_tools.append({
                "type": "function",
                "function": {
                    "name":        t["name"],
                    "description": t.get("description",""),
                    "parameters":  t.get("parameters",{
                        "type":       "object",
                        "properties": {},
                        "required":   [],
                    }),
                },
            })
        return litellm_tools

    # ─── LLM CALL ─────────────────────────────────────────────

    async def call_llm(
        self,
        messages:     list,
        agent_config: dict,
        context:      dict,
        rag_tools:    list,
    ) -> dict:
        """
        Appel LLM avec :
        - System prompt construit dynamiquement
        - tool_choice="required" si domaine connu
        - Vérification post-appel que le LLM a appelé un tool
        - Retry avec prompt renforcé si pas de tool call
        """
        system = self.build_system_prompt(agent_config, context, rag_tools)
        litellm_tools = self._build_litellm_tools(agent_config)

        full_messages = [
            {"role": "system", "content": system},
            *messages,
        ]

        # tool_choice : "required" si tools dispo + domaine connu
        tool_choice = (
            "required"
            if litellm_tools and self.domain != "default"
            else "auto"
        )

        kwargs = {
            "messages":        full_messages,
            "tools":           litellm_tools or None,
            "tool_choice":     tool_choice if litellm_tools else None,
            "max_tokens":      AGENT_MAX_TOKENS,
            "temperature":     AGENT_TEMPERATURE,
        }

        # response_format JSON si pas de tools (agents de débat)
        if not litellm_tools:
            kwargs["response_format"] = {"type": "json_object"}
            del kwargs["tools"]
            del kwargs["tool_choice"]

        for attempt in range(MAX_TOOL_RETRIES + 1):
            t0     = time.time()
            result = await self._llm_call_once(kwargs)
            latency = int((time.time()-t0)*1000)

            tool_calls = result.get("tool_calls",[])
            self._log_call(result, attempt, latency)

            # ── Vérification tool call ────────────────────────
            if tool_calls:
                return result

            # Pas de tool call → retry avec prompt renforcé
            if attempt < MAX_TOOL_RETRIES and litellm_tools:
                log.warning(
                    f"[{self.name}] Attempt {attempt+1}: "
                    f"LLM n'a pas appelé de tool → retry renforcé"
                )
                reinforce = {
                    "role":    "user",
                    "content": (
                        "⚠️ RAPPEL CRITIQUE: Tu DOIS appeler un tool. "
                        f"Les tools disponibles sont: "
                        f"{[t['name'] for t in agent_config.get('tools',[])]}\n"
                        "Appelle le tool approprié MAINTENANT. "
                        "Ne réponds PAS en texte."
                    ),
                }
                kwargs["messages"] = [*full_messages, reinforce]
            else:
                # Dernier retry ou pas de tools → retourner tel quel
                break

        return result

    async def _llm_call_once(self, kwargs: dict) -> dict:
        """Un seul appel LLM. Retourne un dict normalisé."""
        import asyncio
        try:
            resp = await self.llm.acomplete(
                domain=self.domain,
                model=self.model,
                **kwargs,
            )
            msg = resp.choices[0].message

            # Extraire tool_calls
            tool_calls_raw = getattr(msg, "tool_calls", None) or []
            tool_calls = []
            for tc in tool_calls_raw:
                try:
                    args = json.loads(tc.function.arguments)
                except Exception:
                    args = {}
                tool_calls.append({
                    "id":   tc.id,
                    "name": tc.function.name,
                    "args": args,
                })

            # Extraire contenu texte
            content = getattr(msg, "content", "") or ""

            # Tokens
            usage  = getattr(resp, "usage", None)
            tokens = getattr(usage, "total_tokens", 0) if usage else 0

            # Modèle réel (openrouter/free peut choisir un sous-modèle)
            actual_model = getattr(resp, "model", self.model) or self.model

            return {
                "text":         content,
                "tool_calls":   tool_calls,
                "tokens":       tokens,
                "model_used":   actual_model,
                "raw_response": resp,
            }

        except Exception as e:
            log.error(f"[{self.name}] LLM error: {e!s:.100}")
            return {
                "text":       f"Erreur LLM: {str(e)[:100]}",
                "tool_calls": [],
                "tokens":     0,
                "model_used": self.model,
                "error":      str(e),
            }

    def _log_call(self, result: dict, attempt: int, latency: int):
        """Log structuré d'un appel LLM."""
        tool_calls = result.get("tool_calls",[])
        tool_names = [tc["name"] for tc in tool_calls] if tool_calls else []
        model      = result.get("model_used", self.model)[:45]
        tokens     = result.get("tokens",0)

        log.info(
            f"[{self.name}] "
            f"model={model} | "
            f"attempt={attempt+1} | "
            f"{latency}ms | "
            f"tokens={tokens} | "
            f"tools={tool_names or 'none'}"
        )

    # ─── EXÉCUTION DES TOOL CALLS ─────────────────────────────

    async def execute_tool_calls(
        self,
        tool_calls: list,
        context:    dict,
    ) -> list:
        """
        Exécute les tool calls via MCPClient.
        Retourne les résultats dans l'ordre.
        """
        from mcp_client import get_mcp_client
        client  = get_mcp_client()
        results = []

        for tc in tool_calls:
            tool_name = tc["name"]
            args      = dict(tc.get("args",{}))

            # Injecter bbox depuis contexte si absente
            if not args.get("bbox") and context.get("current_bbox"):
                args["bbox"] = context["current_bbox"]

            # Injecter dates depuis contexte si absentes
            if not args.get("start_date") and context.get("current_dates"):
                args.update(context["current_dates"])

            t0 = time.time()
            try:
                result  = await client.call_tool(
                    tool_name, args,
                    server_name=self.server or None,
                )
                latency = int((time.time()-t0)*1000)
                log.info(
                    f"[{self.name}] tool={tool_name} ✓ {latency}ms "
                    f"| action={result.get('action','?')}"
                )
                results.append({
                    "tool_call_id": tc.get("id",""),
                    "tool_name":    tool_name,
                    "result":       result,
                    "latency_ms":   latency,
                    "cache_hit":    result.get("_cache_hit", False),
                })
            except Exception as e:
                log.error(f"[{self.name}] tool={tool_name} error: {e!s:.80}")
                results.append({
                    "tool_call_id": tc.get("id",""),
                    "tool_name":    tool_name,
                    "result":       {"error": str(e)},
                    "latency_ms":   int((time.time()-t0)*1000),
                    "cache_hit":    False,
                })

        return results

    # ─── RUN ──────────────────────────────────────────────────

    async def run(
        self,
        query:        str,
        context:      dict,
        rag_tools:    list,
        agent_config: Optional[dict] = None,
    ) -> dict:
        """
        Point d'entrée de l'agent.
        Sous-classes peuvent surcharger pour comportement custom.
        """
        if agent_config is None:
            agent_config = self._default_config()

        messages = [{"role": "user", "content": query}]

        # 1. Appel LLM
        llm_result = await self.call_llm(
            messages, agent_config, context, rag_tools
        )

        if "error" in llm_result and not llm_result.get("tool_calls"):
            return {
                "text":         llm_result["error"],
                "tool_calls":   [],
                "tool_results": [],
            }

        # 2. Exécuter les tool calls
        tool_calls = llm_result.get("tool_calls",[])
        tool_results_raw = []
        if tool_calls:
            tool_results_raw = await self.execute_tool_calls(
                tool_calls, context
            )

        # 3. Formatter la sortie
        tool_results = [tr["result"] for tr in tool_results_raw]

        return {
            "text":         llm_result.get("text",""),
            "tool_calls":   [
                {"name": tc["name"], "args": tc["args"]}
                for tc in tool_calls
            ],
            "tool_results": tool_results,
            "_agent":       self.name,
            "_domain":      self.domain,
            "_tokens":      llm_result.get("tokens",0),
            "_model":       llm_result.get("model_used",self.model),
        }

    def _default_config(self) -> dict:
        """Config par défaut de l'agent basée sur le domaine."""
        # Récupérer les tools depuis le registry
        tools = []
        try:
            from rag.tool_registry import get_tools_for_server
            entries = get_tools_for_server(self.server)
            for entry in entries[:5]:  # max 5 tools par agent
                tools.append({
                    "name":        entry["tool"],
                    "description": entry["description"][:200],
                    "parameters": {
                        "type":       "object",
                        "properties": {
                            "bbox": {
                                "type":        "array",
                                "items":       {"type":"number"},
                                "description": "Bounding box [xmin,ymin,xmax,ymax]",
                            },
                        },
                        "required": [],
                    },
                })
        except Exception:
            pass

        return {
            "role":          DOMAIN_ROLE.get(self.domain, DOMAIN_ROLE["default"]),
            "tools":         tools,
            "output_action": "add_markers",
            "few_shot":      [],
        }


# ═══════════════════════════════════════════════════════════════
# AGENTS SPÉCIALISÉS
# ═══════════════════════════════════════════════════════════════

class SatelliteAgent(BaseAgent):
    """Agent spécialisé GEE — NDVI, SAR, LST, ERA5, timelapse..."""

    def __init__(self):
        super().__init__(
            name="satellite_agent",
            domain="gee",
            server="gee",
        )

    def _default_config(self) -> dict:
        return {
            "role": DOMAIN_ROLE["gee"],
            "tools": [
                {
                    "name": "compute_ndvi",
                    "description": (
                        "Calcule NDVI depuis Sentinel-2 ou Landsat. "
                        "Retourne tile_url pour MapLibre."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "bbox":       {"type":"array","items":{"type":"number"},
                                          "description":"[xmin,ymin,xmax,ymax]"},
                            "start_date": {"type":"string","description":"YYYY-MM-DD"},
                            "end_date":   {"type":"string","description":"YYYY-MM-DD"},
                            "collection": {"type":"string",
                                          "enum":["sentinel2","landsat8","landsat9"],
                                          "description":"Collection satellite"},
                            "cloud_cover":{"type":"integer","description":"% nuages max"},
                        },
                        "required": ["bbox"],
                    },
                },
                {
                    "name": "compute_timelapse",
                    "description": "Génère timelapse NDVI/RGB/EVI multi-années.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "bbox":       {"type":"array","items":{"type":"number"}},
                            "start_date": {"type":"string"},
                            "end_date":   {"type":"string"},
                            "interval":   {"type":"string","enum":["month","quarter","year"]},
                            "index":      {"type":"string","enum":["ndvi","rgb","evi","ndwi"]},
                        },
                        "required": ["bbox","start_date","end_date"],
                    },
                },
                {
                    "name": "compute_lst_modis",
                    "description": "LST MODIS jour/nuit en °C.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "bbox":       {"type":"array","items":{"type":"number"}},
                            "start_date": {"type":"string"},
                            "end_date":   {"type":"string"},
                            "mode":       {"type":"string","enum":["day","night","both"]},
                        },
                        "required": ["bbox"],
                    },
                },
                {
                    "name": "compute_sar_vv",
                    "description": "SAR Sentinel-1 VV — bâti, inondations.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "bbox":       {"type":"array","items":{"type":"number"}},
                            "start_date": {"type":"string"},
                            "end_date":   {"type":"string"},
                        },
                        "required": ["bbox"],
                    },
                },
                {
                    "name": "compute_era5_temp",
                    "description": "Température air ERA5 (°C, 9km).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "bbox":       {"type":"array","items":{"type":"number"}},
                            "start_date": {"type":"string"},
                            "end_date":   {"type":"string"},
                            "stat":       {"type":"string","enum":["mean","max","min"]},
                        },
                        "required": ["bbox"],
                    },
                },
            ],
            "output_action": "add_layer",
            "few_shot": [
                {"user":"NDVI 2024 sur Dakar","tool":"compute_ndvi",
                 "params":{"bbox":[-17.55,14.63,-17.33,14.82],
                           "start_date":"2024-01-01","end_date":"2024-12-31"}},
                {"user":"timelapse végétation 2018-2024","tool":"compute_timelapse",
                 "params":{"start_date":"2018-01-01","end_date":"2024-12-31",
                           "interval":"year","index":"ndvi"}},
            ],
        }


class RoutingAgent(BaseAgent):
    """Agent spécialisé ORS — isochrones, routes, matrice."""

    def __init__(self):
        super().__init__(
            name="routing_agent",
            domain="routing",
            server="ors",
        )

    def _default_config(self) -> dict:
        return {
            "role": DOMAIN_ROLE["routing"],
            "tools": [
                {
                    "name": "compute_isochrone",
                    "description": (
                        "Calcule isochrone depuis un point. "
                        "Retourne GeoJSON polygone."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "center":       {"type":"array","items":{"type":"number"},
                                            "description":"[lng, lat]"},
                            "time_minutes": {"type":"integer",
                                            "description":"Durée max en minutes"},
                            "profile":      {"type":"string",
                                            "enum":["foot","bike","car","wheelchair"]},
                        },
                        "required": ["center","time_minutes"],
                    },
                },
                {
                    "name": "compute_isochrones_multi",
                    "description": "Isochrones multi-intervalles [5,10,15]min.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "center":    {"type":"array","items":{"type":"number"}},
                            "intervals": {"type":"array","items":{"type":"integer"}},
                            "profile":   {"type":"string","enum":["foot","bike","car"]},
                        },
                        "required": ["center","intervals"],
                    },
                },
                {
                    "name": "compute_route",
                    "description": "Itinéraire avec instructions de navigation.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "waypoints": {"type":"array",
                                         "items":{"type":"array",
                                                  "items":{"type":"number"}},
                                         "description":"[[lng,lat],[lng,lat],...]"},
                            "profile":  {"type":"string","enum":["foot","bike","car"]},
                        },
                        "required": ["waypoints"],
                    },
                },
            ],
            "output_action": "add_isochrone",
            "few_shot": [
                {"user":"isochrone 15min à pied depuis Dakar",
                 "tool":"compute_isochrone",
                 "params":{"center":[-17.44,14.69],"time_minutes":15,"profile":"foot"}},
                {"user":"isochrones 5,10,15 min",
                 "tool":"compute_isochrones_multi",
                 "params":{"intervals":[5,10,15],"profile":"foot"}},
            ],
        }


class WorldBankAgent(BaseAgent):
    """Agent spécialisé WorldBank — choroplèthes mondiaux."""

    def __init__(self):
        super().__init__(
            name="worldbank_agent",
            domain="worldbank",
            server="worldbank",
        )

    def _default_config(self) -> dict:
        return {
            "role": DOMAIN_ROLE["worldbank"],
            "tools": [
                {
                    "name": "get_indicator",
                    "description": (
                        "Affiche un indicateur WorldBank sur carte choroplèthe. "
                        "Codes: NY.GDP.PCAP.CD (PIB/hab), SP.POP.TOTL (pop), "
                        "SP.DYN.LE00.IN (espérance vie), EN.ATM.CO2E.PC (CO2)..."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "indicator": {"type":"string",
                                         "description":"Code WB (ex: NY.GDP.PCAP.CD)"},
                            "year":      {"type":"integer",
                                         "description":"Année (défaut: dernière dispo)"},
                            "keyword":   {"type":"string",
                                         "description":"Mot-clé si code inconnu"},
                        },
                        "required": [],
                    },
                },
                {
                    "name": "get_country_profile",
                    "description": "Profil complet d'un pays (10 indicateurs clés).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "country": {"type":"string",
                                       "description":"Nom ou ISO3 du pays"},
                            "year":    {"type":"integer"},
                        },
                        "required": ["country"],
                    },
                },
            ],
            "output_action": "add_choropleth",
            "few_shot": [
                {"user":"PIB par habitant mondial","tool":"get_indicator",
                 "params":{"indicator":"NY.GDP.PCAP.CD","year":2023}},
                {"user":"population mondiale","tool":"get_indicator",
                 "params":{"indicator":"SP.POP.TOTL"}},
                {"user":"espérance de vie","tool":"get_indicator",
                 "params":{"indicator":"SP.DYN.LE00.IN"}},
            ],
        }


class GeoDataAgent(BaseAgent):
    """Agent spécialisé Overture + OSM."""

    def __init__(self):
        super().__init__(
            name="geo_data_agent",
            domain="overture",
            server="overture",
        )

    def _default_config(self) -> dict:
        return {
            "role": DOMAIN_ROLE["overture"],
            "tools": [
                {
                    "name": "query_places",
                    "description": "POI Overture : restaurants, pharmacies, écoles...",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "bbox":           {"type":"array","items":{"type":"number"}},
                            "category":       {"type":"string"},
                            "name_filter":    {"type":"string"},
                            "min_confidence": {"type":"number"},
                            "limit":          {"type":"integer"},
                        },
                        "required": [],
                    },
                },
                {
                    "name": "query_buildings",
                    "description": "Bâtiments Overture avec hauteur et étages.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "bbox":           {"type":"array","items":{"type":"number"}},
                            "min_height":     {"type":"number"},
                            "max_height":     {"type":"number"},
                            "building_class": {"type":"string"},
                        },
                        "required": [],
                    },
                },
                {
                    "name": "get_amenities",
                    "description": "Équipements OSM Overpass.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "bbox":         {"type":"array","items":{"type":"number"}},
                            "amenity_type": {"type":"string"},
                            "limit":        {"type":"integer"},
                        },
                        "required": ["amenity_type"],
                    },
                },
            ],
            "output_action": "add_markers",
            "few_shot": [
                {"user":"restaurants autour de moi","tool":"query_places",
                 "params":{"category":"restaurant","radius_m":1000}},
                {"user":"bâtiments de plus de 20m","tool":"query_buildings",
                 "params":{"min_height":20}},
            ],
        }


class SpatialAgent(BaseAgent):
    """Agent spécialisé opérations spatiales PostGIS + turf.js."""

    def __init__(self):
        super().__init__(
            name="spatial_agent",
            domain="spatial",
            server="postgis",
        )

    def _default_config(self) -> dict:
        return {
            "role": DOMAIN_ROLE["spatial"],
            "tools": [
                {
                    "name": "spatial_buffer",
                    "description": "Zone tampon autour d'une géométrie (mètres).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "geojson":  {"type":"object"},
                            "radius_m": {"type":"number",
                                        "description":"Rayon en mètres"},
                            "dissolve": {"type":"boolean"},
                        },
                        "required": ["radius_m"],
                    },
                },
                {
                    "name": "spatial_analysis",
                    "description": (
                        "Opérations spatiales turf.js : buffer, intersection, "
                        "union, clip, points_in_polygon, dissolve..."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "operation":    {"type":"string",
                                           "enum":["buffer","intersection","union",
                                                   "difference","clip","spatial_join",
                                                   "points_in_polygon","dissolve",
                                                   "centroid","convex_hull"]},
                            "layer_a_name": {"type":"string"},
                            "layer_b_name": {"type":"string"},
                            "params":       {"type":"object"},
                            "result_name":  {"type":"string"},
                        },
                        "required": ["operation","layer_a_name"],
                    },
                },
            ],
            "output_action": "add_layer",
            "few_shot": [
                {"user":"buffer 500m autour de la couche","tool":"spatial_buffer",
                 "params":{"radius_m":500}},
                {"user":"intersection entre A et B","tool":"spatial_analysis",
                 "params":{"operation":"intersection",
                           "layer_a_name":"A","layer_b_name":"B"}},
            ],
        }


class DatabaseAgent(BaseAgent):
    """Agent spécialisé requêtes PostGIS directes."""

    def __init__(self):
        super().__init__(
            name="database_agent",
            domain="postgis",
            server="postgis",
        )

    def _default_config(self) -> dict:
        return {
            "role": (
                "Tu es un expert PostGIS. Tu requêtes les tables "
                "de la base openmapagents via SELECT uniquement."
            ),
            "tools": [
                {
                    "name": "query_table",
                    "description": (
                        "Requête SELECT sur une table PostGIS. "
                        "Tables : communes, arbres, batiments_auran, "
                        "canopy_nantes, equipements, voiries..."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table":   {"type":"string",
                                       "description":"Nom de la table"},
                            "bbox":    {"type":"array","items":{"type":"number"}},
                            "filters": {"type":"object"},
                            "columns": {"type":"array","items":{"type":"string"}},
                            "limit":   {"type":"integer"},
                        },
                        "required": ["table"],
                    },
                },
            ],
            "output_action": "add_layer",
            "few_shot": [
                {"user":"communes de Loire-Atlantique","tool":"query_table",
                 "params":{"table":"communes","filters":{"dept":"44"}}},
                {"user":"arbres Nantes","tool":"query_table",
                 "params":{"table":"arbres","limit":1000}},
            ],
        }


# ── Factory ───────────────────────────────────────────────────

_AGENT_REGISTRY: dict[str, type] = {
    "satellite_agent": SatelliteAgent,
    "routing_agent":   RoutingAgent,
    "worldbank_agent": WorldBankAgent,
    "geo_data_agent":  GeoDataAgent,
    "spatial_agent":   SpatialAgent,
    "database_agent":  DatabaseAgent,
}

_instances: dict[str, BaseAgent] = {}


def get_agent(name: str) -> BaseAgent:
    """Retourne l'instance singleton de l'agent demandé."""
    if name not in _instances:
        cls = _AGENT_REGISTRY.get(name)
        if not cls:
            raise ValueError(
                f"Agent '{name}' inconnu. "
                f"Disponibles: {list(_AGENT_REGISTRY.keys())}"
            )
        _instances[name] = cls()
    return _instances[name]


def get_agent_for_domain(domain: str) -> BaseAgent:
    """Retourne l'agent approprié pour un domaine."""
    domain_to_agent = {
        "gee":        "satellite_agent",
        "satellite":  "satellite_agent",
        "routing":    "routing_agent",
        "worldbank":  "worldbank_agent",
        "overture":   "geo_data_agent",
        "osm":        "geo_data_agent",
        "spatial":    "spatial_agent",
        "postgis":    "database_agent",
    }
    agent_name = domain_to_agent.get(domain, "geo_data_agent")
    return get_agent(agent_name)
