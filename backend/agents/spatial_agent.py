"""
agents/spatial_agent.py — Agent Spatial
=========================================
Orchestre mcp_postgis pour toutes les opérations spatiales.
Fallback turf.js frontend si PostGIS indispo.
"""

import re
import logging
from agents.base_agent import BaseAgent

log = logging.getLogger("spatial_agent")

OPERATION_MAP = {
    "buffer":              "spatial_buffer",
    "zone tampon":         "spatial_buffer",
    "tampon":              "spatial_buffer",
    "intersection":        "spatial_intersect",
    "intersect":           "spatial_intersect",
    "union":               "spatial_union",
    "différence":          "spatial_difference",
    "difference":          "spatial_difference",
    "clip":                "spatial_clip",
    "découper":            "spatial_clip",
    "points dans":         "points_in_polygon",
    "dans la zone":        "points_in_polygon",
    "jointure":            "spatial_join",
    "join":                "spatial_join",
}


class SpatialAgent(BaseAgent):

    def __init__(self):
        super().__init__(
            name="spatial_agent",
            domain="spatial",
            server="postgis",
        )

    async def run(self, query: str, context: dict,
                  rag_tools: list, agent_config: dict = None) -> dict:

        from mcp_client import get_mcp_client
        client = get_mcp_client()
        q      = query.lower()

        # ── Tables PostGIS ────────────────────────────────────
        if any(w in q for w in ["tables","liste","disponible","base"]):
            return await self._list_tables(client)

        # ── Query table directe ───────────────────────────────
        if any(w in q for w in ["communes","arbres","bâtiments auran","canopée nantes",
                                  "voiries","équipements","parcelles"]):
            return await self._query_table(client, query, context)

        # ── Détection opération spatiale ─────────────────────
        for kw, op_tool in OPERATION_MAP.items():
            if kw in q:
                return await self._spatial_op(client, op_tool, query, context)

        # ── Buffer par défaut si rayon mentionné ─────────────
        m = re.search(r'(\d+)\s*(?:m|mètre|meter|km)', q)
        if m:
            radius = int(m.group(1))
            if "km" in q: radius *= 1000
            return await self._buffer(client, radius, context)

        # ── Fallback turf.js ──────────────────────────────────
        return {
            "text": (
                "Quelle opération spatiale souhaitez-vous réaliser ? "
                "(buffer, intersection, union, clip, jointure...)"
            ),
            "tool_calls":[], "tool_results":[],
        }

    # ── BUFFER ───────────────────────────────────────────────

    async def _buffer(self, client, radius_m: int, context: dict) -> dict:
        layers = context.get("active_layers",[])
        # Choisir le premier layer vecteur
        geojson = None
        layer_name = ""
        for l in layers:
            if l.type in ("vector_points","vector_polygon","vector_line"):
                layer_name = l.name
                break

        args: dict = {"radius_m": radius_m}
        if geojson: args["geojson"] = geojson

        if not geojson and not layer_name:
            # Fallback turf.js avec nom de couche
            return {
                "text":         f"Buffer {radius_m}m appliqué sur la couche active.",
                "tool_calls":   [{"name":"spatial_analysis",
                                  "args":{"operation":"buffer",
                                          "layer_a_name": layer_name or "active_layer",
                                          "params":{"radius":radius_m}}}],
                "tool_results": [{"action":"spatial_analysis",
                                  "operation":"buffer",
                                  "layer_a_name": layer_name or "active_layer",
                                  "params":{"radius":radius_m}}],
            }

        r = await client.call_tool("spatial_buffer", args, server_name="postgis")
        return self._wrap(r, "spatial_buffer", args)

    # ── OPÉRATION SPATIALE GÉNÉRIQUE ──────────────────────────

    async def _spatial_op(self, client, tool: str, query: str, context: dict) -> dict:
        layers = context.get("active_layers",[])
        names  = [l.name for l in layers]

        if tool == "spatial_buffer":
            m = re.search(r'(\d+)\s*(?:m|mètre|km)', query)
            radius = int(m.group(1)) * (1000 if "km" in query else 1) if m else 500
            return await self._buffer(client, radius, context)

        # Pour les opérations binaires → fallback turf si pas de GeoJSON direct
        layer_a = names[0] if len(names) > 0 else "layer_a"
        layer_b = names[1] if len(names) > 1 else "layer_b"

        op_map = {
            "spatial_intersect": "intersection",
            "spatial_union":     "union",
            "spatial_difference":"difference",
            "spatial_clip":      "clip",
            "spatial_join":      "spatial_join",
            "points_in_polygon": "points_in_polygon",
        }
        operation = op_map.get(tool,"intersection")

        result = {
            "action":       "spatial_analysis",
            "operation":    operation,
            "layer_a_name": layer_a,
            "layer_b_name": layer_b,
            "params":       {},
            "result_name":  f"{operation}_result",
            "provider":     "turf.js",
        }
        return {
            "text":         f"Opération **{operation}** entre {layer_a} et {layer_b}.",
            "tool_calls":   [{"name":tool,"args":{"operation":operation,
                                                   "layer_a_name":layer_a,
                                                   "layer_b_name":layer_b}}],
            "tool_results": [result],
        }

    # ── QUERY TABLE ──────────────────────────────────────────

    async def _query_table(self, client, query: str, context: dict) -> dict:
        table_map = {
            "communes":       "communes",
            "arbres":         "arbres",
            "bâtiments auran":"batiments_auran",
            "canopée":        "canopy_nantes",
            "voiries":        "voiries",
            "équipements":    "equipements",
            "parcelles":      "parcelles",
        }
        q     = query.lower()
        table = next((v for k,v in table_map.items() if k in q), "communes")
        bbox  = context.get("current_bbox")

        args: dict = {"table": table, "limit": 2000}
        if bbox: args["bbox"] = bbox

        r = await client.call_tool("query_table", args, server_name="postgis")
        return self._wrap(r, "query_table", args)

    # ── LIST TABLES ──────────────────────────────────────────

    async def _list_tables(self, client) -> dict:
        r = await client.call_tool("get_db_tables", {}, server_name="postgis")
        tables = [t["table"] for t in r.get("tables",[])]
        return {
            "text":         f"Tables disponibles : {', '.join(tables)}",
            "tool_calls":   [{"name":"get_db_tables","args":{}}],
            "tool_results": [r],
        }

    # ── WRAP ─────────────────────────────────────────────────

    def _wrap(self, result: dict, tool: str, args: dict) -> dict:
        if "error" in result:
            return {"text":f"Erreur PostGIS: {result['error']}",
                    "tool_calls":[{"name":tool,"args":args}],"tool_results":[result]}
        n     = result.get("feature_count",
                len(result.get("geojson",{}).get("features",[])))
        layer = result.get("layer_name","Résultat spatial")
        return {
            "text":         f"**{layer}** : {n} éléments.",
            "tool_calls":   [{"name":tool,"args":args}],
            "tool_results": [result],
        }
