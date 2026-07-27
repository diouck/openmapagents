"""
agents/geo_data_agent.py — Agent Données Géographiques
=======================================================
Orchestre mcp_overture + mcp_osm selon le type de donnée.
"""

import logging
from agents.base_agent import BaseAgent

log = logging.getLogger("geo_data_agent")

# Mapping mots-clés → tool Overture
OVERTURE_TOOLS = {
    "restaurant": ("query_places",    {"category":"restaurant"}),
    "pharmacie":  ("query_places",    {"category":"pharmacie"}),
    "hôpital":    ("query_places",    {"category":"hôpital"}),
    "école":      ("query_places",    {"category":"école"}),
    "hôtel":      ("query_places",    {"category":"hôtel"}),
    "café":       ("query_places",    {"category":"café"}),
    "banque":     ("query_places",    {"category":"banque"}),
    "parking":    ("query_places",    {"category":"parking"}),
    "bâtiments":  ("query_buildings", {}),
    "batiments":  ("query_buildings", {}),
    "buildings":  ("query_buildings", {}),
    "routes":     ("query_roads",     {}),
    "réseau routier": ("query_roads", {}),
    "communes":   ("query_divisions", {"admin_level":"locality"}),
    "régions":    ("query_divisions", {"admin_level":"region"}),
    "pays":       ("query_divisions", {"admin_level":"country"}),
}

# Mapping mots-clés → tool OSM
OSM_TOOLS = {
    "parcs":           ("get_green_spaces",    {"green_type":"park"}),
    "espaces verts":   ("get_green_spaces",    {"green_type":"all"}),
    "forêt":           ("get_green_spaces",    {"green_type":"forest"}),
    "cours d'eau":     ("get_water_features",  {"water_type":"river"}),
    "lac":             ("get_water_features",  {"water_type":"lake"}),
    "rivière":         ("get_water_features",  {"water_type":"river"}),
    "bus":             ("get_public_transport",{"mode":"bus"}),
    "tram":            ("get_public_transport",{"mode":"tram"}),
    "métro":           ("get_public_transport",{"mode":"metro"}),
    "gare":            ("get_public_transport",{"mode":"train"}),
    "transport":       ("get_public_transport",{"mode":"all"}),
    "landuse":         ("get_landuse",         {"landuse_type":"all"}),
    "occupation sol":  ("get_landuse",         {"landuse_type":"all"}),
    "industriel":      ("get_landuse",         {"landuse_type":"industrial"}),
    "résidentiel":     ("get_landuse",         {"landuse_type":"residential"}),
}


def _detect_tool(query: str) -> tuple[str, dict, str] | None:
    """
    Retourne (tool_name, extra_args, server) ou None.
    Priorité : Overture → OSM.
    """
    q = query.lower()
    for kw, (tool, extra) in OVERTURE_TOOLS.items():
        if kw in q:
            return tool, extra, "overture"
    for kw, (tool, extra) in OSM_TOOLS.items():
        if kw in q:
            return tool, extra, "osm"
    return None


class GeoDataAgent(BaseAgent):

    def __init__(self):
        super().__init__(
            name="geo_data_agent",
            domain="overture",
            server="overture",
        )

    async def run(self, query: str, context: dict,
                  rag_tools: list, agent_config: dict = None) -> dict:

        from mcp_client import get_mcp_client
        client = get_mcp_client()
        q      = query.lower()
        bbox   = context.get("current_bbox")

        # ── Stats rapides ─────────────────────────────────────
        if any(w in q for w in ["combien","stats","statistiques","nombre"]):
            return await self._stats(client, q, bbox)

        # ── Détection automatique du tool ─────────────────────
        detected = _detect_tool(query)
        if detected:
            tool, extra, server = detected
            args = {**extra}
            if bbox: args["bbox"] = bbox
            return await self._query(client, tool, args, server, query)

        # ── Fallback : places génériques ──────────────────────
        args = {}
        if bbox: args["bbox"] = bbox

        # Extraire catégorie depuis la query
        import re
        m = re.search(
            r'(?:chercher?|trouver?|montrer?|afficher?)?\s*'
            r'(?:les?|des?|du)?\s*([a-zA-Zà-ü\-]+)',
            q
        )
        if m:
            cat = m.group(1).strip()
            if len(cat) > 3:
                args["category"] = cat

        r = await client.call_tool("query_overture", {**args,"theme":"places"}, server_name="overture")
        return self._wrap(r, "query_overture", {**args,"theme":"places"})

    # ── QUERY GÉNÉRIQUE ──────────────────────────────────────

    async def _query(self, client, tool, args, server, query) -> dict:
        r = await client.call_tool(tool, args, server_name=server)
        if "error" in r:
            # Fallback sur l'autre source
            alt_server = "osm" if server == "overture" else "overture"
            try:
                r2 = await client.call_tool(tool, args, server_name=alt_server)
                if "error" not in r2:
                    return self._wrap(r2, tool, args)
            except Exception:
                pass
        return self._wrap(r, tool, args)

    # ── STATS ────────────────────────────────────────────────

    async def _stats(self, client, query, bbox) -> dict:
        theme = "places"
        if "bâtiment" in query: theme = "buildings"
        elif "route" in query:  theme = "transportation"

        args = {"theme": theme}
        if bbox: args["bbox"] = bbox

        r = await client.call_tool("get_theme_stats", args, server_name="overture")
        stats = r.get("stats",{})
        total = stats.get("total",0)
        return {
            "text":         f"**{total}** {theme} dans cette zone.",
            "tool_calls":   [{"name":"get_theme_stats","args":args}],
            "tool_results": [r],
        }

    # ── WRAP ─────────────────────────────────────────────────

    def _wrap(self, result: dict, tool: str, args: dict) -> dict:
        if "error" in result:
            return {"text":f"Données non disponibles: {result['error']}",
                    "tool_calls":[{"name":tool,"args":args}],"tool_results":[result]}

        n     = result.get("feature_count",
                len(result.get("geojson",{}).get("features",[])))
        layer = result.get("layer_name","Données géographiques")
        return {
            "text":         f"**{layer}** : {n} éléments affichés.",
            "tool_calls":   [{"name":tool,"args":args}],
            "tool_results": [result],
        }
