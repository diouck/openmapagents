"""
agents/satellite_agent.py — Agent Satellite
============================================
Orchestre mcp_stac + mcp_gee via MCPClient.
Vérifie la disponibilité STAC avant tout appel GEE.

Workflow :
  1. mcp_stac.get_available_dates → dates + interval recommandé
  2. mcp_gee.compute_* selon la requête
  3. Valide tile_url retournée
"""

import logging
from agents.base_agent import BaseAgent, DOMAIN_ROLE

log = logging.getLogger("satellite_agent")


class SatelliteAgent(BaseAgent):

    def __init__(self):
        super().__init__(
            name="satellite_agent",
            domain="gee",
            server="gee",
        )

    async def run(self, query: str, context: dict,
                  rag_tools: list, agent_config: dict = None) -> dict:

        from mcp_client import get_mcp_client
        client = get_mcp_client()
        q      = query.lower()
        bbox   = (context.get("current_bbox") or [])
        dates  = context.get("current_dates",{})
        coll   = context.get("current_collection","sentinel2")

        start = dates.get("start_date","2024-01-01")
        end   = dates.get("end_date","2024-12-31")

        # ── Détection du tool GEE à appeler ──────────────────
        if any(w in q for w in ["timelapse","animation","évolution","années"]):
            return await self._timelapse(client, bbox, start, end, coll, q)

        if any(w in q for w in ["sar","radar","sentinel-1","vv","vh"]):
            return await self._sar(client, bbox, start, end, q)

        if any(w in q for w in ["lst","température surface","chaleur","modis temp"]):
            return await self._lst(client, bbox, start, end, q)

        if any(w in q for w in ["landsat lst","température landsat"]):
            return await self._lst_landsat(client, bbox, start, end)

        if any(w in q for w in ["era5","précipitation","pluie","humidité","température air"]):
            return await self._era5(client, bbox, start, end, q)

        if any(w in q for w in ["worldcover","occupation","land cover","land use"]):
            return await self._worldcover(client, bbox)

        if any(w in q for w in ["forêt","deforestation","hansen","forest watch","gfw"]):
            return await self._forest_watch(client, bbox, q)

        if any(w in q for w in ["canopée","canopy","hauteur forêt"]):
            return await self._canopy(client, bbox)

        if any(w in q for w in ["élévation","altitude","srtm","relief","pente","ombrage"]):
            return await self._relief(client, bbox, q)

        if any(w in q for w in ["ndwi","eau","inondation","humidité sol"]):
            return await self._index(client, bbox, start, end, coll, "compute_ndwi")

        if any(w in q for w in ["evi","enhanced vegetation"]):
            return await self._index(client, bbox, start, end, coll, "compute_evi")

        if any(w in q for w in ["savi","semi-aride","sahel"]):
            return await self._index(client, bbox, start, end, coll, "compute_savi")

        if any(w in q for w in ["rgb","vraie couleur","couleur naturelle","image"]):
            return await self._rgb(client, bbox, start, end, coll)

        # Défaut → NDVI
        return await self._ndvi(client, bbox, start, end, coll)

    # ── STAC CHECK ───────────────────────────────────────────

    async def _check_stac(self, client, bbox, start, end, collection) -> dict:
        """Vérifie disponibilité STAC avant appel GEE."""
        if not bbox:
            return {"ok": True, "recommended_interval": "month"}
        try:
            r = await client.call_tool("get_available_dates", {
                "bbox": bbox, "start_date": start, "end_date": end,
                "collection": collection, "cloud_cover": 50,
            }, server_name="stac", use_cache=True)
            if r.get("total_scenes",0) == 0:
                return {"ok": False, "error": r.get("suggestion",
                    f"Aucune scène {collection} disponible pour cette zone/période.")}
            return {
                "ok":                   True,
                "total_scenes":         r.get("total_scenes",0),
                "recommended_interval": r.get("recommended_interval","month"),
            }
        except Exception as e:
            log.debug(f"STAC check skipped: {e}")
            return {"ok": True, "recommended_interval": "month"}

    # ── NDVI ─────────────────────────────────────────────────

    async def _ndvi(self, client, bbox, start, end, collection) -> dict:
        stac = await self._check_stac(client, bbox, start, end, collection)
        if not stac["ok"]:
            return {"text": stac["error"], "tool_calls":[], "tool_results":[]}

        args = {"start_date":start,"end_date":end,"collection":collection}
        if bbox: args["bbox"] = bbox

        r = await client.call_tool("compute_ndvi", args, server_name="gee")
        return self._wrap(r, "compute_ndvi", args)

    # ── TIMELAPSE ────────────────────────────────────────────

    async def _timelapse(self, client, bbox, start, end, collection, query) -> dict:
        stac = await self._check_stac(client, bbox, start, end, collection)
        interval = stac.get("recommended_interval","month")

        index = "ndvi"
        if "rgb" in query:   index = "rgb"
        elif "evi" in query: index = "evi"
        elif "ndwi" in query: index = "ndwi"
        elif "sar" in query: index = "sar_vv"

        args = {
            "start_date": start, "end_date": end,
            "collection": collection, "interval": interval, "index": index,
        }
        if bbox: args["bbox"] = bbox

        r = await client.call_tool("compute_timelapse", args, server_name="gee")
        return self._wrap(r, "compute_timelapse", args)

    # ── SAR ──────────────────────────────────────────────────

    async def _sar(self, client, bbox, start, end, query) -> dict:
        if "vh" in query:          tool = "compute_sar_vh"
        elif "ratio" in query or "vv/vh" in query: tool = "compute_sar_vv_vh"
        elif "rgb" in query:       tool = "compute_sar_rgb"
        else:                      tool = "compute_sar_vv"

        args = {"start_date":start,"end_date":end}
        if bbox: args["bbox"] = bbox
        r = await client.call_tool(tool, args, server_name="gee")
        return self._wrap(r, tool, args)

    # ── LST MODIS ────────────────────────────────────────────

    async def _lst(self, client, bbox, start, end, query) -> dict:
        mode = "both" if ("jour" in query and "nuit" in query) else \
               "night" if "nuit" in query else "day"
        args = {"start_date":start,"end_date":end,"mode":mode}
        if bbox: args["bbox"] = bbox
        r = await client.call_tool("compute_lst_modis", args, server_name="gee")
        return self._wrap(r, "compute_lst_modis", args)

    # ── LST LANDSAT ──────────────────────────────────────────

    async def _lst_landsat(self, client, bbox, start, end) -> dict:
        args = {"start_date":start,"end_date":end,"collection":"landsat9"}
        if bbox: args["bbox"] = bbox
        r = await client.call_tool("compute_lst_landsat", args, server_name="gee")
        return self._wrap(r, "compute_lst_landsat", args)

    # ── ERA5 ─────────────────────────────────────────────────

    async def _era5(self, client, bbox, start, end, query) -> dict:
        if any(w in query for w in ["précipitation","pluie","rainfall"]):
            tool = "compute_era5_precip"
        elif any(w in query for w in ["humidité","humidity"]):
            tool = "compute_era5_humidity"
        else:
            tool = "compute_era5_temp"

        args = {"start_date":start,"end_date":end}
        if bbox: args["bbox"] = bbox
        r = await client.call_tool(tool, args, server_name="gee")
        return self._wrap(r, tool, args)

    # ── WORLDCOVER ───────────────────────────────────────────

    async def _worldcover(self, client, bbox) -> dict:
        args = {}
        if bbox: args["bbox"] = bbox
        r = await client.call_tool("compute_esa_worldcover", args, server_name="gee")
        return self._wrap(r, "compute_esa_worldcover", args)

    # ── FOREST WATCH ─────────────────────────────────────────

    async def _forest_watch(self, client, bbox, query) -> dict:
        import re
        args = {}
        if bbox: args["bbox"] = bbox
        yr = re.search(r"20\d\d", query)
        if yr: args["year_loss"] = int(yr.group())
        r = await client.call_tool("compute_forest_watch", args, server_name="gee")
        return self._wrap(r, "compute_forest_watch", args)

    # ── CANOPY ───────────────────────────────────────────────

    async def _canopy(self, client, bbox) -> dict:
        args = {}
        if bbox: args["bbox"] = bbox
        r = await client.call_tool("compute_canopy_height", args, server_name="gee")
        return self._wrap(r, "compute_canopy_height", args)

    # ── RELIEF ───────────────────────────────────────────────

    async def _relief(self, client, bbox, query) -> dict:
        if any(w in query for w in ["pente","slope"]):
            tool = "compute_slope"
        elif any(w in query for w in ["ombrage","hillshade"]):
            tool = "compute_hillshade"
        else:
            tool = "compute_elevation"
        args = {}
        if bbox: args["bbox"] = bbox
        r = await client.call_tool(tool, args, server_name="gee")
        return self._wrap(r, tool, args)

    # ── INDEX GÉNÉRIQUE ──────────────────────────────────────

    async def _index(self, client, bbox, start, end, collection, tool) -> dict:
        args = {"start_date":start,"end_date":end,"collection":collection}
        if bbox: args["bbox"] = bbox
        r = await client.call_tool(tool, args, server_name="gee")
        return self._wrap(r, tool, args)

    # ── RGB ──────────────────────────────────────────────────

    async def _rgb(self, client, bbox, start, end, collection) -> dict:
        args = {"start_date":start,"end_date":end,"collection":collection}
        if bbox: args["bbox"] = bbox
        r = await client.call_tool("compute_rgb", args, server_name="gee")
        return self._wrap(r, "compute_rgb", args)

    # ── WRAP ─────────────────────────────────────────────────

    def _wrap(self, result: dict, tool: str, args: dict) -> dict:
        """Formate la réponse dans le format standard agent.py."""
        if "error" in result:
            return {
                "text":         f"Erreur GEE: {result['error']}",
                "tool_calls":   [{"name":tool,"args":args}],
                "tool_results": [result],
            }
        # Valider tile_url
        tile_url = result.get("tile_url","")
        if tile_url and "{z}" not in tile_url:
            result["_warning"] = "tile_url manque {z}/{x}/{y}"

        layer = result.get("layer_name","Couche satellite")
        n     = result.get("image_count","")
        text  = f"**{layer}** ajouté sur la carte."
        if n: text += f" ({n} images composées)"

        return {
            "text":         text,
            "tool_calls":   [{"name":tool,"args":args}],
            "tool_results": [result],
        }
