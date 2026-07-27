"""
mcp_servers/mcp_gee.py — MCP Server Google Earth Engine
========================================================
Bridge entre le MCPClient et gee_routes.py existant.

Expose les tools GEE au format MCP :
  compute_ndvi, compute_lst, compute_ndwi, compute_evi,
  compute_rgb, compute_timelapse, compute_worldcover,
  compute_change_detection, get_available_dates

Réutilise EXACTEMENT la logique de gee_routes.py (indices, VIS_PARAMS,
composite, filtres nuages) sans la dupliquer.
"""

import logging
from typing import Optional

log = logging.getLogger("mcp_gee")

# ── Import gee_routes — source de vérité pour tout le calcul GEE ─
try:
    from gee_routes import (
        DATASETS, VIS_PARAMS,
        init_gee, get_ee,
    )
    _GEE_ROUTES_OK = True
except ImportError as e:
    log.warning(f"gee_routes non disponible: {e}")
    _GEE_ROUTES_OK = False


# ── Mapping index → dataset par défaut ───────────────────────────
INDEX_TO_DATASET = {
    "NDVI":            "sentinel2",
    "NDWI":            "sentinel2",
    "NDBI":            "sentinel2",
    "EVI":             "sentinel2",
    "LST":             "landsat9",
    "RGB":             "sentinel2",
    "WorldCover":      "worldcover",
    "Hauteur canopée":    "canopy_height",
    "SAR":                "sentinel1",
    "VV":                 "sentinel1",
    "VH":                 "sentinel1",
    "NBR":                "landsat9",
    "Couverture forêt 2000": "hansen",
    "Perte forêt":        "hansen",
    "Gain forêt":         "hansen",
    "Élévation":          "srtm",
    "Pente":              "srtm",
    "Ombrage":            "srtm",
}

# Noms d'index tels qu'attendus par gee_routes VIS_PARAMS
INDEX_LABEL = {
    "LST":             "LST (température)",
    "NDVI":            "NDVI",
    "NDWI":            "NDWI",
    "NDBI":            "NDBI",
    "EVI":             "EVI",
    "RGB":             "RGB",
    "WorldCover":      "Occupation du sol",
    "Hauteur canopée": "Hauteur canopée",  # dataset=canopy_height dans gee_routes
    "NBR":             "NDVI",             # NBR via landsat8/9 normalizedDiff B5/B7
    "SAR":             "VV",
}


class GeeServer:
    """
    MCP Server GEE — bridge vers gee_routes.py.
    Instancié une seule fois par MCPClient (singleton lazy).
    """

    def __init__(self):
        self._initialized = False

    def _ensure_init(self):
        if not self._initialized:
            if _GEE_ROUTES_OK:
                init_gee()
            self._initialized = True
            log.info("✓ GEE initialisé")

    async def call(self, tool_name: str, args: dict) -> dict:
        """Dispatch vers la méthode sync dans un thread."""
        import asyncio
        self._ensure_init()
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._dispatch, tool_name, args)

    def _dispatch(self, tool_name: str, args: dict) -> dict:
        dispatch = {
            "compute_ndvi":             lambda a: self._compute_index(a, "NDVI"),
            "compute_ndwi":             lambda a: self._compute_index(a, "NDWI"),
            "compute_evi":              lambda a: self._compute_index(a, "EVI"),
            "compute_lst":              lambda a: self._compute_index(a, "LST"),
            "compute_rgb":              lambda a: self._compute_index(a, "RGB"),
            "compute_worldcover":       lambda a: self._compute_index(a, "WorldCover"),
            "compute_canopy_height":    lambda a: self._compute_index(a, "Hauteur canopée"),
            "compute_forest_watch":     lambda a: self._compute_forest_watch(a),
            "compute_srtm":             lambda a: self._compute_srtm(a),
            "compute_nbr":              lambda a: self._compute_index(a, "NBR"),
            "compute_sar":              lambda a: self._compute_index(a, "SAR"),
            "compute_timelapse":        self._compute_timelapse,
            "compute_change_detection": self._compute_change_detection,
            "get_available_dates":      self._get_available_dates,
        }
        fn = dispatch.get(tool_name)
        if not fn:
            raise RuntimeError(f"GEE tool inconnu: '{tool_name}'")
        try:
            return fn(args)
        except Exception as e:
            log.error(f"GEE {tool_name} error: {e}")
            return {"error": str(e), "tool": tool_name}

    # ══════════════════════════════════════════════════════════════
    # _compute_index — moteur commun pour NDVI / LST / NDWI / EVI
    # Réutilise directement le endpoint /api/gee/tiles de gee_routes
    # ══════════════════════════════════════════════════════════════
    def _compute_index(self, args: dict, index: str) -> dict:
        """
        Appelle la logique de gee_routes.compute_tiles() via son endpoint interne.
        Retourne {tile_url, vis_params, bbox, index, date_start, date_end, action}.
        """
        # ── Résoudre le dataset ───────────────────────────────────
        # LST : toujours Landsat, jamais Sentinel-2
        if index == "LST":
            dataset = "landsat9"  # forced — LST n'existe pas sur Sentinel-2
        elif index in ("Hauteur canopée", "WorldCover", "Occupation du sol",
                        "Couverture forêt 2000", "Perte forêt", "Gain forêt",
                        "Élévation", "Pente", "Ombrage"):
            dataset = INDEX_TO_DATASET.get(index, "srtm")
        else:
            dataset = args.get("dataset") or args.get("collection_alias") or INDEX_TO_DATASET.get(index, "sentinel2")
            # Si collection fournie en clair (ex: COPERNICUS/S2_SR_HARMONIZED) → mapper vers dataset logique
            collection = args.get("collection", "")
            if collection and "/" in collection:
                for ds_name, ds in DATASETS.items():
                    if ds.get("collection") == collection:
                        dataset = ds_name
                        break

        # ── Dates ─────────────────────────────────────────────────
        date_start = args.get("date_start") or "2024-01-01"
        date_end   = args.get("date_end")   or "2024-12-31"

        # ── Bbox → xmin/ymin/xmax/ymax ───────────────────────────
        bbox = args.get("bbox") or args.get("clip_bbox")
        if bbox and len(bbox) == 4:
            xmin, ymin, xmax, ymax = bbox
        else:
            xmin = args.get("xmin"); ymin = args.get("ymin")
            xmax = args.get("xmax"); ymax = args.get("ymax")

        if not all(v is not None for v in [xmin, ymin, xmax, ymax]):
            return {"error": "bbox manquante (xmin/ymin/xmax/ymax ou bbox=[w,s,e,n])"}

        cloud_cover = int(args.get("cloud_cover", 20))
        # LST : least_cloudy donne de meilleurs résultats thermiques qu'une médiane
        default_composite = "least_cloudy" if index == "LST" else "median"
        composite   = args.get("composite", default_composite)

        # ── Appel direct à la fonction de gee_routes ──────────────
        # Datasets statiques : pas de dates ni cloud_cover
        _STATIC_DS = {"canopy_height", "hansen", "srtm", "worldcover"}
        is_static  = dataset in _STATIC_DS

        try:
            from gee_routes import compute_tiles_internal
            kwargs = dict(
                dataset=dataset,
                index=INDEX_LABEL.get(index, index),
                xmin=float(xmin), ymin=float(ymin),
                xmax=float(xmax), ymax=float(ymax),
            )
            if not is_static:
                kwargs["date_start"]  = date_start
                kwargs["date_end"]    = date_end
                kwargs["cloud_cover"] = cloud_cover
                kwargs["composite"]   = composite

            result = compute_tiles_internal(**kwargs)
            result["action"] = "add_layer"
            result["index"]  = index
            if not is_static:
                result["date_start"] = date_start
                result["date_end"]   = date_end
            return result

        except ImportError:
            # compute_tiles_internal n'existe pas → appel HTTP local
            return self._compute_via_http(
                dataset, INDEX_LABEL.get(index, index),
                date_start, date_end,
                float(xmin), float(ymin), float(xmax), float(ymax),
                cloud_cover, composite, index,
            )

    def _compute_via_http(self, dataset, index_label, date_start, date_end,
                          xmin, ymin, xmax, ymax, cloud_cover, composite, index_key) -> dict:
        """
        Fallback : appel HTTP vers /api/gee/tiles si compute_tiles_internal absent.
        """
        import requests
        _STATIC_DS = {"canopy_height", "hansen", "srtm", "worldcover"}
        is_static  = dataset in _STATIC_DS
        try:
            payload = {
                "dataset": dataset,
                "index":   index_label,
                "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
            }
            if not is_static:
                payload["date_start"]  = date_start
                payload["date_end"]    = date_end
                payload["cloud_cover"] = cloud_cover
                payload["composite"]   = composite
            else:
                # Dates fictives acceptées par l'endpoint (ignorées pour les statiques)
                payload["date_start"] = "2020-01-01"
                payload["date_end"]   = "2024-12-31"
            resp = requests.post(
                "http://localhost:8000/api/gee/tiles",
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            result = resp.json()
            result["action"]     = "add_layer"
            result["index"]      = index_key
            result["date_start"] = date_start
            result["date_end"]   = date_end
            # S'assurer que bbox est présente
            if not result.get("bbox"):
                result["bbox"] = [xmin, ymin, xmax, ymax]
            return result
        except Exception as e:
            log.error(f"GEE HTTP fallback error: {e}")
            return {"error": f"GEE tiles error: {e}"}

    # ══════════════════════════════════════════════════════════════
    # Forest Watch (Hansen) — dataset statique, choix de l'index
    # ══════════════════════════════════════════════════════════════
    def _compute_forest_watch(self, args: dict) -> dict:
        """
        Hansen Global Forest Watch — 3 layers : couverture 2000, perte, gain.
        Détecte l'index demandé depuis les args ou utilise la couverture par défaut.
        Dataset statique → pas de dates ni cloud_cover.
        """
        q = (args.get("query", "") or args.get("index", "") or "couverture forêt 2000").lower()
        if "perte" in q or "loss" in q or "deforest" in q:
            index = "Perte forêt"
        elif "gain" in q:
            index = "Gain forêt"
        else:
            index = "Couverture forêt 2000"
        # Nettoyer les args temporels
        clean_args = {k: v for k, v in args.items()
                      if k not in ("collection", "cloud_cover", "composite",
                                   "date_start", "date_end")}
        clean_args["dataset"] = "hansen"
        return self._compute_index(clean_args, index)

    # ══════════════════════════════════════════════════════════════
    # SRTM — dataset statique : élévation, pente, ombrage
    # ══════════════════════════════════════════════════════════════
    def _compute_srtm(self, args: dict) -> dict:
        """
        SRTM Relief — 3 layers : élévation, pente, ombrage.
        Dataset statique → pas de dates ni cloud_cover.
        """
        q = (args.get("query", "") or args.get("index", "") or "élévation").lower()
        if "pente" in q or "slope" in q:
            index = "Pente"
        elif "ombrage" in q or "hillshade" in q or "ombre" in q:
            index = "Ombrage"
        else:
            index = "Élévation"
        clean_args = {k: v for k, v in args.items()
                      if k not in ("collection", "cloud_cover", "composite",
                                   "date_start", "date_end")}
        clean_args["dataset"] = "srtm"
        return self._compute_index(clean_args, index)

    # ══════════════════════════════════════════════════════════════
    # Timelapse
    # ══════════════════════════════════════════════════════════════
    def _compute_timelapse(self, args: dict) -> dict:
        try:
            import requests
            resp = requests.post(
                "http://localhost:8000/api/gee/timelapse",
                json=args, timeout=120,
            )
            resp.raise_for_status()
            result = resp.json()
            result["action"] = "add_timelapse"
            return result
        except Exception as e:
            return {"error": f"Timelapse error: {e}"}

    # ══════════════════════════════════════════════════════════════
    # Change detection
    # ══════════════════════════════════════════════════════════════
    def _compute_change_detection(self, args: dict) -> dict:
        try:
            import requests
            resp = requests.post(
                "http://localhost:8000/api/gee/change-detection",
                json=args, timeout=90,
            )
            resp.raise_for_status()
            result = resp.json()
            result["action"] = "add_layer"
            return result
        except Exception as e:
            return {"error": f"Change detection error: {e}"}

    # ══════════════════════════════════════════════════════════════
    # Dates disponibles
    # ══════════════════════════════════════════════════════════════
    def _get_available_dates(self, args: dict) -> dict:
        try:
            import requests
            resp = requests.post(
                "http://localhost:8000/api/gee/dates",
                json=args, timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"error": f"Dates error: {e}"}
