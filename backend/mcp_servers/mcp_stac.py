"""
mcp_servers/mcp_stac.py — MCP Server STAC (SpatioTemporal Asset Catalog)
=========================================================================
Endpoint : Element84 Earth Search (Sentinel-2, Landsat, NAIP, Copernicus DEM)
           https://earth-search.aws.element84.com/v1

Utilisé principalement AVANT les appels GEE pour :
  - Vérifier quelles scènes sont réellement disponibles
  - Trouver les dates avec le moins de nuages
  - Obtenir les previews des scènes
  - Eviter les appels GEE sur des périodes sans données

Tools exposés :
    search_catalog          → recherche de scènes par bbox/dates/collection
    get_available_dates     → dates disponibles avec stats nuages
    get_least_cloudy_scene  → meilleure scène pour une période
    get_scene_preview       → URL preview d'une scène
    list_collections        → collections STAC disponibles
"""

import os
import json
import hashlib
import logging
import asyncio
from pathlib import Path
from typing import Optional

import httpx

log = logging.getLogger("mcp_stac")

# ── Config ────────────────────────────────────────────────────
STAC_URL     = os.getenv(
    "STAC_URL",
    "https://earth-search.aws.element84.com/v1"
)
TIMEOUT      = int(os.getenv("STAC_TIMEOUT", "15"))
CACHE_DIR    = Path(os.getenv("CACHE_DIR", "./data/cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
MAX_ITEMS    = int(os.getenv("STAC_MAX_ITEMS", "100"))

# ── Collections disponibles ───────────────────────────────────
COLLECTIONS = {
    # Alias court → ID STAC complet
    "sentinel2":      "sentinel-2-l2a",
    "sentinel2_l1c":  "sentinel-2-l1c",
    "landsat8":       "landsat-c2-l2",
    "landsat9":       "landsat-c2-l2",
    "landsat":        "landsat-c2-l2",
    "cop_dem_30":     "cop-dem-glo-30",
    "cop_dem_90":     "cop-dem-glo-90",
    "naip":           "naip",
    "sentinel1_rtc":  "sentinel-1-rtc",
    # IDs STAC directs acceptés tels quels
    "sentinel-2-l2a": "sentinel-2-l2a",
    "sentinel-2-l1c": "sentinel-2-l1c",
    "landsat-c2-l2":  "landsat-c2-l2",
    "cop-dem-glo-30": "cop-dem-glo-30",
    "sentinel-1-rtc": "sentinel-1-rtc",
}

# ── Champs cloud cover par collection ─────────────────────────
CLOUD_FIELDS = {
    "sentinel-2-l2a":  "eo:cloud_cover",
    "sentinel-2-l1c":  "eo:cloud_cover",
    "landsat-c2-l2":   "eo:cloud_cover",
    "sentinel-1-rtc":  None,   # SAR — pas de cloud cover
    "cop-dem-glo-30":  None,
    "naip":            "eo:cloud_cover",
}


def _resolve_collection(hint: str) -> str:
    resolved = COLLECTIONS.get(hint.lower())
    if not resolved:
        raise ValueError(
            f"Collection '{hint}' inconnue. "
            f"Disponibles: {list(COLLECTIONS.keys())}"
        )
    return resolved


def _validate_bbox(bbox: list) -> list:
    if len(bbox) != 4:
        raise ValueError("bbox doit être [xmin, ymin, xmax, ymax]")
    xmin, ymin, xmax, ymax = [float(v) for v in bbox]
    if not (-180 <= xmin < xmax <= 180) or not (-90 <= ymin < ymax <= 90):
        raise ValueError(f"bbox invalide: {bbox}")
    return [xmin, ymin, xmax, ymax]


def _validate_dates(start: str, end: str):
    from datetime import datetime
    try:
        dt_start = datetime.strptime(start, "%Y-%m-%d")
        dt_end   = datetime.strptime(end,   "%Y-%m-%d")
    except ValueError:
        raise ValueError("Dates invalides. Format requis: YYYY-MM-DD")
    if dt_start >= dt_end:
        raise ValueError(f"start_date ({start}) doit être < end_date ({end})")
    if dt_end > datetime.now():
        raise ValueError(f"end_date ({end}) ne peut pas être dans le futur")


def _cache_key(params: dict) -> str:
    return hashlib.md5(
        json.dumps(params, sort_keys=True).encode()
    ).hexdigest()


def _get_cached(key: str) -> Optional[dict]:
    path = CACHE_DIR / f"stac_{key}.json"
    if path.exists():
        try:
            data = json.loads(path.read_text())
            log.debug(f"[STAC Cache hit] {key[:8]}")
            return data
        except Exception:
            pass
    return None


def _set_cached(key: str, data: dict):
    path = CACHE_DIR / f"stac_{key}.json"
    try:
        path.write_text(json.dumps(data, default=str))
    except Exception as e:
        log.warning(f"STAC cache write error: {e}")


def _stac_search(
    collection: str,
    bbox: list,
    start: str,
    end: str,
    cloud_max: int = 30,
    limit: int = 50,
    sort_by: str = "datetime",
    sort_dir: str = "desc",
) -> Optional[dict]:
    """Appel POST /search sur l'API STAC."""
    cloud_field = CLOUD_FIELDS.get(collection, "eo:cloud_cover")
    body = {
        "collections": [collection],
        "bbox":        bbox,
        "datetime":    f"{start}T00:00:00Z/{end}T23:59:59Z",
        "limit":       min(limit, MAX_ITEMS),
        "sortby":      [{"field": sort_by, "direction": sort_dir}],
    }

    # Filtre cloud cover si applicable
    if cloud_field:
        body["filter"] = {
            "op":   "<=",
            "args": [{"property": cloud_field}, cloud_max],
        }
        body["filter-lang"] = "cql2-json"

    cache_params = {**body, "_url": STAC_URL}
    ck = _cache_key(cache_params)
    cached = _get_cached(ck)
    if cached:
        cached["_cache_hit"] = True
        return cached

    try:
        resp = httpx.post(
            f"{STAC_URL}/search",
            json=body,
            timeout=TIMEOUT,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code != 200:
            log.warning(f"STAC search {resp.status_code}: {resp.text[:200]}")
            return None
        data = resp.json()
        _set_cached(ck, data)
        return data
    except Exception as e:
        log.error(f"STAC search error: {e}")
        return None


def _parse_item(item: dict, collection: str) -> dict:
    """Extrait les infos essentielles d'un item STAC."""
    props      = item.get("properties", {})
    cloud_field = CLOUD_FIELDS.get(collection, "eo:cloud_cover")
    cloud      = props.get(cloud_field) if cloud_field else None
    assets     = item.get("assets", {})

    # Trouver l'asset preview (thumbnail ou overview)
    preview_url = None
    for key in ("thumbnail", "overview", "visual", "rendered_preview"):
        if key in assets:
            preview_url = assets[key].get("href")
            break

    # Trouver l'asset TCI (True Color Image) pour Sentinel-2
    tci_url = None
    for key in ("visual", "TCI", "tci"):
        if key in assets:
            tci_url = assets[key].get("href")
            break

    return {
        "id":          item.get("id"),
        "date":        props.get("datetime", "")[:10],
        "cloud_cover": round(float(cloud), 1) if cloud is not None else None,
        "bbox":        item.get("bbox", []),
        "preview_url": preview_url,
        "tci_url":     tci_url,
        "platform":    props.get("platform", ""),
        "instrument":  props.get("instruments", [""])[0] if props.get("instruments") else "",
        "gsd":         props.get("gsd"),          # résolution sol (m)
        "collection":  collection,
        "assets":      list(assets.keys()),
    }


# ═══════════════════════════════════════════════════════════════
# STAC SERVER
# ═══════════════════════════════════════════════════════════════

class StacServer:

    async def call(self, tool: str, args: dict) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._run, tool, args)

    def _run(self, tool: str, args: dict) -> dict:
        fn = {
            "search_catalog":         self.search_catalog,
            "get_available_dates":    self.get_available_dates,
            "get_least_cloudy_scene": self.get_least_cloudy_scene,
            "get_scene_preview":      self.get_scene_preview,
            "list_collections":       self.list_collections,
        }.get(tool)
        if not fn:
            return {"error": f"STAC tool inconnu: '{tool}'"}
        try:
            return fn(args)
        except ValueError as e:
            return {"error": str(e), "tool": tool}
        except Exception as e:
            log.error(f"STAC {tool}: {e}")
            return {"error": f"Erreur STAC: {e}", "tool": tool}

    # ─── SEARCH CATALOG ───────────────────────────────────────

    def search_catalog(self, a: dict) -> dict:
        """
        Recherche de scènes satellite dans le catalog STAC.

        Args:
            bbox:        [xmin, ymin, xmax, ymax]
            start_date:  YYYY-MM-DD
            end_date:    YYYY-MM-DD
            collection:  sentinel2 | landsat8 | landsat9 | sentinel1_rtc | ...
            cloud_cover: max % nuages (défaut 30, ignoré pour SAR/DEM)
            limit:       max scènes retournées (défaut 20, max 100)
            sort:        cloud_asc | date_desc | date_asc (défaut: date_desc)

        Returns:
            liste de scènes avec id, date, cloud_cover, preview_url, bbox
        """
        bbox        = a.get("bbox")
        start       = a.get("start_date", "2024-01-01")
        end         = a.get("end_date",   "2024-12-31")
        coll_hint   = a.get("collection", "sentinel2")
        cloud_max   = int(a.get("cloud_cover", 30))
        limit       = min(int(a.get("limit", 20)), MAX_ITEMS)
        sort        = a.get("sort", "date_desc")

        if not bbox:
            return {"error": "bbox requis: [xmin, ymin, xmax, ymax]"}

        bbox       = _validate_bbox(bbox)
        _validate_dates(start, end)
        collection = _resolve_collection(coll_hint)

        # Tri
        sort_map = {
            "cloud_asc":  ("eo:cloud_cover",  "asc"),
            "date_desc":  ("datetime",         "desc"),
            "date_asc":   ("datetime",         "asc"),
        }
        sort_field, sort_dir = sort_map.get(sort, ("datetime", "desc"))

        data = _stac_search(
            collection, bbox, start, end,
            cloud_max, limit, sort_field, sort_dir
        )
        if not data:
            return {
                "error":      "STAC API indisponible ou aucune scène trouvée",
                "collection": collection,
                "period":     f"{start} → {end}",
            }

        items = data.get("features", [])
        if not items:
            return {
                "error":       (
                    f"Aucune scène {coll_hint} disponible "
                    f"(cloud<{cloud_max}%) pour {start}→{end}"
                ),
                "suggestion":  "Augmentez cloud_cover ou élargissez la période",
                "collection":  collection,
            }

        scenes = [_parse_item(it, collection) for it in items]

        # Stats
        clouds = [s["cloud_cover"] for s in scenes
                  if s["cloud_cover"] is not None]
        stats = {}
        if clouds:
            stats = {
                "min_cloud": round(min(clouds), 1),
                "max_cloud": round(max(clouds), 1),
                "avg_cloud": round(sum(clouds) / len(clouds), 1),
            }

        return {
            "action":      "show_scenes",
            "scenes":      scenes,
            "total":       len(scenes),
            "matched":     data.get("context", {}).get("matched", len(scenes)),
            "collection":  collection,
            "period":      f"{start} → {end}",
            "cloud_max":   cloud_max,
            "stats":       stats,
            "_cache_hit":  data.get("_cache_hit", False),
        }

    # ─── GET AVAILABLE DATES ──────────────────────────────────

    def get_available_dates(self, a: dict) -> dict:
        """
        Dates disponibles pour une bbox/collection avec stats nuages.
        Utilisé avant compute_timelapse pour valider les frames disponibles.

        Args:
            bbox, start_date, end_date, collection, cloud_cover

        Returns:
            liste de dates + cloud_cover pour chaque date
            + recommandation intervalle timelapse
        """
        bbox      = a.get("bbox")
        start     = a.get("start_date", "2020-01-01")
        end       = a.get("end_date",   "2024-12-31")
        coll_hint = a.get("collection", "sentinel2")
        cloud_max = int(a.get("cloud_cover", 50))

        if not bbox:
            return {"error": "bbox requis"}

        bbox       = _validate_bbox(bbox)
        _validate_dates(start, end)
        collection = _resolve_collection(coll_hint)

        data = _stac_search(
            collection, bbox, start, end,
            cloud_max, MAX_ITEMS, "datetime", "asc"
        )
        if not data:
            return {"error": "STAC API indisponible"}

        items = data.get("features", [])
        if not items:
            return {
                "total_scenes":    0,
                "available_dates": [],
                "suggestion":      (
                    f"Aucune scène disponible (cloud<{cloud_max}%). "
                    f"Augmentez cloud_cover."
                ),
            }

        # Grouper par mois
        from collections import defaultdict
        monthly: dict = defaultdict(list)
        for item in items:
            props  = item.get("properties", {})
            date   = props.get("datetime", "")[:10]
            cloud_field = CLOUD_FIELDS.get(collection, "eo:cloud_cover")
            cloud  = props.get(cloud_field) if cloud_field else 0
            month  = date[:7]  # YYYY-MM
            monthly[month].append({
                "date":        date,
                "cloud_cover": round(float(cloud), 1) if cloud is not None else None,
                "scene_id":    item.get("id"),
            })

        # Meilleure scène par mois
        best_by_month = []
        for month, scenes in sorted(monthly.items()):
            # Trier par cloud cover croissant
            scenes_sorted = sorted(
                scenes,
                key=lambda s: s["cloud_cover"] if s["cloud_cover"] is not None else 999
            )
            best = scenes_sorted[0]
            best["month"]       = month
            best["scene_count"] = len(scenes)
            best_by_month.append(best)

        all_dates = sorted(set(
            item["properties"]["datetime"][:10]
            for item in items
        ))

        # Recommandation intervalle timelapse
        n_months = len(best_by_month)
        if n_months >= 24:   interval = "quarter"
        elif n_months >= 6:  interval = "month"
        else:                interval = "week"

        return {
            "collection":      collection,
            "total_scenes":    len(items),
            "available_dates": all_dates[:200],
            "best_by_month":   best_by_month,
            "months_count":    n_months,
            "period":          f"{start} → {end}",
            "cloud_cover_max": cloud_max,
            "recommended_interval": interval,
            "_cache_hit":      data.get("_cache_hit", False),
        }

    # ─── GET LEAST CLOUDY SCENE ───────────────────────────────

    def get_least_cloudy_scene(self, a: dict) -> dict:
        """
        Meilleure scène (moins de nuages) pour une bbox et période.
        Utilisé pour sélectionner l'image optimale avant un appel GEE.

        Args:
            bbox, start_date, end_date, collection

        Returns:
            meilleure scène avec id, date, cloud_cover, preview_url
        """
        bbox      = a.get("bbox")
        start     = a.get("start_date", "2024-01-01")
        end       = a.get("end_date",   "2024-12-31")
        coll_hint = a.get("collection", "sentinel2")

        if not bbox:
            return {"error": "bbox requis"}

        bbox       = _validate_bbox(bbox)
        _validate_dates(start, end)
        collection = _resolve_collection(coll_hint)

        # Chercher triées par cloud_cover asc
        data = _stac_search(
            collection, bbox, start, end,
            cloud_max=100, limit=10,
            sort_by="eo:cloud_cover", sort_dir="asc"
        )
        if not data or not data.get("features"):
            return {
                "error":     f"Aucune scène {coll_hint} pour {start}→{end}",
                "collection": collection,
            }

        best  = data["features"][0]
        scene = _parse_item(best, collection)

        return {
            "action":      "show_best_scene",
            "scene":       scene,
            "collection":  collection,
            "period":      f"{start} → {end}",
            "message":     (
                f"Meilleure scène: {scene['date']} "
                f"({scene['cloud_cover']}% nuages)"
                if scene["cloud_cover"] is not None
                else f"Meilleure scène: {scene['date']}"
            ),
            "_cache_hit":  data.get("_cache_hit", False),
        }

    # ─── GET SCENE PREVIEW ────────────────────────────────────

    def get_scene_preview(self, a: dict) -> dict:
        """
        URL de preview (thumbnail) d'une scène par son ID STAC.

        Args:
            scene_id:   ID STAC de la scène (depuis search_catalog)
            collection: collection STAC

        Returns:
            preview_url, tci_url, métadonnées scène
        """
        scene_id  = a.get("scene_id", "")
        coll_hint = a.get("collection", "sentinel2")

        if not scene_id:
            return {"error": "scene_id requis"}

        collection = _resolve_collection(coll_hint)
        ck = _cache_key({"scene_id": scene_id, "collection": collection})
        cached = _get_cached(ck)
        if cached:
            return {**cached, "_cache_hit": True}

        try:
            url  = f"{STAC_URL}/collections/{collection}/items/{scene_id}"
            resp = httpx.get(url, timeout=TIMEOUT)
            if resp.status_code != 200:
                return {
                    "error": f"Scène '{scene_id}' non trouvée "
                             f"(HTTP {resp.status_code})"
                }
            item   = resp.json()
            scene  = _parse_item(item, collection)
            result = {
                "action":      "show_preview",
                "scene":       scene,
                "preview_url": scene.get("preview_url"),
                "tci_url":     scene.get("tci_url"),
                "collection":  collection,
            }
            _set_cached(ck, result)
            return result
        except Exception as e:
            return {"error": f"Erreur récupération scène: {e}"}

    # ─── LIST COLLECTIONS ─────────────────────────────────────

    def list_collections(self, a: dict) -> dict:
        """
        Liste les collections STAC disponibles sur Earth Search.

        Returns:
            liste avec id, description, étendue spatiale/temporelle
        """
        ck = _cache_key({"op": "list_collections", "url": STAC_URL})
        cached = _get_cached(ck)
        if cached:
            return {**cached, "_cache_hit": True}

        try:
            resp = httpx.get(
                f"{STAC_URL}/collections",
                timeout=TIMEOUT,
            )
            if resp.status_code != 200:
                # Retourner la liste statique connue
                return self._static_collections()

            data        = resp.json()
            raw_colls   = data.get("collections", [])
            collections = []
            for c in raw_colls:
                extent   = c.get("extent", {})
                temporal = extent.get("temporal", {}).get("interval", [[None, None]])[0]
                collections.append({
                    "id":          c.get("id"),
                    "title":       c.get("title", ""),
                    "description": c.get("description", "")[:200],
                    "start_date":  temporal[0][:10] if temporal[0] else None,
                    "end_date":    temporal[1][:10] if temporal[1] else "présent",
                    "alias":       next(
                        (k for k, v in COLLECTIONS.items() if v == c.get("id")),
                        c.get("id")
                    ),
                })

            result = {
                "action":      "show_list",
                "collections": collections,
                "total":       len(collections),
                "endpoint":    STAC_URL,
                "aliases":     {k: v for k, v in COLLECTIONS.items()
                                if k != v},
            }
            _set_cached(ck, result)
            return result

        except Exception as e:
            log.warning(f"list_collections error: {e} → static list")
            return self._static_collections()

    def _static_collections(self) -> dict:
        """Collections connues statiquement si l'API est indispo."""
        return {
            "action": "show_list",
            "collections": [
                {"id": "sentinel-2-l2a",  "alias": "sentinel2",
                 "title": "Sentinel-2 L2A (SR)",
                 "description": "Sentinel-2 surface reflectance, 10m, global"},
                {"id": "sentinel-2-l1c",  "alias": "sentinel2_l1c",
                 "title": "Sentinel-2 L1C (TOA)",
                 "description": "Sentinel-2 top of atmosphere, 10m, global"},
                {"id": "landsat-c2-l2",   "alias": "landsat",
                 "title": "Landsat Collection 2 L2",
                 "description": "Landsat 8/9 surface reflectance, 30m, global"},
                {"id": "sentinel-1-rtc",  "alias": "sentinel1_rtc",
                 "title": "Sentinel-1 RTC",
                 "description": "Sentinel-1 SAR Radiometric Terrain Corrected"},
                {"id": "cop-dem-glo-30",  "alias": "cop_dem_30",
                 "title": "Copernicus DEM 30m",
                 "description": "Global DEM at 30m resolution"},
                {"id": "naip",            "alias": "naip",
                 "title": "NAIP (USA)",
                 "description": "National Agriculture Imagery Program, 1m, USA"},
            ],
            "total":    6,
            "endpoint": STAC_URL,
            "aliases":  {k: v for k, v in COLLECTIONS.items() if k != v},
            "_static":  True,
        }
