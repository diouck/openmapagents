"""
maxar_routes.py — Maxar Open Data Program : imagerie satellite haute résolution
AVANT / APRÈS catastrophe, importée directement depuis le catalogue public.

Le programme Maxar Open Data met gratuitement à disposition des images HR (COG)
des zones frappées par des catastrophes (inondations, incendies, cyclones,
séismes, éruptions…), généralement en plusieurs acquisitions datées — donc des
prises AVANT et APRÈS l'événement. Le catalogue est un STAC statique hébergé sur
le bucket AWS public `maxar-opendata` (lecture anonyme, sans compte AWS).

Arborescence STAC :
  events/catalog.json                                   → 1 enfant par événement
  events/<EVENT>/collection.json                        → 1 enfant par acquisition
  events/<EVENT>/ard/acquisition_collections/<ID>_collection.json → liens item
  events/<EVENT>/ard/<z>/<quadkey>/<date>/<ID>.json     → item (asset « visual »)
  …-visual.tif  = COG RVB 8 bits (0.3–0.5 m) → parfait pour un aperçu carte.

Endpoints (routeur /api/maxar) :
  GET  /events                       → liste des événements du catalogue.
  POST /acquisitions {event}         → acquisitions datées d'un événement
                                       (date, emprise, nb de tuiles) triées.
  POST /mosaic {event, catalog_id,   → mosaïque les tuiles « visual » de
               bbox?, max_side?}       l'acquisition (intersectant bbox) en un
                                       PNG géoréférencé EPSG:3857 → overlay carte.

Licence des données : CC BY-NC 4.0 (usage non commercial), © Maxar Open Data.

Sécurité : toutes les URL sont construites à partir d'une base FIXE
(`maxar-opendata.s3.amazonaws.com`) + un identifiant d'événement / d'acquisition
validé (`^[A-Za-z0-9._-]+$`) → pas d'URL arbitraire (anti-SSRF). GDAL /vsicurl
en lecture anonyme (AWS_NO_SIGN_REQUEST).

Déps : rasterio numpy Pillow (déjà requises par raster_routes / stac_routes).
"""
import io
import re
import json
import base64
import urllib.request
import urllib.error
from typing import Optional, List
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import os
os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
os.environ.setdefault("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif,.TIF,.tiff")
os.environ.setdefault("GDAL_HTTP_TIMEOUT", "30")
os.environ.setdefault("VSI_CACHE", "TRUE")
os.environ.setdefault("AWS_NO_SIGN_REQUEST", "YES")   # bucket public, lecture anonyme

router = APIRouter(prefix="/api/maxar", tags=["maxar"])

_BASE = "https://maxar-opendata.s3.amazonaws.com"
_ROOT = _BASE + "/events/catalog.json"
_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")     # identifiants d'événement / catalog_id sûrs
_MAX_TILES = 40                               # tuiles mosaïquées au plus (perf)
_MAX_ITEMS = 600                              # items STAC listés au plus par acquisition
_MAX_SIDE = 2048                              # côté max du PNG de sortie (défaut)
_MAX_SIDE_CEIL = 4096                         # plafond « haute déf » (limite texture WebGL)
_UA = {"User-Agent": "OpenMapAgents/1.0"}

# Caches en processus (le catalogue S3 est statique).
_CACHE_EVENTS: Optional[list] = None
_CACHE_ACQ: dict = {}


# ─────────────────────────────── util HTTP ────────────────────────────────
def _fetch_json(url: str, timeout: int = 25):
    rq = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return json.load(r)


def _fetch_json_safe(url: str):
    """Version sans exception (pour les fetch parallèles) → (url, data|None)."""
    try:
        return url, _fetch_json(url, timeout=20)
    except Exception:
        return url, None


def _pretty(event_id: str) -> str:
    """Nom lisible : « Brazil-Flooding-May24 » → « Brazil Flooding May24 »."""
    s = re.sub(r"[-_]+", " ", event_id).strip()
    return s or event_id


def _check_id(value: str, what: str) -> str:
    v = (value or "").strip()
    if not v or not _ID_RE.match(v) or ".." in v:
        raise HTTPException(400, f"{what} invalide.")
    return v


# ─────────────────────────────── requêtes ─────────────────────────────────
class AcqReq(BaseModel):
    event: str


class MosaicReq(BaseModel):
    event: str
    catalog_id: str
    bbox: Optional[List[float]] = None     # [ouest, sud, est, nord] (WGS84) — vue courante
    max_side: int = _MAX_SIDE


# ─────────────────────────────── endpoints ────────────────────────────────
@router.get("/events")
def maxar_events():
    """Liste les événements (catastrophes) du catalogue Maxar Open Data."""
    global _CACHE_EVENTS
    if _CACHE_EVENTS is None:
        try:
            root = _fetch_json(_ROOT)
        except Exception as e:
            raise HTTPException(502, f"Catalogue Maxar injoignable : {e}")
        evs = []
        for l in root.get("links", []):
            if l.get("rel") != "child":
                continue
            href = l.get("href") or ""
            # ./<EVENT>/collection.json → <EVENT>
            eid = href.lstrip("./").split("/")[0]
            if eid and _ID_RE.match(eid):
                evs.append({"id": eid, "title": _pretty(eid)})
        evs.sort(key=lambda e: e["title"].lower())
        _CACHE_EVENTS = evs
    return {"count": len(_CACHE_EVENTS), "events": _CACHE_EVENTS}


@router.post("/acquisitions")
def maxar_acquisitions(req: AcqReq):
    """Acquisitions datées d'un événement (chaque prise = AVANT ou APRÈS)."""
    event = _check_id(req.event, "Événement")
    if event in _CACHE_ACQ:
        return _CACHE_ACQ[event]

    coll_url = f"{_BASE}/events/{event}/collection.json"
    try:
        coll = _fetch_json(coll_url)
    except urllib.error.HTTPError as e:
        raise HTTPException(404 if e.code == 404 else 502, f"Événement introuvable ({e.code}).")
    except Exception as e:
        raise HTTPException(502, f"Événement injoignable : {e}")

    child_urls = [urljoin(coll_url, l["href"]) for l in coll.get("links", [])
                  if l.get("rel") == "child" and l.get("href")]
    if not child_urls:
        raise HTTPException(404, "Aucune acquisition pour cet événement.")

    # Fetch parallèle des collections d'acquisition (date + emprise + nb tuiles).
    acqs = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        for url, d in ex.map(_fetch_json_safe, child_urls):
            if not d:
                continue
            interval = (((d.get("extent") or {}).get("temporal") or {}).get("interval") or [[None, None]])
            start = interval[0][0] if interval and interval[0] else None
            spatial = (((d.get("extent") or {}).get("spatial") or {}).get("bbox") or [None])
            bbox = spatial[0] if spatial else None
            n_items = sum(1 for l in d.get("links", []) if l.get("rel") == "item")
            acqs.append({
                "catalog_id": d.get("id") or url.split("/")[-1].replace("_collection.json", ""),
                "datetime": start,
                "date": (start or "")[:10],
                "bbox": [float(x) for x in bbox] if bbox else None,
                "n_tiles": n_items,
            })

    acqs.sort(key=lambda a: a["datetime"] or "")

    # Emprise globale de l'événement = union des acquisitions (pour recadrer).
    evbb = None
    for a in acqs:
        b = a["bbox"]
        if not b:
            continue
        evbb = b[:] if evbb is None else [min(evbb[0], b[0]), min(evbb[1], b[1]),
                                          max(evbb[2], b[2]), max(evbb[3], b[3])]

    out = {"event": event, "title": _pretty(event), "bbox": evbb,
           "count": len(acqs), "acquisitions": acqs,
           "license": "CC BY-NC 4.0 · © Maxar Open Data"}
    _CACHE_ACQ[event] = out
    return out


@router.post("/mosaic")
def maxar_mosaic(req: MosaicReq):
    """Mosaïque les tuiles « visual » d'une acquisition en un overlay carte."""
    try:
        import numpy as np
        import rasterio
        from rasterio.vrt import WarpedVRT
        from rasterio.merge import merge
        from rasterio.enums import Resampling
        from rasterio.transform import array_bounds
        from rasterio.warp import transform as warp_transform, transform_bounds
        from PIL import Image
    except ImportError as e:
        raise HTTPException(503, f"Dépendance raster manquante : « {getattr(e, 'name', e)} ».")

    event = _check_id(req.event, "Événement")
    catalog_id = _check_id(req.catalog_id, "Acquisition")

    acq_url = f"{_BASE}/events/{event}/ard/acquisition_collections/{catalog_id}_collection.json"
    try:
        acq = _fetch_json(acq_url)
    except urllib.error.HTTPError as e:
        raise HTTPException(404 if e.code == 404 else 502, f"Acquisition introuvable ({e.code}).")
    except Exception as e:
        raise HTTPException(502, f"Acquisition injoignable : {e}")

    item_urls = [urljoin(acq_url, l["href"]) for l in acq.get("links", [])
                 if l.get("rel") == "item" and l.get("href")][:_MAX_ITEMS]
    if not item_urls:
        raise HTTPException(404, "Aucune tuile dans cette acquisition.")

    # Fetch parallèle des items → (bbox WGS84, href COG visual absolu).
    tiles = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        for url, d in ex.map(_fetch_json_safe, item_urls):
            if not d:
                continue
            bb = d.get("bbox")
            va = (d.get("assets") or {}).get("visual") or {}
            href = va.get("href")
            if bb and href:
                tiles.append({"bbox": [float(x) for x in bb], "href": urljoin(url, href)})
    if not tiles:
        raise HTTPException(502, "Tuiles illisibles pour cette acquisition.")

    n_available = len(tiles)

    # Filtre par emprise (vue courante) si fournie.
    bb = req.bbox
    if bb and len(bb) == 4:
        w, s, e, n = float(bb[0]), float(bb[1]), float(bb[2]), float(bb[3])

        def _hit(t):
            tw, ts, te, tn = t["bbox"]
            return not (te < w or tw > e or tn < s or ts > n)
        inview = [t for t in tiles if _hit(t)]
        if not inview:
            raise HTTPException(422, "Aucune tuile dans la vue courante — recadrez la carte "
                                     "sur l'événement (ou décochez « limiter à la vue »).")
        cx, cy = (w + e) / 2, (s + n) / 2
        inview.sort(key=lambda t: ((t["bbox"][0] + t["bbox"][2]) / 2 - cx) ** 2
                                  + ((t["bbox"][1] + t["bbox"][3]) / 2 - cy) ** 2)
        tiles = inview

    truncated = len(tiles) > _MAX_TILES
    tiles = tiles[:_MAX_TILES]

    # Emprise cible (WGS84) = union des tuiles retenues, éventuellement clampée à bbox.
    uw = min(t["bbox"][0] for t in tiles); us = min(t["bbox"][1] for t in tiles)
    ue = max(t["bbox"][2] for t in tiles); un = max(t["bbox"][3] for t in tiles)
    if bb and len(bb) == 4:
        uw = max(uw, float(bb[0])); us = max(us, float(bb[1]))
        ue = min(ue, float(bb[2])); un = min(un, float(bb[3]))
    if not (ue > uw and un > us):
        raise HTTPException(422, "Emprise cible vide.")

    # → EPSG:3857
    x3857, y3857 = warp_transform("EPSG:4326", "EPSG:3857", [uw, ue], [us, un])
    tw3857, te3857 = min(x3857), max(x3857)
    ts3857, tn3857 = min(y3857), max(y3857)
    span = max(te3857 - tw3857, tn3857 - ts3857)
    if span <= 0:
        raise HTTPException(422, "Emprise cible invalide.")
    max_side = max(256, min(int(req.max_side or _MAX_SIDE), _MAX_SIDE_CEIL))

    # Ouvre les COG, reprojette chacun en 3857 (WarpedVRT), mosaïque (merge).
    srcs, vrts = [], []
    try:
        for t in tiles:
            try:
                s = rasterio.open("/vsicurl/" + t["href"])
            except Exception:
                continue
            srcs.append(s)
            vrts.append(WarpedVRT(s, crs="EPSG:3857", resampling=Resampling.bilinear))
        if not vrts:
            raise HTTPException(502, "Aucune tuile lisible (COG).")
        # Résolution cible = la plus FINE possible sans (a) dépasser max_side px
        # sur le grand côté, ni (b) suréchantillonner au-delà du natif des COG
        # (inutile + lourd). `vrts[0].res` = résolution native reprojetée en
        # mètres 3857 → sur une petite emprise on atteint le natif (~0.5 m).
        native = min(abs(vrts[0].res[0]), abs(vrts[0].res[1]))
        res = max(span / float(max_side), native)
        if res <= 0:
            raise HTTPException(422, "Résolution cible invalide.")
        mosaic, transform = merge(
            vrts, bounds=(tw3857, ts3857, te3857, tn3857), res=res,
            nodata=0, resampling=Resampling.bilinear,
            indexes=[1, 2, 3] if vrts[0].count >= 3 else None,
        )
        native_res = native
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Mosaïque impossible : {e}")
    finally:
        for v in vrts:
            try: v.close()
            except Exception: pass
        for s in srcs:
            try: s.close()
            except Exception: pass

    arr = mosaic
    if arr.dtype != np.uint8:
        arr = np.clip(arr, 0, 255).astype(np.uint8)
    nb = arr.shape[0]
    oh, ow = arr.shape[1], arr.shape[2]
    rgba = np.zeros((oh, ow, 4), np.uint8)
    for b in range(min(nb, 3)):
        rgba[..., b] = arr[b]
    if nb == 1:
        rgba[..., 1] = rgba[..., 2] = rgba[..., 0]
    # nodata = pixels noirs (bordures / hors-emprise) → transparent
    opaque = (rgba[..., 0].astype(np.uint16) + rgba[..., 1] + rgba[..., 2]) > 0
    rgba[..., 3] = np.where(opaque, 255, 0).astype(np.uint8)

    xmin, ymin, xmax, ymax = array_bounds(oh, ow, transform)
    lons, lats = warp_transform("EPSG:3857", "EPSG:4326",
                                [xmin, xmax, xmax, xmin], [ymax, ymax, ymin, ymin])
    coords = [[lons[0], lats[0]], [lons[1], lats[1]], [lons[2], lats[2]], [lons[3], lats[3]]]
    w4, e4 = min(lons), max(lons)
    s4, n4 = min(lats), max(lats)

    buf = io.BytesIO(); Image.fromarray(rgba, "RGBA").save(buf, "PNG")
    png_b64 = base64.b64encode(buf.getvalue()).decode("ascii")

    # Résolution au sol approx. : `res` est en mètres 3857 (étirés de 1/cos(lat)).
    import math
    lat_c = math.radians((s4 + n4) / 2.0)
    ground_res = float(res) * math.cos(lat_c)
    at_native = res <= native_res * 1.001   # a-t-on atteint le natif ?

    start = (((acq.get("extent") or {}).get("temporal") or {}).get("interval") or [[None]])[0][0]
    date = (start or "")[:10]
    return {
        "name": f"Maxar {_pretty(event)} · {date}" if date else f"Maxar {_pretty(event)}",
        "datetime": start,
        "date": date,
        "bbox": [float(w4), float(s4), float(e4), float(n4)],
        "image_coordinates": coords,
        "png_b64": png_b64,
        "width": int(ow), "height": int(oh),
        "ground_res_m": round(ground_res, 2),
        "at_native": bool(at_native),
        "n_tiles": len(tiles),
        "n_available": n_available,
        "truncated": truncated,
        "license": "CC BY-NC 4.0 · © Maxar Open Data",
    }
