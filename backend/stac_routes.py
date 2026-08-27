"""
stac_routes.py — Navigateur STAC + COG : chercher des scènes satellite et les
ajouter à la carte comme overlay (lecture Cloud-Optimized GeoTIFF à la volée).

POST /api/stac/search  {collection, bbox, date_from, date_to, cloud_max, limit}
    → interroge l'API STAC Earth Search (Element84, public, sans clé) et renvoie
      une liste d'items simplifiés (id, date, nuages, bbox, vignette, href COG).
POST /api/stac/scene   {href, name}
    → lit le COG « visual » (RVB) via /vsicurl (overviews → aperçu downsamplé),
      reprojette en EPSG:3857 et renvoie un PNG géoréférencé + 4 coins lon/lat.
      L'overlay se branche sur addImageLayer, comme un GeoTIFF importé.

Sécurité (app publique) :
  • L'endpoint STAC interrogé est FIXE (constante) → pas de SSRF côté recherche.
  • /scene n'accepte que des href https vers des hôtes d'assets whitelistés
    (bucket public sentinel-cogs) → pas de lecture d'URL arbitraire via GDAL.

Déps : rasterio numpy Pillow (déjà requises par raster_routes).
"""
import io
import json
import base64
import urllib.request
import urllib.error
from typing import Optional, List
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# GDAL : lecture efficace des COG distants (overviews via HTTP range).
import os
os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
os.environ.setdefault("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif,.TIF,.tiff")
os.environ.setdefault("GDAL_HTTP_TIMEOUT", "30")
os.environ.setdefault("VSI_CACHE", "TRUE")

router = APIRouter(prefix="/api/stac", tags=["stac"])

_STAC_URL = "https://earth-search.aws.element84.com/v1/search"
_MAX_SIDE = 1024

# Collections proposées (Earth Search v1). `visual` = présence d'un COG RVB TCI.
COLLECTIONS = {
    "sentinel-2-l2a": {"label": "Sentinel-2 L2A (surface)", "visual": True},
    "sentinel-2-l1c": {"label": "Sentinel-2 L1C (TOA)", "visual": True},
    "landsat-c2-l2": {"label": "Landsat C2 L2", "visual": False},
    "sentinel-2-c1-l2a": {"label": "Sentinel-2 C1 L2A", "visual": True},
}

# Hôtes d'assets autorisés pour la lecture COG (anti-SSRF).
_ALLOWED_HOSTS = {
    "sentinel-cogs.s3.us-west-2.amazonaws.com",
    "sentinel-cogs.s3.amazonaws.com",
}


class StacSearchReq(BaseModel):
    collection: str = "sentinel-2-l2a"
    bbox: List[float]                      # [ouest, sud, est, nord]
    date_from: Optional[str] = None        # AAAA-MM-JJ
    date_to: Optional[str] = None
    cloud_max: Optional[float] = None      # % couverture nuageuse max
    limit: int = 12


class StacSceneReq(BaseModel):
    href: str
    name: Optional[str] = None


@router.get("/collections")
def stac_collections():
    return {"collections": [{"id": k, **v} for k, v in COLLECTIONS.items()]}


@router.post("/search")
def stac_search(req: StacSearchReq):
    if req.collection not in COLLECTIONS:
        raise HTTPException(400, "Collection inconnue.")
    if not req.bbox or len(req.bbox) != 4:
        raise HTTPException(422, "bbox invalide (attendu [ouest, sud, est, nord]).")

    body = {"collections": [req.collection], "bbox": [float(x) for x in req.bbox],
            "limit": max(1, min(int(req.limit or 12), 50))}
    if req.date_from or req.date_to:
        a = f"{req.date_from}T00:00:00Z" if req.date_from else ".."
        b = f"{req.date_to}T23:59:59Z" if req.date_to else ".."
        body["datetime"] = f"{a}/{b}"
    if req.cloud_max is not None:
        body["query"] = {"eo:cloud_cover": {"lt": float(req.cloud_max)}}
        body["sortby"] = [{"field": "properties.eo:cloud_cover", "direction": "asc"}]

    data = json.dumps(body).encode()
    rq = urllib.request.Request(_STAC_URL, data=data,
                                headers={"Content-Type": "application/json",
                                         "Accept": "application/json",
                                         "User-Agent": "OpenMapAgents/1.0"})
    try:
        with urllib.request.urlopen(rq, timeout=25) as r:
            res = json.load(r)
    except urllib.error.HTTPError as e:
        raise HTTPException(502, f"STAC {e.code} : {e.reason}")
    except Exception as e:
        raise HTTPException(502, f"STAC injoignable : {e}")

    items = []
    for it in res.get("features", []):
        props = it.get("properties") or {}
        assets = it.get("assets") or {}
        def _href(*keys):
            for k in keys:
                a = assets.get(k)
                if a and a.get("href"):
                    return a["href"]
            return None
        visual = _href("visual", "rendered_preview")
        thumb = _href("thumbnail", "rendered_preview") or visual
        items.append({
            "id": it.get("id"),
            "collection": it.get("collection") or req.collection,
            "datetime": props.get("datetime"),
            "cloud": props.get("eo:cloud_cover"),
            "bbox": it.get("bbox"),
            "thumb": thumb,
            "visual": visual if (visual and urlparse(visual).hostname in _ALLOWED_HOSTS) else None,
        })
    return {"count": len(items), "matched": res.get("numberMatched"), "items": items}


@router.post("/scene")
def stac_scene(req: StacSceneReq):
    try:
        import numpy as np
        import rasterio
        from rasterio.vrt import WarpedVRT
        from rasterio.enums import Resampling
        from rasterio.transform import Affine, array_bounds
        from rasterio.warp import transform as warp_transform
        from PIL import Image
    except ImportError as e:
        raise HTTPException(503, f"Dépendance raster manquante : « {getattr(e, 'name', e)} ».")

    href = (req.href or "").strip()
    u = urlparse(href)
    if u.scheme != "https" or u.hostname not in _ALLOWED_HOSTS:
        raise HTTPException(400, "Source COG non autorisée (hôte hors liste blanche).")

    try:
        with rasterio.open("/vsicurl/" + href) as src:
            with WarpedVRT(src, crs="EPSG:3857", resampling=Resampling.bilinear) as vrt:
                W, H = vrt.width, vrt.height
                if max(W, H) > _MAX_SIDE:
                    sc = _MAX_SIDE / float(max(W, H))
                    ow, oh = max(1, int(W * sc)), max(1, int(H * sc))
                else:
                    ow, oh = W, H
                nb = min(vrt.count, 3)
                arr = vrt.read(indexes=list(range(1, nb + 1)),
                               out_shape=(nb, oh, ow), resampling=Resampling.bilinear)
                transform = vrt.transform * Affine.scale(W / float(ow), H / float(oh))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Lecture COG impossible : {e}")

    if arr.dtype != np.uint8:
        # normalise en 8 bits (p2–p98) si le COG n'est pas déjà un TCI uint8
        out = np.zeros_like(arr, dtype=np.uint8)
        for b in range(arr.shape[0]):
            band = arr[b].astype(np.float32)
            fin = band[np.isfinite(band) & (band != 0)]
            lo, hi = (np.percentile(fin, 2), np.percentile(fin, 98)) if fin.size else (0.0, 1.0)
            if hi <= lo:
                hi = lo + 1.0
            out[b] = (np.clip((band - lo) / (hi - lo), 0, 1) * 255).astype(np.uint8)
        arr = out

    oh, ow = arr.shape[1], arr.shape[2]
    rgba = np.zeros((oh, ow, 4), np.uint8)
    for b in range(min(arr.shape[0], 3)):
        rgba[..., b] = arr[b]
    if arr.shape[0] == 1:
        rgba[..., 1] = rgba[..., 2] = rgba[..., 0]
    # nodata = pixels noirs (bordures de tuile) → transparent
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
    return {
        "name": req.name or "Scène STAC",
        "bands": int(min(arr.shape[0], 3)),
        "bbox": [float(w4), float(s4), float(e4), float(n4)],
        "image_coordinates": coords,
        "png_b64": png_b64,
        "raster_token": None,
    }
