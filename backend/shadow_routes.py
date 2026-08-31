"""
shadow_routes.py — Support serveur de l'outil « Ombres portées ».

L'ombrage bâtiments est 100 % client-side (bâtiments des tuiles MapLibre). Pour
la CANOPÉE, on utilise le modèle **WRI/Meta High Resolution Canopy Height 2020
(~1 m)** (fallback ETH Lang 10 m), servi par Google Earth Engine.

POST /api/shadow/canopy {bbox, min_height?}
  → renvoie un APERÇU RASTER PNG de la canopée (vraie emprise, lissé — pas de
    polygones « carrés »), colorisé en vert par la hauteur, transparent hors
    canopée, + la HAUTEUR MOYENNE (pour décaler l'ombre côté client). Le front
    pose l'image comme overlay (vraie emprise) et en décale une copie sombre
    (l'ombre) selon la position du soleil.

Réutilise l'auth GEE existante (gee_auth.get_ee) et le même asset Meta que
/api/gee/canopy.
"""
import io
import math
import base64
import urllib.request
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/shadow", tags=["shadow"])

_META_ASSET = "projects/meta-forest-monitoring-okw37/assets/CanopyHeight"     # ~1 m
_ETH_ASSET = "users/nlang/ETH_GlobalCanopyHeight_2020_10m_v1"                 # 10 m (fallback)
_MAX_AREA_DEG2 = 0.20          # garde-fou : bbox trop grande → demander de zoomer
_GREENS = ["#c2e699", "#78c679", "#31a354", "#006837"]   # rampe hauteur (clair→foncé)


class CanopyReq(BaseModel):
    bbox: List[float]                 # [ouest, sud, est, nord] WGS84
    min_height: float = 3.0           # ignore la végétation basse (< 3 m)
    dimensions: int = 1024            # côté max de l'aperçu PNG


@router.post("/canopy")
def shadow_canopy(req: CanopyReq):
    if not req.bbox or len(req.bbox) != 4:
        raise HTTPException(422, "bbox invalide (attendu [ouest, sud, est, nord]).")
    w, s, e, n = (float(x) for x in req.bbox)
    if not (e > w and n > s):
        raise HTTPException(422, "bbox vide.")
    if (e - w) * (n - s) > _MAX_AREA_DEG2:
        raise HTTPException(422, "Emprise trop grande pour la canopée — zoomez davantage.")

    try:
        from gee_auth import get_ee
        ee = get_ee()
    except Exception as ex:
        raise HTTPException(503, f"Earth Engine indisponible : {ex}")

    rect = ee.Geometry.Rectangle([w, s, e, n])
    minh = float(req.min_height)

    # ── Dataset : Meta ~1 m, fallback ETH 10 m ────────────────────────────────
    height = None
    label = None
    try:
        img = ee.ImageCollection(_META_ASSET).mosaic().select(0)
        img.bandNames().getInfo()          # lève si l'asset est inaccessible
        height = img.rename("h")
        label = "WRI/Meta High Resolution Canopy Height 2020 (~1 m)"
    except Exception:
        try:
            img = ee.Image(_ETH_ASSET).select(0)
            img.bandNames().getInfo()
            height = img.rename("h")
            label = "ETH GlobalCanopyHeight 2020 (Lang et al., 10 m)"
        except Exception as ex2:
            raise HTTPException(502, f"Aucun dataset canopée disponible : {ex2}")

    # Canopée = hauteur ≥ min_height ; masquée ailleurs → PNG transparent hors arbres.
    veg = height.updateMask(height.gte(minh))
    vis = veg.visualize(min=minh, max=25, palette=_GREENS)
    dims = max(256, min(int(req.dimensions or 1024), 2048))

    # ── Aperçu PNG (vraie emprise, lissé) via getThumbURL ────────────────────
    try:
        url = vis.getThumbURL({"region": rect, "dimensions": dims, "format": "png"})
    except Exception as ex:
        raise HTTPException(502, f"Rendu canopée impossible : {ex}")
    try:
        rq = urllib.request.Request(url, headers={"User-Agent": "OpenMapAgents/1.0"})
        with urllib.request.urlopen(rq, timeout=45) as r:
            png = r.read()
    except Exception as ex:
        raise HTTPException(502, f"Téléchargement aperçu canopée impossible : {ex}")
    b64 = base64.b64encode(png).decode("ascii")

    # ── Hauteur moyenne + AIRE de canopée (pour l'ombre + les stats) ─────────
    mean_h = None
    area_m2 = None
    try:
        stat = veg.reduceRegion(
            reducer=ee.Reducer.mean(), geometry=rect,
            scale=10, maxPixels=int(1e9), bestEffort=True,
        ).get("h").getInfo()
        if stat is not None:
            mean_h = round(float(stat), 1)
    except Exception:
        mean_h = None
    try:
        a = ee.Image.pixelArea().updateMask(height.gte(minh)).reduceRegion(
            reducer=ee.Reducer.sum(), geometry=rect,
            scale=10, maxPixels=int(1e9), bestEffort=True,
        ).get("area").getInfo()
        if a is not None:
            area_m2 = round(float(a), 1)
    except Exception:
        area_m2 = None

    # Coins de l'image (l'aperçu couvre exactement la bbox) : TL, TR, BR, BL.
    coords = [[w, n], [e, n], [e, s], [w, s]]
    return {
        "canopy_b64": b64,
        "image_coordinates": coords,
        "bbox": [w, s, e, n],
        "mean_height": mean_h,
        "canopy_area_m2": area_m2,
        "min_height": minh,
        "dataset": label,
    }
