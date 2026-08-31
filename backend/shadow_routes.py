"""
shadow_routes.py — Support serveur de l'outil « Ombres portées ».

L'ombrage bâtiments est 100 % client-side (bâtiments des tuiles MapLibre). Pour
la CANOPÉE, en revanche, on a besoin de vraies hauteurs d'arbres : le modèle
**WRI/Meta High Resolution Canopy Height 2020 (~1 m)** (fallback ETH Lang 10 m),
servi par Google Earth Engine (les tuiles brutes AWS sont des GeoTIFF 65536²
non-COG, illisibles à la volée).

POST /api/shadow/canopy {bbox, scale?, min_height?, max_features?}
  → charge la canopée Meta, la seuille (≥ min_height), la vectorise en polygones
    (reduceToVectors) avec la HAUTEUR MOYENNE par polygone, et renvoie un GeoJSON
    WGS84 (propriété `height` en m). Le front projette ces polygones en ombre
    comme les bâtiments, mais avec leur vraie hauteur.

Réutilise l'auth GEE existante (gee_auth.get_ee) et le même asset Meta que
/api/gee/canopy.
"""
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/shadow", tags=["shadow"])

_META_ASSET = "projects/meta-forest-monitoring-okw37/assets/CanopyHeight"     # ~1 m
_ETH_ASSET = "users/nlang/ETH_GlobalCanopyHeight_2020_10m_v1"                 # 10 m (fallback)
_MAX_AREA_DEG2 = 0.25          # garde-fou : bbox trop grande → demander de zoomer


class CanopyReq(BaseModel):
    bbox: List[float]                 # [ouest, sud, est, nord] WGS84
    scale: int = 10                   # résolution de vectorisation (m) — 10 = bon compromis
    min_height: float = 3.0           # ignore la végétation basse (< 3 m)
    max_features: int = 1500          # borne de sortie


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

    minh = float(req.min_height)
    scale = max(5, min(int(req.scale or 10), 60))

    # ── Seuillage + vectorisation avec hauteur moyenne par polygone ──────────
    # bande 'lbl' (0/1) = étiquette des régions ; bande 'h' = hauteur → réduite
    # en moyenne par polygone. On ne garde que lbl==1 (canopée ≥ min_height).
    mask = height.gte(minh).rename("lbl")
    labeled = mask.addBands(height)
    try:
        vectors = labeled.reduceToVectors(
            reducer=ee.Reducer.mean(),
            geometry=rect,
            scale=scale,
            geometryType="polygon",
            labelProperty="lbl",
            eightConnected=False,
            maxPixels=1e10,
            bestEffort=True,
        ).filter(ee.Filter.eq("lbl", 1)).limit(int(req.max_features))
        gj = vectors.getInfo()
    except Exception as ex:
        raise HTTPException(502, f"Vectorisation canopée impossible : {ex}")

    feats = []
    for f in gj.get("features", []):
        p = f.get("properties", {}) or {}
        h = p.get("mean")
        if h is None:
            h = p.get("h") if p.get("h") is not None else p.get("h_mean")
        if h is None:
            continue
        h = float(h)
        if h < minh:
            continue
        g = f.get("geometry")
        if not g:
            continue
        feats.append({"type": "Feature", "properties": {"height": round(h, 1)}, "geometry": g})

    return {
        "type": "FeatureCollection",
        "features": feats,
        "count": len(feats),
        "dataset": label,
        "scale_m": scale,
    }
