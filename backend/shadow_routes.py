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
import os
import json
import math
import base64
import urllib.request
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/shadow", tags=["shadow"])

# Jetons de routage (backend) — l'itinéraire ombragé du front passe par ici pour
# NE PAS dépendre du build frontend (VITE_MAPBOX_TOKEN souvent absent au build).
_ORS_KEY = os.getenv("ORS_API_KEY", "")
_ORS_BASE = os.getenv("ORS_BASE_URL", "https://api.openrouteservice.org")
_MAPBOX = os.getenv("MAPBOX_ACCESS_TOKEN") or os.getenv("VITE_MAPBOX_TOKEN") or ""


class RouteReq(BaseModel):
    a: List[float]           # [lon, lat] départ
    b: List[float]           # [lon, lat] arrivée
    via: Optional[List[float]] = None  # [lon, lat] point de passage (force un tracé distinct)
    profile: str = "foot"    # (piéton uniquement pour l'ombrage)


def _ors_routes(a, b, via=None):
    url = f"{_ORS_BASE}/v2/directions/foot-walking/geojson"
    coords = [[float(a[0]), float(a[1])]]
    if via:
        coords.append([float(via[0]), float(via[1])])
    coords.append([float(b[0]), float(b[1])])
    body = {"coordinates": coords, "instructions": False}
    if not via:   # les alternatives ORS ne se combinent pas avec un point de passage
        body["alternative_routes"] = {"target_count": 3, "weight_factor": 1.6, "share_factor": 0.6}
    rq = urllib.request.Request(url, data=json.dumps(body).encode(),
                                headers={"Authorization": _ORS_KEY, "Content-Type": "application/json"})
    with urllib.request.urlopen(rq, timeout=20) as r:
        d = json.load(r)
    out = []
    for f in d.get("features", []):
        coords = (f.get("geometry") or {}).get("coordinates") or []
        summ = (f.get("properties") or {}).get("summary") or {}
        if coords:
            out.append({"coordinates": coords, "distance": summ.get("distance", 0), "duration": summ.get("duration", 0)})
    return out


def _mapbox_routes(a, b, via=None):
    pts = f"{a[0]},{a[1]};" + (f"{via[0]},{via[1]};" if via else "") + f"{b[0]},{b[1]}"
    alt = "false" if via else "true"
    url = (f"https://api.mapbox.com/directions/v5/mapbox/walking/"
           f"{pts}?alternatives={alt}&geometries=geojson&overview=full&access_token={_MAPBOX}")
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    out = []
    for rt in d.get("routes", []):
        coords = (rt.get("geometry") or {}).get("coordinates") or []
        if coords:
            out.append({"coordinates": coords, "distance": rt.get("distance", 0), "duration": rt.get("duration", 0)})
    return out


class LoopReq(BaseModel):
    point: List[float]          # [lon, lat] départ = arrivée (boucle)
    length_m: float = 2000      # longueur cible de la boucle (m)
    count: int = 6              # nombre de boucles candidates à générer


def _ors_loop(point, length_m, seed, points=5):
    url = f"{_ORS_BASE}/v2/directions/foot-walking/geojson"
    body = {
        "coordinates": [[float(point[0]), float(point[1])]],
        "options": {"round_trip": {"length": float(length_m), "points": int(points), "seed": int(seed)}},
        "instructions": False,
    }
    rq = urllib.request.Request(url, data=json.dumps(body).encode(),
                                headers={"Authorization": _ORS_KEY, "Content-Type": "application/json"})
    with urllib.request.urlopen(rq, timeout=25) as r:
        d = json.load(r)
    out = []
    for f in d.get("features", []):
        coords = (f.get("geometry") or {}).get("coordinates") or []
        summ = (f.get("properties") or {}).get("summary") or {}
        if coords:
            out.append({"coordinates": coords, "distance": summ.get("distance", 0), "duration": summ.get("duration", 0)})
    return out


def _mapbox_loop(point, length_m, bearing_deg, points=4):
    """Repli sans round-trip natif : boucle par points de passage sur un cercle."""
    R = max(80.0, length_m / (2 * math.pi))
    lat0 = float(point[1])
    mlat = 111320.0
    mlng = 111320.0 * math.cos(lat0 * math.pi / 180.0) or 1.0
    wps = []
    for k in range(points):
        a = (bearing_deg + k * (360.0 / points)) * math.pi / 180.0
        dx = R * math.sin(a)
        dy = R * math.cos(a)
        wps.append([float(point[0]) + dx / mlng, lat0 + dy / mlat])
    pts = f"{point[0]},{point[1]};" + ";".join(f"{w[0]},{w[1]}" for w in wps) + f";{point[0]},{point[1]}"
    url = (f"https://api.mapbox.com/directions/v5/mapbox/walking/"
           f"{pts}?alternatives=false&geometries=geojson&overview=full&access_token={_MAPBOX}")
    with urllib.request.urlopen(url, timeout=25) as r:
        d = json.load(r)
    out = []
    for rt in d.get("routes", []):
        coords = (rt.get("geometry") or {}).get("coordinates") or []
        if coords:
            out.append({"coordinates": coords, "distance": rt.get("distance", 0), "duration": rt.get("duration", 0)})
    return out


@router.post("/loop")
def shadow_loop(req: LoopReq):
    """Boucles piétonnes (balades) d'une longueur cible depuis un point. ORS round-trip
    (plusieurs seeds) sinon repli Mapbox par points de passage. Le front les note à l'ombre."""
    if not req.point or len(req.point) != 2:
        raise HTTPException(422, "Point invalide (attendu [lon, lat]).")
    L = max(300.0, min(float(req.length_m or 2000), 20000.0))
    n = max(2, min(int(req.count or 6), 8))
    loops, errors = [], []
    if _ORS_KEY:
        for seed in range(n):
            try:
                loops.extend(_ors_loop(req.point, L, seed))
            except Exception as e:
                errors.append(f"ORS s{seed}: {e}")
    if not loops and _MAPBOX:
        for k in range(n):
            try:
                loops.extend(_mapbox_loop(req.point, L, k * (360.0 / n)))
            except Exception as e:
                errors.append(f"Mapbox {k}: {e}")
    if not loops:
        detail = "Boucles indisponibles : aucun jeton (ORS_API_KEY / MAPBOX_ACCESS_TOKEN)."
        if errors:
            detail = "Génération de boucles en échec — " + " ; ".join(errors[:3])
        raise HTTPException(502, detail)
    return {"routes": loops, "provider": "ors" if _ORS_KEY and loops else "mapbox"}


@router.post("/route")
def shadow_route(req: RouteReq):
    """Itinéraires piétons A→B (avec alternatives) via le backend (ORS ou Mapbox).
    Le front les note ensuite par l'ombre. Le jeton vient de l'env backend."""
    if not req.a or not req.b or len(req.a) != 2 or len(req.b) != 2:
        raise HTTPException(422, "Points a/b invalides (attendu [lon, lat]).")
    routes, errors = [], []
    via = req.via if (req.via and len(req.via) == 2) else None
    if _ORS_KEY:
        try:
            routes = _ors_routes(req.a, req.b, via)
        except Exception as e:
            errors.append(f"ORS: {e}")
    if not routes and _MAPBOX:
        try:
            routes = _mapbox_routes(req.a, req.b, via)
        except Exception as e:
            errors.append(f"Mapbox: {e}")
    if not routes:
        detail = "Routage indisponible : aucun jeton (ORS_API_KEY / MAPBOX_ACCESS_TOKEN) dans l'env backend."
        if errors:
            detail = "Routage en échec — " + " ; ".join(errors)
        raise HTTPException(502, detail)
    return {"routes": routes, "provider": "ors" if _ORS_KEY and routes else "mapbox"}

_META_ASSET = "projects/meta-forest-monitoring-okw37/assets/CanopyHeight"     # ~1 m
_ETH_ASSET = "users/nlang/ETH_GlobalCanopyHeight_2020_10m_v1"                 # 10 m (fallback)
_MAX_AREA_DEG2 = 0.20          # garde-fou : bbox trop grande → demander de zoomer
_GREENS = ["#ffffcc", "#d9f0a3", "#addd8e", "#78c679", "#41ab5d", "#238443", "#005a32"]   # rampe hauteur YlGn (dégradé fin, clair=bas → foncé=haut)


class CanopyReq(BaseModel):
    bbox: List[float]                 # [ouest, sud, est, nord] WGS84
    min_height: float = 3.0           # ignore la végétation basse (< 3 m)
    dimensions: int = 1024            # côté max de l'aperçu PNG
    geometry: Optional[dict] = None   # GeoJSON (clip exact à l'emprise importée)


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
    # Clip exact à l'emprise importée (GeoJSON) → transparent hors de la zone.
    if req.geometry:
        try:
            veg = veg.clip(ee.Geometry(req.geometry))
        except Exception:
            pass
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


class PatchReq(BaseModel):
    bbox: List[float]                 # [ouest, sud, est, nord] WGS84
    min_height: float = 2.0           # canopée ≥ 2 m (tronc/houppier)
    scale: int = 4                    # résolution de vectorisation (m)
    max_features: int = 1500
    geometry: Optional[dict] = None   # clip exact (emprise importée)


@router.post("/canopy_patches")
def canopy_patches(req: PatchReq):
    """Canopée ≥ min_height vectorisée en POLYGONES par tache (composantes
    connexes) avec leur hauteur → pour un rendu 3D fill-extrusion (natif, sans
    GPU dédié) épousant l'emprise exacte."""
    if not req.bbox or len(req.bbox) != 4:
        raise HTTPException(422, "bbox invalide.")
    w, s, e, n = (float(x) for x in req.bbox)
    if not (e > w and n > s):
        raise HTTPException(422, "bbox vide.")
    if (e - w) * (n - s) > _MAX_AREA_DEG2:
        raise HTTPException(422, "Emprise trop grande — zoomez.")

    try:
        from gee_auth import get_ee
        ee = get_ee()
    except Exception as ex:
        raise HTTPException(503, f"Earth Engine indisponible : {ex}")

    rect = ee.Geometry.Rectangle([w, s, e, n])
    minh = float(req.min_height)
    scale = max(2, min(int(req.scale or 4), 20))

    height = None
    label = None
    try:
        img = ee.ImageCollection(_META_ASSET).mosaic().select(0)
        img.bandNames().getInfo()
        height = img.rename("h"); label = "WRI/Meta ~1 m"
    except Exception:
        try:
            img = ee.Image(_ETH_ASSET).select(0)
            img.bandNames().getInfo()
            height = img.rename("h"); label = "ETH 10 m"
        except Exception as ex2:
            raise HTTPException(502, f"Aucun dataset canopée : {ex2}")

    if req.geometry:
        try:
            height = height.clip(ee.Geometry(req.geometry))
        except Exception:
            pass

    # Lissage morphologique (fermeture + moyenne) → contours arrondis, moins « pixel ».
    mask = height.gte(minh)
    sm = mask.focalMax(1, "circle", "pixels").focalMin(1, "circle", "pixels")
    sm = sm.focalMean(1.5, "circle", "pixels").gte(0.5)
    labeled = sm.selfMask().rename("lbl").addBands(height)
    try:
        vectors = labeled.reduceToVectors(
            reducer=ee.Reducer.mean(), geometry=rect, scale=scale,
            geometryType="polygon", labelProperty="lbl", eightConnected=True,
            maxPixels=int(1e10), bestEffort=True,
        ).limit(min(int(req.max_features or 1500), 6000))
        # simplification LÉGÈRE → garde la forme réelle de la canopée (épouse l'emprise exacte)
        vectors = vectors.map(lambda f: f.simplify(maxError=float(scale) * 1.2))
        gj = vectors.getInfo()
    except Exception as ex:
        raise HTTPException(502, f"Vectorisation canopée impossible : {ex}")

    feats = []
    for f in gj.get("features", []):
        p = f.get("properties", {}) or {}
        h = p.get("mean")
        if h is None:
            h = p.get("h") if p.get("h") is not None else p.get("h_mean")
        g = f.get("geometry")
        if h is None or not g:
            continue
        h = float(h)
        if h < minh:
            continue
        feats.append({"type": "Feature", "properties": {"height": round(h, 1)}, "geometry": g})

    return {"type": "FeatureCollection", "features": feats, "count": len(feats),
            "min_height": minh, "dataset": label}
