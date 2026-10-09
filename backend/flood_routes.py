"""
flood_routes.py — Module « Simulation d'inondation » (pluvial 2D).

POST /api/flood/simulate
  Entrées : emprise (bbox), MNT (GEE SRTM par défaut, ou raster importé via
  raster_token), pluie (mm/h), durée (min), rugosité/infiltration.
  → récupère le MNT, lance le solveur WCA2D (flood_sim.simulate_flood) et renvoie
    les frames de profondeur (PNG overlay), la profondeur max, et les séries KPI
    (surface inondée, profondeur max, volume) pour l'animation côté frontend.

Version simple (décisions : backend numpy · pluvial urbain · MNT GEE/importé ·
profondeur dans le temps + max + surface inondée). Grille bornée (~220 cell/côté)
pour rester rapide en synchrone.
"""
import math
import os

import numpy as np
import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from gee_auth import init_gee, get_ee
from whitebox_local import _fetch_dem_geotiff
from flood_sim import simulate_flood, frames_to_payload, flow_field

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

router = APIRouter(prefix="/flood", tags=["flood-sim"])

# MNT GEE sûrs pour un export GeoTIFF direct (Image, pas ImageCollection).
DEM_ASSETS = {"SRTM_30m": "USGS/SRTMGL1_003"}


class FloodSimReq(BaseModel):
    bbox: list[float]                 # [w,s,e,n] en 4326
    dem_source: str = "gee"           # gee | imported
    dem_asset: str = "SRTM_30m"
    raster_token: str | None = None   # si dem_source == imported
    band: int = 1
    rainfall_mm_h: float = 50.0
    duration_min: float = 60.0
    manning: float = 0.05
    infiltration_mm_h: float = 0.0
    n_frames: int = 12
    max_cells: int = 220
    buildings: bool = True            # brûle les bâtiments OSM comme obstacles (murs)
    flow_arrows: bool = True          # calcule les flèches de sens d'écoulement


def _load_gee_dem(bbox, asset, max_cells):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible.")
    ee = get_ee()
    w, s, e, n = bbox
    latm = math.radians((n + s) / 2.0)
    wm = (e - w) * 111320.0 * max(0.1, math.cos(latm))
    hm = (n - s) * 111320.0
    scale = max(30.0, wm / max_cells, hm / max_cells)
    asset_id = DEM_ASSETS.get(asset, DEM_ASSETS["SRTM_30m"])
    region = ee.Geometry.BBox(w, s, e, n)
    path = _fetch_dem_geotiff(ee, region, scale, asset_id)
    import rasterio
    try:
        with rasterio.open(path) as src:
            arr = src.read(1).astype(float)
            b = src.bounds
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
    arr[arr < -1000] = np.nan
    return arr, [float(b.left), float(b.bottom), float(b.right), float(b.top)]


def _cell_m(arr, bbox):
    w, s, e, n = bbox
    ny, nx = arr.shape
    latm = math.radians((n + s) / 2.0)
    dx = (e - w) / nx * 111320.0 * max(0.1, math.cos(latm))
    dy = (n - s) / ny * 111320.0
    return (dx + dy) / 2.0


def _building_mask(bbox, shape):
    """Masque des bâtiments OSM (Overpass) rasterisé sur la grille (True = bâti)."""
    w, s, e, n = bbox
    ny, nx = shape
    q = f'[out:json][timeout:40];way["building"]({s},{w},{n},{e});out geom;'
    try:
        r = requests.post(OVERPASS_URL, data={"data": q}, timeout=45)
        r.raise_for_status()
        els = r.json().get("elements", [])
    except Exception:
        return None, 0
    from shapely.geometry import Polygon
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds
    polys = []
    for el in els:
        g = el.get("geometry")
        if not g or len(g) < 3:
            continue
        ring = [(p["lon"], p["lat"]) for p in g]
        try:
            poly = Polygon(ring)
            if poly.is_valid and poly.area > 0:
                polys.append(poly)
        except Exception:
            continue
    if not polys:
        return None, 0
    transform = from_bounds(w, s, e, n, nx, ny)
    mask = rasterize([(p, 1) for p in polys], out_shape=(ny, nx), transform=transform,
                     fill=0, all_touched=True, dtype="uint8").astype(bool)
    return mask, len(polys)


def _arrows_geojson(arrows, spd_ref, bbox, shape):
    """Flèches (MultiLineString avec pointe) du sens d'écoulement, colorables par vitesse."""
    w, s, e, n = bbox
    ny, nx = shape
    dlon = (e - w) / nx
    dlat = (n - s) / ny
    feats = []
    for r, c, ux, uy, sp in arrows:
        lon = w + (c + 0.5) * dlon
        lat = n - (r + 0.5) * dlat
        # Déplacement aval (uy = sud+ → latitude décroît).
        vx, vy = ux * 0.9 * dlon, -uy * 0.9 * dlat
        hx, hy = lon + vx, lat + vy
        vn = math.hypot(vx, vy) or 1e-9
        bx, by = vx / vn, vy / vn
        bl = 0.4 * math.hypot(vx, vy)
        lines = [[[lon, lat], [hx, hy]]]
        for ang in (2.6, -2.6):  # ~150° : barbes de la pointe
            rx = math.cos(ang) * bx - math.sin(ang) * by
            ry = math.sin(ang) * bx + math.cos(ang) * by
            lines.append([[hx, hy], [hx + bl * rx, hy + bl * ry]])
        feats.append({"type": "Feature",
                      "geometry": {"type": "MultiLineString", "coordinates": lines},
                      "properties": {"speed": round(sp, 2), "rel": round(min(1.0, sp / max(1e-6, spd_ref)), 3)}})
    return {"type": "FeatureCollection", "features": feats}


@router.post("/simulate")
def simulate(req: FloodSimReq):
    """Simulation pluviale 2D → frames de profondeur + KPI."""
    if not req.bbox or len(req.bbox) != 4:
        raise HTTPException(422, "Emprise (bbox) requise.")

    if req.dem_source == "imported" and req.raster_token:
        from raster_routes import _load_band
        band, meta = _load_band(req.raster_token, req.band)
        arr = np.asarray(band, dtype=float)
        bbox = meta.get("bbox") or req.bbox
    else:
        arr, bbox = _load_gee_dem(req.bbox, req.dem_asset, req.max_cells)

    if arr is None or arr.size == 0:
        raise HTTPException(422, "MNT indisponible sur cette emprise.")

    # Borne la grille (vitesse en synchrone).
    ny, nx = arr.shape
    if max(ny, nx) > req.max_cells:
        step = int(np.ceil(max(ny, nx) / req.max_cells))
        arr = arr[::step, ::step]

    cell = _cell_m(arr, bbox)
    if not np.isfinite(arr).any():
        raise HTTPException(422, "MNT vide (que des nodata) sur cette emprise.")

    # Bâtiments OSM → murs (canalisent l'eau dans les rues).
    base_mask = ~np.isfinite(arr)
    n_buildings = 0
    if req.buildings:
        bmask, n_buildings = _building_mask(bbox, arr.shape)
        if bmask is not None:
            base_mask = base_mask | bmask

    sim = simulate_flood(
        arr, cell, req.rainfall_mm_h, req.duration_min,
        manning=req.manning, infil_mm_h=req.infiltration_mm_h, n_frames=req.n_frames,
        nodata_mask=base_mask,
    )
    payload = frames_to_payload(sim, bbox)
    payload["cell_m"] = round(cell, 1)
    payload["grid"] = [int(arr.shape[0]), int(arr.shape[1])]
    payload["buildings"] = n_buildings
    payload["params"] = {"rainfall_mm_h": req.rainfall_mm_h, "duration_min": req.duration_min,
                         "manning": req.manning, "infiltration_mm_h": req.infiltration_mm_h}

    # Sens de l'écoulement (flèches) sur l'enveloppe de profondeur max.
    if req.flow_arrows:
        arrows, spd_ref = flow_field(np.where(np.isfinite(arr), arr, 0.0), sim["max_depth"],
                                     cell, req.manning, wall=base_mask, step=6)
        payload["flow_geojson"] = _arrows_geojson(arrows, spd_ref, bbox, arr.shape)
        payload["spd_ref"] = round(spd_ref, 2)
    return payload
