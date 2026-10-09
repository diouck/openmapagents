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
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from gee_auth import init_gee, get_ee
from whitebox_local import _fetch_dem_geotiff
from flood_sim import simulate_flood, frames_to_payload

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

    sim = simulate_flood(
        arr, cell, req.rainfall_mm_h, req.duration_min,
        manning=req.manning, infil_mm_h=req.infiltration_mm_h, n_frames=req.n_frames,
    )
    payload = frames_to_payload(sim, bbox)
    payload["cell_m"] = round(cell, 1)
    payload["grid"] = [int(arr.shape[0]), int(arr.shape[1])]
    payload["params"] = {"rainfall_mm_h": req.rainfall_mm_h, "duration_min": req.duration_min,
                         "manning": req.manning, "infiltration_mm_h": req.infiltration_mm_h}
    return payload
