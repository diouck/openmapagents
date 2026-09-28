"""
whitebox_local.py — Outils raster calculés EN LOCAL (hors GEE).

Certains traitements hydrologiques (routage D8, percement de dépressions, ordre
des cours d'eau, bassin versant) ne sont pas calculables dans Earth Engine. Ils
sont exécutés ici avec **pysheds** sur un MNT :
  • soit récupéré depuis GEE (SRTM) pour la ROI ;
  • soit fourni par l'appelant (MNT importé) via `params["dem_tif_path"]`.

Réponse = overlay image PNG (`png_b64` + `image_coordinates`), pas des tuiles :
le frontend l'ajoute comme couche `kind:"image"` (cf. addRasterLayer).
"""
import io
import os
import base64
import tempfile

import numpy as np
import requests
import rasterio
from fastapi import HTTPException

LOCAL_TOOLS = {"breach_depressions", "stream_order", "watershed"}

_TERRAIN = ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]
LOCAL_VIS = {
    "breach_depressions": {"min": 0, "max": 3000, "palette": _TERRAIN},
    "stream_order":       {"min": 1, "max": 7,    "palette": ["#f7fbff", "#c6dbef", "#6baed6", "#2171b5", "#08306b"]},
    "watershed":          {"min": 0, "max": 1,    "palette": ["#ffffff", "#1D9E75"]},
}


def _hex(h):
    h = h.lstrip("#")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def _colormap(arr, vis, mask=None):
    """arr float2D → RGBA uint8 par palette linéaire ; NaN et `mask` → transparent."""
    pal = np.array([_hex(c) for c in vis["palette"]], dtype=float)
    mn, mx = float(vis["min"]), float(vis["max"])
    a = np.clip((arr - mn) / max(1e-9, (mx - mn)), 0, 1)
    n = len(pal) - 1
    idx = a * n
    lo = np.clip(np.floor(idx).astype(int), 0, n - 1)
    hi = np.clip(lo + 1, 0, n)
    frac = (idx - lo)[..., None]
    rgb = pal[lo] * (1 - frac) + pal[hi] * frac
    rgba = np.dstack([rgb.astype(np.uint8), np.full(arr.shape, 255, np.uint8)])
    transparent = ~np.isfinite(arr)
    if mask is not None:
        transparent = transparent | mask
    rgba[..., 3] = np.where(transparent, 0, 255).astype(np.uint8)
    return rgba


def _png_b64(rgba):
    from PIL import Image
    img = Image.fromarray(rgba, "RGBA")
    buf = io.BytesIO()
    img.save(buf, "PNG")
    return base64.b64encode(buf.getvalue()).decode()


def _fetch_dem_geotiff(ee, region, scale, dem_asset):
    """Télécharge le MNT clipé sur la ROI depuis GEE en GeoTIFF (fichier temporaire)."""
    dem = ee.Image(dem_asset).select(0)
    url = dem.clip(region).getDownloadURL({
        "region": region, "scale": int(scale), "format": "GEO_TIFF", "crs": "EPSG:4326",
    })
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    fd, path = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with open(path, "wb") as f:
        f.write(r.content)
    return path


def run_local(ee, tool, region, params, dem_asset):
    """Exécute un outil hydrologique local et renvoie un overlay image."""
    try:
        from pysheds.grid import Grid
    except Exception as e:
        raise HTTPException(503, f"Moteur local indisponible (pysheds non installé) : {e}")

    if region is None:
        raise HTTPException(422, "Une emprise (ROI) est requise pour les outils locaux — choisissez « Vue carte » ou une couche.")

    scale = int(params.get("scale", 90) or 90)
    imported = params.get("dem_tif_path")           # MNT importé (chemin serveur) éventuel
    tif = imported or _fetch_dem_geotiff(ee, region, scale, dem_asset)

    try:
        grid = Grid.from_raster(tif)
        dem = grid.read_raster(tif)
        with rasterio.open(tif) as src:
            b = src.bounds                          # left, bottom, right, top (WGS84)

        pit = grid.fill_pits(dem)
        flooded = grid.fill_depressions(pit)
        inflated = grid.resolve_flats(flooded)
        fdir = grid.flowdir(inflated)
        acc = grid.accumulation(fdir)
        thr = float(params.get("threshold", 1000) or 1000)

        if tool == "breach_depressions":
            arr = np.asarray(flooded, dtype=float)
            vis = LOCAL_VIS[tool]
            mask = None

        elif tool == "stream_order":
            accv = np.asarray(acc, dtype=float)
            streams = accv >= thr
            # Hiérarchie proxy (log10 de l'accumulation, 1..7) sur le réseau.
            order = np.where(streams, np.clip(np.floor(np.log10(np.maximum(accv, 1))), 1, 7), np.nan)
            arr, vis, mask = order, LOCAL_VIS[tool], ~streams

        elif tool == "watershed":
            accv = np.asarray(acc, dtype=float)
            iy, ix = np.unravel_index(np.nanargmax(accv), accv.shape)
            x = b.left + (ix + 0.5) * (b.right - b.left) / accv.shape[1]
            y = b.top - (iy + 0.5) * (b.top - b.bottom) / accv.shape[0]
            try:
                x, y = grid.snap_to_mask(accv >= thr, (x, y))
            except Exception:
                pass
            catch = grid.catchment(x=x, y=y, fdir=fdir, xytype="coordinate")
            ca = np.asarray(catch, dtype=float)
            inside = ca > 0
            arr, vis, mask = np.where(inside, 1.0, np.nan), LOCAL_VIS[tool], ~inside

        else:
            raise HTTPException(422, f"Outil local inconnu : {tool}")

        rgba = _colormap(arr, vis, mask)
        png = _png_b64(rgba)
        coords = [[b.left, b.top], [b.right, b.top], [b.right, b.bottom], [b.left, b.bottom]]
        return {
            "status": "ok", "tool": tool, "engine": "local",
            "png_b64": png, "image_coordinates": coords,
            "bbox": [b.left, b.bottom, b.right, b.top], "vis_params": vis,
        }
    finally:
        if not imported:
            try: os.remove(tif)
            except Exception: pass
