"""
whitebox_local.py — Outils raster calculés EN LOCAL (hors GEE).

Certains traitements hydrologiques (routage D8, remplissage de dépressions, ordre
des cours d'eau, bassin versant) ne sont pas calculables dans Earth Engine. Ils
sont exécutés ici en **numpy + scikit-image** (aucune dépendance lourde) sur un MNT :
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


def _fill_depressions(dem):
    """Remplissage des cuvettes par reconstruction morphologique (priority-flood)."""
    from skimage.morphology import reconstruction
    seed = dem.copy()
    seed[1:-1, 1:-1] = dem.max()
    return reconstruction(seed, dem, method="erosion")


_OFFS = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
_DIST = [1.4142, 1.0, 1.4142, 1.0, 1.0, 1.4142, 1.0, 1.4142]


def _d8(dem):
    """Direction D8 (récepteur de plus forte pente) + accumulation d'écoulement."""
    ny, nx = dem.shape
    flat = dem.ravel()
    n = dem.size
    idxgrid = np.arange(n).reshape(dem.shape)
    receiver = np.arange(n)
    best = np.zeros(n)
    for k, (dy, dx) in enumerate(_OFFS):
        ys = slice(max(0, dy), ny + min(0, dy)); xs = slice(max(0, dx), nx + min(0, dx))
        yt = slice(max(0, -dy), ny + min(0, -dy)); xt = slice(max(0, -dx), nx + min(0, -dx))
        drop = np.full(dem.shape, -np.inf)
        drop[yt, xt] = (dem[yt, xt] - dem[ys, xs]) / _DIST[k]
        nidx = np.full(dem.shape, -1, dtype=np.int64)
        nidx[yt, xt] = idxgrid[ys, xs]
        d = drop.ravel(); ni = nidx.ravel()
        better = (d > best) & (ni >= 0)
        best = np.where(better, d, best)
        receiver = np.where(better, ni, receiver)
    # Accumulation : traiter les cellules de la plus haute à la plus basse.
    acc = np.ones(n)
    for i in np.argsort(-flat):
        r = receiver[i]
        if r != i:
            acc[r] += acc[i]
    return acc.reshape(dem.shape), receiver


def _catchment(receiver, outlet, shape):
    """Bassin versant : cellules dont l'écoulement atteint l'exutoire (remontée amont)."""
    n = receiver.size
    donors = [[] for _ in range(n)]
    for i in range(n):
        r = receiver[i]
        if r != i:
            donors[r].append(i)
    seen = np.zeros(n, dtype=bool)
    stack = [outlet]; seen[outlet] = True
    while stack:
        c = stack.pop()
        for d in donors[c]:
            if not seen[d]:
                seen[d] = True; stack.append(d)
    return seen.reshape(shape)


_VIRIDIS = ["#440154", "#3b528b", "#21918c", "#5ec962", "#fde725"]


def _idw(xs, ys, zs, gx, gy, power=2.0):
    """Interpolation par pondération inverse à la distance (sans dépendance)."""
    GX, GY = np.meshgrid(gx, gy)
    num = np.zeros(GX.shape)
    den = np.zeros(GX.shape)
    for x, y, z in zip(xs, ys, zs):
        d = np.hypot(GX - x, GY - y)
        d = np.where(d < 1e-9, 1e-9, d)
        w = 1.0 / d ** power
        num += w * z
        den += w
    return num / den


def interpolate_surface(points_geojson, field, bbox, resolution=120, method="kriging"):
    """Interpole une couche de points (attribut `field`) en surface raster → overlay image."""
    xs, ys, zs = [], [], []
    for f in (points_geojson or {}).get("features", []):
        g = f.get("geometry") or {}
        if g.get("type") != "Point":
            continue
        try:
            v = float(f.get("properties", {}).get(field))
        except (TypeError, ValueError):
            continue
        if not np.isfinite(v):
            continue
        c = g.get("coordinates") or []
        if len(c) >= 2:
            xs.append(c[0]); ys.append(c[1]); zs.append(v)

    if len(zs) < 3:
        raise HTTPException(422, "Au moins 3 points avec une valeur numérique sont requis pour l'interpolation.")

    xs = np.asarray(xs); ys = np.asarray(ys); zs = np.asarray(zs)
    w, s, e, n = bbox
    res = int(resolution or 120)
    gx = np.linspace(w, e, res)
    gy = np.linspace(n, s, res)                     # haut → bas (image)

    used = method
    if method == "kriging":
        try:
            from pykrige.ok import OrdinaryKriging
            ok = OrdinaryKriging(xs, ys, zs, variogram_model="spherical", enable_plotting=False)
            z, _ = ok.execute("grid", gx, gy)
            grid = np.asarray(z, dtype=float)
        except Exception:
            grid = _idw(xs, ys, zs, gx, gy); used = "idw"  # repli si pykrige absent
    else:
        grid = _idw(xs, ys, zs, gx, gy)

    vis = {"min": float(np.nanmin(grid)), "max": float(np.nanmax(grid)), "palette": _VIRIDIS}
    rgba = _colormap(grid, vis)
    png = _png_b64(rgba)
    coords = [[w, n], [e, n], [e, s], [w, s]]
    return {
        "status": "ok", "engine": "local", "method": used, "field": field,
        "png_b64": png, "image_coordinates": coords,
        "bbox": [w, s, e, n], "vis_params": vis,
    }


def extract_contours(ee, region, params, dem_asset):
    """Courbes de niveau VECTORIELLES (GeoJSON LineStrings) — sortie exportable (QGIS-like)."""
    if region is None:
        raise HTTPException(422, "Une emprise (ROI) est requise pour les courbes de niveau.")
    from skimage import measure
    interval = float(params.get("interval", 100) or 100)
    scale = int(params.get("scale", 90) or 90)
    imported = params.get("dem_tif_path")
    tif = imported or _fetch_dem_geotiff(ee, region, scale, dem_asset)
    try:
        with rasterio.open(tif) as src:
            dem = src.read(1).astype(float)
            b = src.bounds
            nod = src.nodata
        ny, nx = dem.shape
        mask = np.isfinite(dem)
        if nod is not None:
            mask &= (dem != nod)
        vals = dem[mask]
        if vals.size == 0:
            raise HTTPException(422, "MNT vide sur cette emprise.")
        mn, mx = float(np.min(vals)), float(np.max(vals))
        if interval <= 0 or (mx - mn) / interval > 80:      # borne le nombre de niveaux
            interval = max(1.0, (mx - mn) / 40.0)
        levels = np.arange(np.ceil(mn / interval) * interval, mx, interval)
        demf = np.where(mask, dem, mn)

        def to_lonlat(r, c):
            lon = b.left + (c + 0.5) * (b.right - b.left) / nx
            lat = b.top - (r + 0.5) * (b.top - b.bottom) / ny
            return [round(lon, 6), round(lat, 6)]

        feats = []
        for lev in levels:
            for contour in measure.find_contours(demf, float(lev)):
                if len(contour) < 2:
                    continue
                coords = [to_lonlat(r, c) for r, c in contour]
                feats.append({"type": "Feature", "geometry": {"type": "LineString", "coordinates": coords}, "properties": {"elevation": round(float(lev), 1)}})
        return {
            "status": "ok", "engine": "local", "tool": "contours",
            "geojson": {"type": "FeatureCollection", "features": feats},
            "count": len(feats), "interval": round(interval, 2),
        }
    finally:
        if not imported:
            try: os.remove(tif)
            except Exception: pass


def run_local(ee, tool, region, params, dem_asset):
    """Exécute un outil hydrologique local (numpy/skimage) et renvoie un overlay image."""
    if region is None:
        raise HTTPException(422, "Une emprise (ROI) est requise pour les outils locaux — choisissez « Vue carte » ou une couche.")

    scale = int(params.get("scale", 90) or 90)
    imported = params.get("dem_tif_path")           # MNT importé (chemin serveur) éventuel
    tif = imported or _fetch_dem_geotiff(ee, region, scale, dem_asset)

    try:
        with rasterio.open(tif) as src:
            raw = src.read(1).astype(float)
            b = src.bounds                          # left, bottom, right, top (WGS84)
            nod = src.nodata
        nodata_mask = ~np.isfinite(raw)
        if nod is not None:
            nodata_mask |= (raw == nod)
        fillv = np.nanmin(np.where(nodata_mask, np.nan, raw)) if nodata_mask.any() else raw.min()
        dem = np.where(nodata_mask, fillv, raw)

        filled = _fill_depressions(dem)
        thr = float(params.get("threshold", 1000) or 1000)

        if tool == "breach_depressions":
            arr, vis, mask = filled, LOCAL_VIS[tool], nodata_mask
        elif tool == "stream_order":
            acc, _ = _d8(filled)
            streams = acc >= thr
            order = np.where(streams, np.clip(np.floor(np.log10(np.maximum(acc, 1))), 1, 7), np.nan)
            arr, vis, mask = order, LOCAL_VIS[tool], ~streams | nodata_mask
        elif tool == "watershed":
            acc, receiver = _d8(filled)
            outlet = int(np.argmax(acc))
            inside = _catchment(receiver, outlet, dem.shape)
            arr, vis, mask = np.where(inside, 1.0, np.nan), LOCAL_VIS[tool], ~inside | nodata_mask
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
