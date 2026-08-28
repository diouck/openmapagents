"""
viewshed_routes.py — Analyse de visibilité (viewshed) depuis un point.

POST /api/viewshed/compute {lng, lat, observer_height, target_height, radius_km,
    curvature, dem_source, raster_token}
    → assemble un MNT autour du point selon la source choisie, lance un viewshed
      par lancer de rayons (angle d'élévation cumulé), renvoie un overlay PNG
      (vert = visible) géoréférencé + surface visible.

Sources de MNT :
  • "terrarium" (défaut) : MNT mondial ~30 m (tuiles Terrarium AWS, sans clé).
  • "ign" : RGE ALTI (France), une requête WMS BIL32 (float32) sur l'emprise.
  • "raster" : MNT GeoTIFF importé par l'utilisateur (jeton `raster_token`, déjà
    en cache EPSG:3857 ; sous-échantillonné à l'import s'il est trop lourd).

L'overlay se branche sur addImageLayer. Déps : numpy scipy Pillow (urllib stdlib).
"""
import io
import math
import base64
import urllib.request
from urllib.parse import urlencode
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/viewshed", tags=["viewshed"])

_TERR = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
_IGN_WMS = "https://data.geopf.fr/wms-r/wms"
_MAX_TILES = 49
_R_EARTH = 6371000.0


def _lonlat_to_tile(lon, lat, z):
    n = 2 ** z
    x = (lon + 180.0) / 360.0 * n
    latr = math.radians(lat)
    y = (1.0 - math.log(math.tan(latr) + 1.0 / math.cos(latr)) / math.pi) / 2.0 * n
    return x, y


def _tile_to_lonlat(x, y, z):
    n = 2 ** z
    lon = x / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / n))))
    return lon, lat


class ViewshedReq(BaseModel):
    lng: float
    lat: float
    observer_height: float = 2.0
    target_height: float = 0.0
    radius_km: float = 5.0
    curvature: bool = True
    dem_source: str = "terrarium"     # "terrarium" | "ign" | "raster"
    raster_token: Optional[str] = None


def _run_viewshed(dem, obs_row, obs_col, res, radius_m, obs_h, tgt_h, curvature):
    """Viewshed par lancer de rayons. dem = MNT (float, sans NaN), res = m sol/px."""
    import numpy as np
    from scipy.ndimage import map_coordinates, binary_closing, median_filter
    H, W = dem.shape
    Rpx = max(2, int(radius_m / res))
    oi, oj = int(round(obs_row)), int(round(obs_col))
    obs_elev = float(dem[oi, oj]) + obs_h
    vis = np.zeros((H, W), bool); vis[oi, oj] = True
    rr = np.arange(1, Rpx + 1)
    gdist = rr * res
    curv = (gdist ** 2) / (2 * _R_EARTH) if curvature else np.zeros_like(gdist)
    # densité de rayons ≈ 2 rayons / cellule sur l'anneau extérieur (moins de trous)
    n_az = int(min(4000, max(1440, 4 * math.pi * Rpx)))
    for k in range(n_az):
        th = 2 * math.pi * k / n_az
        cols = obs_col + rr * math.sin(th)
        rows = obs_row - rr * math.cos(th)
        inb = (cols >= 0) & (cols < W - 1) & (rows >= 0) & (rows < H - 1)
        if not inb.any():
            continue
        cf, rf, gd, cv = cols[inb], rows[inb], gdist[inb], curv[inb]
        demv = map_coordinates(dem, [rf, cf], order=1, mode="nearest")   # bilinéaire
        ang = (demv + tgt_h - cv - obs_elev) / gd
        prevmax = np.concatenate(([-1e18], np.maximum.accumulate(ang)[:-1]))
        seen = ang >= prevmax
        ci = np.round(cf[seen]).astype(np.int32); ri = np.round(rf[seen]).astype(np.int32)
        vis[ri, ci] = True
    # Lissage anti-« saccadé » : fermeture (comble les fentes radiales entre rayons)
    # + filtre médian (retire le moucheté isolé), tout en restant borné au rayon.
    vis = binary_closing(vis, structure=np.ones((3, 3), bool), iterations=1)
    vis = median_filter(vis.astype(np.uint8), size=3).astype(bool)
    yy, xx = np.ogrid[:H, :W]
    vis &= (xx - obs_col) ** 2 + (yy - obs_row) ** 2 <= (Rpx + 2) ** 2
    return vis, obs_elev, Rpx


def _overlay_png(vis, obs_col, obs_row, Rpx):
    import numpy as np
    from PIL import Image
    H, W = vis.shape
    rgba = np.zeros((H, W, 4), np.uint8)
    rgba[vis] = (34, 197, 94, 130)
    yy, xx = np.ogrid[:H, :W]
    dot = (xx - obs_col) ** 2 + (yy - obs_row) ** 2 <= (max(2, Rpx // 60)) ** 2
    rgba[dot] = (255, 255, 255, 235)
    buf = io.BytesIO(); Image.fromarray(rgba, "RGBA").save(buf, "PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _dem_terrarium(lon, lat, radius_m):
    """Mosaïque MNT mondiale (tuiles Terrarium). → (dem, obs_col, obs_row, res, coords, bbox)."""
    import numpy as np
    from PIL import Image
    res_target = (2 * radius_m) / 512.0
    z = int(round(math.log2(156543.03 * math.cos(math.radians(lat)) / max(res_target, 1e-3))))
    z = max(9, min(z, 14))
    dlat = radius_m / 111320.0
    dlon = radius_m / (111320.0 * math.cos(math.radians(lat)))
    x0f, _ = _lonlat_to_tile(lon - dlon, lat, z); x1f, _ = _lonlat_to_tile(lon + dlon, lat, z)
    _, y0f = _lonlat_to_tile(lon, lat + dlat, z); _, y1f = _lonlat_to_tile(lon, lat - dlat, z)
    tx0, tx1 = int(math.floor(x0f)), int(math.floor(x1f))
    ty0, ty1 = int(math.floor(y0f)), int(math.floor(y1f))
    nx, ny = tx1 - tx0 + 1, ty1 - ty0 + 1
    if nx * ny > _MAX_TILES:
        raise HTTPException(422, f"Zone trop grande ({nx * ny} tuiles) — réduisez le rayon.")
    W, H = nx * 256, ny * 256
    dem = np.zeros((H, W), np.float32)
    for j in range(ny):
        for i in range(nx):
            url = _TERR.format(z=z, x=tx0 + i, y=ty0 + j)
            try:
                rq = urllib.request.Request(url, headers={"User-Agent": "OpenMapAgents/1.0"})
                with urllib.request.urlopen(rq, timeout=20) as r:
                    im = Image.open(io.BytesIO(r.read())).convert("RGB")
            except Exception as e:
                raise HTTPException(502, f"MNT indisponible : {e}")
            a = np.asarray(im, np.float32)
            dem[j * 256:(j + 1) * 256, i * 256:(i + 1) * 256] = a[:, :, 0] * 256 + a[:, :, 1] + a[:, :, 2] / 256.0 - 32768.0
    gx, gy = _lonlat_to_tile(lon, lat, z)
    obs_col = (gx - tx0) * 256.0; obs_row = (gy - ty0) * 256.0
    res = 156543.03 * math.cos(math.radians(lat)) / (2 ** z)
    tl = _tile_to_lonlat(tx0, ty0, z); tr = _tile_to_lonlat(tx1 + 1, ty0, z)
    br = _tile_to_lonlat(tx1 + 1, ty1 + 1, z); bl = _tile_to_lonlat(tx0, ty1 + 1, z)
    coords = [[tl[0], tl[1]], [tr[0], tr[1]], [br[0], br[1]], [bl[0], bl[1]]]
    bbox = [min(tl[0], bl[0]), min(bl[1], br[1]), max(tr[0], br[0]), max(tl[1], tr[1])]
    return dem, obs_col, obs_row, res, coords, bbox


def _dem_ign(lon, lat, radius_m):
    """RGE ALTI (France) via une requête WMS BIL32 (float32). → même tuple."""
    import numpy as np
    from rasterio.warp import transform as warp_transform
    xo = warp_transform("EPSG:4326", "EPSG:3857", [lon], [lat])[0][0]
    yo = warp_transform("EPSG:4326", "EPSG:3857", [lon], [lat])[1][0]
    r3857 = radius_m / math.cos(math.radians(lat))
    xmin, ymin, xmax, ymax = xo - r3857, yo - r3857, xo + r3857, yo + r3857
    size = int(max(256, min(1500, round(2 * radius_m / 15.0))))   # ~15 m/px
    q = {"SERVICE": "WMS", "VERSION": "1.3.0", "REQUEST": "GetMap",
         "LAYERS": "ELEVATION.ELEVATIONGRIDCOVERAGE", "STYLES": "",
         "CRS": "EPSG:3857", "BBOX": f"{xmin},{ymin},{xmax},{ymax}",
         "WIDTH": size, "HEIGHT": size, "FORMAT": "image/x-bil;bits=32"}
    url = _IGN_WMS + "?" + urlencode(q)
    try:
        rq = urllib.request.Request(url, headers={"User-Agent": "OpenMapAgents/1.0"})
        with urllib.request.urlopen(rq, timeout=30) as r:
            data = r.read()
    except Exception as e:
        raise HTTPException(502, f"IGN RGE ALTI indisponible : {e}")
    if len(data) < size * size * 4:
        raise HTTPException(502, "Réponse IGN inattendue (hors couverture France ?).")
    dem = np.frombuffer(data[:size * size * 4], dtype="<f4").reshape(size, size).astype(np.float32)
    bad = ~np.isfinite(dem) | (dem < -1000) | (dem > 9000)
    if bad.all():
        raise HTTPException(422, "Hors couverture IGN (France métropolitaine).")
    if bad.any():
        dem = np.where(bad, float(np.nanmin(dem[~bad])), dem)
    obs_col = obs_row = size / 2.0
    res = 2 * radius_m / size
    tl = warp_transform("EPSG:3857", "EPSG:4326", [xmin], [ymax])
    tr = warp_transform("EPSG:3857", "EPSG:4326", [xmax], [ymax])
    br = warp_transform("EPSG:3857", "EPSG:4326", [xmax], [ymin])
    bl = warp_transform("EPSG:3857", "EPSG:4326", [xmin], [ymin])
    coords = [[tl[0][0], tl[1][0]], [tr[0][0], tr[1][0]], [br[0][0], br[1][0]], [bl[0][0], bl[1][0]]]
    bbox = [bl[0][0], bl[1][0], tr[0][0], tr[1][0]]
    return dem, obs_col, obs_row, res, coords, bbox


def _dem_raster(token, lon, lat):
    """MNT GeoTIFF importé (cache EPSG:3857). → même tuple."""
    import numpy as np
    from rasterio.transform import Affine
    from rasterio.warp import transform as warp_transform
    from raster_routes import _load_job
    band, meta, _ = _load_job(token)
    tr, crs = meta.get("transform"), meta.get("crs")
    if not tr or not crs:
        raise HTTPException(422, "Raster importé sans géoréférencement complet — réimportez le GeoTIFF.")
    affine = Affine(*tr)
    dem = np.asarray(band, np.float32)
    fin = np.isfinite(dem)
    if not fin.any():
        raise HTTPException(422, "MNT vide.")
    dem = np.where(fin, dem, float(np.nanmin(dem[fin])))
    xs = warp_transform("EPSG:4326", crs, [lon], [lat])[0][0]
    ys = warp_transform("EPSG:4326", crs, [lon], [lat])[1][0]
    col, row = ~affine * (xs, ys)
    H, W = dem.shape
    if not (0 <= col < W and 0 <= row < H):
        raise HTTPException(422, "Observateur hors de l'emprise du MNT importé.")
    res = abs(affine.a) * math.cos(math.radians(lat))   # 3857 → m sol
    coords = meta.get("coords"); bbox = meta.get("bbox")
    return dem, col, row, res, coords, bbox


@router.post("/compute")
def viewshed(req: ViewshedReq):
    lat, lon = float(req.lat), float(req.lng)
    radius_m = max(0.2, min(float(req.radius_km), 30.0)) * 1000.0
    src = (req.dem_source or "terrarium").lower()

    if src == "raster":
        if not req.raster_token:
            raise HTTPException(422, "Aucun MNT importé sélectionné.")
        dem, oc, orow, res, coords, bbox = _dem_raster(req.raster_token, lon, lat)
        label = "MNT importé"
    elif src == "ign":
        if not (-90 < lat < 90):
            raise HTTPException(422, "Latitude invalide.")
        dem, oc, orow, res, coords, bbox = _dem_ign(lon, lat, radius_m)
        label = "IGN RGE ALTI"
    else:
        if not (-85 < lat < 85):
            raise HTTPException(422, "Latitude hors couverture.")
        dem, oc, orow, res, coords, bbox = _dem_terrarium(lon, lat, radius_m)
        label = "MNT mondial"

    vis, obs_elev, Rpx = _run_viewshed(dem, orow, oc, res, radius_m,
                                       float(req.observer_height), float(req.target_height), req.curvature)
    png_b64 = _overlay_png(vis, oc, orow, Rpx)
    import numpy as np
    visible_km2 = float(vis.sum()) * (res ** 2) / 1e6
    return {
        "name": f"Visibilité · {req.radius_km:g} km ({label})",
        "bands": 3,
        "image_coordinates": coords,
        "bbox": bbox,
        "png_b64": png_b64,
        "raster_token": None,
        "visible_km2": round(visible_km2, 3),
        "observer_elev_m": round(float(dem[int(round(orow)), int(round(oc))]), 1),
        "resolution_m": round(res, 1),
        "source": label,
    }
