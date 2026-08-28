"""
viewshed_routes.py — Analyse de visibilité (viewshed) depuis un point.

POST /api/viewshed/compute {lng, lat, observer_height, target_height, radius_km,
    curvature}
    → assemble un MNT autour du point (tuiles Terrarium AWS, publiques, sans clé),
      lance un viewshed par lancer de rayons (angle d'élévation cumulé), et renvoie
      un overlay PNG (vert = visible) géoréférencé en EPSG:3857 + surface visible.

L'overlay se branche sur addImageLayer, comme un GeoTIFF importé.
Déps : numpy Pillow (urllib stdlib). Aucune clé, aucun GEE.
"""
import io
import math
import base64
import urllib.request

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/viewshed", tags=["viewshed"])

_TERR = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
_MAX_TILES = 49            # 7×7 tuiles max
_R_EARTH = 6371000.0


def _lonlat_to_tile(lon, lat, z):
    n = 2 ** z
    x = (lon + 180.0) / 360.0 * n
    latr = math.radians(lat)
    y = (1.0 - math.log(math.tan(latr) + 1.0 / math.cos(latr)) / math.pi) / 2.0 * n
    return x, y            # coords de tuile fractionnaires


def _tile_to_lonlat(x, y, z):
    n = 2 ** z
    lon = x / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / n))))
    return lon, lat


class ViewshedReq(BaseModel):
    lng: float
    lat: float
    observer_height: float = 2.0     # m au-dessus du sol
    target_height: float = 0.0       # m (cible)
    radius_km: float = 5.0
    curvature: bool = True


@router.post("/compute")
def viewshed(req: ViewshedReq):
    import numpy as np
    from PIL import Image

    lat, lon = float(req.lat), float(req.lng)
    if not (-85 < lat < 85):
        raise HTTPException(422, "Latitude hors couverture.")
    radius_m = max(0.2, min(float(req.radius_km), 30.0)) * 1000.0

    # zoom ≈ diamètre 512 px
    res_target = (2 * radius_m) / 512.0
    z = int(round(math.log2(156543.03 * math.cos(math.radians(lat)) / max(res_target, 1e-3))))
    z = max(9, min(z, 14))

    dlat = radius_m / 111320.0
    dlon = radius_m / (111320.0 * math.cos(math.radians(lat)))
    x0f, _ = _lonlat_to_tile(lon - dlon, lat, z)
    x1f, _ = _lonlat_to_tile(lon + dlon, lat, z)
    _, y0f = _lonlat_to_tile(lon, lat + dlat, z)   # nord → petit y
    _, y1f = _lonlat_to_tile(lon, lat - dlat, z)
    tx0, tx1 = int(math.floor(x0f)), int(math.floor(x1f))
    ty0, ty1 = int(math.floor(y0f)), int(math.floor(y1f))
    nx, ny = tx1 - tx0 + 1, ty1 - ty0 + 1
    if nx * ny > _MAX_TILES:
        raise HTTPException(422, f"Zone trop grande ({nx * ny} tuiles) — réduisez le rayon.")

    # Mosaïque MNT (décodage Terrarium : h = R*256 + G + B/256 − 32768)
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

    # Observateur (pixel dans la mosaïque)
    gx, gy = _lonlat_to_tile(lon, lat, z)
    obs_col = (gx - tx0) * 256.0
    obs_row = (gy - ty0) * 256.0
    oc, orow = int(round(obs_col)), int(round(obs_row))
    if not (0 <= oc < W and 0 <= orow < H):
        raise HTTPException(422, "Observateur hors zone.")

    res = 156543.03 * math.cos(math.radians(lat)) / (2 ** z)   # m/px
    Rpx = max(2, int(radius_m / res))
    obs_elev = float(dem[orow, oc]) + float(req.observer_height)

    from scipy.ndimage import map_coordinates
    vis = np.zeros((H, W), bool)
    vis[orow, oc] = True
    rr = np.arange(1, Rpx + 1)
    gdist = rr * res
    curv = (gdist ** 2) / (2 * _R_EARTH) if req.curvature else np.zeros_like(gdist)
    tgt = float(req.target_height)
    # assez d'azimuts pour couvrir l'anneau extérieur (≈ 1 rayon / cellule)
    n_az = int(min(2400, max(720, 2 * math.pi * Rpx)))
    for k in range(n_az):
        th = 2 * math.pi * k / n_az
        cols = obs_col + rr * math.sin(th)
        rows = obs_row - rr * math.cos(th)
        inb = (cols >= 0) & (cols < W - 1) & (rows >= 0) & (rows < H - 1)
        if not inb.any():
            continue
        cf, rf, gd, cv = cols[inb], rows[inb], gdist[inb], curv[inb]
        # échantillonnage BILINÉAIRE du MNT le long du rayon (le plus-proche-voisin
        # génère un bruit d'angle près de l'observateur qui bloque tout le rayon).
        demv = map_coordinates(dem, [rf, cf], order=1, mode="nearest")
        ang = (demv + tgt - cv - obs_elev) / gd
        prevmax = np.concatenate(([-1e18], np.maximum.accumulate(ang)[:-1]))
        seen = ang >= prevmax
        ci = np.round(cf[seen]).astype(np.int32); ri = np.round(rf[seen]).astype(np.int32)
        vis[ri, ci] = True

    # Overlay RVBA : vert translucide sur le visible
    rgba = np.zeros((H, W, 4), np.uint8)
    rgba[vis] = (34, 197, 94, 130)
    # marqueur observateur (petit disque blanc)
    yy, xx = np.ogrid[:H, :W]
    dot = (xx - obs_col) ** 2 + (yy - obs_row) ** 2 <= (max(2, Rpx // 60)) ** 2
    rgba[dot] = (255, 255, 255, 235)

    buf = io.BytesIO(); Image.fromarray(rgba, "RGBA").save(buf, "PNG")
    png_b64 = base64.b64encode(buf.getvalue()).decode("ascii")

    # Coins lon/lat de la mosaïque (tuiles entières)
    tl = _tile_to_lonlat(tx0, ty0, z)
    tr = _tile_to_lonlat(tx1 + 1, ty0, z)
    br = _tile_to_lonlat(tx1 + 1, ty1 + 1, z)
    bl = _tile_to_lonlat(tx0, ty1 + 1, z)
    lons = [tl[0], tr[0], br[0], bl[0]]; lats = [tl[1], tr[1], br[1], bl[1]]

    visible_km2 = float(vis.sum()) * (res ** 2) / 1e6
    return {
        "name": f"Visibilité · {req.radius_km:g} km",
        "bands": 3,
        "image_coordinates": [[tl[0], tl[1]], [tr[0], tr[1]], [br[0], br[1]], [bl[0], bl[1]]],
        "bbox": [min(lons), min(lats), max(lons), max(lats)],
        "png_b64": png_b64,
        "raster_token": None,
        "visible_km2": round(visible_km2, 3),
        "observer_elev_m": round(float(dem[orow, oc]), 1),
        "zoom": z, "resolution_m": round(res, 1),
    }
