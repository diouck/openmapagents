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
    sea_level_m: float = 0.5          # altitude ≤ ce seuil = plan d'eau (mer) exclu de l'inondation
    vector3d: bool = True             # vectorise la profondeur max → polygones extrudables (vue 3D)


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


IGN_WMS = "https://data.geopf.fr/wms-r/wms"


def _load_ign_dem(bbox, max_cells):
    """MNT IGN RGE ALTI (~1–5 m, France) via WMS Géoplateforme en BIL float32 (altitudes réelles)."""
    w, s, e, n = bbox
    latm = math.radians((n + s) / 2.0)
    wm = (e - w) * 111320.0 * max(0.1, math.cos(latm))
    hm = (n - s) * 111320.0
    if wm >= hm:
        width = max_cells; height = max(2, int(max_cells * hm / max(1e-9, wm)))
    else:
        height = max_cells; width = max(2, int(max_cells * wm / max(1e-9, hm)))
    params = {
        "SERVICE": "WMS", "VERSION": "1.3.0", "REQUEST": "GetMap",
        "LAYERS": "ELEVATION.ELEVATIONGRIDCOVERAGE", "STYLES": "",
        "CRS": "EPSG:4326", "BBOX": f"{s},{w},{n},{e}",   # 1.3.0 + EPSG:4326 = lat,lon
        "WIDTH": str(width), "HEIGHT": str(height),
        "FORMAT": "image/x-bil;bits=32",
    }
    try:
        r = requests.get(IGN_WMS, params=params, timeout=60)
        r.raise_for_status()
    except Exception as ex:
        raise HTTPException(502, f"IGN WMS injoignable : {ex}")
    if len(r.content) != width * height * 4:
        raise HTTPException(502, f"IGN WMS : réponse inattendue ({len(r.content)} octets, "
                                 f"attendu {width*height*4}). Emprise hors France ? {r.text[:160]}")
    arr = np.frombuffer(r.content, dtype="<f4").reshape(height, width).astype(float)
    finite = arr[np.isfinite(arr)]
    if finite.size and float(np.nanmax(np.abs(finite))) > 1e5:   # mauvaise endianness
        arr = np.frombuffer(r.content, dtype=">f4").reshape(height, width).astype(float)
    arr[arr <= -99998] = np.nan
    arr[arr < -1000] = np.nan
    return arr, [w, s, e, n]


def _cell_m(arr, bbox):
    w, s, e, n = bbox
    ny, nx = arr.shape
    latm = math.radians((n + s) / 2.0)
    dx = (e - w) / nx * 111320.0 * max(0.1, math.cos(latm))
    dy = (n - s) / ny * 111320.0
    return (dx + dy) / 2.0


def _rasterize(polys, bbox, shape):
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds
    if not polys:
        return None
    w, s, e, n = bbox
    ny, nx = shape
    transform = from_bounds(w, s, e, n, nx, ny)
    return rasterize([(p, 1) for p in polys], out_shape=(ny, nx), transform=transform,
                     fill=0, all_touched=True, dtype="uint8").astype(bool)


def _osm_masks(bbox, shape, want_buildings=True):
    """Récupère bâtiments + plans d'eau OSM (Overpass, 1 requête) → (mask_bati, mask_eau, n_bati)."""
    w, s, e, n = bbox
    bb = f"{s},{w},{n},{e}"
    parts = []
    if want_buildings:
        parts.append(f'way["building"]({bb});')
    parts += [f'way["natural"="water"]({bb});', f'way["waterway"="riverbank"]({bb});',
              f'way["landuse"="reservoir"]({bb});', f'relation["natural"="water"]({bb});']
    q = f'[out:json][timeout:45];({"".join(parts)});out geom;'
    try:
        r = requests.post(OVERPASS_URL, data={"data": q}, timeout=50)
        r.raise_for_status()
        els = r.json().get("elements", [])
    except Exception:
        return None, None, 0
    from shapely.geometry import Polygon
    b_polys, w_polys = [], []
    for el in els:
        tags = el.get("tags", {}) or {}
        is_water = ("natural" in tags and tags.get("natural") == "water") or \
                   tags.get("waterway") == "riverbank" or tags.get("landuse") == "reservoir" or "water" in tags
        is_building = "building" in tags
        rings = []
        if el.get("type") == "relation":
            for mem in el.get("members", []):
                g = mem.get("geometry")
                if mem.get("role") == "outer" and g and len(g) >= 3:
                    rings.append([(p["lon"], p["lat"]) for p in g])
        else:
            g = el.get("geometry")
            if g and len(g) >= 3:
                rings.append([(p["lon"], p["lat"]) for p in g])
        for ring in rings:
            try:
                poly = Polygon(ring)
                if not poly.is_valid or poly.area <= 0:
                    continue
            except Exception:
                continue
            if is_building:
                b_polys.append(poly)
            elif is_water:
                w_polys.append(poly)
    return _rasterize(b_polys, bbox, shape), _rasterize(w_polys, bbox, shape), len(b_polys)


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


def _depth_polygons(depth, bbox, vmax, nbands=6, dry=0.02):
    """Vectorise la profondeur (2D, m) en polygones classés avec attribut `depth` (m) → GeoJSON 3D extrudable."""
    from rasterio.features import shapes
    from rasterio.transform import from_bounds
    from shapely.geometry import shape as shp_shape
    ny, nx = depth.shape
    w, s, e, n = bbox
    edges = np.linspace(dry, max(vmax, dry + 0.1), nbands + 1)
    cls = np.digitize(np.where(np.isfinite(depth), depth, 0.0), edges).astype("int32")
    cls[depth < dry] = 0
    if not (cls > 0).any():
        return {"type": "FeatureCollection", "features": []}
    transform = from_bounds(w, s, e, n, nx, ny)
    tol = abs(e - w) / nx * 1.2   # simplification ~1 cellule
    feats = []
    for geom, val in shapes(cls, mask=cls > 0, transform=transform):
        k = int(val)
        d_lo = edges[min(k - 1, nbands - 1)]; d_hi = edges[min(k, nbands)]
        dmid = float((d_lo + d_hi) / 2.0)
        try:
            poly = shp_shape(geom).simplify(tol)
            if poly.is_empty:
                continue
        except Exception:
            poly = shp_shape(geom)
        feats.append({"type": "Feature", "geometry": poly.__geo_interface__,
                      "properties": {"depth": round(dmid, 2), "depth_cm": int(round(dmid * 100))}})
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
    elif req.dem_source == "ign":
        arr, bbox = _load_ign_dem(req.bbox, req.max_cells)
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

    # Plans d'eau : mer (altitude ≤ niveau marin) + rivières/lacs OSM (ex. la Loire)
    # → exclus de l'inondation (sinon ils se « remplissent » de pluie et deviennent bleus).
    base_mask = ~np.isfinite(arr)
    base_mask = base_mask | (np.isfinite(arr) & (arr <= req.sea_level_m))

    n_buildings = n_water = 0
    bmask, wmask, n_buildings = _osm_masks(bbox, arr.shape, want_buildings=req.buildings)
    if req.buildings and bmask is not None:
        base_mask = base_mask | bmask            # bâtiments = murs
    if wmask is not None:
        base_mask = base_mask | wmask            # plans d'eau = exclus
        n_water = int(wmask.sum())

    sim = simulate_flood(
        arr, cell, req.rainfall_mm_h, req.duration_min,
        manning=req.manning, infil_mm_h=req.infiltration_mm_h, n_frames=req.n_frames,
        nodata_mask=base_mask,
    )
    payload = frames_to_payload(sim, bbox)
    payload["cell_m"] = round(cell, 1)
    payload["grid"] = [int(arr.shape[0]), int(arr.shape[1])]
    payload["buildings"] = n_buildings
    payload["water_cells"] = n_water
    payload["params"] = {"rainfall_mm_h": req.rainfall_mm_h, "duration_min": req.duration_min,
                         "manning": req.manning, "infiltration_mm_h": req.infiltration_mm_h}

    # Sens de l'écoulement (flèches) sur l'enveloppe de profondeur max.
    if req.flow_arrows:
        arrows, spd_ref = flow_field(np.where(np.isfinite(arr), arr, 0.0), sim["max_depth"],
                                     cell, req.manning, wall=base_mask, step=6)
        payload["flow_geojson"] = _arrows_geojson(arrows, spd_ref, bbox, arr.shape)
        payload["spd_ref"] = round(spd_ref, 2)

    # Vue 3D : profondeur max vectorisée en polygones extrudables (hauteur = profondeur).
    if req.vector3d:
        payload["depth3d_geojson"] = _depth_polygons(sim["max_depth"], bbox, sim["vmax"])
    return payload
