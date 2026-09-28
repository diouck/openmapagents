"""
whitebox_routes.py — Outils Whitebox/Morphologie via Google Earth Engine.

POST /api/whitebox/run {tool, bbox, roi_geojson, params}
    → calcule l'outil morphologique demandé sur un MNT (SRTM 30 m) et renvoie
      une URL de tuiles XYZ (getMapId) + les paramètres de visualisation, à
      brancher comme couche raster sur la carte (addTileLayer côté frontend).

Stratégie « GEE d'abord » (priorité au calcul le plus simple) :
  • slope / aspect / hillshade : natifs ee.Terrain.
  • curvature : laplacien approché par convolution (kernel Laplacien 3×3).
  • tpi (Topographic Position Index) : élévation − moyenne focale du voisinage.

Le MNT source par défaut est SRTM GL1 (USGS/SRTMGL1_003, ~30 m mondial).
Déps : earthengine-api (déjà présent). Réutilise gee_auth.init_gee().
"""
from typing import Optional, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from gee_auth import init_gee, get_ee

router = APIRouter(prefix="/whitebox", tags=["whitebox"])

# ── MNT sources disponibles ────────────────────────────────────
DEM_SOURCES = {
    "SRTM_30m":   "USGS/SRTMGL1_003",       # 30 m mondial
    "GEBCO_2024": "USGS/SRTMGL1_003",       # (bathymétrie non branchée → repli SRTM)
    "COPDEM_30m": "COPERNICUS/DEM/GLO30",   # Copernicus GLO-30
}
DEFAULT_DEM = "USGS/SRTMGL1_003"

# ── Palettes de visualisation par outil ────────────────────────
VIS = {
    "slope":     {"min": 0,   "max": 60,  "palette": ["#ffffff", "#fdae61", "#f46d43", "#d73027", "#9e0142"]},
    "aspect":    {"min": 0,   "max": 360, "palette": ["#e60000", "#ffaa00", "#ffff00", "#00b050", "#00b0f0", "#0000ff", "#7030a0", "#e60000"]},
    "curvature": {"min": -1,  "max": 1,   "palette": ["#2166ac", "#67a9cf", "#f7f7f7", "#ef8a62", "#b2182b"]},
    "hillshade": {"min": 0,   "max": 255, "palette": ["#000000", "#ffffff"]},
    "tpi":       {"min": -50, "max": 50,  "palette": ["#2166ac", "#67a9cf", "#f7f7f7", "#ef8a62", "#b2182b"]},
    # ── Filtres (VAGUE 2) ──
    # Lissages → sortie « élévation » (palette terrain large, approximative selon la région).
    "mean_filter":     {"min": 0, "max": 3000, "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]},
    "median_filter":   {"min": 0, "max": 3000, "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]},
    "gaussian_filter": {"min": 0, "max": 3000, "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]},
    # Détails / contours → diverging & magnitude.
    "highpass_filter": {"min": -40, "max": 40, "palette": ["#2166ac", "#67a9cf", "#f7f7f7", "#ef8a62", "#b2182b"]},
    "sobel_filter":    {"min": 0, "max": 40, "palette": ["#000004", "#3b0f70", "#8c2981", "#de4968", "#fe9f6d", "#fcfdbf"]},
}


class WhiteboxRunRequest(BaseModel):
    tool:        str                          # slope | aspect | curvature | hillshade | tpi
    bbox:        Optional[List[float]] = None  # [west, south, east, north]
    roi_geojson: Optional[dict]        = None
    dem_source:  Optional[str]         = "SRTM_30m"
    params:      Optional[dict]        = {}


def _dem_asset(source: str):
    return DEM_SOURCES.get(source, DEFAULT_DEM)


def _build_image(ee, tool: str, dem, params: dict):
    """Construit l'image GEE de l'outil demandé à partir du MNT `dem`."""
    z = float(params.get("zfactor", 1.0) or 1.0)
    demz = dem.multiply(z) if z != 1.0 else dem

    if tool == "slope":
        img = ee.Terrain.slope(demz)                      # degrés
        units = params.get("units", "degrees")
        if units == "percent":
            img = img.multiply(3.14159265 / 180.0).tan().multiply(100)
        elif units == "radians":
            img = img.multiply(3.14159265 / 180.0)
        return img.rename("slope")

    if tool == "aspect":
        return ee.Terrain.aspect(demz).rename("aspect")   # 0-360°

    if tool == "hillshade":
        az  = float(params.get("azimuth", 315) or 315)
        alt = float(params.get("altitude", 45) or 45)
        return ee.Terrain.hillshade(demz, az, alt).rename("hillshade")

    if tool == "curvature":
        # Laplacien 3×3 : convexité (+) / concavité (−). Approxime la courbure générale.
        kernel = ee.Kernel.laplacian8(normalize=False)
        return demz.convolve(kernel).rename("curvature")

    if tool == "tpi":
        # TPI = élévation − moyenne focale sur un voisinage circulaire de `radius` px.
        radius = int(params.get("radius", 10) or 10)
        mean = demz.focal_mean(radius=radius, kernelType="circle", units="pixels")
        return demz.subtract(mean).rename("tpi")

    # ── Filtres (VAGUE 2) ─────────────────────────────────────
    if tool == "mean_filter":
        radius = int(params.get("radius", 3) or 3)
        return demz.focal_mean(radius=radius, kernelType="square", units="pixels").rename("mean")

    if tool == "median_filter":
        radius = int(params.get("radius", 3) or 3)
        return demz.focal_median(radius=radius, kernelType="square", units="pixels").rename("median")

    if tool == "gaussian_filter":
        sigma = float(params.get("sigma", 1.0) or 1.0)
        rad = max(1, int(round(3 * sigma)))
        k = ee.Kernel.gaussian(radius=rad, sigma=sigma, units="pixels", normalize=True)
        return demz.convolve(k).rename("gaussian")

    if tool == "highpass_filter":
        # Passe-haut = élévation − lissage (rehausse les détails/contours).
        radius = int(params.get("radius", 3) or 3)
        low = demz.focal_mean(radius=radius, kernelType="square", units="pixels")
        return demz.subtract(low).rename("highpass")

    if tool == "sobel_filter":
        # Magnitude du gradient (Sobel-like) : sqrt(dx² + dy²).
        grad = demz.gradient()
        return grad.select("x").pow(2).add(grad.select("y").pow(2)).sqrt().rename("sobel")

    raise HTTPException(422, f"Outil inconnu : {tool}")


@router.post("/run")
def whitebox_run(req: WhiteboxRunRequest):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    ee = get_ee()

    tool = (req.tool or "").lower()
    if tool not in VIS:
        raise HTTPException(422, f"Outil non supporté : {tool}")

    try:
        dem = ee.Image(_dem_asset(req.dem_source or "SRTM_30m")).select(0)

        # ── Emprise (ROI) ──────────────────────────────────────
        region = None
        if req.roi_geojson:
            try:
                geom = req.roi_geojson.get("geometry", req.roi_geojson)
                region = ee.Geometry(geom)
            except Exception:
                region = None
        if region is None and req.bbox and len(req.bbox) == 4:
            w, s, e, n = req.bbox
            # Emprise ~mondiale → pas de clip (inutile, plus lent)
            if not (w <= -179 and s <= -89 and e >= 179 and n >= 89):
                region = ee.Geometry.BBox(w, s, e, n)

        img = _build_image(ee, tool, dem, req.params or {})
        if region is not None:
            img = img.clip(region)

        vis = dict(VIS[tool])
        map_id  = img.getMapId(vis)
        fetcher = map_id.get("tile_fetcher")
        tile_url = fetcher.url_format if (fetcher and hasattr(fetcher, "url_format")) else map_id.get("urlFormat", "")
        if not tile_url:
            raise HTTPException(500, "Impossible de générer l'URL de tuiles GEE")

        return {
            "status":     "ok",
            "tool":       tool,
            "tile_url":   tile_url,
            "vis_params": vis,
            "dem_source": req.dem_source,
            "engine":     "gee",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erreur calcul Whitebox/{tool} : {e}")
