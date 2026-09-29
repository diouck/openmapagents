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
from whitebox_local import run_local, LOCAL_TOOLS, interpolate_surface, extract_contours

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
    # ── Stats locales (focales) ──
    "local_mean":   {"min": 0, "max": 3000, "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]},
    "local_max":    {"min": 0, "max": 3000, "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]},
    "local_median": {"min": 0, "max": 3000, "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]},
    "local_std":    {"min": 0, "max": 100,  "palette": ["#440154", "#3b528b", "#21918c", "#5ec962", "#fde725"]},
    # ── Reclassification ──
    "threshold": {"min": 0, "max": 1,  "palette": ["#111111", "#f7f7f7"]},
    "slice":     {"min": 0, "max": 12, "palette": ["#9e0142", "#f46d43", "#fee08b", "#e6f598", "#66c2a5", "#5e4fa2"]},
    "normalize": {"min": 0, "max": 1,  "palette": ["#440154", "#3b528b", "#21918c", "#5ec962", "#fde725"]},
    # ── Image / Texture ──
    "edge_detection": {"min": 0, "max": 1,   "palette": ["#111111", "#f7f7f7"]},
    "texture":        {"min": 0, "max": 100, "palette": ["#000004", "#8c2981", "#de4968", "#fe9f6d", "#fcfdbf"]},
    "directional":    {"min": -50, "max": 50, "palette": ["#2166ac", "#67a9cf", "#f7f7f7", "#ef8a62", "#b2182b"]},
    # ── Morphologie (reste) ──
    "tri":               {"min": 0, "max": 50,  "palette": ["#ffffcc", "#fd8d3c", "#e31a1c", "#800026"]},
    "roughness":         {"min": 0, "max": 200, "palette": ["#ffffcc", "#fd8d3c", "#e31a1c", "#800026"]},
    "plan_curvature":    {"min": -1, "max": 1,  "palette": ["#2166ac", "#67a9cf", "#f7f7f7", "#ef8a62", "#b2182b"]},
    "profile_curvature": {"min": -1, "max": 1,  "palette": ["#2166ac", "#67a9cf", "#f7f7f7", "#ef8a62", "#b2182b"]},
    "relative_position": {"min": -30, "max": 30, "palette": ["#2166ac", "#67a9cf", "#f7f7f7", "#ef8a62", "#b2182b"]},
    # ── Hydrologie (HydroSHEDS ~500 m) ──
    "fill_depressions":   {"min": 0, "max": 3000, "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]},
    "flow_direction":     {"min": 1, "max": 128,  "palette": ["#9e0142", "#f46d43", "#fee08b", "#e6f598", "#66c2a5", "#5e4fa2"]},
    "flow_accumulation":  {"min": 0, "max": 6,    "palette": ["#f7fbff", "#c6dbef", "#6baed6", "#2171b5", "#08306b"]},
    "stream_network":     {"min": 0, "max": 1,    "palette": ["#ffffff", "#08519c"]},
    # ── Avancé ──
    "kmeans": {"min": 0, "max": 20, "palette": ["#9e0142", "#f46d43", "#fee08b", "#66c2a5", "#5e4fa2", "#3288bd"]},
    # ── Nettoyage MNT / Distance ──
    "smooth_dem":         {"min": 0, "max": 3000,  "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]},
    "euclidean_distance": {"min": 0, "max": 20000, "palette": ["#08306b", "#2171b5", "#6baed6", "#c6dbef", "#f7fbff"]},
    "fill_missing_data":  {"min": 0, "max": 3000,  "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]},
    "cost_distance":      {"min": 0, "max": 50000, "palette": ["#440154", "#3b528b", "#21918c", "#5ec962", "#fde725"]},
    # ── Segmentation ──
    "connected_components": {"min": 0, "max": 60, "palette": ["#9e0142", "#f46d43", "#fee08b", "#e6f598", "#66c2a5", "#5e4fa2", "#3288bd"]},
    "clump":                {"min": 0, "max": 60, "palette": ["#9e0142", "#f46d43", "#fee08b", "#e6f598", "#66c2a5", "#5e4fa2", "#3288bd"]},
    "sieve":                {"min": 0, "max": 7,  "palette": ["#9e0142", "#f46d43", "#fee08b", "#e6f598", "#66c2a5", "#5e4fa2"]},
    # ── Avancé ──
    "pca": {"min": -500, "max": 500, "palette": ["#440154", "#3b528b", "#21918c", "#5ec962", "#fde725"]},
}


class WhiteboxRunRequest(BaseModel):
    tool:        str                          # slope | aspect | curvature | hillshade | tpi
    bbox:        Optional[List[float]] = None  # [west, south, east, north]
    roi_geojson: Optional[dict]        = None
    dem_source:  Optional[str]         = "SRTM_30m"
    params:      Optional[dict]        = {}
    vis_params_override: Optional[dict] = None  # {palette, min, max} pour re-symboliser


def _dem_asset(source: str):
    return DEM_SOURCES.get(source, DEFAULT_DEM)


def _build_image(ee, tool: str, dem, params: dict, region=None):
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

    # ── Stats locales (focales) ───────────────────────────────
    if tool == "local_mean":
        r = int(params.get("radius", 3) or 3)
        return demz.focal_mean(radius=r, kernelType="square", units="pixels").rename("local_mean")
    if tool == "local_median":
        r = int(params.get("radius", 3) or 3)
        return demz.focal_median(radius=r, kernelType="square", units="pixels").rename("local_median")
    if tool == "local_max":
        r = int(params.get("radius", 3) or 3)
        return demz.focal_max(radius=r, kernelType="square", units="pixels").rename("local_max")
    if tool == "local_std":
        r = int(params.get("radius", 3) or 3)
        return demz.reduceNeighborhood(ee.Reducer.stdDev(), ee.Kernel.square(r, "pixels")).rename("local_std")

    # ── Reclassification ──────────────────────────────────────
    if tool == "threshold":
        v = float(params.get("value", 0) or 0)
        return demz.gt(v).rename("threshold")
    if tool in ("normalize", "slice"):
        v = demz.rename("v")
        reg = region if region is not None else v.geometry()
        mm = v.reduceRegion(reducer=ee.Reducer.minMax(), geometry=reg, scale=90, maxPixels=int(1e9), bestEffort=True)
        mn = ee.Number(mm.get("v_min")); mx = ee.Number(mm.get("v_max"))
        norm = v.subtract(mn).divide(mx.subtract(mn).max(1e-9))
        if tool == "normalize":
            return norm.rename("normalize")
        n = int(params.get("n_classes", 5) or 5)
        return norm.multiply(n).floor().min(n - 1).rename("slice")

    # ── Image / Texture ───────────────────────────────────────
    if tool == "edge_detection":
        thr = float(params.get("threshold", 0.5) or 0.5)
        return ee.Algorithms.CannyEdgeDetector(image=demz, threshold=thr, sigma=1).rename("edge_detection")
    if tool == "texture":
        size = int(params.get("size", 3) or 3)
        glcm = demz.toInt32().rename("v").glcmTexture(size=size)
        return glcm.select("v_contrast").rename("texture")
    if tool == "directional":
        import math
        az = math.radians(float(params.get("azimuth", 45) or 45))
        grad = demz.gradient()
        return grad.select("x").multiply(math.sin(az)).add(grad.select("y").multiply(math.cos(az))).rename("directional")

    # ── Morphologie (reste) ───────────────────────────────────
    if tool == "tri":
        r = int(params.get("radius", 3) or 3)
        return demz.reduceNeighborhood(ee.Reducer.stdDev(), ee.Kernel.square(r, "pixels")).rename("tri")
    if tool == "roughness":
        r = int(params.get("radius", 3) or 3)
        hi = demz.focal_max(radius=r, kernelType="square", units="pixels")
        lo = demz.focal_min(radius=r, kernelType="square", units="pixels")
        return hi.subtract(lo).rename("roughness")
    if tool in ("plan_curvature", "profile_curvature"):
        # Zevenbergen-Thorne via dérivées de gradient.
        g = demz.gradient(); gx = g.select("x"); gy = g.select("y")
        gxx = gx.gradient().select("x"); gyy = gy.gradient().select("y")
        gxy = gx.gradient().select("y")
        p = gx.pow(2).add(gy.pow(2)).max(1e-9)
        if tool == "profile_curvature":
            num = gxx.multiply(gx.pow(2)).add(gyy.multiply(gy.pow(2))).add(gxy.multiply(gx).multiply(gy).multiply(2))
            return num.divide(p).rename("profile_curvature")
        num = gxx.multiply(gy.pow(2)).add(gyy.multiply(gx.pow(2))).subtract(gxy.multiply(gx).multiply(gy).multiply(2))
        return num.divide(p).rename("plan_curvature")
    if tool == "relative_position":
        r0 = int(params.get("min_radius", 3) or 3)
        r1 = int(params.get("max_radius", 30) or 30)
        tpi0 = demz.subtract(demz.focal_mean(radius=r0, kernelType="circle", units="pixels"))
        tpi1 = demz.subtract(demz.focal_mean(radius=r1, kernelType="circle", units="pixels"))
        return tpi0.add(tpi1).divide(2).rename("relative_position")

    # ── Hydrologie (produits HydroSHEDS pré-calculés, ~500 m) ──
    if tool == "fill_depressions":
        return ee.Image("WWF/HydroSHEDS/03CONDEM").select(0).rename("fill_depressions")
    if tool == "flow_direction":
        return ee.Image("WWF/HydroSHEDS/03DIR").select(0).rename("flow_direction")
    if tool == "flow_accumulation":
        acc = ee.Image("WWF/HydroSHEDS/15ACC").select(0)
        return acc.add(1).log10().rename("flow_accumulation")
    if tool == "stream_network":
        thr = float(params.get("threshold", 1000) or 1000)
        acc = ee.Image("WWF/HydroSHEDS/15ACC").select(0)
        return acc.gte(thr).selfMask().rename("stream_network")

    # ── Avancé : classification non supervisée k-means ────────
    if tool == "kmeans":
        n = int(params.get("clusters", 5) or 5)
        slope = ee.Terrain.slope(demz)
        tpi = demz.subtract(demz.focal_mean(radius=10, kernelType="circle", units="pixels"))
        stack = demz.rename("elev").addBands(slope.rename("slope")).addBands(tpi.rename("tpi"))
        reg = region if region is not None else demz.geometry()
        training = stack.sample(region=reg, scale=90, numPixels=5000, seed=1, tileScale=4)
        clusterer = ee.Clusterer.wekaKMeans(n).train(training)
        return stack.cluster(clusterer).rename("kmeans")

    # ── Nettoyage MNT : lissage (médiane focale) ──────────────
    if tool == "smooth_dem":
        r = int(params.get("radius", 3) or 3)
        return demz.focal_median(radius=r, kernelType="square", units="pixels").rename("smooth_dem")

    # ── Distance euclidienne au réseau hydrographique ─────────
    if tool == "euclidean_distance":
        streams = ee.Image("WWF/HydroSHEDS/15ACC").select(0).gte(1000)
        # fastDistanceTransform → distance² en pixels ; ~463 m par pixel (15 arc-sec)
        return streams.fastDistanceTransform(256).sqrt().multiply(463).rename("euclidean_distance")

    # ── Nettoyage : combler les NoData par interpolation focale ──
    if tool == "fill_missing_data":
        filled = demz.focal_mean(radius=2, kernelType="square", units="pixels", iterations=3)
        return demz.unmask(filled).rename("fill_missing_data")

    # ── Distance de coût (pente comme coût, source = réseau hydro) ──
    if tool == "cost_distance":
        cost = ee.Terrain.slope(demz).add(1)
        source = ee.Image("WWF/HydroSHEDS/15ACC").select(0).gte(1000)
        return cost.cumulativeCost(source=source, maxDistance=50000).rename("cost_distance")

    # ── Segmentation : classification auto (8 tranches) puis composantes ──
    if tool in ("connected_components", "clump", "sieve"):
        v = demz.rename("v")
        reg = region if region is not None else v.geometry()
        mm = v.reduceRegion(reducer=ee.Reducer.minMax(), geometry=reg, scale=90, maxPixels=int(1e9), bestEffort=True)
        mn = ee.Number(mm.get("v_min")); mx = ee.Number(mm.get("v_max"))
        classes = v.subtract(mn).divide(mx.subtract(mn).max(1e-9)).multiply(8).floor().min(7).toInt()
        diag = str(params.get("diag", "oui")) != "non"
        if tool == "sieve":
            min_size = int(params.get("min_size", 10) or 10)
            count = classes.connectedPixelCount(maxSize=256, eightConnected=diag)
            return classes.updateMask(count.gte(min_size)).rename("sieve")
        kernel = ee.Kernel.square(1) if diag else ee.Kernel.plus(1)
        labeled = classes.connectedComponents(connectedness=kernel, maxSize=256)
        # labels bruts très grands → repliés (mod) pour un rendu catégoriel lisible
        return labeled.select("labels").mod(60).rename(tool)

    # ── Avancé : ACP (PCA) sur pile élévation/pente/TPI → PC1 ──
    if tool == "pca":
        slope = ee.Terrain.slope(demz)
        tpi = demz.subtract(demz.focal_mean(radius=10, kernelType="circle", units="pixels"))
        stack = demz.rename("b1").addBands(slope.rename("b2")).addBands(tpi.rename("b3"))
        reg = region if region is not None else stack.geometry()
        names = stack.bandNames()
        mean_dict = stack.reduceRegion(reducer=ee.Reducer.mean(), geometry=reg, scale=90, maxPixels=int(1e9), bestEffort=True)
        means = ee.Image.constant(mean_dict.values(names))
        centered = stack.subtract(means)
        arrays = centered.toArray()
        covar = arrays.reduceRegion(reducer=ee.Reducer.centeredCovariance(), geometry=reg, scale=90, maxPixels=int(1e9), bestEffort=True)
        covar_array = ee.Array(covar.get("array"))
        eigens = covar_array.eigen()
        eigen_vectors = eigens.slice(1, 1)                 # retire la colonne des valeurs propres
        principal = ee.Image(eigen_vectors).matrixMultiply(arrays.toArray(1))
        pc = principal.arrayProject([0]).arrayFlatten([["pc1", "pc2", "pc3"]])
        return pc.select("pc1").rename("pca")

    raise HTTPException(422, f"Outil inconnu : {tool}")


@router.post("/run")
def whitebox_run(req: WhiteboxRunRequest):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    ee = get_ee()

    tool = (req.tool or "").lower()

    # ── Emprise (ROI) — commune GEE & local ────────────────────
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

    # ── MNT importé (raster_token) → calcul LOCAL numpy/scipy sur la couche de l'utilisateur ──
    rtok = (req.params or {}).get("raster_token")
    if rtok:
        try:
            from raster_routes import _load_band
            from whitebox_local import compute_on_array, contours_from_array, _colormap, _png_b64
            band, meta = _load_band(str(rtok), int((req.params or {}).get("band", 1) or 1))
            bbox4 = meta.get("bbox") or [0, 0, 1, 1]           # [w,s,e,n] en 4326
            coords = meta.get("image_coordinates") or meta.get("coords")
            if not coords:
                w, s, e, n = bbox4; coords = [[w, n], [e, n], [e, s], [w, s]]
            if tool == "contours":
                return contours_from_array(band, bbox4, req.params or {})
            tr = meta.get("transform") or [1, 0, 0, 0, -1, 0]
            arr, mask = compute_on_array(tool, band, tr[0], tr[4], req.params or {})
            vis = dict(VIS.get(tool) or {"min": float(arr[~mask].min()) if (~mask).any() else 0, "max": float(arr[~mask].max()) if (~mask).any() else 1, "palette": ["#276419", "#addd8e", "#ffffbf", "#fdae61", "#a50026", "#ffffff"]})
            ov = req.vis_params_override or {}
            if ov.get("palette"): vis["palette"] = ["#" + c.lstrip("#") for c in ov["palette"]]
            if ov.get("min") is not None: vis["min"] = ov["min"]
            if ov.get("max") is not None: vis["max"] = ov["max"]
            import numpy as _np
            rgba = _colormap(_np.where(mask, _np.nan, arr), vis, None)
            return {"status": "ok", "tool": tool, "engine": "local", "png_b64": _png_b64(rgba), "image_coordinates": coords, "bbox": bbox4, "vis_params": vis}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"Erreur calcul local (MNT importé)/{tool} : {e}")

    # ── Courbes de niveau → sortie VECTORIELLE (GeoJSON, exportable) ──
    if tool == "contours":
        try:
            return extract_contours(ee, region, req.params or {}, _dem_asset(req.dem_source or "SRTM_30m"))
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"Erreur courbes de niveau : {e}")

    # ── Outils calculés EN LOCAL (hydrologie hors GEE) → overlay image ──
    if tool in LOCAL_TOOLS:
        try:
            return run_local(ee, tool, region, req.params or {}, _dem_asset(req.dem_source or "SRTM_30m"))
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"Erreur calcul local/{tool} : {e}")

    if tool not in VIS:
        raise HTTPException(422, f"Outil non supporté : {tool}")

    try:
        dem = ee.Image(_dem_asset(req.dem_source or "SRTM_30m")).select(0)

        img = _build_image(ee, tool, dem, req.params or {}, region)
        if region is not None:
            img = img.clip(region)

        vis = dict(VIS[tool])
        ov = req.vis_params_override or {}
        if ov.get("palette"):
            vis["palette"] = ["#" + c.lstrip("#") for c in ov["palette"]]
        if ov.get("min") is not None: vis["min"] = ov["min"]
        if ov.get("max") is not None: vis["max"] = ov["max"]
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


class InterpolateRequest(BaseModel):
    points_geojson: dict                       # FeatureCollection de points
    field:          str                        # attribut numérique à interpoler
    bbox:           List[float]                # [west, south, east, north]
    resolution:     Optional[int] = 120        # cellules par côté
    method:         Optional[str] = "kriging"  # kriging | idw


@router.post("/geotiff")
def whitebox_geotiff(req: WhiteboxRunRequest):
    """Export GeoTIFF d'un outil Whitebox GEE (re-calcul + getDownloadURL)."""
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    ee = get_ee()
    tool = (req.tool or "").lower()
    if tool in LOCAL_TOOLS or tool not in VIS:
        raise HTTPException(422, f"Export GeoTIFF non supporté pour : {tool}")
    region = None
    if req.roi_geojson:
        try:
            region = ee.Geometry(req.roi_geojson.get("geometry", req.roi_geojson))
        except Exception:
            region = None
    if region is None and req.bbox and len(req.bbox) == 4:
        w, s, e, n = req.bbox
        region = ee.Geometry.BBox(w, s, e, n)
    if region is None:
        raise HTTPException(422, "Une emprise (ROI) est requise pour l'export GeoTIFF.")
    try:
        dem = ee.Image(_dem_asset(req.dem_source or "SRTM_30m")).select(0)
        img = _build_image(ee, tool, dem, req.params or {}, region).clip(region)
        scale = int((req.params or {}).get("export_scale", 90) or 90)
        url = img.getDownloadURL({"region": region, "scale": scale, "format": "GEO_TIFF", "crs": "EPSG:4326"})
        return {"status": "ok", "download_url": url, "tool": tool}
    except Exception as e:
        raise HTTPException(500, f"Erreur export GeoTIFF/{tool} : {e}")


@router.post("/interpolate")
def whitebox_interpolate(req: InterpolateRequest):
    """Interpolation d'une couche de points (kriging/IDW) → surface raster (overlay image)."""
    try:
        return interpolate_surface(req.points_geojson, req.field, req.bbox, req.resolution, req.method)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erreur interpolation : {e}")
