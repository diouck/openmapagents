"""
gee_routes.py — FastAPI router pour Google Earth Engine
Intégration dans agent.py :
    from gee_routes import router as gee_router
    app.include_router(gee_router)

Installation :
    pip install earthengine-api

Authentification (choisir une méthode) :
    A. Service Account (prod) :
       ee.Initialize(credentials=ee.ServiceAccountCredentials(EMAIL, KEY_FILE))
    B. Utilisateur (dev) :
       ee.Authenticate()  # une seule fois en terminal
       ee.Initialize()
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import json

router = APIRouter(prefix="/api/gee", tags=["gee"])

# ── Initialisation GEE ────────────────────────────────────────
_gee_ready = False

def init_gee():
    global _gee_ready
    if _gee_ready:
        return True
    try:
        import ee

        # Token stocké par `earthengine authenticate` — aucune config manuelle
        # GEE cherche automatiquement dans ~/.config/earthengine/credentials
        try:
            ee.Initialize()            # tente sans projet explicite
        except Exception:
            # Si GEE demande un projet Cloud, le lire depuis la variable d'env
            # ou utiliser le projet par défaut du token
            import os
            project = os.environ.get("GEE_PROJECT", "")
            if project:
                ee.Initialize(project=project)
            else:
                ee.Initialize()        # laisse GEE déduire le projet

        _gee_ready = True
        print("✓ Google Earth Engine initialisé")
        return True
    except Exception as e:
        print(f"✗ GEE init error: {e}")
        return False

# ── Catalogue des datasets ────────────────────────────────────
DATASETS = {
    "sentinel2": {
        "label": "Sentinel-2 (10m)",
        "collection": "COPERNICUS/S2_SR_HARMONIZED",
        "cloud_property": "CLOUDY_PIXEL_PERCENTAGE",
        "indices": ["RGB", "NDVI", "NDWI", "NDBI", "EVI", "False Color (NIR)"],
        "temporal": "5 jours",
    },
    "landsat9": {
        "label": "Landsat 9 (30m)",
        "collection": "LANDSAT/LC09/C02/T1_L2",
        "cloud_property": "CLOUD_COVER",
        "indices": ["RGB", "NDVI", "NDWI", "LST (température)"],
        "temporal": "16 jours",
    },
    "landsat8": {
        "label": "Landsat 8 (30m)",
        "collection": "LANDSAT/LC08/C02/T1_L2",
        "cloud_property": "CLOUD_COVER",
        "indices": ["RGB", "NDVI", "NDWI", "LST (température)"],
        "temporal": "16 jours",
    },
    "modis_lst": {
        "label": "MODIS LST Température (1km)",
        "collection": "MODIS/061/MOD11A1",
        "cloud_property": None,
        "indices": ["LST Jour", "LST Nuit"],
        "temporal": "1 jour",
    },
    "modis_ndvi": {
        "label": "MODIS NDVI (500m)",
        "collection": "MODIS/061/MOD13A1",
        "cloud_property": None,
        "indices": ["NDVI", "EVI"],
        "temporal": "16 jours",
    },
    "worldcover": {
        "label": "ESA WorldCover 2021 (10m)",
        "collection": "ESA/WorldCover/v200",
        "cloud_property": None,
        "indices": ["Occupation du sol"],
        "temporal": "Annuel",
    },
    "sentinel1": {
        "label": "Sentinel-1 SAR (10m)",
        "collection": "COPERNICUS/S1_GRD",
        "cloud_property": None,
        "indices": ["VV", "VH", "VV/VH"],
        "temporal": "6-12 jours",
    },
    "hansen": {
        "label": "Global Forest Watch (30m)",
        "collection": "UMD/hansen/global_forest_change_2023_v1_11",
        "cloud_property": None,
        "indices": ["Couverture forêt 2000", "Perte forêt", "Gain forêt"],
        "temporal": "Annuel",
    },
    "era5": {
        "label": "ERA5 Climat mensuel (11km)",
        "collection": "ECMWF/ERA5_LAND/MONTHLY_AGGR",
        "cloud_property": None,
        "indices": ["Température air", "Précipitations", "Humidité"],
        "temporal": "Mensuel",
    },
    "srtm": {
        "label": "SRTM Relief (30m)",
        "collection": "USGS/SRTMGL1_003",
        "cloud_property": None,
        "indices": ["Élévation", "Pente", "Ombrage"],
        "temporal": "Statique",
    },
}

# ── Paramètres de visualisation par dataset+indice ────────────
VIS_PARAMS = {
    # Sentinel-2
    ("sentinel2", "RGB"):                  {"bands": ["B4","B3","B2"], "min": 0, "max": 3000, "gamma": 1.4},
    ("sentinel2", "NDVI"):                 {"palette": ["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"], "min": -0.2, "max": 0.8},
    ("sentinel2", "NDWI"):                 {"palette": ["#8B4513","#DEB887","#F5DEB3","#ffffff","#AED6F1","#3498DB","#1A5276"], "min": -0.5, "max": 0.5},
    ("sentinel2", "NDBI"):                 {"palette": ["#1a9850","#fee08b","#d73027"], "min": -0.5, "max": 0.5},
    ("sentinel2", "EVI"):                  {"palette": ["#d73027","#fdae61","#fee08b","#d9ef8b","#66bd63","#1a9850"], "min": -0.2, "max": 0.8},
    ("sentinel2", "False Color (NIR)"):    {"bands": ["B8","B4","B3"], "min": 0, "max": 5000},
    # Landsat 8/9
    ("landsat9", "RGB"):                   {"bands": ["SR_B4","SR_B3","SR_B2"], "min": 5000, "max": 25000, "gamma": 1.4},
    ("landsat8", "RGB"):                   {"bands": ["SR_B4","SR_B3","SR_B2"], "min": 5000, "max": 25000, "gamma": 1.4},
    ("landsat9", "NDVI"):                  {"palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"], "min": -0.2, "max": 0.8},
    ("landsat8", "NDVI"):                  {"palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"], "min": -0.2, "max": 0.8},
    ("landsat9", "NDWI"):                  {"palette": ["#8B4513","#ffffff","#1A5276"], "min": -0.5, "max": 0.5},
    ("landsat8", "NDWI"):                  {"palette": ["#8B4513","#ffffff","#1A5276"], "min": -0.5, "max": 0.5},
    ("landsat9", "LST (température)"):     {"palette": ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"], "min": 270, "max": 320},
    ("landsat8", "LST (température)"):     {"palette": ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"], "min": 270, "max": 320},
    # MODIS
    ("modis_lst", "LST Jour"):             {"palette": ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"], "min": 270, "max": 330},
    ("modis_lst", "LST Nuit"):             {"palette": ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"], "min": 260, "max": 300},
    ("modis_ndvi", "NDVI"):               {"palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"], "min": -2000, "max": 10000},
    ("modis_ndvi", "EVI"):                {"palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"], "min": -2000, "max": 10000},
    # WorldCover
    ("worldcover", "Occupation du sol"):   {
        "min": 10, "max": 100,
        "palette": ["006400","ffbb22","ffff4c","f096ff","fa0000","b4b4b4","f0f0f0","0064c8","0096a0","00cf75","fae6a0"]
    },
    # Sentinel-1 SAR
    ("sentinel1", "VV"):                   {"bands": ["VV"], "min": -20, "max": 0},
    ("sentinel1", "VH"):                   {"bands": ["VH"], "min": -25, "max": -5},
    ("sentinel1", "VV/VH"):               {"bands": ["VV","VH","VV"], "min": [-20,-25,-20], "max": [0,-5,0]},
    # Hansen Forest
    ("hansen", "Couverture forêt 2000"):   {"bands": ["treecover2000"], "palette": ["#ffffff","#1a9850"], "min": 0, "max": 100},
    ("hansen", "Perte forêt"):             {"bands": ["lossyear"], "palette": ["#ffffe5","#f7fcb9","#d9f0a3","#addd8e","#78c679","#41ab5d","#238443","#006837","#004529"], "min": 0, "max": 23},
    ("hansen", "Gain forêt"):              {"bands": ["gain"], "palette": ["#ffffff","#00ff00"], "min": 0, "max": 1},
    # ERA5
    ("era5", "Température air"):           {"bands": ["temperature_2m"], "palette": ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"], "min": 250, "max": 310},
    ("era5", "Précipitations"):            {"bands": ["total_precipitation_sum"], "palette": ["#ffffff","#AED6F1","#2980B9","#1A5276"], "min": 0, "max": 0.3},
    # SRTM
    ("srtm", "Élévation"):                {"palette": ["#313695","#74add1","#e0f3f8","#fee090","#f46d43","#a50026"], "min": 0, "max": 3000},
    ("srtm", "Pente"):                    {"palette": ["#ffffff","#fdae61","#d73027"], "min": 0, "max": 60},
    ("srtm", "Ombrage"):                  {"palette": ["#000000","#ffffff"], "min": 0, "max": 255},
}

# ── Models ────────────────────────────────────────────────────
class TileRequest(BaseModel):
    dataset:    str
    index:      str
    date_start: str              # YYYY-MM-DD
    date_end:   str              # YYYY-MM-DD
    bbox:       Optional[List[float]] = None  # [west, south, east, north]
    cloud_max:  Optional[float] = 20.0
    composite:  Optional[str]   = "least_cloudy"  # "least_cloudy" | "median" | "mosaic"

class DatesRequest(BaseModel):
    dataset:   str
    bbox:      Optional[List[float]] = None
    date_start: str = "2020-01-01"
    date_end:   str = "2024-12-31"
    cloud_max:  Optional[float] = 30.0

# ── Endpoints ─────────────────────────────────────────────────


@router.post("/dates")
def get_available_dates(req: DatesRequest):
    """Retourne les dates disponibles pour un dataset dans une bbox"""
    if not init_gee():
        raise HTTPException(503, "Google Earth Engine non initialisé")
    try:
        import ee
        ds = DATASETS.get(req.dataset)
        if not ds:
            raise HTTPException(404, f"Dataset inconnu: {req.dataset}")

        col = ee.ImageCollection(ds["collection"]) \
            .filterDate(req.date_start, req.date_end)

        if req.bbox:
            w, s, e, n = req.bbox
            col = col.filterBounds(ee.Geometry.BBox(w, s, e, n))

        if ds.get("cloud_property") and req.cloud_max is not None:
            col = col.filter(ee.Filter.lte(ds["cloud_property"], req.cloud_max))

        # Récupérer les dates (max 200 images)
        dates_list = col.limit(200).aggregate_array("system:time_start").getInfo()
        import datetime
        dates = sorted(set([
            datetime.datetime.fromtimestamp(d / 1000).strftime("%Y-%m-%d")
            for d in dates_list if d
        ]))

        return {"dates": dates, "count": len(dates)}
    except Exception as e:
        raise HTTPException(500, str(e))

@router.post("/tiles")
def get_tiles(req: TileRequest):
    """Génère une URL de tuiles MapLibre pour un dataset/indice/date"""
    if not init_gee():
        raise HTTPException(503, "Google Earth Engine non initialisé")
    try:
        import ee
        ds = DATASETS.get(req.dataset)
        if not ds:
            raise HTTPException(404, f"Dataset inconnu: {req.dataset}")

        col = ee.ImageCollection(ds["collection"]) \
            .filterDate(req.date_start, req.date_end)

        if req.bbox:
            w, s, e, n = req.bbox
            col = col.filterBounds(ee.Geometry.BBox(w, s, e, n))

        if ds.get("cloud_property") and req.cloud_max is not None:
            col = col.filter(ee.Filter.lte(ds["cloud_property"], req.cloud_max))

        # Composer l'image selon la méthode choisie
        if req.composite == "median":
            image = col.median()
        elif req.composite == "mosaic":
            image = col.mosaic()
        else:  # least_cloudy
            if ds.get("cloud_property"):
                image = col.sort(ds["cloud_property"]).first()
            else:
                image = col.sort("system:time_start", False).first()

        # Calculer l'indice si nécessaire
        image = compute_index(image, req.dataset, req.index)

        # Paramètres de visualisation
        vis = VIS_PARAMS.get((req.dataset, req.index), {})

        # Générer l'URL de tuiles MapLibre
        # getMapId retourne un objet avec tile_fetcher
        map_id   = image.getMapId(vis)
        fetcher  = map_id.get("tile_fetcher")
        if fetcher and hasattr(fetcher, "url_format"):
            tile_url = fetcher.url_format
        else:
            # Fallback pour les nouvelles versions de l'API ee
            tile_url = map_id.get("urlFormat") or str(map_id)

        # Infos sur l'image sélectionnée
        try:
            info = image.getInfo()
            properties = info.get("properties", {})
            date_str = properties.get("DATE_ACQUIRED") or \
                       properties.get("system:index", req.date_start)
        except:
            date_str = req.date_start

        return {
            "tile_url": tile_url,
            "dataset":  req.dataset,
            "index":    req.index,
            "date":     date_str,
            "name":     f"{ds['label']} — {req.index}",
        }

    except Exception as e:
        raise HTTPException(500, f"GEE error: {str(e)}")

@router.get("/health")
def gee_health():
    """Test de connexion GEE + infos de diagnostic"""
    ok = init_gee()
    info = {"status": "ok" if ok else "error", "ready": ok}
    if ok:
        try:
            import ee
            # Test rapide : lire une image SRTM (toujours disponible)
            dem = ee.Image("USGS/SRTMGL1_003")
            val = dem.getInfo()
            info["test"] = "ok"
            info["message"] = "Connexion GEE fonctionnelle"
        except Exception as e:
            info["test"] = "error"
            info["message"] = str(e)
    else:
        info["message"] = (
            "Token non trouvé. Lancez 'earthengine authenticate' "
            "dans un terminal, puis redémarrez FastAPI."
        )
    return info

@router.get("/datasets")
def list_datasets():
    """Liste tous les datasets disponibles (ne nécessite pas GEE connecté)"""
    return {k: {**v, "id": k} for k, v in DATASETS.items()}

# ── Calcul des indices ─────────────────────────────────────────
def compute_index(image, dataset, index):
    """Calcule l'indice demandé sur une image GEE"""
    import ee

    try:
        if index == "NDVI":
            if dataset in ("sentinel2",):
                nir, red = "B8", "B4"
            elif dataset in ("landsat8", "landsat9"):
                nir, red = "SR_B5", "SR_B4"
            elif dataset == "modis_ndvi":
                return image.select("NDVI")
            else:
                return image
            return image.normalizedDifference([nir, red]).rename("NDVI")

        elif index == "NDWI":
            if dataset == "sentinel2":
                green, nir = "B3", "B8"
            elif dataset in ("landsat8", "landsat9"):
                green, nir = "SR_B3", "SR_B5"
            else:
                return image
            return image.normalizedDifference([green, nir]).rename("NDWI")

        elif index == "NDBI":
            if dataset == "sentinel2":
                swir, nir = "B11", "B8"
            else:
                return image
            return image.normalizedDifference([swir, nir]).rename("NDBI")

        elif index == "EVI":
            if dataset == "sentinel2":
                nir = image.select("B8").multiply(0.0001)
                red = image.select("B4").multiply(0.0001)
                blue = image.select("B2").multiply(0.0001)
            elif dataset in ("landsat8", "landsat9"):
                nir = image.select("SR_B5").multiply(0.0000275).add(-0.2)
                red = image.select("SR_B4").multiply(0.0000275).add(-0.2)
                blue = image.select("SR_B2").multiply(0.0000275).add(-0.2)
            else:
                return image.select("EVI")
            evi = nir.subtract(red).multiply(2.5).divide(
                nir.add(red.multiply(6)).subtract(blue.multiply(7.5)).add(1)
            ).rename("EVI")
            return evi

        elif index == "LST (température)" or index.startswith("LST"):
            if dataset in ("landsat8", "landsat9"):
                # Convertir en Kelvin
                lst = image.select("ST_B10").multiply(0.00341802).add(149.0).rename("LST")
                return lst
            elif dataset == "modis_lst":
                band = "LST_Day_1km" if "Jour" in index else "LST_Night_1km"
                return image.select(band).multiply(0.02).rename("LST")

        elif index == "Pente":
            terrain = ee.Terrain.products(image)
            return terrain.select("slope")

        elif index == "Ombrage":
            terrain = ee.Terrain.products(image)
            return terrain.select("hillshade")

        elif index == "VV" and dataset == "sentinel1":
            return image.select("VV")
        elif index == "VH" and dataset == "sentinel1":
            return image.select("VH")

        # Défaut : retourner l'image telle quelle (RGB, WorldCover, etc.)
        return image

    except Exception as e:
        print(f"compute_index error ({dataset}/{index}): {e}")
        return image
