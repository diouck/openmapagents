"""
gee_routes.py — FastAPI router Google Earth Engine
Ajout dans agent.py :
    from gee_routes import router as gee_router
    app.include_router(gee_router)
Installation :
    pip install earthengine-api google-auth google-auth-httplib2
"""
import ee
import os
import json
from ee import oauth as ee_oauth
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter(prefix="/api/gee", tags=["gee"])

# ── State global ──────────────────────────────────────────────
_gee_ready = False

# ── Datasets catalogue ────────────────────────────────────────
DATASETS = {
    "sentinel2": {"label":"Sentinel-2 (10m)","collection":"COPERNICUS/S2_SR_HARMONIZED","cloud_property":"CLOUDY_PIXEL_PERCENTAGE","indices":["RGB","NDVI","NDWI","NDBI","EVI","False Color (NIR)"],"temporal":"5 jours"},
    "landsat9":  {"label":"Landsat 9 (30m)","collection":"LANDSAT/LC09/C02/T1_L2","cloud_property":"CLOUD_COVER","indices":["RGB","NDVI","NDWI","LST (température)"],"temporal":"16 jours"},
    "landsat8":  {"label":"Landsat 8 (30m)","collection":"LANDSAT/LC08/C02/T1_L2","cloud_property":"CLOUD_COVER","indices":["RGB","NDVI","NDWI","LST (température)"],"temporal":"16 jours"},
    "modis_lst": {"label":"MODIS LST Temp. (1km)","collection":"MODIS/061/MOD11A1","cloud_property":None,"indices":["LST Jour","LST Nuit"],"temporal":"1 jour"},
    "modis_ndvi":{"label":"MODIS NDVI (500m)","collection":"MODIS/061/MOD13A1","cloud_property":None,"indices":["NDVI","EVI"],"temporal":"16 jours"},
    "worldcover":{"label":"ESA WorldCover 2021 (10m)","collection":"ESA/WorldCover/v200","cloud_property":None,"indices":["Occupation du sol"],"temporal":"Annuel","static":True},
    "sentinel1": {"label":"Sentinel-1 SAR (10m)","collection":"COPERNICUS/S1_GRD","cloud_property":None,"indices":["VV","VH","VV/VH"],"temporal":"6-12 jours"},
    "hansen":    {"label":"Global Forest Watch (30m)","collection":None,"cloud_property":None,"indices":["Couverture forêt 2000","Perte forêt","Gain forêt"],"temporal":"Annuel","static":True},
    "era5":      {"label":"ERA5 Climat mensuel (11km)","collection":"ECMWF/ERA5_LAND/MONTHLY_AGGR","cloud_property":None,"indices":["Température air","Précipitations"],"temporal":"Mensuel"},
    "srtm":      {"label":"SRTM Relief (30m)","collection":None,"cloud_property":None,"indices":["Élévation","Pente","Ombrage"],"temporal":"Statique","static":True},
    "canopy_height": {"label":"Hauteur de Canopée WRI/Meta 2020 (~1m)","collection":None,"cloud_property":None,"indices":["Hauteur canopée"],"temporal":"Statique","static":True},
}

VIS_PARAMS = {
    ("sentinel2","RGB"):                {"bands":["B4","B3","B2"],"min":0,"max":3000,"gamma":1.4},
    ("sentinel2","NDVI"):               {"palette":["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"],"min":-0.2,"max":0.8},
    ("sentinel2","NDWI"):               {"palette":["#8B4513","#DEB887","#ffffff","#AED6F1","#1A5276"],"min":-0.5,"max":0.5},
    ("sentinel2","NDBI"):               {"palette":["#1a9850","#fee08b","#d73027"],"min":-0.5,"max":0.5},
    ("sentinel2","EVI"):                {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-0.2,"max":0.8},
    ("sentinel2","False Color (NIR)"):  {"bands":["B8","B4","B3"],"min":0,"max":5000},
    ("landsat9","RGB"):                 {"bands":["SR_B4","SR_B3","SR_B2"],"min":5000,"max":25000,"gamma":1.4},
    ("landsat8","RGB"):                 {"bands":["SR_B4","SR_B3","SR_B2"],"min":5000,"max":25000,"gamma":1.4},
    ("landsat9","NDVI"):                {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-0.2,"max":0.8},
    ("landsat8","NDVI"):                {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-0.2,"max":0.8},
    ("landsat9","NDWI"):                {"palette":["#8B4513","#ffffff","#1A5276"],"min":-0.5,"max":0.5},
    ("landsat8","NDWI"):                {"palette":["#8B4513","#ffffff","#1A5276"],"min":-0.5,"max":0.5},
    ("landsat9","LST (température)"):   {"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":270,"max":320},
    ("landsat8","LST (température)"):   {"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":270,"max":320},
    ("modis_lst","LST Jour"):           {"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":270,"max":330},
    ("modis_lst","LST Nuit"):           {"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":260,"max":300},
    ("modis_ndvi","NDVI"):              {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-0.2,"max":1.0},  # FIX: plage après scale 0.0001
    ("modis_ndvi","EVI"):               {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-0.2,"max":1.0},  # FIX: plage après scale 0.0001
    ("worldcover","Occupation du sol"): {"min":10,"max":100,"palette":["006400","ffbb22","ffff4c","f096ff","fa0000","b4b4b4","f0f0f0","0064c8","0096a0","00cf75","fae6a0"]},
    ("sentinel1","VV"):                 {"bands":["VV"],"min":-20,"max":0},
    ("sentinel1","VH"):                 {"bands":["VH"],"min":-25,"max":-5},
    ("sentinel1","VV/VH"):              {"bands":["VV","VH","VV"],"min":[-20,-25,-20],"max":[0,-5,0]},
    ("hansen","Couverture forêt 2000"): {"bands":["treecover2000"],"palette":["#ffffff","#1a9850"],"min":0,"max":100},
    ("hansen","Perte forêt"):           {"bands":["lossyear"],"palette":["#ffffe5","#78c679","#004529"],"min":0,"max":23},
    ("hansen","Gain forêt"):            {"bands":["gain"],"palette":["#ffffff","#00ff00"],"min":0,"max":1},
    ("era5","Température air"):         {"bands":["temperature_2m"],"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":250,"max":310},
    ("era5","Précipitations"):          {"bands":["total_precipitation_sum"],"palette":["#ffffff","#AED6F1","#1A5276"],"min":0,"max":0.3},
    ("srtm","Élévation"):               {"palette":["#313695","#74add1","#e0f3f8","#fee090","#f46d43","#a50026"],"min":0,"max":3000},
    ("srtm","Pente"):                   {"palette":["#ffffff","#fdae61","#d73027"],"min":0,"max":60},
    ("srtm","Ombrage"):                 {"palette":["#000000","#ffffff"],"min":0,"max":255},
    ("canopy_height","Hauteur canopée"): {"palette":["#ffffff","#d9f0a3","#addd8e","#78c679","#41ab5d","#238443","#005a32"],"min":0,"max":30},
    
}

# ── Initialisation GEE via service account ────────────────────
# ── Init GEE — délégué à gee_auth (Windows + Linux) ─────────────────────────
from gee_auth import get_ee, init_gee


def init_gee_Linux():
    global _gee_ready
    if _gee_ready:
        return True
    try:
        import ee
        credentials = ee.ServiceAccountCredentials(
            email='mcpopenmapagents@laravelauth-477918.iam.gserviceaccount.com',
            key_file='/var/www/google/laravelauth-477918-9f353bf03d0b.json'
        )
        ee.Initialize(credentials)
        _gee_ready = True
        print("✓ GEE initialisé via service account")
        return True
    except Exception as e:
        print(f"✗ GEE init error: {e}")
        return False




def init_gee1():
    cred_path = os.path.expanduser("~/.config/earthengine/credentials")
    with open(cred_path) as f:
        raw = json.load(f)

    creds = Credentials(
        token=None,
        refresh_token=raw["refresh_token"],
        client_id=ee_oauth.CLIENT_ID,
        client_secret=ee_oauth.CLIENT_SECRET,
        token_uri=ee_oauth.TOKEN_URI,
        scopes=ee_oauth.SCOPES
    )
    creds.refresh(Request())
    ee.Initialize(credentials=creds, project="laravelauth-477918")
    return True




# ── Calcul des indices ────────────────────────────────────────
def compute_index(image, dataset, index):
    try:
        import ee
        if index == "NDVI":
            if dataset == "sentinel2":        return image.normalizedDifference(["B8","B4"]).rename("NDVI")
            if dataset in ("landsat8","landsat9"): return image.normalizedDifference(["SR_B5","SR_B4"]).rename("NDVI")
            if dataset == "modis_ndvi":        return image.select("NDVI").multiply(0.0001).rename("NDVI")  # FIX: facteur échelle DN→[-1,1]
        if index == "NDWI":
            if dataset == "sentinel2":        return image.normalizedDifference(["B3","B8"]).rename("NDWI")
            if dataset in ("landsat8","landsat9"): return image.normalizedDifference(["SR_B3","SR_B5"]).rename("NDWI")
        if index == "NDBI":
            if dataset == "sentinel2":        return image.normalizedDifference(["B11","B8"]).rename("NDBI")
        if index == "EVI":
            if dataset == "sentinel2":
                nir=image.select("B8").multiply(0.0001); red=image.select("B4").multiply(0.0001); blue=image.select("B2").multiply(0.0001)
                return nir.subtract(red).multiply(2.5).divide(nir.add(red.multiply(6)).subtract(blue.multiply(7.5)).add(1)).rename("EVI")
            if dataset == "modis_ndvi": return image.select("EVI").multiply(0.0001).rename("EVI")  # FIX: facteur échelle DN→[-1,1]
        if index in ("LST (température)","LST Jour","LST Nuit"):
            if dataset in ("landsat8","landsat9"):
                return image.select("ST_B10").multiply(0.00341802).add(149.0).subtract(273.15).rename("LST")  # FIX: Kelvin→°C
            if dataset == "modis_lst":
                band = "LST_Day_1km" if "Jour" in index or "température" in index else "LST_Night_1km"
                return image.select(band).multiply(0.02).subtract(273.15).rename("LST")  # FIX: Kelvin→°C
        if index == "Pente":
            return ee.Terrain.products(image).select("slope")
        if index == "Ombrage":
            return ee.Terrain.products(image).select("hillshade")
        if index == "VV":  return image.select("VV")
        if index == "VH":  return image.select("VH")
    except Exception as e:
        print(f"compute_index error ({dataset}/{index}): {e}")
    return image

# ── Models ────────────────────────────────────────────────────
class TileRequest(BaseModel):
    dataset:    str
    index:      str
    date_start: str
    date_end:   str
    bbox:             Optional[List[float]] = None
    cloud_max:        Optional[float] = 20.0
    composite:        Optional[str]   = "least_cloudy"  # tuiles : image la moins nuageuse par défaut
    roi_geojson:      Optional[dict]  = None   # GeoJSON geometry pour masquer les tuiles
    vis_params_override: Optional[dict] = None  # Écrase palette/min/max depuis LayerPanel

class DatesRequest(BaseModel):
    dataset:    str
    bbox:       Optional[List[float]] = None
    roi_geojson: Optional[dict] = None
    date_start: str = "2023-01-01"
    date_end:   str = "2024-12-31"
    cloud_max:  Optional[float] = 30.0

# ── Helper ROI GeoJSON → ee.Geometry ─────────────────────────
def geojson_to_ee_geometry(geojson: dict):
    """Convertit un dict GeoJSON geometry en ee.Geometry."""
    import ee
    t = geojson.get("type", "")
    if t == "Polygon":
        return ee.Geometry.Polygon(geojson["coordinates"])
    if t == "MultiPolygon":
        return ee.Geometry.MultiPolygon(geojson["coordinates"])
    if t == "GeometryCollection":
        geoms = [geojson_to_ee_geometry(g) for g in geojson.get("geometries", [])]
        return ee.Geometry.MultiPolygon([g for g in geoms])
    # Fallback bbox via bounds
    coords = geojson.get("coordinates", [])
    if coords:
        flat = coords[0] if t == "Polygon" else []
        if flat:
            xs = [c[0] for c in flat]; ys = [c[1] for c in flat]
            return ee.Geometry.BBox(min(xs), min(ys), max(xs), max(ys))
    raise ValueError(f"Type GeoJSON non supporté: {t}")

# ── Endpoints ─────────────────────────────────────────────────
@router.get("/health")
def gee_health():
    ok = init_gee()
    return {"status": "ok" if ok else "error", "ready": ok,
            "message": "GEE connecté" if ok else "GEE non disponible"}

@router.get("/datasets")
def gee_datasets():
    return {k: {**v, "id": k} for k, v in DATASETS.items()}

@router.post("/dates")
def gee_dates(req: DatesRequest):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    try:
        import ee, datetime
        ds = DATASETS.get(req.dataset)
        if not ds:
            raise HTTPException(404, f"Dataset inconnu: {req.dataset}")

        # Datasets statiques : pas de dates temporelles
        if ds.get("static") or not ds.get("collection"):
            return {"dates": [], "count": 0, "static": True,
                    "message": "Dataset statique — pas de sélection de date nécessaire"}

        col = ee.ImageCollection(ds["collection"]).filterDate(req.date_start, req.date_end)
        if req.roi_geojson:
            try:
                col = col.filterBounds(geojson_to_ee_geometry(req.roi_geojson).bounds())
            except Exception:
                if req.bbox:
                    w,s,e,n = req.bbox
                    col = col.filterBounds(ee.Geometry.BBox(w,s,e,n))
        elif req.bbox:
            w,s,e,n = req.bbox
            col = col.filterBounds(ee.Geometry.BBox(w,s,e,n))
        if ds.get("cloud_property") and req.cloud_max is not None and req.cloud_max < 100:
            cp = ds["cloud_property"]
            col = col.filter(ee.Filter.And(
                ee.Filter.notNull([cp]),
                ee.Filter.lte(cp, req.cloud_max)
            ))

        ts_list = col.limit(200).aggregate_array("system:time_start").getInfo()
        dates = sorted(set([
            datetime.datetime.fromtimestamp(d/1000).strftime("%Y-%m-%d")
            for d in ts_list if d
        ]))
        return {"dates": dates, "count": len(dates)}
    except Exception as e:
        raise HTTPException(500, str(e))

@router.post("/tiles")
def gee_tiles(req: TileRequest):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    try:
        import ee
        ds = DATASETS.get(req.dataset)
        if not ds:
            raise HTTPException(404, f"Dataset inconnu: {req.dataset}")

        # Collections temporelles seulement (SRTM/Hansen/WorldCover sont statiques)
        if ds.get("collection") and not ds.get("static"):
            col = ee.ImageCollection(ds["collection"]).filterDate(req.date_start, req.date_end)

            # filterBounds TOUJOURS sur bbox englobante — jamais géométrie exacte.
            # La géométrie exacte est utilisée uniquement pour .clip() plus bas.
            # Raison : filterBounds avec contour précis rate les tuiles à cheval sur le ROI.
            if req.roi_geojson:
                try:
                    col = col.filterBounds(geojson_to_ee_geometry(req.roi_geojson).bounds())
                except Exception:
                    if req.bbox:
                        w, s, e, n = req.bbox
                        col = col.filterBounds(ee.Geometry.BBox(w, s, e, n))
            elif req.bbox:
                w, s, e, n = req.bbox
                col = col.filterBounds(ee.Geometry.BBox(w, s, e, n))

            # Filtre nuages : uniquement si < 100% ET propriété présente sur les images.
            # lte() filtre les images dont la propriété est null → on ajoute notNull.
            if ds.get("cloud_property") and req.cloud_max is not None and req.cloud_max < 100:
                cp = ds["cloud_property"]
                col = col.filter(ee.Filter.And(
                    ee.Filter.notNull([cp]),
                    ee.Filter.lte(cp, req.cloud_max)
                ))
        else:
            col = None  # dataset statique

        # ── Datasets statiques : pas besoin de vérifier la collection ──
        STATIC_DATASETS = {"srtm", "hansen", "worldcover", "canopy_height"}

        if req.dataset not in STATIC_DATASETS:
            # ── Vérifier que la collection n'est pas vide ─────
            size = col.size().getInfo()
            if size == 0:
                # Retry SANS filtre nuages mais AVEC même zone
                col_retry = ee.ImageCollection(ds["collection"]).filterDate(req.date_start, req.date_end)
                if req.roi_geojson:
                    try:
                        col_retry = col_retry.filterBounds(geojson_to_ee_geometry(req.roi_geojson).bounds())
                    except Exception:
                        if req.bbox:
                            w, s, e, n = req.bbox
                            col_retry = col_retry.filterBounds(ee.Geometry.BBox(w, s, e, n))
                elif req.bbox:
                    w, s, e, n = req.bbox
                    col_retry = col_retry.filterBounds(ee.Geometry.BBox(w, s, e, n))
                size_retry = col_retry.size().getInfo()
                if size_retry == 0:
                    raise HTTPException(422,
                        f"Aucune image disponible pour '{ds['label']}' "
                        f"entre {req.date_start} et {req.date_end} sur cette zone. "
                        f"Essayez une période plus longue."
                    )
                # Images disponibles mais filtrées par nuages → prendre sans filtre
                col = col_retry
                size = size_retry
        else:
            size = 1  # statique = toujours disponible

        # ── Datasets statiques = ee.Image directement ──────────
        STATIC_IMAGES = {
            "srtm":          "USGS/SRTMGL1_003",
            "hansen":        "UMD/hansen/global_forest_change_2023_v1_11",
            "canopy_height": "projects/meta-forest-monitoring-okw37/assets/CanopyHeight",
        }
        STATIC_COLLECTIONS = {
            "worldcover": "ESA/WorldCover/v200",
        }

        if req.dataset in STATIC_IMAGES:
            asset_id = STATIC_IMAGES[req.dataset]
            if req.dataset == "canopy_height":
                image = ee.ImageCollection(asset_id).mosaic()
            else:
                image = ee.Image(asset_id)
        elif req.dataset in STATIC_COLLECTIONS:
            image = ee.ImageCollection(STATIC_COLLECTIONS[req.dataset]).first()
        elif req.composite == "median":
            image = col.median()
        elif req.composite == "mosaic":
            image = col.mosaic()
        else:
            # Moins nuageux en premier, sinon plus récent
            if ds.get("cloud_property"):
                image = col.sort(ds["cloud_property"]).first()
            else:
                image = col.sort("system:time_start", False).first()

        # ── Calcul de l'indice ────────────────────────────────
        image = compute_index(image, req.dataset, req.index)

        # ── Clip ROI : masquer l'image au contour exact du polygone ──
        # Sans ce clip, GEE retourne des tuiles mondiales même en mode "couche"
        roi_geom = None
        if req.roi_geojson:
            try:
                roi_geom = geojson_to_ee_geometry(req.roi_geojson)
                image = image.clip(roi_geom)
            except Exception as clip_err:
                print(f"ROI clip warning: {clip_err}")
                # Fallback : clip via bbox si le clip exact échoue
                if req.bbox:
                    w, s, e, n = req.bbox
                    image = image.clip(ee.Geometry.BBox(w, s, e, n))
        elif req.bbox:
            # Même sans ROI polygon, clipper sur la bbox améliore les perfs
            w, s, e, n = req.bbox
            image = image.clip(ee.Geometry.BBox(w, s, e, n))
        if req.dataset == "canopy_height":
            image = image.updateMask(image.gte(1))

        # ligne 367
        vis_default = VIS_PARAMS.get((req.dataset, req.index), {})

        # ── Génération URL tuiles ─────────────────────────────
        # vis_params_override permet de styler depuis le LayerPanel sans recharger
        vis_default = VIS_PARAMS.get((req.dataset, req.index), {})
        if req.vis_params_override:
            vis = {**vis_default, **req.vis_params_override}
        else:
            vis = vis_default
        map_id   = image.getMapId(vis)
        fetcher  = map_id.get("tile_fetcher")
        tile_url = fetcher.url_format if (fetcher and hasattr(fetcher, "url_format")) else map_id.get("urlFormat", "")

        if not tile_url:
            raise HTTPException(500, "Impossible de générer l'URL de tuiles GEE")

        # Date de l'image sélectionnée
        try:
            img_date = image.date().format("YYYY-MM-dd").getInfo()
        except Exception:
            img_date = req.date_start

        # Bbox du clip (ROI ou bbox carte) pour cadrer la légende
        clip_bbox = None
        if req.roi_geojson:
            try:
                clip_bbox = geojson_to_ee_geometry(req.roi_geojson).bounds().getInfo()["coordinates"][0]
                xs = [c[0] for c in clip_bbox]; ys = [c[1] for c in clip_bbox]
                clip_bbox = [min(xs), min(ys), max(xs), max(ys)]
            except Exception:
                clip_bbox = req.bbox
        elif req.bbox:
            clip_bbox = req.bbox

        return {
            "tile_url":  tile_url,
            "dataset":   req.dataset,
            "index":     req.index,
            "name":      f"{ds['label']} — {req.index} ({img_date})",
            "date":      img_date,
            "count":     size,
            "vis_params": vis,
            "clip_bbox":  clip_bbox,
        }
    except HTTPException:
        raise
    except Exception as e:
        msg = str(e)
        # Messages d'erreur lisibles
        if "empty" in msg.lower() or "no images" in msg.lower():
            raise HTTPException(422, f"Collection vide pour la période {req.date_start} → {req.date_end}. Élargissez la période.")
        if "Permission" in msg or "403" in msg:
            raise HTTPException(403, "Accès refusé GEE. Vérifiez que l'API Earth Engine est activée.")
        raise HTTPException(500, f"Erreur GEE : {msg}")

# ── Canopée GEE (WRI/Meta High Resolution Canopy Height) ────────────────────

import logging
log = logging.getLogger("elevation")


class CanopyRequest(BaseModel):
    """Corps de la requête POST /api/gee/canopy.

    - ``points`` : liste de coordonnées [longitude, latitude].
    - ``scale``  : résolution d'échantillonnage en mètres (défaut 10 m pour
                   la vitesse ; descendre à 1 m pour une précision maximale
                   mais sur de petites zones seulement).
    - ``dataset_priority`` : ordre de tentative des datasets.  Valeurs
                             acceptées : ``"eth"`` (ETH Lang 10 m) et
                             ``"meta"`` (WRI/Meta ~1 m).  Le premier de la
                             liste qui répond est utilisé ; le second sert
                             de fallback automatique.
    """
    points: List[List[float]]                  # [[lon, lat], ...]
    scale: Optional[int] = 10                  # mètres
    dataset_priority: Optional[List[str]] = ["eth", "meta"]


# Registre des datasets canopée disponibles
_CANOPY_DATASETS = {
    "eth":  {
        "asset":  "users/nlang/ETH_GlobalCanopyHeight_2020_10m_v1",
        "bands":  ["b1"],          # hauteur canopée en mètres
        "label":  "ETH GlobalCanopyHeight 2020 (Lang et al., 10 m)",
    },
    "meta": {
        "asset":  "projects/meta-forest-monitoring-okw37/assets/CanopyHeight",
        "bands":  ["cover_code"],  # hauteur canopée en mètres
        "label":  "WRI/Meta High Resolution Canopy Height 2020 (~1 m)",
    },
}


def get_canopy_gee(sampled: list, scale: int = 10,
                   dataset_priority: list | None = None) -> dict:
    """Extrait la hauteur de canopée depuis WRI/Meta High Resolution Canopy
    Height 2020 ou ETH GlobalCanopyHeight 2020 (Lang et al.).

    Parameters
    ----------
    sampled:
        Liste de coordonnées ``[longitude, latitude]``.
    scale:
        Résolution GEE en mètres (10 m par défaut pour la vitesse ;
        descendre à 1 m uniquement sur de petites zones).
    dataset_priority:
        Ordre de tentative des datasets (``"eth"`` → ``"meta"`` par défaut).
        Le premier dataset disponible est utilisé ; l'autre sert de fallback.

    Returns
    -------
    dict avec les clés :
    - ``"values"``  : liste de hauteurs en mètres (float, 0.0 si absent).
    - ``"dataset"`` : identifiant du dataset effectivement utilisé.
    - ``"label"``   : libellé lisible du dataset.
    - ``"n_valid"`` : nombre de points ayant une valeur > 0.
    """
    if dataset_priority is None:
        dataset_priority = ["eth", "meta"]

    try:
        from gee_auth import get_ee
        ee = get_ee()

        # ── Sélection du dataset avec fallback automatique ────────────────
        canopy_img   = None
        used_key     = None
        used_bands   = None

        for key in dataset_priority:
            info = _CANOPY_DATASETS.get(key)
            if not info:
                continue
            try:
                if key == "meta":
                    img = ee.ImageCollection(info["asset"]).mosaic()
                else:
                    img = ee.Image(info["asset"])
                # Vérification légère : récupérer les métadonnées (lève si asset absent)
                img.bandNames().getInfo()
                canopy_img = img
                used_key   = key
                used_bands = info["bands"]
                log.info(f"Canopee GEE: dataset '{key}' ({info['label']}) chargé")
                break
            except Exception as ds_err:
                log.warning(f"Canopee GEE: dataset '{key}' indisponible — {ds_err}")

        if canopy_img is None:
            raise RuntimeError(
                "Aucun dataset canopée disponible "
                f"(essayés : {dataset_priority})"
            )

        # ── Construction de la FeatureCollection de points ────────────────
        features = [
            ee.Feature(ee.Geometry.Point([float(p[0]), float(p[1])]), {"idx": i})
            for i, p in enumerate(sampled)
        ]
        fc = ee.FeatureCollection(features)

        # ── Échantillonnage ───────────────────────────────────────────────
        sampled_fc = canopy_img.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.first(),
            scale=scale,
        )
        result = sampled_fc.getInfo()

        # ── Extraction des hauteurs ───────────────────────────────────────
        alt_map: dict[int, float] = {}
        for feat in result.get("features", []):
            props = feat.get("properties", {})
            idx   = props.get("idx")
            # La valeur peut s'appeler "first", ou du nom de la bande selon la version GEE
            z = (
                props.get("first")
                or props.get("cover_code")
                or props.get("b1")
                or next((props.get(b) for b in used_bands if props.get(b) is not None), None)
            )
            if idx is not None:
                alt_map[int(idx)] = round(float(z), 1) if z is not None else 0.0

        vals    = [alt_map.get(i, 0.0) for i in range(len(sampled))]
        n_valid = sum(1 for v in vals if v > 0)
        log.info(
            f"Canopee GEE [{used_key}]: {n_valid}/{len(sampled)} points "
            f"avec valeur (scale={scale}m)"
        )

        return {
            "values":  vals,
            "dataset": used_key,
            "label":   _CANOPY_DATASETS[used_key]["label"],
            "n_valid": n_valid,
        }

    except Exception as ex:
        log.warning(f"Canopy GEE error: {ex}")
        return {
            "values":  [0.0] * len(sampled),
            "dataset": None,
            "label":   "Erreur — fallback zéro",
            "n_valid": 0,
        }


@router.post("/canopy")
def gee_canopy(req: CanopyRequest):
    """Retourne la hauteur de canopée (en mètres) pour une liste de points.

    **Body JSON** :
    ```json
    {
        "points": [[lon1, lat1], [lon2, lat2], ...],
        "scale": 10,
        "dataset_priority": ["eth", "meta"]
    }
    ```

    **Réponse** :
    ```json
    {
        "values":  [12.3, 0.0, 8.7, ...],
        "dataset": "eth",
        "label":   "ETH GlobalCanopyHeight 2020 (Lang et al., 10 m)",
        "n_valid": 2,
        "n_points": 3
    }
    ```
    Les hauteurs sont en **mètres** (`float`).  
    `0.0` signifie absence de donnée (sol nu, eau, zone non couverte).
    """
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")

    if not req.points:
        raise HTTPException(422, "La liste 'points' ne peut pas être vide")

    if len(req.points) > 5000:
        raise HTTPException(
            422,
            f"Trop de points ({len(req.points)}) — maximum 5 000 par requête. "
            "Découpez votre liste."
        )

    result = get_canopy_gee(
        sampled=req.points,
        scale=req.scale or 10,
        dataset_priority=req.dataset_priority or ["eth", "meta"],
    )

    return {
        **result,
        "n_points": len(req.points),
    }


@router.get("/debug")
def gee_debug():
    import os, pathlib
    paths = {
        "USERPROFILE": os.environ.get("USERPROFILE",""),
        "APPDATA":     os.environ.get("APPDATA",""),
        "HOME":        str(pathlib.Path.home()),
    }
    cred_paths = [
        pathlib.Path(os.environ.get("USERPROFILE","")) / ".config" / "earthengine" / "credentials",
        pathlib.Path.home() / ".config" / "earthengine" / "credentials",
    ]
    return {
        "env":   paths,
        "creds": {str(p): p.exists() for p in cred_paths},
        "ready": _gee_ready,
    }



"""
À ajouter dans gee_routes.py — endpoint POST /api/gee/index/stats

Supporte tous les indices : NDVI, NDWI, NDBI, EVI, LST Jour/Nuit/température,
MODIS NDVI/EVI, ESA WorldCover, Hansen Forest Watch, ERA5, SRTM, Canopée WRI/Meta
"""
from pydantic import BaseModel
from typing import Optional, List

class IndexStatsRequest(BaseModel):
    dataset:     str
    index:       str
    bbox:        Optional[List[float]] = None
    roi_geojson: Optional[dict]        = None
    date_start:  Optional[str]         = None
    date_end:    Optional[str]         = None
    cloud_max:   Optional[float]       = 20.0
    composite:   Optional[str]         = "median"
    scale:       Optional[int]         = None


# ── Helpers ───────────────────────────────────────────────────

def _pixel_km2(scale): return (scale * scale) / 1_000_000

def _count_range(ee, img, band, lo, hi, region, scale):
    mask = img.select(band).gte(lo).And(img.select(band).lt(hi))
    r = img.select(band).updateMask(mask).reduceRegion(
        reducer=ee.Reducer.count(), geometry=region,
        scale=scale, maxPixels=1e9, bestEffort=True,
    ).getInfo()
    return r.get(band, 0) or 0

def _build_classes(ee, img, band, defs, region, scale, total):
    out = []
    for label, (lo, hi), color in defs:
        n   = _count_range(ee, img, band, lo, hi, region, scale)
        km2 = round(n * _pixel_km2(scale), 4)
        out.append({ "label": label, "pct": round(n / max(total, 1) * 100, 1),
                     "km2": km2, "ha": round(km2 * 100, 1), "color": color })
    return out

def _global_stats(ee, img, band, region, scale):
    r = img.select(band).reduceRegion(
        reducer=ee.Reducer.mean()
            .combine(ee.Reducer.min(),    sharedInputs=True)
            .combine(ee.Reducer.max(),    sharedInputs=True)
            .combine(ee.Reducer.stdDev(), sharedInputs=True)
            .combine(ee.Reducer.count(),  sharedInputs=True),
        geometry=region, scale=scale, maxPixels=1e9, bestEffort=True,
    ).getInfo()
    get = lambda k: r.get(f"{band}_{k}") or r.get(k) or 0
    return get("mean"), get("min"), get("max"), get("stdDev"), int(get("count") or 0)

def _get_composite(ee, dataset, date_start, date_end, cloud_max, composite, region):
    COLLS = {
        "sentinel2":  ("COPERNICUS/S2_SR_HARMONIZED",   10,    "CLOUDY_PIXEL_PERCENTAGE"),
        "landsat9":   ("LANDSAT/LC09/C02/T1_L2",        30,    "CLOUD_COVER"),
        "landsat8":   ("LANDSAT/LC08/C02/T1_L2",        30,    "CLOUD_COVER"),
        "modis_ndvi": ("MODIS/061/MOD13A1",              500,   None),
        "modis_lst":  ("MODIS/061/MOD11A1",              1000,  None),
        "era5":       ("ECMWF/ERA5_LAND/MONTHLY_AGGR",  11000, None),
    }
    cid, scale, cp = COLLS[dataset]
    col = ee.ImageCollection(cid).filterDate(date_start, date_end).filterBounds(region)
    if cp and cloud_max is not None and cloud_max < 100:
        col = col.filter(ee.Filter.And(ee.Filter.notNull([cp]), ee.Filter.lte(cp, cloud_max)))
    if composite == "median":   img = col.median()
    elif composite == "mosaic": img = col.mosaic()
    else: img = col.sort(cp or "system:time_start", not bool(cp)).first()
    return img, scale

def _compute_band(ee, img, dataset, index):
    if index == "NDVI":
        if dataset == "sentinel2":              return img.normalizedDifference(["B8","B4"]).rename("NDVI"), "NDVI"
        if dataset in ("landsat8","landsat9"):  return img.normalizedDifference(["SR_B5","SR_B4"]).rename("NDVI"), "NDVI"
        if dataset == "modis_ndvi":             return img.select("NDVI").multiply(0.0001), "NDVI"
    if index == "EVI":
        if dataset == "sentinel2":
            nir=img.select("B8").multiply(0.0001); red=img.select("B4").multiply(0.0001); blue=img.select("B2").multiply(0.0001)
            return nir.subtract(red).multiply(2.5).divide(nir.add(red.multiply(6)).subtract(blue.multiply(7.5)).add(1)).rename("EVI"), "EVI"
        if dataset == "modis_ndvi": return img.select("EVI").multiply(0.0001), "EVI"
    if index == "NDWI":
        if dataset == "sentinel2":             return img.normalizedDifference(["B3","B8"]).rename("NDWI"), "NDWI"
        if dataset in ("landsat8","landsat9"): return img.normalizedDifference(["SR_B3","SR_B5"]).rename("NDWI"), "NDWI"
    if index == "NDBI":
        if dataset == "sentinel2": return img.normalizedDifference(["B11","B8"]).rename("NDBI"), "NDBI"
    if index in ("LST Jour","LST (température)"):
        if dataset in ("landsat8","landsat9"): return img.select("ST_B10").multiply(0.00341802).add(149.0).subtract(273.15).rename("LST"), "LST"
        if dataset == "modis_lst":             return img.select("LST_Day_1km").multiply(0.02).subtract(273.15).rename("LST"), "LST"
    if index == "LST Nuit":
        if dataset == "modis_lst": return img.select("LST_Night_1km").multiply(0.02).subtract(273.15).rename("LST"), "LST"
    if index == "Température air":
        return img.select("temperature_2m").subtract(273.15).rename("TEMP"), "TEMP"
    if index == "Précipitations":
        return img.select("total_precipitation_sum").multiply(1000).rename("PRECIP"), "PRECIP"
    if index == "Humidité":
        return img.select("dewpoint_temperature_2m").subtract(273.15).rename("HUM"), "HUM"
    raise ValueError(f"Index {index!r} non supporté pour {dataset!r}")


# ── Classes de chaque index ───────────────────────────────────

NDVI_DIST = [("< 0 · eau/sol",(-1,.0),"#d73027"),("0–0.2 · sol nu",(.0,.2),"#fdae61"),("0.2–0.4 · arbustes",(.2,.4),"#d9ef8b"),("0.4–0.6 · prairie",(.4,.6),"#a6d96a"),("0.6–0.8 · forêt",(.6,.8),"#66bd63"),("> 0.8 · dense",(.8,1.),"#1a9850")]
NDVI_SURF = [("Sol nu / minéral",(-1,.2),"#fdae61"),("Arbustes / cultures",(.2,.4),"#d9ef8b"),("Prairie / herbe",(.4,.6),"#a6d96a"),("Forêt dense",(.6,1.),"#1a9850")]
NDWI_DIST = [("< -0.3 · sec/sol",(-1,-.3),"#8B4513"),("-0.3–0 · aride",(-.3,.0),"#DEB887"),("0–0.2 · semi-humide",(.0,.2),"#d9ef8b"),("0.2–0.4 · humide",(.2,.4),"#AED6F1"),("> 0.4 · eau libre",(.4,1.),"#1A5276")]
NDWI_SURF = [("Sol sec",(-1,.0),"#8B4513"),("Semi-aride",(.0,.2),"#DEB887"),("Zone humide",(.2,.4),"#AED6F1"),("Eau libre",(.4,1.),"#1A5276")]
NDBI_DIST = [("< -0.2 · végétation",(-1,-.2),"#1a9850"),("-0.2–0 · sol/mixte",(-.2,.0),"#a6d96a"),("0–0.1 · péri-urbain",(.0,.1),"#fee08b"),("0.1–0.3 · urbain",(.1,.3),"#f46d43"),("> 0.3 · dense bâti",(.3,1.),"#d73027")]
NDBI_SURF = [("Végétation",(-1,.0),"#1a9850"),("Sol nu / mixte",(.0,.1),"#fee08b"),("Péri-urbain",(.1,.2),"#f46d43"),("Zone urbaine",(.2,1.),"#d73027")]
EVI_DIST  = [("< 0 · eau/sol nu",(-.2,.0),"#d73027"),("0–0.2 · faible",(.0,.2),"#fdae61"),("0.2–0.4 · modéré",(.2,.4),"#d9ef8b"),("0.4–0.6 · élevé",(.4,.6),"#66bd63"),("> 0.6 · très élevé",(.6,1.),"#1a9850")]
EVI_SURF  = [("Sol nu / minéral",(-.2,.1),"#fdae61"),("Végétation faible",(.1,.3),"#d9ef8b"),("Végétation modérée",(.3,.5),"#66bd63"),("Végétation dense",(.5,1.),"#1a9850")]
LST_DIST  = [("< 5°C · très froid",(-50,5),"#040274"),("5–15°C · froid",(5,15),"#3288bd"),("15–25°C · tempéré",(15,25),"#abdda4"),("25–30°C · chaud",(25,30),"#fdae61"),("30–35°C · très chaud",(30,35),"#d53e4f"),("> 35°C · extrême",(35,80),"#9e0142")]
LST_SURF  = [("Zone froide (< 15°C)",(-50,15),"#3288bd"),("Zone tempérée (15–25°C)",(15,25),"#abdda4"),("Zone chaude (25–35°C)",(25,35),"#fdae61"),("Zone très chaude (> 35°C)",(35,80),"#9e0142")]
ERA5T_DIST= [("< 0°C",(-100,0),"#040274"),("0–10°C",(0,10),"#3288bd"),("10–20°C",(10,20),"#abdda4"),("20–30°C",(20,30),"#fdae61"),("> 30°C",(30,100),"#d53e4f")]
PRECIP_DIST=[("< 10 mm · très sec",(0,10),"#f7fbff"),("10–30 mm · sec",(10,30),"#c6dbef"),("30–60 mm · modéré",(30,60),"#6baed6"),("60–100 mm · humide",(60,100),"#2171b5"),("> 100 mm · très humide",(100,9999),"#084594")]
HUM_DIST  = [("< 30% · très sec",(0,30),"#f7fbff"),("30–50% · sec",(30,50),"#c6dbef"),("50–70% · modéré",(50,70),"#6baed6"),("70–85% · humide",(70,85),"#2171b5"),("> 85% · saturé",(85,100),"#084594")]
SRTM_ELEV = [("< 50 m",(0,50),"#313695"),("50–200 m",(50,200),"#74add1"),("200–500 m",(200,500),"#e0f3f8"),("500–1000 m",(500,1000),"#fee090"),("1000–2000 m",(1000,2000),"#f46d43"),("> 2000 m",(2000,9999),"#a50026")]
SRTM_SLOPE= [("0–3° · plat",(0,3),"#ffffff"),("3–10° · légère",(3,10),"#fdae61"),("10–20° · modérée",(10,20),"#f46d43"),("20–35° · forte",(20,35),"#d73027"),("> 35° · très forte",(35,90),"#9e0142")]
SRTM_HILL = [("< 64 · ombre",(0,64),"#000000"),("64–128 · mi-ombre",(64,128),"#444444"),("128–192 · mi-soleil",(128,192),"#aaaaaa"),("> 192 · soleil",(192,256),"#ffffff")]
CANOPY_DIST=[("1–5 m",(1,5),"#d9f0a3"),("5–10 m",(5,10),"#addd8e"),("10–20 m",(10,20),"#78c679"),("20–30 m",(20,30),"#41ab5d"),("> 30 m",(30,100),"#005a32")]
CANOPY_SURF=[("Arbres bas (0–10 m)",(1,10),"#addd8e"),("Canopée moyenne (10–20 m)",(10,20),"#78c679"),("Canopée haute (20–30 m)",(20,30),"#41ab5d"),("Très grande canopée (> 30 m)",(30,100),"#005a32")]

# WorldCover classes (valeurs entières)
WC_CLASSES = [
    (10,  "Forêt arborée",         "#006400"),
    (20,  "Arbustes",              "#ffbb22"),
    (30,  "Prairie / herbe",       "#ffff4c"),
    (40,  "Cultures",              "#f096ff"),
    (50,  "Bâti / urbain",         "#fa0000"),
    (60,  "Sol nu / sparse",       "#b4b4b4"),
    (70,  "Neige / glace",         "#f0f0f0"),
    (80,  "Eau",                   "#0064c8"),
    (90,  "Zone humide herbacée",  "#0096a0"),
    (95,  "Mangrove",              "#00cf75"),
    (100, "Mousse / lichen",       "#fae6a0"),
]
WC_SURF = [
    ("Végétation naturelle", [10,20,30,90,95,100], "#006400"),
    ("Terres agricoles",     [40],                  "#f096ff"),
    ("Zones artificielles",  [50],                  "#fa0000"),
    ("Eau & zones humides",  [80,90],               "#0064c8"),
    ("Sol nu / neige",       [60,70],               "#b4b4b4"),
]


@router.post("/index/stats")
def gee_index_stats(req: IndexStatsRequest):
    """Calcule les statistiques d'un index GEE (générique pour tous les indices)."""
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    if not req.bbox and not req.roi_geojson:
        raise HTTPException(422, "Fournir 'bbox' ou 'roi_geojson'")

    try:
        from gee_auth import get_ee
        ee = get_ee()

        # ── Région ────────────────────────────────────────────
        if req.roi_geojson:
            try:    region = geojson_to_ee_geometry(req.roi_geojson)
            except Exception:
                if req.bbox: w,s,e,n=req.bbox; region=ee.Geometry.BBox(w,s,e,n)
                else: raise HTTPException(422, "ROI invalide")
        else:
            w,s,e,n=req.bbox; region=ee.Geometry.BBox(w,s,e,n)

        date_start = req.date_start or "2024-01-01"
        date_end   = req.date_end   or "2025-01-01"

        # ══════════════════════════════════════════════════════
        # CAS SPÉCIAUX : datasets statiques
        # ══════════════════════════════════════════════════════

        # ── ESA WorldCover ────────────────────────────────────
        if req.dataset == "worldcover" and req.index == "Occupation du sol":
            scale = req.scale or 10
            wc = ee.ImageCollection("ESA/WorldCover/v200").first().select("Map")
            total_pixels = wc.reduceRegion(
                reducer=ee.Reducer.count(), geometry=region, scale=scale, maxPixels=1e9, bestEffort=True
            ).getInfo().get("Map", 1) or 1
            total_km2 = round(total_pixels * _pixel_km2(scale), 3)

            # Classes WorldCover
            classes = []
            for val, label, color in WC_CLASSES:
                mask = wc.eq(val)
                count = wc.updateMask(mask).reduceRegion(
                    reducer=ee.Reducer.count(), geometry=region, scale=scale, maxPixels=1e9, bestEffort=True
                ).getInfo().get("Map", 0) or 0
                km2 = round(count * _pixel_km2(scale), 4)
                classes.append({"label": label, "pct": round(count/max(total_pixels,1)*100,1), "km2": km2, "ha": round(km2*100,1), "color": color})

            # Grandes catégories
            surfaces = []
            for name, vals, color in WC_SURF:
                total_c = sum(
                    (wc.updateMask(wc.eq(v)).reduceRegion(
                        reducer=ee.Reducer.count(), geometry=region, scale=scale, maxPixels=1e9, bestEffort=True
                    ).getInfo().get("Map", 0) or 0) for v in vals
                )
                km2 = round(total_c * _pixel_km2(scale), 4)
                surfaces.append({"name": name, "km2": km2, "ha": round(km2*100,1), "color": color})

            veg_vals = [10,20,30,90,95,100]
            veg_km2  = sum(c["km2"] for c in classes if any(c["label"] == lbl for _,lbl,_ in WC_CLASSES if _ in [x[2] for x in WC_CLASSES] and _[0] in veg_vals))
            # Simplification : sommer directement par index
            veg_km2  = sum(c["km2"] for c in classes[:3]) + sum(c["km2"] for i,c in enumerate(classes) if i in [8,9,10])
            urb_km2  = classes[4]["km2"] if len(classes) > 4 else 0
            veg_pct  = round(veg_km2 / max(total_km2,0.001) * 100, 1)
            urb_pct  = round(urb_km2 / max(total_km2,0.001) * 100, 1)

            return {
                "n_classes": len([c for c in classes if c["pct"] > 0]),
                "veg_pct": veg_pct, "urban_pct": urb_pct,
                "total_km2": total_km2, "valid_pixels": total_pixels,
                "scale": scale, "year": 2021,
                "classes": [c for c in classes if c["pct"] > 0],
                "surfaces": surfaces,
                "mean": 0, "min": 0, "max": 0,
            }

        # ── Hansen Forest Watch ───────────────────────────────
        if req.dataset == "hansen":
            scale = req.scale or 30
            hansen = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
            total_pixels = hansen.select("treecover2000").reduceRegion(
                reducer=ee.Reducer.count(), geometry=region, scale=scale, maxPixels=1e9, bestEffort=True
            ).getInfo().get("treecover2000", 1) or 1
            total_km2 = round(total_pixels * _pixel_km2(scale), 3)

            if req.index == "Couverture forêt 2000":
                tc = hansen.select("treecover2000")
                # Stats globales couvert
                st = tc.reduceRegion(reducer=ee.Reducer.mean().combine(ee.Reducer.count(),sharedInputs=True),
                    geometry=region, scale=scale, maxPixels=1e9, bestEffort=True).getInfo()
                cover_mean = st.get("treecover2000_mean") or 0
                cover_pct  = round(float(cover_mean), 1)

                # Distribution couvert
                tc_defs = [("0–25% · faible",(0,25),"#d9ef8b"),("25–50% · modéré",(25,50),"#66bd63"),("50–75% · dense",(50,75),"#238443"),("> 75% · très dense",(75,101),"#005a32")]
                classes  = _build_classes(ee, tc, "treecover2000", tc_defs, region, scale, total_pixels)

                # Perte totale
                loss = hansen.select("loss")
                loss_count = loss.reduceRegion(reducer=ee.Reducer.sum(),
                    geometry=region, scale=scale, maxPixels=1e9, bestEffort=True).getInfo().get("loss",0) or 0
                loss_km2   = round(loss_count * _pixel_km2(scale), 4)
                loss_pct   = round(loss_km2 / max(total_km2,0.001) * 100, 1)

                # Gain
                gain = hansen.select("gain")
                gain_count = gain.reduceRegion(reducer=ee.Reducer.sum(),
                    geometry=region, scale=scale, maxPixels=1e9, bestEffort=True).getInfo().get("gain",0) or 0
                gain_km2   = round(gain_count * _pixel_km2(scale), 4)
                gain_pct   = round(gain_km2 / max(total_km2,0.001) * 100, 1)

                cover_km2  = round(cover_pct / 100 * total_km2, 3)
                net_km2    = round(cover_km2 - loss_km2 + gain_km2, 3)
                net_pct    = round(net_km2 / max(total_km2,0.001) * 100, 1)

                # Perte par décennie
                lossyr = hansen.select("lossyear")
                decades = [("2000–2010",(1,11),"#fdae61"),("2010–2020",(11,21),"#f46d43"),("2020–2023",(21,24),"#d73027")]
                loss_by_decade = []
                for dlabel,(lo,hi),dcolor in decades:
                    dmask = lossyr.gte(lo).And(lossyr.lt(hi))
                    dcount = lossyr.updateMask(dmask).reduceRegion(
                        reducer=ee.Reducer.count(), geometry=region, scale=scale, maxPixels=1e9, bestEffort=True
                    ).getInfo().get("lossyear",0) or 0
                    dkm2 = round(dcount * _pixel_km2(scale), 4)
                    dpct = round(dkm2 / max(loss_km2,0.001) * 100, 1)
                    loss_by_decade.append({"label":dlabel,"pct":dpct,"km2":dkm2,"ha":round(dkm2*100,1),"color":dcolor})

                surfaces = [
                    {"name":"Forêt initiale (2000)", "km2":cover_km2,       "ha":round(cover_km2*100,1),   "color":"#1a9850"},
                    {"name":"Perte 2000–2023",        "km2":loss_km2,        "ha":round(loss_km2*100,1),    "color":"#d73027"},
                    {"name":"Gain (reboisement)",     "km2":gain_km2,        "ha":round(gain_km2*100,1),    "color":"#66bd63"},
                    {"name":"Forêt nette actuelle",   "km2":max(net_km2,0),  "ha":round(max(net_km2,0)*100,1),"color":"#238443"},
                ]
                return {"cover_pct":cover_pct,"loss_pct":loss_pct,"gain_pct":gain_pct,"net_pct":net_pct,
                        "total_km2":total_km2,"valid_pixels":total_pixels,"scale":scale,
                        "classes":classes,"surfaces":surfaces,"loss_by_decade":loss_by_decade,
                        "mean":cover_mean,"min":0,"max":100}

            if req.index == "Perte forêt":
                lossyr = hansen.select("lossyear")
                loss   = hansen.select("loss")
                loss_count = loss.reduceRegion(reducer=ee.Reducer.sum(),
                    geometry=region, scale=scale, maxPixels=1e9, bestEffort=True).getInfo().get("loss", 0) or 0
                loss_km2 = round(loss_count * _pixel_km2(scale), 4)
                loss_pct = round(loss_km2 / max(total_km2, 0.001) * 100, 1)

                decades = [("2000–2010",(1,11),"#fdae61"),("2010–2020",(11,21),"#f46d43"),("2020–2023",(21,24),"#d73027")]
                classes = []
                for dlabel,(lo,hi),dcolor in decades:
                    dmask  = lossyr.gte(lo).And(lossyr.lt(hi))
                    dcount = lossyr.updateMask(dmask).reduceRegion(
                        reducer=ee.Reducer.count(), geometry=region, scale=scale, maxPixels=1e9, bestEffort=True
                    ).getInfo().get("lossyear", 0) or 0
                    dkm2 = round(dcount * _pixel_km2(scale), 4)
                    dpct = round(dkm2 / max(loss_km2, 0.001) * 100, 1)
                    classes.append({"label":dlabel,"pct":dpct,"km2":dkm2,"ha":round(dkm2*100,1),"color":dcolor})

                surfaces = [
                    {"name":"Perte totale 2000–2023","km2":loss_km2,"ha":round(loss_km2*100,1),"color":"#d73027"},
                    {"name":"Zone non affectée","km2":round(total_km2-loss_km2,3),"ha":round((total_km2-loss_km2)*100,1),"color":"#1a9850"},
                ]
                return {"loss_pct":loss_pct,"total_km2":total_km2,"valid_pixels":total_pixels,"scale":scale,
                        "classes":classes,"surfaces":surfaces,"mean":loss_pct,"min":0,"max":100}

            if req.index == "Gain forêt":
                gain       = hansen.select("gain")
                gain_count = gain.reduceRegion(reducer=ee.Reducer.sum(),
                    geometry=region, scale=scale, maxPixels=1e9, bestEffort=True).getInfo().get("gain", 0) or 0
                gain_km2 = round(gain_count * _pixel_km2(scale), 4)
                gain_pct = round(gain_km2 / max(total_km2, 0.001) * 100, 1)

                classes = [
                    {"label":"Gain (reboisement)","pct":gain_pct,"km2":gain_km2,"ha":round(gain_km2*100,1),"color":"#00ff00"},
                    {"label":"Pas de gain","pct":round(100-gain_pct,1),"km2":round(total_km2-gain_km2,3),"ha":round((total_km2-gain_km2)*100,1),"color":"#ffffff"},
                ]
                surfaces = [
                    {"name":"Zone reboisée","km2":gain_km2,"ha":round(gain_km2*100,1),"color":"#00ff00"},
                    {"name":"Zone non reboisée","km2":round(total_km2-gain_km2,3),"ha":round((total_km2-gain_km2)*100,1),"color":"#1a9850"},
                ]
                return {"gain_pct":gain_pct,"total_km2":total_km2,"valid_pixels":total_pixels,"scale":scale,
                        "classes":classes,"surfaces":surfaces,"mean":gain_pct,"min":0,"max":1}

        # ── SRTM ─────────────────────────────────────────────
        if req.dataset == "srtm":
            scale = req.scale or 30
            srtm  = ee.Image("USGS/SRTMGL1_003")

            if req.index == "Élévation":
                img, band = srtm, "elevation"
                mean,mn,mx,std,n = _global_stats(ee, img, band, region, scale)
                total_km2 = round(n * _pixel_km2(scale), 3)
                classes  = _build_classes(ee, img, band, SRTM_ELEV,  region, scale, n)
                surfaces = _build_classes(ee, img, band, [
                    ("Plaine (< 200 m)",    (0,200),   "#74add1"),
                    ("Collines (200–500 m)",(200,500), "#e0f3f8"),
                    ("Montagne (500–2000 m)",(500,2000),"#f46d43"),
                    ("Haute montagne (> 2000 m)",(2000,9999),"#a50026"),
                ], region, scale, n)
                return {"mean":round(mean,1),"min":round(mn,1),"max":round(mx,1),"amplitude":round(mx-mn,1),
                        "total_km2":total_km2,"valid_pixels":n,"scale":scale,
                        "classes":classes,"surfaces":surfaces}

            if req.index == "Pente":
                slope = ee.Terrain.slope(srtm)
                mean,mn,mx,std,n = _global_stats(ee, slope, "slope", region, scale)
                total_km2 = round(n * _pixel_km2(scale), 3)
                flat_n  = _count_range(ee, slope, "slope", 0,  5,  region, scale)
                steep_n = _count_range(ee, slope, "slope", 20, 90, region, scale)
                classes  = _build_classes(ee, slope, "slope", SRTM_SLOPE, region, scale, n)
                surfaces = _build_classes(ee, slope, "slope", [
                    ("Terrain plat (0–5°)",   (0,5),   "#ffffff"),
                    ("Légère (5–15°)",         (5,15),  "#fdae61"),
                    ("Modérée (15–25°)",       (15,25), "#f46d43"),
                    ("Forte (> 25°)",          (25,90), "#d73027"),
                ], region, scale, n)
                return {"mean":round(mean,1),"max":round(mx,1),
                        "flat_pct":round(flat_n/max(n,1)*100,1),"steep_pct":round(steep_n/max(n,1)*100,1),
                        "total_km2":total_km2,"valid_pixels":n,"scale":scale,
                        "classes":classes,"surfaces":surfaces}

            if req.index == "Ombrage":
                hill = ee.Terrain.hillshade(srtm)
                mean,mn,mx,std,n = _global_stats(ee, hill, "hillshade", region, scale)
                total_km2 = round(n * _pixel_km2(scale), 3)
                shad_n = _count_range(ee, hill, "hillshade", 0,   128, region, scale)
                expo_n = _count_range(ee, hill, "hillshade", 128, 256, region, scale)
                classes  = _build_classes(ee, hill, "hillshade", SRTM_HILL, region, scale, n)
                return {"mean":round(mean,1),"std":round(std,1),
                        "shadow_pct":round(shad_n/max(n,1)*100,1),"exposed_pct":round(expo_n/max(n,1)*100,1),
                        "total_km2":total_km2,"valid_pixels":n,"scale":scale,
                        "classes":classes,"surfaces":[]}

        # ── Canopée WRI/Meta ──────────────────────────────────
        if req.dataset == "canopy_height" or req.index == "Hauteur canopée":
            scale = req.scale or 10
            canopy = ee.ImageCollection(
                "projects/meta-forest-monitoring-okw37/assets/CanopyHeight"
            ).mosaic().rename("height")
            canopy_masked = canopy.updateMask(canopy.gt(0))
            mean,mn,mx,std,n_valid = _global_stats(ee, canopy_masked, "height", region, scale)
            n_total  = canopy.reduceRegion(reducer=ee.Reducer.count(),
                geometry=region, scale=scale, maxPixels=1e9, bestEffort=True
            ).getInfo().get("height", 1) or 1
            total_km2 = round(n_total * _pixel_km2(scale), 3)
            cover_km2 = round(n_valid * _pixel_km2(scale), 4)
            cover_pct = round(n_valid / max(n_total,1) * 100, 1)
            classes   = _build_classes(ee, canopy_masked, "height", CANOPY_DIST, region, scale, n_valid)
            surfaces  = _build_classes(ee, canopy_masked, "height", CANOPY_SURF, region, scale, n_valid)
            return {"mean":round(mean,1),"min":round(mn,1),"max":round(mx,1),
                    "cover_pct":cover_pct,"cover_km2":cover_km2,
                    "total_km2":total_km2,"valid_pixels":n_valid,"scale":scale,
                    "classes":classes,"surfaces":surfaces}

        # ══════════════════════════════════════════════════════
        # CAS GÉNÉRAL : collections temporelles
        # ══════════════════════════════════════════════════════
        img, auto_scale = _get_composite(ee, req.dataset, date_start, date_end, req.cloud_max, req.composite, region)
        scale = req.scale or auto_scale
        index_img, band = _compute_band(ee, img, req.dataset, req.index)

        mean, mn, mx, std, n_valid = _global_stats(ee, index_img, band, region, scale)
        n_total   = index_img.reduceRegion(reducer=ee.Reducer.count(),
            geometry=region, scale=scale, maxPixels=1e9, bestEffort=True
        ).getInfo().get(band, 1) or 1
        total_km2 = round(n_total * _pixel_km2(scale), 3)

        DIST_MAP = {
            "NDVI": (NDVI_DIST, NDVI_SURF),
            "NDWI": (NDWI_DIST, NDWI_SURF),
            "NDBI": (NDBI_DIST, NDBI_SURF),
            "EVI":  (EVI_DIST,  EVI_SURF),
            "LST":  (LST_DIST,  LST_SURF),
            "TEMP": (ERA5T_DIST, []),
            "PRECIP":(PRECIP_DIST,[]),
            "HUM":  (HUM_DIST,  []),
        }
        dist_defs, surf_defs = DIST_MAP.get(band, ([], []))
        classes  = _build_classes(ee, index_img, band, dist_defs, region, scale, n_total)
        surfaces = _build_classes(ee, index_img, band, surf_defs, region, scale, n_total)

        extra = {}
        if req.index in ("NDVI","EVI"):
            veg_n = sum(_count_range(ee, index_img, band, lo, hi, region, scale) for _,(lo,hi),_ in (NDVI_DIST if req.index=="NDVI" else EVI_DIST) if lo >= 0.2)
            extra.update({"veg_pct":round(veg_n/max(n_total,1)*100,1),"veg_km2":round(veg_n*_pixel_km2(scale),4)})
            if req.index == "EVI": extra["biomass_label"] = "Élevée" if mean>0.4 else "Modérée" if mean>0.2 else "Faible"
        if req.index == "NDWI":
            wn = _count_range(ee, index_img, band, 0.4, 1.0, region, scale)
            extra.update({"water_pct":round(wn/max(n_total,1)*100,1),"water_km2":round(wn*_pixel_km2(scale),4)})
        if req.index == "NDBI":
            un = _count_range(ee, index_img, band, 0.1, 1.0, region, scale)
            extra.update({"urban_pct":round(un/max(n_total,1)*100,1),"urban_km2":round(un*_pixel_km2(scale),4)})
        if req.index in ("LST Jour","LST Nuit","LST (température)","Température air"):
            extra["amplitude"] = round(mx - mn, 2)
        if req.index == "Précipitations":
            extra.update({"total_mm":round(mean,1),"daily_mean":round(mean/30,2),"max_daily":round(mx,1),"rainy_days":round(n_valid/max(n_total,0.001)*30,0)})
        if req.cloud_max is not None:
            extra["cloud_max"] = req.cloud_max

        return {"mean":round(float(mean),3),"min":round(float(mn),3),"max":round(float(mx),3),
                "std":round(float(std),3),"valid_pixels":n_valid,"total_km2":total_km2,
                "scale":scale,"classes":classes,"surfaces":surfaces,**extra}

    except HTTPException: raise
    except Exception as e:
        raise HTTPException(500, f"Erreur stats index GEE : {e}")






# ══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT AGRICULTURE DE PRÉCISION — /api/gee/agri/stats
#
#  Calcule NDVI / NDWI / NDRE / LST sur le polygone exact depuis GEE.
#  Toutes les bandes sont ramenées à la résolution native S2 10m par
#  reproject() avant les réducteurs → cohérence spatiale garantie.
#
#  Pipeline :
#   1. S2 SR composite (médiane, masque nuages SCL) → NDVI(10m), NDWI(10m)
#   2. B8A(20m) et B5(20m) reprojected → 10m → NDRE(10m)
#   3. Landsat 8/9 ST_B10 reprojected → 10m → LST °C (même grille)
#   4. Zones de prescription calculées pixel à pixel (GEE expression)
#      via .where() chaîné — pas d'heuristique, pas de valeur par défaut
# ══════════════════════════════════════════════════════════════════════════════

class AgriStatsRequest(BaseModel):
    roi_geojson:  dict             # GeoJSON geometry (Polygon / Feature)
    date_start:   str              # "YYYY-MM-DD"
    date_end:     str              # "YYYY-MM-DD"
    cloud_max:    Optional[float] = 20.0   # % couverture nuageuse S2 max
    composite:    Optional[str]   = "least_cloudy"  # "least_cloudy" | "median" | "mosaic"
    scale:        Optional[int]   = 10     # résolution de sortie en mètres


def _s2_mask_scl(image):
    """Masque nuages/ombres Sentinel-2 via bande SCL (Scene Classification Layer)."""
    import ee as _ee
    scl = image.select("SCL")
    # Garder : végétation(4), sol nu(5), eau(6), neige(11)
    # Rejeter : défauts(0,1), ombre(3), nuage probable(8,9,10)
    good = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6)).Or(scl.eq(11))
    return image.updateMask(good)


def _compute_agri_indices_gee(ee, region, date_start, date_end, cloud_max,
                               composite, scale_out):
    """
    Retourne (stack: ee.Image multi-bandes, meta: dict) contenant :
      NDVI, NDWI, NDRE, LST (°C) — toutes à scale_out mètres.

    composite : "least_cloudy" | "median" | "mosaic"
      - least_cloudy : image unique la moins nuageuse (date réelle précise)
      - median       : médiane temporelle de toutes les images disponibles
      - mosaic       : mosaïque ordonnée par date la plus récente

    Retourne aussi un dict meta avec :
      n_images, date_used, cloud_pct (pour least_cloudy), cloud_max_used
    """
    import datetime as _dt

    # ── 1. Collection Sentinel-2 SR avec filtre nuages ──────────────────────
    s2_base = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
               .filterDate(date_start, date_end)
               .filterBounds(region))

    # Filtre nuages sur propriété image (pas pixel) — rapide
    s2_filtered = s2_base.filter(ee.Filter.And(
        ee.Filter.notNull(["CLOUDY_PIXEL_PERCENTAGE"]),
        ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", cloud_max)
    ))

    size = s2_filtered.size().getInfo()
    cloud_max_used = cloud_max
    used_filter = True

    if size == 0:
        # Aucune image sous le seuil → on élargit progressivement (50, 80, 100)
        for fallback_max in [50, 80, 100]:
            s2_try = s2_base.filter(ee.Filter.And(
                ee.Filter.notNull(["CLOUDY_PIXEL_PERCENTAGE"]),
                ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", fallback_max)
            ))
            size = s2_try.size().getInfo()
            if size > 0:
                s2_filtered = s2_try
                cloud_max_used = fallback_max
                break

        if size == 0:
            # Vraiment aucune image sur la période
            raise ValueError(
                f"Aucune image Sentinel-2 disponible entre {date_start} et "
                f"{date_end} sur cette zone. Élargissez la période ou le seuil nuages."
            )
        used_filter = False

    # ── Sélection de l'image composite selon le mode ─────────────────────────
    if composite == "least_cloudy":
        # Image unique la moins nuageuse — date précise connue
        s2_sorted = s2_filtered.sort("CLOUDY_PIXEL_PERCENTAGE")
        best_img  = s2_sorted.first()
        s2_img    = _s2_mask_scl(best_img)
        # Récupérer date et % nuages de l'image sélectionnée
        img_props = best_img.toDictionary(["system:time_start", "CLOUDY_PIXEL_PERCENTAGE"]).getInfo()
        ts = img_props.get("system:time_start")
        date_used = _dt.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d") if ts else date_start
        cloud_pct_used = round(float(img_props.get("CLOUDY_PIXEL_PERCENTAGE") or 0), 1)

    elif composite == "mosaic":
        # Mosaïque par date la plus récente (pixels les plus frais en priorité)
        s2_img    = s2_filtered.sort("system:time_start", False).mosaic()
        s2_img    = _s2_mask_scl(s2_img)
        date_used = f"mosaïque {size} img"
        cloud_pct_used = None

    else:  # "median" (défaut de sécurité)
        s2_img    = s2_filtered.map(_s2_mask_scl).median()
        date_used = f"médiane {size} img"
        cloud_pct_used = None

    meta = {
        "n_images":       size,
        "date_used":      date_used,
        "cloud_pct_used": cloud_pct_used,
        "cloud_max_used": cloud_max_used,
        "composite":      composite,
    }

    # ── 2. Calcul indices S2 ────────────────────────────────────────────────
    SCALE_S2 = 0.0001  # DN → réflectance [0,1]

    B4  = s2_img.select("B4").multiply(SCALE_S2)   # Rouge
    B3  = s2_img.select("B3").multiply(SCALE_S2)   # Vert
    B8  = s2_img.select("B8").multiply(SCALE_S2)   # PIR large (10m)

    ndvi = B8.subtract(B4).divide(B8.add(B4)).rename("NDVI")
    ndwi = B3.subtract(B8).divide(B3.add(B8)).rename("NDWI")

    # NDRE : B8A/B5 à 20m natif → reproject → 10m
    crs_10m = s2_img.select("B4").projection()
    B5  = s2_img.select("B5").multiply(SCALE_S2).reproject(crs=crs_10m, scale=scale_out)
    B8A = s2_img.select("B8A").multiply(SCALE_S2).reproject(crs=crs_10m, scale=scale_out)
    ndre = B8A.subtract(B5).divide(B8A.add(B5)).rename("NDRE")

    # ── 3. LST : Landsat 9 → 8 → fallback MODIS ─────────────────────────────
    lst_img  = None
    lst_src  = None  # pour le meta
    for ls_col, ls_name in [("LANDSAT/LC09/C02/T1_L2", "Landsat9"),
                             ("LANDSAT/LC08/C02/T1_L2", "Landsat8")]:
        ls = (ee.ImageCollection(ls_col)
              .filterDate(date_start, date_end)
              .filterBounds(region)
              .filter(ee.Filter.And(
                  ee.Filter.notNull(["CLOUD_COVER"]),
                  ee.Filter.lte("CLOUD_COVER", cloud_max + 10)
              )))
        n_ls = ls.size().getInfo()
        if n_ls > 0:
            if composite == "least_cloudy":
                ls_img_raw = ls.sort("CLOUD_COVER").first()
            elif composite == "mosaic":
                ls_img_raw = ls.sort("system:time_start", False).mosaic()
            else:  # median
                ls_img_raw = ls.median()
            lst_img = (ls_img_raw.select("ST_B10")
                       .multiply(0.00341802).add(149.0).subtract(273.15)
                       .rename("LST")
                       .reproject(crs=crs_10m, scale=scale_out))
            lst_src = f"{ls_name} ({n_ls} img)"
            break

    if lst_img is None:
        # Fallback MODIS LST
        modis = (ee.ImageCollection("MODIS/061/MOD11A1")
                 .filterDate(date_start, date_end)
                 .filterBounds(region)
                 .filter(ee.Filter.lte("QC_Day", 17)))
        if modis.size().getInfo() == 0:
            modis = (ee.ImageCollection("MODIS/061/MOD11A1")
                     .filterDate(date_start, date_end)
                     .filterBounds(region))
        n_modis = modis.size().getInfo()
        if n_modis > 0:
            lst_img = (modis.median()
                       .select("LST_Day_1km")
                       .multiply(0.02).subtract(273.15)
                       .rename("LST")
                       .reproject(crs=crs_10m, scale=scale_out))
            lst_src = f"MODIS ({n_modis} img)"
        else:
            lst_img = ndvi.multiply(0).rename("LST").updateMask(ee.Image(0))
            lst_src = "indisponible"

    meta["lst_source"] = lst_src

    # ── 4. Stack multi-bandes à scale_out mètres ────────────────────────────
    stack = (ndvi.addBands(ndwi).addBands(ndre).addBands(lst_img)
             .clip(region))

    return stack, meta


def _stats_band(ee, stack, band, region, scale):
    """Statistiques réduites d'une bande sur la région."""
    img = stack.select(band)
    r = img.reduceRegion(
        reducer=(ee.Reducer.mean()
                 .combine(ee.Reducer.min(),    sharedInputs=True)
                 .combine(ee.Reducer.max(),    sharedInputs=True)
                 .combine(ee.Reducer.stdDev(), sharedInputs=True)
                 .combine(ee.Reducer.count(),  sharedInputs=True)),
        geometry=region, scale=scale, maxPixels=1e9, bestEffort=True,
    ).getInfo()
    def get(k):
        v = r.get(f"{band}_{k}") if f"{band}_{k}" in r else r.get(k)
        return v  # peut être None si aucun pixel valide

    def safe_float(v, fallback=0.0):
        try:
            return round(float(v), 4) if v is not None else fallback
        except (TypeError, ValueError):
            return fallback

    return {
        "mean":  safe_float(get("mean")),
        "min":   safe_float(get("min")),
        "max":   safe_float(get("max")),
        "std":   safe_float(get("stdDev")),
        "count": int(get("count") or 0),
    }


def _zone_pct(ee, stack, region, scale):
    """
    Calcule les zones de prescription pixel à pixel sur GEE.
    Priorité : traiter > fertiliser > irriguer > sain
    Retourne un dict avec les % de chaque zone.
    """
    ndvi = stack.select("NDVI")
    ndwi = stack.select("NDWI")
    ndre = stack.select("NDRE")

    # Zone numérique : 1=traiter, 2=fertiliser, 3=irriguer, 4=sain
    # Règles de priorité (appliquées en ordre inverse — la dernière .where gagne) :
    #   sain par défaut (4)
    #   → stress hydrique si NDWI < -0.1 (3)
    #   → carence azote si NDRE < 0.28 (2)
    #   → à traiter si NDVI < 0.22 ET NDRE < 0.18 (1)
    #   → OVERRIDE sain si végétation dense (NDVI > 0.55) : forêt/prairie dense = sain
    #     même si NDRE < 0.28 (les indices azote ne s'appliquent pas aux forêts)
    zone = (ee.Image(4).rename("zone")
            .where(ndwi.lt(-0.1),                        ee.Image(3))  # irriguer
            .where(ndre.lt(0.28),                        ee.Image(2))  # fertiliser
            .where(ndvi.lt(0.22).And(ndre.lt(0.18)),     ee.Image(1))  # traiter
            .where(ndvi.gt(0.55),                        ee.Image(4))  # override : végétation dense = sain
            .clip(region))

    total = zone.reduceRegion(
        reducer=ee.Reducer.count(), geometry=region,
        scale=scale, maxPixels=1e9, bestEffort=True
    ).getInfo().get("zone", 1) or 1

    def _pct_zone(val):
        mask  = zone.eq(val)
        count = zone.updateMask(mask).reduceRegion(
            reducer=ee.Reducer.count(), geometry=region,
            scale=scale, maxPixels=1e9, bestEffort=True
        ).getInfo().get("zone", 0) or 0
        return round(count / max(total, 1) * 100, 1)

    traiter    = _pct_zone(1)
    fertiliser = _pct_zone(2)
    irriguer   = _pct_zone(3)
    sain       = round(max(0.0, 100.0 - traiter - fertiliser - irriguer), 1)

    return {
        "zoneSaine":   sain,
        "stressHyd":   irriguer,
        "carenceN":    fertiliser,
        "zoneMaladie": traiter,
        "total_pixels": int(total),
    }


@router.post("/agri/stats")
def gee_agri_stats(req: AgriStatsRequest):
    """
    Calcule les indices de précision agricole depuis GEE sur le polygone exact.

    Toutes les bandes sont ramenées à `scale` mètres (défaut 10m = résolution
    native S2) par reproject() avant tout calcul — il n'y a aucune valeur par
    défaut ni simulation. Les résultats reflètent les pixels réels.

    Body :
        roi_geojson  : GeoJSON geometry (Polygon) ou Feature
        date_start   : "YYYY-MM-DD"
        date_end     : "YYYY-MM-DD"
        cloud_max    : % couverture nuageuse maximum (défaut 20)
        composite    : "median" | "least_cloudy"
        scale        : résolution en mètres (défaut 10)

    Réponse :
        ndvi / ndwi / ndre / lst : {mean, min, max, std, count}
        zones                    : {zoneSaine, stressHyd, carenceN, zoneMaladie, total_pixels}
        scale                    : résolution effective
        s2_images                : nb d'images S2 utilisées
    """
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")

    try:
        from gee_auth import get_ee
        ee = get_ee()

        # ── Extraire geometry depuis GeoJSON (Feature ou Geometry) ──────────
        geojson = req.roi_geojson
        if geojson.get("type") == "Feature":
            geojson = geojson["geometry"]
        region = geojson_to_ee_geometry(geojson)

        scale = req.scale or 10

        # ── Calcul des indices raster unifiés ────────────────────────────────
        stack, meta = _compute_agri_indices_gee(
            ee, region,
            req.date_start, req.date_end,
            req.cloud_max or 20.0,
            req.composite or "least_cloudy",
            scale,
        )

        # ── Statistiques par bande ────────────────────────────────────────────
        stats_ndvi = _stats_band(ee, stack, "NDVI", region, scale)
        stats_ndwi = _stats_band(ee, stack, "NDWI", region, scale)
        stats_ndre = _stats_band(ee, stack, "NDRE", region, scale)
        stats_lst  = _stats_band(ee, stack, "LST",  region, scale)

        # ── Zones de prescription pixel à pixel ──────────────────────────────
        zones = _zone_pct(ee, stack, region, scale)

        return {
            "ndvi":         stats_ndvi,
            "ndwi":         stats_ndwi,
            "ndre":         stats_ndre,
            "lst":          stats_lst,
            "zones":        zones,
            "scale":        scale,
            # Métadonnées images réellement utilisées
            "s2_images":    meta["n_images"],
            "date_used":    meta["date_used"],       # date précise (least_cloudy) ou "médiane N img"
            "cloud_pct":    meta["cloud_pct_used"],  # % nuages image sélectionnée (least_cloudy)
            "cloud_max_used": meta["cloud_max_used"],
            "composite":    meta["composite"],
            "lst_source":   meta["lst_source"],
            # Paramètres requête
            "date_start":   req.date_start,
            "date_end":     req.date_end,
            "cloud_max":    req.cloud_max,
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(422, str(e))
    except Exception as e:
        raise HTTPException(500, f"Erreur calcul agri GEE : {e}")

# ══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT AGRICULTURE TUILES — /api/gee/agri/tiles
#
#  Génère des tuiles XYZ réelles GEE pour NDVI / NDWI / NDRE / LST
#  en utilisant le MÊME composite S2 que /agri/stats.
#  Retourne tile_url (XYZ template), vis_params, date_used, cloud_pct.
# ══════════════════════════════════════════════════════════════════════════════

# Paramètres de visualisation par indice agri (min/max adaptés valeurs réelles S2)
AGRI_VIS = {
    "NDVI":         {"palette": ["#d73027","#f46d43","#fdae61","#fee08b","#a6d96a","#66bd63","#1a9850"], "min": -0.2, "max": 0.8},
    "NDWI":         {"palette": ["#8c510a","#d8b365","#f6e8c3","#c7eae5","#5ab4ac","#2980b9","#01665e"], "min": -0.5, "max": 0.4},
    "NDRE":         {"palette": ["#762a83","#af8dc3","#e67e22","#d9ef8b","#7fbf7b","#1b7837"], "min": -0.1, "max": 0.7},
    "LST":          {"palette": ["#313695","#4575b4","#74add1","#abd9e9","#fee090","#f46d43","#d73027"], "min": 0, "max": 45},
    "PRESCRIPTION": {"palette": ["#e05a3a","#e8c84a","#3498db","#27ae60"], "min": 1, "max": 4},
}


class AgriTilesRequest(BaseModel):
    roi_geojson:  dict
    date_start:   str
    date_end:     str
    index:        str              # "NDVI" | "NDWI" | "NDRE" | "LST"
    cloud_max:    Optional[float] = 20.0
    composite:    Optional[str]   = "least_cloudy"
    scale:        Optional[int]   = 10


@router.post("/agri/tiles")
def gee_agri_tiles(req: AgriTilesRequest):
    """
    Génère des tuiles XYZ réelles (carte de chaleur pixel) pour un indice agri.

    Pipeline identique à /agri/stats :
      - S2 SR harmonisé, même composite (least_cloudy / median / mosaic)
      - même masque SCL nuages
      - NDRE depuis B8A/B5 20m → reprojection 10m
      - LST Landsat 9 → 8 → fallback MODIS → °C

    Retourne tile_url (template XYZ), date_used, cloud_pct, vis_params.
    """
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")

    index = req.index.upper()
    if index not in AGRI_VIS:
        raise HTTPException(422, f"Index agri inconnu : {req.index}. Valeurs : NDVI, NDWI, NDRE, LST, PRESCRIPTION")

    try:
        from gee_auth import get_ee
        import datetime as _dt
        ee = get_ee()

        geojson = req.roi_geojson
        if geojson.get("type") == "Feature":
            geojson = geojson["geometry"]
        region = geojson_to_ee_geometry(geojson)

        SCALE_S2   = 0.0001
        scale_out  = req.scale or 10
        cloud_max  = req.cloud_max or 20.0
        composite  = req.composite or "least_cloudy"

        # ── S2 collection + filtre nuages avec fallback ──────────────────────
        s2_base = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                   .filterDate(req.date_start, req.date_end)
                   .filterBounds(region.bounds()))

        s2_filtered = s2_base.filter(ee.Filter.And(
            ee.Filter.notNull(["CLOUDY_PIXEL_PERCENTAGE"]),
            ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", cloud_max)
        ))
        size = s2_filtered.size().getInfo()
        cloud_max_used = cloud_max

        if size == 0:
            for fb in [50, 80, 100]:
                s2_try = s2_base.filter(ee.Filter.And(
                    ee.Filter.notNull(["CLOUDY_PIXEL_PERCENTAGE"]),
                    ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", fb)
                ))
                size = s2_try.size().getInfo()
                if size > 0:
                    s2_filtered = s2_try
                    cloud_max_used = fb
                    break

        if size == 0:
            raise HTTPException(422,
                f"Aucune image Sentinel-2 disponible entre {req.date_start} et "
                f"{req.date_end} sur cette zone.")

        # ── Sélection composite ──────────────────────────────────────────────
        if composite == "least_cloudy":
            best       = s2_filtered.sort("CLOUDY_PIXEL_PERCENTAGE").first()
            s2_img     = _s2_mask_scl(best)
            img_props  = best.toDictionary(["system:time_start","CLOUDY_PIXEL_PERCENTAGE"]).getInfo()
            ts         = img_props.get("system:time_start")
            date_used  = _dt.datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d") if ts else req.date_start
            cloud_pct  = round(float(img_props.get("CLOUDY_PIXEL_PERCENTAGE") or 0), 1)
        elif composite == "mosaic":
            s2_img    = _s2_mask_scl(s2_filtered.sort("system:time_start", False).mosaic())
            date_used = f"mosaïque {size} img"
            cloud_pct = None
        else:  # median
            s2_img    = s2_filtered.map(_s2_mask_scl).median()
            date_used = f"médiane {size} img"
            cloud_pct = None

        # ── Calcul de l'indice demandé ───────────────────────────────────────
        crs_10m = s2_img.select("B4").projection()

        if index == "NDVI":
            B8 = s2_img.select("B8").multiply(SCALE_S2)
            B4 = s2_img.select("B4").multiply(SCALE_S2)
            index_img = B8.subtract(B4).divide(B8.add(B4)).rename("NDVI")

        elif index == "NDWI":
            B3 = s2_img.select("B3").multiply(SCALE_S2)
            B8 = s2_img.select("B8").multiply(SCALE_S2)
            index_img = B3.subtract(B8).divide(B3.add(B8)).rename("NDWI")

        elif index == "NDRE":
            B5  = s2_img.select("B5").multiply(SCALE_S2).reproject(crs=crs_10m, scale=scale_out)
            B8A = s2_img.select("B8A").multiply(SCALE_S2).reproject(crs=crs_10m, scale=scale_out)
            index_img = B8A.subtract(B5).divide(B8A.add(B5)).rename("NDRE")

        elif index == "PRESCRIPTION":
            # Carte de prescription pixel à pixel — seuils identiques à _zone_pct
            B4  = s2_img.select("B4").multiply(SCALE_S2)
            B3  = s2_img.select("B3").multiply(SCALE_S2)
            B8  = s2_img.select("B8").multiply(SCALE_S2)
            _ndvi = B8.subtract(B4).divide(B8.add(B4))
            _ndwi = B3.subtract(B8).divide(B3.add(B8))
            _B5  = s2_img.select("B5").multiply(SCALE_S2).reproject(crs=crs_10m, scale=scale_out)
            _B8A = s2_img.select("B8A").multiply(SCALE_S2).reproject(crs=crs_10m, scale=scale_out)
            _ndre = _B8A.subtract(_B5).divide(_B8A.add(_B5))
            # 4=sain, 3=irriguer, 2=fertiliser, 1=traiter (priorité croissante)
            presc = (ee.Image(4)
                     .where(_ndwi.lt(-0.1),                         ee.Image(3))  # irriguer
                     .where(_ndre.lt(0.28),                         ee.Image(2))  # fertiliser
                     .where(_ndvi.lt(0.22).And(_ndre.lt(0.18)),     ee.Image(1))  # traiter
                     .where(_ndvi.gt(0.55),                         ee.Image(4))  # override : végétation dense = sain
                    )
            index_img = presc.rename("PRESCRIPTION")

        elif index == "LST":
            lst_img = None
            for ls_col in ["LANDSAT/LC09/C02/T1_L2", "LANDSAT/LC08/C02/T1_L2"]:
                ls = (ee.ImageCollection(ls_col)
                      .filterDate(req.date_start, req.date_end)
                      .filterBounds(region.bounds())
                      .filter(ee.Filter.And(
                          ee.Filter.notNull(["CLOUD_COVER"]),
                          ee.Filter.lte("CLOUD_COVER", cloud_max_used + 10)
                      )))
                if ls.size().getInfo() > 0:
                    if composite == "least_cloudy":
                        ls_raw = ls.sort("CLOUD_COVER").first()
                    elif composite == "mosaic":
                        ls_raw = ls.sort("system:time_start", False).mosaic()
                    else:
                        ls_raw = ls.median()
                    lst_img = (ls_raw.select("ST_B10")
                               .multiply(0.00341802).add(149.0).subtract(273.15)
                               .rename("LST")
                               .reproject(crs=crs_10m, scale=scale_out))
                    break
            if lst_img is None:
                # Fallback MODIS
                modis = (ee.ImageCollection("MODIS/061/MOD11A1")
                         .filterDate(req.date_start, req.date_end)
                         .filterBounds(region.bounds())
                         .filter(ee.Filter.lte("QC_Day", 17)))
                if modis.size().getInfo() == 0:
                    modis = (ee.ImageCollection("MODIS/061/MOD11A1")
                             .filterDate(req.date_start, req.date_end)
                             .filterBounds(region.bounds()))
                if modis.size().getInfo() == 0:
                    raise HTTPException(422, "Aucune donnée LST disponible sur cette période.")
                lst_img = (modis.median()
                           .select("LST_Day_1km")
                           .multiply(0.02).subtract(273.15)
                           .rename("LST")
                           .reproject(crs=crs_10m, scale=scale_out))
            index_img = lst_img

        # ── Clip au contour exact de la parcelle ─────────────────────────────
        index_img = index_img.clip(region)

        # ── Génération tuiles XYZ ─────────────────────────────────────────────
        vis = AGRI_VIS[index].copy()
        map_id  = index_img.getMapId(vis)
        fetcher = map_id.get("tile_fetcher")
        tile_url = (fetcher.url_format
                    if fetcher and hasattr(fetcher, "url_format")
                    else map_id.get("urlFormat", ""))

        if not tile_url:
            raise HTTPException(500, "Impossible de générer les tuiles GEE")

        # Bbox pour fitBounds côté client
        try:
            bbox_coords = region.bounds().getInfo()["coordinates"][0]
            xs = [c[0] for c in bbox_coords]; ys = [c[1] for c in bbox_coords]
            bbox = [min(xs), min(ys), max(xs), max(ys)]
        except Exception:
            bbox = None

        return {
            "tile_url":       tile_url,
            "index":          index,
            "vis_params":     vis,
            "date_used":      date_used,
            "cloud_pct":      cloud_pct,
            "cloud_max_used": cloud_max_used,
            "s2_images":      size,
            "composite":      composite,
            "bbox":           bbox,
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(422, str(e))
    except Exception as e:
        raise HTTPException(500, f"Erreur tuiles agri GEE : {e}")