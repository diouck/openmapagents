"""
gee_routes.py — FastAPI router Google Earth Engine
Ajout dans agent.py :
    from gee_routes import router as gee_router
    app.include_router(gee_router)
Installation :
    pip install earthengine-api google-auth google-auth-httplib2
"""
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
    ("modis_ndvi","NDVI"):              {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-2000,"max":10000},
    ("modis_ndvi","EVI"):               {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-2000,"max":10000},
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
}

# ── Initialisation GEE via service account ────────────────────
def init_gee():
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

# ── Calcul des indices ────────────────────────────────────────
def compute_index(image, dataset, index):
    try:
        import ee
        if index == "NDVI":
            if dataset == "sentinel2":        return image.normalizedDifference(["B8","B4"]).rename("NDVI")
            if dataset in ("landsat8","landsat9"): return image.normalizedDifference(["SR_B5","SR_B4"]).rename("NDVI")
            if dataset == "modis_ndvi":        return image.select("NDVI")
        if index == "NDWI":
            if dataset == "sentinel2":        return image.normalizedDifference(["B3","B8"]).rename("NDWI")
            if dataset in ("landsat8","landsat9"): return image.normalizedDifference(["SR_B3","SR_B5"]).rename("NDWI")
        if index == "NDBI":
            if dataset == "sentinel2":        return image.normalizedDifference(["B11","B8"]).rename("NDBI")
        if index == "EVI":
            if dataset == "sentinel2":
                nir=image.select("B8").multiply(0.0001); red=image.select("B4").multiply(0.0001); blue=image.select("B2").multiply(0.0001)
                return nir.subtract(red).multiply(2.5).divide(nir.add(red.multiply(6)).subtract(blue.multiply(7.5)).add(1)).rename("EVI")
            if dataset == "modis_ndvi": return image.select("EVI")
        if index in ("LST (température)","LST Jour","LST Nuit"):
            if dataset in ("landsat8","landsat9"): return image.select("ST_B10").multiply(0.00341802).add(149.0).rename("LST")
            if dataset == "modis_lst":
                band = "LST_Day_1km" if "Jour" in index or "température" in index else "LST_Night_1km"
                return image.select(band).multiply(0.02).rename("LST")
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
    composite:        Optional[str]   = "least_cloudy"
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
        STATIC_DATASETS = {"srtm", "hansen", "worldcover"}

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
            "srtm":      "USGS/SRTMGL1_003",
            "hansen":    "UMD/hansen/global_forest_change_2023_v1_11",
        }
        STATIC_COLLECTIONS = {
            "worldcover": "ESA/WorldCover/v200",
        }

        if req.dataset in STATIC_IMAGES:
            image = ee.Image(STATIC_IMAGES[req.dataset])
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