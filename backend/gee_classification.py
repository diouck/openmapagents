"""
gee_classification.py — Classification supervisée hybride
  Couches GEE  → classifieurs natifs ee.Classifier.* (sans limite de taille)
  Couches WMS  → sklearn local (AOI ≤ 1 km²)
  Auto GEE     → datasets pré-classifiés GEE (Dynamic World, WorldCover, MODIS…)

Routes :
  GET  /api/gee/auto-classifiers       → liste des classifieurs auto
  POST /api/gee/auto-classify          → classification auto GEE
  POST /api/gee/classify               → classification supervisée
  POST /api/gee/classify/restyle       → re-coloriser une classification existante
"""

import io, math, uuid, base64, logging
import numpy as np
from typing import List, Optional, Dict, Any, Union

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger("gee-classif")
router = APIRouter(prefix="/api/gee", tags=["classification"])

# ─── In-memory job store (restyle) ───────────────────────────────────────────
_jobs: Dict[str, dict] = {}

# ─── Couleurs par défaut ──────────────────────────────────────────────────────
DEFAULT_COLORS = [
    "#e41a1c","#377eb8","#4daf4a","#984ea3",
    "#ff7f00","#a65628","#f781bf","#66c2a5",
    "#fc8d62","#8da0cb","#e78ac3","#a6d854",
]

# ─── Disponibilité temporelle des satellites ──────────────────────────────────
SENTINEL2_START  = "2015-06-23"  # S2A launch (S2_SR_HARMONIZED)
LANDSAT9_START   = "2021-10-31"
LANDSAT8_START   = "2013-03-18"
LANDSAT7_START   = "1999-05-28"
LANDSAT5_START   = "1984-03-01"
LANDSAT5_END     = "2013-06-05"
LANDSAT7_SLC_END = "2003-05-31"  # après: scan-line corrector off (données dégradées)

# ─── Auto-classifieurs GEE pré-construits ────────────────────────────────────
AUTO_CLASSIFIERS = {
    "dynamic_world": {
        "label": "Dynamic World (Google)",
        "desc":  "Occupation du sol · 10 m · quasi temps-réel",
        "icon":  "🌍",
        "collection":  "GOOGLE/DYNAMICWORLD/V1",
        "band":        "label",
        "needs_dates": True,
        "year_only":   False,
        "date_min":    SENTINEL2_START,
        "date_max":    None,
        "avail_note":  "Disponible à partir du 23 juin 2015 (Sentinel-2). "
                       "Pour des dates antérieures, utilisez MODIS Land Cover (depuis 2001).",
        "classes": [
            (0, "Eau",             "#419BDF"),
            (1, "Arbres",          "#397D49"),
            (2, "Herbe",           "#88B053"),
            (3, "Végét. inondée",  "#7A87C6"),
            (4, "Cultures",        "#E49635"),
            (5, "Arbustes",        "#DFC35A"),
            (6, "Bâti",            "#C4281B"),
            (7, "Sol nu",          "#A59B8F"),
            (8, "Neige / Glace",   "#B39FE1"),
        ],
    },
    "worldcover_auto": {
        "label": "ESA WorldCover 2021",
        "desc":  "Occupation du sol · 10 m · statique",
        "icon":  "🗺️",
        "collection":  "ESA/WorldCover/v200",
        "band":        "Map",
        "needs_dates": False,
        "date_min":    None,
        "date_max":    None,
        "avail_note":  "Carte statique 2021 — indépendante des dates sélectionnées.",
        "classes": [
            (10,  "Forêt (feuillus)",  "#006400"),
            (20,  "Forêt (aiguilles)", "#ffbb22"),
            (30,  "Savane / Prairies", "#ffff4c"),
            (40,  "Cultures",          "#f096ff"),
            (50,  "Bâti",              "#fa0000"),
            (60,  "Sol nu / Sparse",   "#b4b4b4"),
            (70,  "Neige / Glace",     "#f0f0f0"),
            (80,  "Eau permanente",    "#0064c8"),
            (90,  "Zones humides",     "#0096a0"),
            (95,  "Mangroves",         "#00cf75"),
            (100, "Mousses / Lichens", "#fae6a0"),
        ],
    },
    "modis_lc": {
        "label": "MODIS Land Cover (IGBP)",
        "desc":  "Occupation du sol · 500 m · annuel",
        "icon":  "📡",
        "collection":  "MODIS/061/MCD12Q1",
        "band":        "LC_Type1",
        "needs_dates": True,
        "year_only":   True,
        "date_min":    "2001-01-01",
        "date_max":    None,
        "avail_note":  "Disponible depuis 2001. Seule l'année est utilisée (carte annuelle).",
        "classes": [
            (1,  "Forêt tj. verte (aig.)",  "#05450A"),
            (2,  "Forêt décidue (aig.)",    "#086A10"),
            (3,  "Forêt tj. verte (feu.)",  "#54A708"),
            (4,  "Forêt décidue (feu.)",    "#78D203"),
            (5,  "Forêt mixte",             "#009900"),
            (6,  "Savane boisée",           "#C6B044"),
            (7,  "Savane",                  "#68ABCA"),
            (8,  "Prairies",                "#749F8D"),
            (9,  "Zones humides",           "#86A73D"),
            (10, "Cultures",                "#E9DA8C"),
            (11, "Bâti",                    "#E60000"),
            (12, "Mixte cultures/nat.",     "#A5A5A5"),
            (13, "Neige / Glace",           "#DCF0F8"),
            (14, "Eau",                     "#4E66D8"),
        ],
    },
    "copernicus_lc": {
        "label": "Copernicus LC 100 m",
        "desc":  "Occupation du sol · 100 m · annuel",
        "icon":  "🌐",
        "collection":  "COPERNICUS/Landcover/100m/Proba-V-C3/Global",
        "band":        "discrete_classification",
        "needs_dates": True,
        "year_only":   True,
        "date_min":    "2015-01-01",
        "date_max":    "2019-12-31",
        "avail_note":  "Disponible de 2015 à 2019. Entrez une date dans cette plage.",
        "classes": [
            (0,   "Inconnu",         "#282828"),
            (20,  "Végét. éparse",   "#FFBB22"),
            (30,  "Herbacé",         "#FFFF4C"),
            (40,  "Cultures",        "#F096FF"),
            (50,  "Bâti",            "#FA0000"),
            (60,  "Sol nu",          "#B4B4B4"),
            (70,  "Neige / Glace",   "#F0F0F0"),
            (80,  "Eau permanente",  "#0032C8"),
            (90,  "Marais herbeux",  "#0096A0"),
            (100, "Mousses",         "#FAE6A0"),
            (111, "Forêt fermée",    "#58481F"),
            (112, "Forêt feuillus",  "#009900"),
            (114, "Forêt mixte",     "#00CC00"),
            (116, "Forêt inondée",   "#007800"),
        ],
    },
}

# ─── Échelle (mètres) par classifieur auto ───────────────────────────────────
AUTO_SCALE = {
    "dynamic_world": 10, "worldcover_auto": 10,
    "modis_lc": 500,     "copernicus_lc": 100,
}

# ─── Bandes par dataset ───────────────────────────────────────────────────────
DATASET_CFG = {
    "sentinel2":  {"scale": 10,  "spectral": ["B2","B3","B4","B5","B6","B7","B8","B8A","B11","B12"]},
    "landsat9":   {"scale": 30,  "spectral": ["SR_B2","SR_B3","SR_B4","SR_B5","SR_B6","SR_B7"]},
    "landsat8":   {"scale": 30,  "spectral": ["SR_B2","SR_B3","SR_B4","SR_B5","SR_B6","SR_B7"]},
    "sentinel1":  {"scale": 10,  "spectral": ["VV","VH"]},
    "modis_ndvi": {"scale": 500, "spectral": ["NDVI","EVI"]},
    "srtm":       {"scale": 30,  "spectral": ["elevation","slope"]},
}

# ─── Pydantic ─────────────────────────────────────────────────────────────────
class RoiItem(BaseModel):
    geometry:  dict
    label:     str
    class_id:  Union[int, str]   # le frontend peut envoyer un string ou un int
    color:     Optional[str] = None

class ClassifyRequest(BaseModel):
    layer_id:     str
    layer_type:   str
    gee_params:   Optional[dict]      = None
    tile_url:     Optional[str]       = None
    aoi:          dict
    rois:         List[RoiItem]
    model:        str   = "smileRandomForest"
    model_params: dict  = {}
    train_ratio:  float = 0.7
    class_colors: Optional[List[str]] = None

class AutoClassifyRequest(BaseModel):
    classifier_id: str
    aoi:           dict
    date_start:    str        = "2023-01-01"
    date_end:      str        = "2023-12-31"
    class_colors:  Optional[List[str]] = None  # liste ordonnée par index de classe

class RestyleRequest(BaseModel):
    job_id:       str
    class_colors: List[str]   # ordonné par index de légende

class PeriodItem(BaseModel):
    start: str
    end:   str
    label: str = ""

class MultidateRequest(BaseModel):
    layer_id:     str
    gee_params:   dict
    aoi:          dict
    rois:         List[RoiItem]
    model:        str   = "smileRandomForest"
    model_params: dict  = {}
    train_ratio:  float = 0.7
    class_colors: Optional[List[str]] = None
    date_periods: List[PeriodItem]    # ≥ 2 périodes

# ─── Helpers ─────────────────────────────────────────────────────────────────
def bbox_from_geojson(gj):
    coords = gj["coordinates"][0]
    xs=[c[0] for c in coords]; ys=[c[1] for c in coords]
    return [min(xs),min(ys),max(xs),max(ys)]

def area_km2(gj):
    w,s,e,n = bbox_from_geojson(gj)
    lm = (s+n)/2
    return (e-w)*111.32*math.cos(math.radians(lm))*(n-s)*110.574

def hex_to_rgb(h):
    h=h.lstrip("#"); return (int(h[0:2],16),int(h[2:4],16),int(h[4:6],16))

# ─── Tile helpers (WMS) ───────────────────────────────────────────────────────
def _ll_to_tile(lon,lat,z):
    n=2**z; x=int((lon+180)/360*n)
    lr=math.radians(lat)
    y=int((1-math.log(math.tan(lr)+1/math.cos(lr))/math.pi)/2*n)
    return x,y

def _tile_nw(tx,ty,z):
    n=2**z; lon=tx/n*360-180
    lat=math.degrees(math.atan(math.sinh(math.pi*(1-2*ty/n))))
    return lon,lat

def _ll_to_px(lon,lat,z,tx0,ty0):
    n=2**z; lr=math.radians(lat)
    gx=(lon+180)/360*n*256-tx0*256
    gy=(1-math.log(math.tan(lr)+1/math.cos(lr))/math.pi)/2*n*256-ty0*256
    return int(round(gx)),int(round(gy))

def _download_tile(url,z,x,y):
    import requests
    from PIL import Image
    u=url.replace("{z}",str(z)).replace("{x}",str(x)).replace("{y}",str(y))
    try:
        r=requests.get(u,timeout=10,headers={"User-Agent":"MCPCartographic/1.0"})
        r.raise_for_status()
        img=Image.open(io.BytesIO(r.content)).convert("RGB").resize((256,256))
        return np.array(img)
    except Exception as e:
        log.warning(f"tile {u}: {e}"); return np.zeros((256,256,3),dtype=np.uint8)

def _build_mosaic(tile_url,bbox,zoom=15):
    w,s,e,n=bbox
    xa,yb=_ll_to_tile(w,s,zoom); xb,ya=_ll_to_tile(e,n,zoom)
    x0,x1=min(xa,xb),max(xa,xb); y0,y1=min(ya,yb),max(ya,yb)
    mw=(x1-x0+1)*256; mh=(y1-y0+1)*256
    mosaic=np.zeros((mh,mw,3),dtype=np.uint8)
    for ix,tx in enumerate(range(x0,x1+1)):
        for iy,ty in enumerate(range(y0,y1+1)):
            tile=_download_tile(tile_url,zoom,tx,ty)
            mosaic[iy*256:(iy+1)*256,ix*256:(ix+1)*256]=tile
    return mosaic,x0,y0,mw,mh

def _rasterize(geometry,zoom,tx0,ty0,mw,mh):
    from PIL import Image as PILImage, ImageDraw
    coords=geometry["coordinates"][0]
    px=[_ll_to_px(c[0],c[1],zoom,tx0,ty0) for c in coords]
    im=PILImage.new("L",(mw,mh),0)
    ImageDraw.Draw(im).polygon(px,fill=255)
    return np.array(im)>0

def _render_wms_image(y_all,rows,cols,mh,mw,class_ids,colors):
    """Génère le PNG RGBA classifié depuis les prédictions."""
    from PIL import Image as PILImage
    # Supporte int et str comme class_id (legacy int() retiré)
    id2rgb={cid:hex_to_rgb(colors[i%len(colors)]) for i,cid in enumerate(class_ids)}
    result=np.zeros((mh,mw,4),dtype=np.uint8)
    for k,(r,c) in enumerate(zip(rows,cols)):
        val=y_all[k]
        rgb=id2rgb.get(val) or id2rgb.get(str(val), (128,128,128))
        result[r,c]=(*rgb,210)
    buf=io.BytesIO(); PILImage.fromarray(result,"RGBA").save(buf,format="PNG")
    return "data:image/png;base64,"+base64.b64encode(buf.getvalue()).decode()

# ─── Sélection automatique du meilleur Landsat pour une période ──────────────
def _pick_landsat(d1: str, d2: str, cl: int):
    """
    Retourne (collection_id, cloud_prop, band_map) selon la date.
    band_map : dict {nom_générique → nom_original_Landsat}
    """
    import ee
    y1 = int(d1[:4])
    # Landsat 9 OLI-2 (2021+)
    if y1 >= 2021:
        cid = "LANDSAT/LC09/C02/T1_L2"
        bmap = {"Blue":"SR_B2","Green":"SR_B3","Red":"SR_B4","NIR":"SR_B5","SWIR1":"SR_B6","SWIR2":"SR_B7"}
    # Landsat 8 OLI (2013–)
    elif y1 >= 2013:
        cid = "LANDSAT/LC08/C02/T1_L2"
        bmap = {"Blue":"SR_B2","Green":"SR_B3","Red":"SR_B4","NIR":"SR_B5","SWIR1":"SR_B6","SWIR2":"SR_B7"}
    # Landsat 7 ETM+ (1999–2012) — SLC-off après mai 2003 (données dégradées mais utilisables)
    elif y1 >= 1999:
        cid = "LANDSAT/LE07/C02/T1_L2"
        bmap = {"Blue":"SR_B1","Green":"SR_B2","Red":"SR_B3","NIR":"SR_B4","SWIR1":"SR_B5","SWIR2":"SR_B7"}
    # Landsat 5 TM (1984–2013)
    elif y1 >= 1984:
        cid = "LANDSAT/LT05/C02/T1_L2"
        bmap = {"Blue":"SR_B1","Green":"SR_B2","Red":"SR_B3","NIR":"SR_B4","SWIR1":"SR_B5","SWIR2":"SR_B7"}
    else:
        raise ValueError(f"Pas de données satellitaires disponibles avant 1984 (année demandée : {y1})")
    col = (ee.ImageCollection(cid).filterDate(d1, d2)
           .filter(ee.Filter.lt("CLOUD_COVER", cl)))
    return col, cid, bmap

# ─── GEE image builder ────────────────────────────────────────────────────────
def _build_gee_image(gp):
    """Retourne (image, bands, scale, meta) où meta contient cloud_pct, image_count et sensor."""
    import ee
    ds=gp.get("dataset","sentinel2"); d1=gp.get("date_start","2023-01-01")
    d2=gp.get("date_end","2023-12-31"); cl=gp.get("cloud_max",20)
    meta={}  # {"cloud_pct": float, "image_count": int, "sensor": str}

    if ds=="sentinel2":
        # ── Détection automatique : bascule vers Landsat si avant Sentinel-2 ──
        if d2 < SENTINEL2_START:
            log.info(f"Sentinel-2 indisponible avant {SENTINEL2_START} — bascule Landsat (d1={d1})")
            col, cid, bmap = _pick_landsat(d1, d2, cl)
            raw = col.median()
            try:
                stats = ee.Dictionary({"count": col.size(),
                                       "cloud_pct": col.aggregate_mean("CLOUD_COVER")}
                                      ).getInfo()
                sensor_name = cid.split("/")[1]  # ex. LC09, LE07, LT05
                meta = {"image_count": int(stats.get("count", 0) or 0),
                        "cloud_pct":   round(float(stats.get("cloud_pct", 0) or 0), 1),
                        "sensor":      sensor_name,
                        "fallback":    True,
                        "fallback_reason": f"Sentinel-2 non disponible avant {SENTINEL2_START}"}
            except Exception as me: log.warning(f"cloud meta Landsat fallback: {me}")
            # Renommer les bandes Landsat → noms génériques
            src_bands = list(bmap.values())
            dst_bands = list(bmap.keys())   # Blue, Green, Red, NIR, SWIR1, SWIR2
            img_base = raw.select(src_bands).rename(dst_bands)
            ndvi = img_base.normalizedDifference(["NIR","Red"]).rename("NDVI")
            ndwi = img_base.normalizedDifference(["Green","NIR"]).rename("NDWI")
            ndbi = img_base.normalizedDifference(["SWIR1","NIR"]).rename("NDBI")
            img  = img_base.addBands(ndvi).addBands(ndwi).addBands(ndbi)
            bands = dst_bands + ["NDVI","NDWI","NDBI"]
            scale = 30
            return img, bands, scale, meta
        # ── Sentinel-2 SR Harmonized (normal) ────────────────────────────────
        col=(ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterDate(d1,d2)
             .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE",cl)))
        raw=col.median()
        # ── Métadonnées couverture nuageuse (un seul appel getInfo groupé) ──
        try:
            stats=ee.Dictionary({"count":col.size(),
                                 "cloud_pct":col.aggregate_mean("CLOUDY_PIXEL_PERCENTAGE")}
                                ).getInfo()
            meta={"image_count":int(stats.get("count",0) or 0),
                  "cloud_pct":round(float(stats.get("cloud_pct",0) or 0),1),
                  "sensor":"S2_SR"}
        except Exception as me: log.warning(f"cloud meta S2: {me}")
        bands=["B2","B3","B4","B5","B6","B7","B8","B11","B12"]
        # ── addBands chaînés (passer une liste Python à addBands ne fonctionne pas
        #    correctement avec l'API GEE Python — chaque bande est ajoutée séparément)
        ndvi=raw.normalizedDifference(["B8","B4"]).rename("NDVI")
        ndwi=raw.normalizedDifference(["B3","B8"]).rename("NDWI")
        ndbi=raw.normalizedDifference(["B11","B8"]).rename("NDBI")
        img=raw.select(bands).addBands(ndvi).addBands(ndwi).addBands(ndbi)
        bands=bands+["NDVI","NDWI","NDBI"]
    elif ds in ("landsat9","landsat8"):
        cid="LANDSAT/LC09/C02/T1_L2" if ds=="landsat9" else "LANDSAT/LC08/C02/T1_L2"
        col=(ee.ImageCollection(cid).filterDate(d1,d2)
             .filter(ee.Filter.lt("CLOUD_COVER",cl)))
        raw=col.median()
        try:
            stats=ee.Dictionary({"count":col.size(),
                                 "cloud_pct":col.aggregate_mean("CLOUD_COVER")}
                                ).getInfo()
            meta={"image_count":int(stats.get("count",0) or 0),
                  "cloud_pct":round(float(stats.get("cloud_pct",0) or 0),1)}
        except Exception as me: log.warning(f"cloud meta Landsat: {me}")
        bands=["SR_B2","SR_B3","SR_B4","SR_B5","SR_B6","SR_B7"]
        ndvi=raw.normalizedDifference(["SR_B5","SR_B4"]).rename("NDVI")
        ndwi=raw.normalizedDifference(["SR_B3","SR_B5"]).rename("NDWI")
        img=raw.select(bands).addBands(ndvi).addBands(ndwi)
        bands=bands+["NDVI","NDWI"]
    elif ds=="sentinel1":
        img=(ee.ImageCollection("COPERNICUS/S1_GRD").filterDate(d1,d2)
             .filter(ee.Filter.listContains("transmitterReceiverPolarisation","VV"))
             .filter(ee.Filter.listContains("transmitterReceiverPolarisation","VH"))
             .select(["VV","VH"]).median())
        bands=["VV","VH"]
    elif ds=="srtm":
        srtm=ee.Image("USGS/SRTMGL1_003").rename("elevation")
        img=srtm.addBands(ee.Terrain.slope(srtm).rename("slope")); bands=["elevation","slope"]
    elif ds=="modis_ndvi":
        img=(ee.ImageCollection("MODIS/061/MOD13A1").filterDate(d1,d2)
             .select(["NDVI","EVI"]).median().multiply(0.0001)); bands=["NDVI","EVI"]
    elif ds=="worldcover":
        img=ee.ImageCollection("ESA/WorldCover/v200").first(); bands=["Map"]
    else:
        raise ValueError(f"Dataset non supporté: {ds}")
    scale=DATASET_CFG.get(ds,{}).get("scale",30)
    return img,bands,scale,meta

def _gee_tile_url(image,class_ids,colors):
    import ee
    # remap vers 0..N-1 pour que la palette soit correctement indexée
    # (indispensable pour classes non-séquentielles : WorldCover 10,20,30...)
    n=len(class_ids)
    remapped=image.remap(class_ids,list(range(n)),defaultValue=0)
    palette=[colors[i%len(colors)].lstrip("#") for i in range(n)]
    vis={"min":0,"max":max(n-1,1),"palette":palette}
    mid=remapped.getMapId(vis); fetcher=mid.get("tile_fetcher")
    return (fetcher.url_format if (fetcher and hasattr(fetcher,"url_format"))
            else mid.get("urlFormat",""))

# ─── GEE classifiers factory ──────────────────────────────────────────────────
def _gee_clf(model,p):
    import ee; p=p or {}
    if model=="smileRandomForest":
        kw=dict(numberOfTrees=p.get("numberOfTrees",100),
                minLeafPopulation=p.get("minLeafPopulation",1),
                bagFraction=p.get("bagFraction",0.5),seed=p.get("seed",42))
        if p.get("maxNodes"): kw["maxNodes"]=p["maxNodes"]
        return ee.Classifier.smileRandomForest(**kw)
    if model=="smileCart":
        kw=dict(minLeafPopulation=p.get("minLeafPopulation",1))
        if p.get("maxNodes"): kw["maxNodes"]=p["maxNodes"]
        return ee.Classifier.smileCart(**kw)
    if model=="libsvm":
        kw=dict(kernelType=p.get("kernelType","RBF"),cost=p.get("cost",1.0))
        if p.get("gamma"): kw["gamma"]=p["gamma"]
        return ee.Classifier.libsvm(**kw)
    if model=="smileNaiveBayes":
        return ee.Classifier.smileNaiveBayes(lambda_=p.get("lambda_",1.0))
    if model=="minimumDistance":
        # Vrai KNN GEE natif (remplace smileKNN pour GEE)
        return ee.Classifier.minimumDistance(
            metric=p.get("metric","euclidean"),
            kNearest=p.get("k",1),
        )
    if model=="smileGradientTreeBoost":
        kw=dict(numberOfTrees=p.get("numberOfTrees",100),
                shrinkage=p.get("shrinkage",0.005),samplingRate=p.get("samplingRate",0.7))
        if p.get("maxNodes"): kw["maxNodes"]=p["maxNodes"]
        return ee.Classifier.smileGradientTreeBoost(**kw)
    raise ValueError(f"Modèle GEE inconnu: {model}")

def _sk_clf(model,p):
    from sklearn.ensemble import RandomForestClassifier,GradientBoostingClassifier
    from sklearn.tree import DecisionTreeClassifier
    from sklearn.svm import SVC
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.naive_bayes import GaussianNB
    p=p or {}
    if model=="smileRandomForest":
        return RandomForestClassifier(n_estimators=p.get("numberOfTrees",100),
                                      min_samples_leaf=p.get("minLeafPopulation",1),
                                      random_state=p.get("seed",42))
    if model=="smileCart":
        return DecisionTreeClassifier(min_samples_leaf=p.get("minLeafPopulation",1),
                                      max_leaf_nodes=p.get("maxNodes") or None,random_state=42)
    if model=="libsvm":
        return SVC(kernel=p.get("kernelType","rbf").lower(),C=p.get("cost",1.0),
                   gamma=p.get("gamma") or "scale",probability=True)
    if model=="smileNaiveBayes":
        return GaussianNB()
    if model=="smileKNN":
        return KNeighborsClassifier(n_neighbors=p.get("k",5))
    if model=="smileGradientTreeBoost":
        return GradientBoostingClassifier(n_estimators=p.get("numberOfTrees",100),
                                          learning_rate=p.get("shrinkage",0.1),
                                          subsample=p.get("samplingRate",0.7),random_state=42)
    raise ValueError(f"Modèle inconnu: {model}")

# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/auto-classifiers")
def list_auto_classifiers():
    """Liste des classifieurs GEE pré-construits — retourne un tableau."""
    return [
        {
            "id":          k,
            "label":       v["label"],
            "description": v["desc"],
            "icon":        v.get("icon","🌍"),
            "needs_dates": v.get("needs_dates",True),
            "year_only":   v.get("year_only",False),
            "date_min":    v.get("date_min"),
            "date_max":    v.get("date_max"),
            "avail_note":  v.get("avail_note",""),
            "classes":     [{"class_id":c[0],"label":c[1],"color":c[2]} for c in v["classes"]],
        }
        for k,v in AUTO_CLASSIFIERS.items()
    ]

@router.post("/auto-classify")
def auto_classify(req: AutoClassifyRequest):
    """Classification GEE pré-construite — aucune restriction de taille."""
    from gee_routes import init_gee, geojson_to_ee_geometry
    import ee
    if not init_gee(): raise HTTPException(503,"GEE non disponible")
    cfg=AUTO_CLASSIFIERS.get(req.classifier_id)
    if not cfg: raise HTTPException(404,f"Classifier inconnu: {req.classifier_id}")
    # ── Validation disponibilité temporelle ───────────────────────────────────
    d_min = cfg.get("date_min"); d_max = cfg.get("date_max")
    if d_min and req.date_start < d_min:
        note = cfg.get("avail_note","")
        raise HTTPException(400,
            f"⚠️ {cfg['label']} n'est pas disponible avant le {d_min}.\n{note}")
    if d_max and req.date_end > d_max:
        note = cfg.get("avail_note","")
        raise HTTPException(400,
            f"⚠️ {cfg['label']} n'est disponible que jusqu'au {d_max}.\n{note}")
    try:
        aoi_geom=geojson_to_ee_geometry(req.aoi)
        if not cfg.get("needs_dates"):
            image=ee.ImageCollection(cfg["collection"]).first()
        elif cfg.get("year_only"):
            yr=req.date_start[:4]
            col=ee.ImageCollection(cfg["collection"]).filter(
                ee.Filter.date(f"{yr}-01-01",f"{yr}-12-31"))
            image=col.first()
        else:
            col=ee.ImageCollection(cfg["collection"]).filterDate(req.date_start,req.date_end)
            image=col.mode()
        image=image.select(cfg["band"]).clip(aoi_geom)

        # Couleurs : liste ordonnée par index (frontend envoie List[str])
        user_colors=req.class_colors or []
        legend=[{"class_id":c[0],"label":c[1],
                 "color":user_colors[i] if i<len(user_colors) else c[2]}
                for i,c in enumerate(cfg["classes"])]
        class_ids=[c[0] for c in cfg["classes"]]
        colors=[l["color"] for l in legend]

        tile_url=_gee_tile_url(image,class_ids,colors)
        # ── Surfaces par classe auto (ha) ─────────────────────────────────────
        auto_scale=AUTO_SCALE.get(req.classifier_id,30)
        try:
            hist=image.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram(),
                geometry=aoi_geom,scale=auto_scale,maxPixels=1e9,bestEffort=True
            ).getInfo()
            pix_ha=auto_scale*auto_scale/10000.0
            counts=hist.get(cfg["band"],{})
            for entry in legend:
                entry["area_ha"]=round(counts.get(str(entry["class_id"]),
                                       counts.get(entry["class_id"],0))*pix_ha,2)
        except Exception as ae: log.warning(f"auto area: {ae}")
        try:
            bb=aoi_geom.bounds().getInfo()["coordinates"][0]
            xs=[c[0] for c in bb]; ys=[c[1] for c in bb]
            img_bounds=[min(xs),min(ys),max(xs),max(ys)]
        except Exception: img_bounds=None

        job_id=str(uuid.uuid4())
        _jobs[job_id]={"type":"auto_gee","class_ids":class_ids,
                       "image_bounds":img_bounds,
                       "request":{"classifier_id":req.classifier_id,
                                  "aoi":req.aoi,"date_start":req.date_start,
                                  "date_end":req.date_end}}
        return {
            "job_id":job_id,"tile_url":tile_url,"image_url":None,
            "image_bounds":img_bounds,"legend":legend,
            "metrics":None,           # pas d'entraînement → pas de métriques
            "confusion_matrix":None,
            "class_labels":[c[1] for c in cfg["classes"]],
            "feature_importance":None,
            "backend":"gee_auto","bands_used":[cfg["band"]],
            "classifier_label":cfg["label"],
        }
    except HTTPException: raise
    except Exception as e:
        log.error(f"auto-classify: {e}",exc_info=True)
        raise HTTPException(500,f"Erreur GEE auto : {e}")

@router.post("/classify")
def classify_raster(req: ClassifyRequest):
    if len(req.rois)<2: raise HTTPException(400,"Minimum 2 classes requises")
    class_ids=sorted({r.class_id for r in req.rois})
    colors=req.class_colors or DEFAULT_COLORS
    labels={r.class_id:r.label for r in req.rois}
    legend=[{"class_id":cid,"label":labels.get(cid,f"Classe {cid}"),
             "color":colors[i%len(colors)]}
            for i,cid in enumerate(class_ids)]
    if req.layer_type=="gee" and req.gee_params:
        return _run_gee(req,legend,class_ids,colors)
    else:
        return _run_wms(req,legend,class_ids,colors)

@router.post("/classify/multidate")
def classify_multidate(req: MultidateRequest):
    """Classification multi-dates : entraîne sur la 1ère période, applique à toutes."""
    from gee_routes import init_gee, geojson_to_ee_geometry
    import ee
    if not init_gee(): raise HTTPException(503, "GEE non disponible")
    if len(req.rois) < 2:          raise HTTPException(400, "Minimum 2 classes requises")
    if len(req.date_periods) < 2:  raise HTTPException(400, "Minimum 2 périodes requises")
    try:
        colors   = req.class_colors or DEFAULT_COLORS
        class_ids= sorted({r.class_id for r in req.rois})
        labels   = {r.class_id: r.label for r in req.rois}
        n_cls    = len(class_ids)
        id_to_idx= {cid: i for i, cid in enumerate(class_ids)}

        aoi_geom = geojson_to_ee_geometry(req.aoi)

        # ── Entraînement sur la 1ère période ──────────────────────────────
        p0   = req.date_periods[0]
        gp0  = {**req.gee_params, "date_start": p0.start, "date_end": p0.end}
        img0, bands, scale, _ = _build_gee_image(gp0)
        img0 = img0.clip(aoi_geom)

        feats = []
        for roi in req.rois:
            try:
                mapped = id_to_idx.get(roi.class_id, roi.class_id)
                feats.append(ee.Feature(geojson_to_ee_geometry(roi.geometry), {"class": mapped}))
            except Exception as ex: log.warning(f"ROI multidate skip: {ex}")
        if len(feats) < 2: raise HTTPException(400, "ROIs insuffisants")

        roi_fc   = ee.FeatureCollection(feats)
        samples  = img0.sampleRegions(collection=roi_fc, properties=["class"],
                                      scale=scale, geometries=False)
        train_d  = samples.filter(ee.Filter.lt("_split", req.train_ratio))
        samples  = samples.randomColumn("_split", 42)
        clf      = _gee_clf(req.model, req.model_params)
        trained  = clf.train(features=train_d, classProperty="class", inputProperties=bands)

        idx_class_ids = list(range(n_cls))
        cls_colors    = [colors[i % len(colors)] for i in range(n_cls)]

        # ── Application à chaque période ──────────────────────────────────
        period_results = []
        for period in req.date_periods:
            gp  = {**req.gee_params, "date_start": period.start, "date_end": period.end}
            img, _, _, pmeta = _build_gee_image(gp)
            img = img.clip(aoi_geom)
            classified = img.select(bands).classify(trained).clip(aoi_geom)

            tile_url = _gee_tile_url(classified, idx_class_ids, cls_colors)

            # Légende avec surfaces
            leg = [{"class_id": cid, "label": labels.get(cid, f"Classe {cid}"),
                    "color": colors[i % len(colors)]}
                   for i, cid in enumerate(class_ids)]
            try:
                hist = classified.reduceRegion(
                    reducer=ee.Reducer.frequencyHistogram(),
                    geometry=aoi_geom, scale=scale, maxPixels=1e9, bestEffort=True
                ).getInfo()
                pix_ha  = scale * scale / 10000.0
                counts  = hist.get("classification", {})
                for j, entry in enumerate(leg):
                    entry["area_ha"] = round(counts.get(str(j), counts.get(j, 0)) * pix_ha, 2)
            except Exception as ae: log.warning(f"multidate area {period.label}: {ae}")

            period_results.append({
                "label":           period.label or period.start[:4],
                "start":           period.start,
                "end":             period.end,
                "tile_url":        tile_url,
                "legend":          leg,
                "cloud_cover_pct": pmeta.get("cloud_pct"),
                "image_count":     pmeta.get("image_count"),
            })

        try:
            bb  = aoi_geom.bounds().getInfo()["coordinates"][0]
            xs  = [c[0] for c in bb]; ys = [c[1] for c in bb]
            img_bounds = [min(xs), min(ys), max(xs), max(ys)]
        except Exception: img_bounds = None

        job_id = str(uuid.uuid4())
        _jobs[job_id] = {"type": "gee_multidate", "class_ids": idx_class_ids,
                         "image_bounds": img_bounds,
                         "period_results": period_results}

        # Première période = couche initiale sur la carte
        first = period_results[0]
        return {
            "job_id":         job_id,
            "backend":        "gee_multidate",
            "tile_url":       first["tile_url"],
            "image_url":      None,
            "image_bounds":   img_bounds,
            "legend":         first["legend"],
            "bands_used":     bands,
            "class_labels":   [labels.get(cid, f"Classe {cid}") for cid in class_ids],
            "period_results": period_results,
            "metrics":        None,
            "confusion_matrix": None,
            "feature_importance": None,
        }
    except HTTPException: raise
    except Exception as e:
        log.error(f"multidate: {e}", exc_info=True)
        raise HTTPException(500, f"Erreur multi-dates : {e}")

@router.post("/classify/restyle")
def restyle_classify(req: RestyleRequest):
    """Re-coloriser une classification existante sans réentraîner."""
    job=_jobs.get(req.job_id)
    if not job: raise HTTPException(404,"Job expiré — relancer la classification")
    t=job["type"]
    if t=="wms":                       return _restyle_wms(job,req.class_colors)
    if t in ("gee","auto_gee"):        return _restyle_gee(job,req.class_colors)
    if t=="cluster_gee":               return _restyle_cluster(job,req.class_colors)
    raise HTTPException(400,f"Type de job inconnu: {t}")

# ── Modèle de requête clustering ─────────────────────────────────────────────
class ClusterRequest(BaseModel):
    gee_params:   dict
    aoi:          dict
    method:       str = "kmeans"   # "kmeans" | "xmeans"
    n_clusters:   int = 5
    max_clusters: int = 10
    sample_size:  int = 5000
    class_colors: Optional[List[str]] = None

@router.post("/cluster")
def cluster_raster(req: ClusterRequest):
    """Classification non supervisée GEE (K-Means / X-Means)."""
    from gee_routes import init_gee,geojson_to_ee_geometry
    import ee
    if not init_gee(): raise HTTPException(503,"GEE non disponible")
    try:
        image,bands,scale,img_meta=_build_gee_image(req.gee_params)
        aoi_geom=geojson_to_ee_geometry(req.aoi)
        image=image.clip(aoi_geom)
        training=image.select(bands).sample(
            region=aoi_geom,scale=scale,
            numPixels=req.sample_size,seed=42,geometries=False)
        if req.method=="xmeans":
            clusterer=ee.Clusterer.wekaXMeans(
                minClusters=2,maxClusters=req.max_clusters,seed=42)
        else:
            clusterer=ee.Clusterer.wekaKMeans(
                nClusters=req.n_clusters,seed=42)
        trained=clusterer.train(training)
        clustered=image.select(bands).cluster(trained).clip(aoi_geom)
        n=req.n_clusters if req.method=="kmeans" else req.max_clusters
        colors=req.class_colors or [DEFAULT_COLORS[i%len(DEFAULT_COLORS)] for i in range(n)]
        palette=[c.lstrip("#") for c in colors[:n]]
        vis={"min":0,"max":max(n-1,1),"palette":palette}
        mid=clustered.getMapId(vis); fetcher=mid.get("tile_fetcher")
        tile_url=(fetcher.url_format if (fetcher and hasattr(fetcher,"url_format"))
                  else mid.get("urlFormat",""))
        legend=[{"class_id":i,"label":f"Cluster {i+1}","color":colors[i%len(colors)]}
                for i in range(n)]
        # ── Surfaces par cluster (ha) ─────────────────────────────────────────
        try:
            hist=clustered.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram(),
                geometry=aoi_geom,scale=scale,maxPixels=1e9,bestEffort=True
            ).getInfo()
            pix_ha=scale*scale/10000.0
            counts=hist.get("cluster",{})
            for entry in legend:
                entry["area_ha"]=round(counts.get(str(entry["class_id"]),
                                       counts.get(entry["class_id"],0))*pix_ha,2)
        except Exception as ae: log.warning(f"cluster area: {ae}")
        try:
            bb=aoi_geom.bounds().getInfo()["coordinates"][0]
            xs=[c[0] for c in bb]; ys=[c[1] for c in bb]
            img_bounds=[min(xs),min(ys),max(xs),max(ys)]
        except Exception: img_bounds=None
        job_id=str(uuid.uuid4())
        _jobs[job_id]={"type":"cluster_gee","n_clusters":n,
                       "class_ids":list(range(n)),"image_bounds":img_bounds,
                       "request":{"gee_params":req.gee_params,"aoi":req.aoi,
                                  "method":req.method,"n_clusters":req.n_clusters,
                                  "max_clusters":req.max_clusters,"sample_size":req.sample_size}}
        return {"job_id":job_id,"tile_url":tile_url,"image_url":None,
                "image_bounds":img_bounds,"legend":legend,
                "metrics":None,"confusion_matrix":None,
                "class_labels":[l["label"] for l in legend],
                "feature_importance":None,"backend":"gee_cluster",
                "bands_used":bands,"method":req.method,"n_clusters":n,
                "cloud_cover_pct":img_meta.get("cloud_pct"),
                "image_count":img_meta.get("image_count")}
    except HTTPException: raise
    except Exception as e:
        log.error(f"cluster: {e}",exc_info=True); raise HTTPException(500,f"Erreur clustering : {e}")

# ─── Helpers métriques GEE robustes ─────────────────────────────────────────
def _safe_array(ee_obj):
    """Extrait une liste 2D depuis un objet EE Array (gère dict ou list)."""
    raw=ee_obj.getInfo()
    if isinstance(raw,list): return raw
    if isinstance(raw,dict):
        for k in ("array","list","values"):
            if k in raw: return raw[k]
    return []

def _safe_acc_vector(ee_obj):
    """Extrait un vecteur 1D depuis producersAccuracy/consumersAccuracy (Nx1 ou N)."""
    raw=_safe_array(ee_obj)
    result=[]
    for item in raw:
        if isinstance(item,(list,tuple)):
            result.append(float(item[0]) if item else 0.0)
        else:
            result.append(float(item) if item is not None else 0.0)
    return result

# ─── Pipeline GEE supervisé ──────────────────────────────────────────────────
def _run_gee(req,legend,class_ids,colors):
    from gee_routes import init_gee,geojson_to_ee_geometry
    import ee
    if not init_gee(): raise HTTPException(503,"GEE non disponible")
    try:
        image,bands,scale,img_meta=_build_gee_image(req.gee_params)
        aoi_geom=geojson_to_ee_geometry(req.aoi)
        image=image.clip(aoi_geom)

        # ── IMPORTANT : remappe les class_id originaux (timestamps JS possible) vers 0..N-1
        # Sinon errorMatrix crée une matrice gigantesque → accuracy = 0 partout
        n_cls=len(class_ids)
        id_to_idx={cid:i for i,cid in enumerate(class_ids)}   # {orig_id → 0..N-1}
        idx_class_ids=list(range(n_cls))                        # [0,1,2,...N-1]

        feats=[]
        for roi in req.rois:
            try:
                mapped=id_to_idx.get(roi.class_id, roi.class_id)
                feats.append(ee.Feature(geojson_to_ee_geometry(roi.geometry),{"class":mapped}))
            except Exception as ex: log.warning(f"ROI skip: {ex}")
        if len(feats)<2: raise HTTPException(400,"ROI insuffisants")

        roi_fc=ee.FeatureCollection(feats)
        samples=image.sampleRegions(collection=roi_fc,properties=["class"],
                                    scale=scale,geometries=False)
        samples=samples.randomColumn("_split",42)
        train_d=samples.filter(ee.Filter.lt("_split",req.train_ratio))
        test_d =samples.filter(ee.Filter.gte("_split",req.train_ratio))

        clf=_gee_clf(req.model,req.model_params)
        supports_imp=req.model in ("smileRandomForest","smileCart","smileGradientTreeBoost")
        trained=clf.train(features=train_d,classProperty="class",inputProperties=bands)
        classified=image.select(bands).classify(trained).clip(aoi_geom)

        # ── Métriques sur TOUS les échantillons ────────────────────────────
        # (évite le problème du test set vide avec de petits ROIs,
        #  donne l'accuracy de resubstitution — optimiste mais non-nulle)
        validated=samples.classify(trained)
        cm_obj=validated.errorMatrix("class","classification")
        oa=float(cm_obj.accuracy().getInfo())
        kappa=float(cm_obj.kappa().getInfo())

        try: cm_list=_safe_array(cm_obj.array())
        except Exception: cm_list=[]

        try:
            pa_vec=_safe_acc_vector(cm_obj.producersAccuracy())   # recall  (lignes)
            ca_vec=_safe_acc_vector(cm_obj.consumersAccuracy())   # precision (colonnes)
        except Exception:
            pa_vec=[0.0]*n_cls; ca_vec=[0.0]*n_cls

        per_class=[]
        for i in range(n_cls):
            prec=ca_vec[i] if i<len(ca_vec) else 0.0
            rec =pa_vec[i] if i<len(pa_vec) else 0.0
            f1  =2*prec*rec/(prec+rec) if (prec+rec)>0 else 0.0
            per_class.append({"label":legend[i]["label"],"class_id":class_ids[i],
                              "precision":round(prec,3),"recall":round(rec,3),
                              "f1":round(f1,3)})

        feat_imp=None
        if supports_imp:
            try:
                raw=trained.explain().getInfo().get("importance",{})
                tot=sum(raw.values()) or 1
                feat_imp={k:round(v/tot,4) for k,v in sorted(raw.items(),key=lambda x:-x[1])}
            except Exception as ex: log.warning(f"importance: {ex}")

        # tile_url utilise idx_class_ids (0..N-1) car classified a les valeurs 0..N-1
        tile_url=_gee_tile_url(classified,idx_class_ids,colors)
        try:
            bb=aoi_geom.bounds().getInfo()["coordinates"][0]
            xs=[c[0] for c in bb]; ys=[c[1] for c in bb]
            img_bounds=[min(xs),min(ys),max(xs),max(ys)]
        except Exception: img_bounds=None

        # ── Surfaces par classe (ha) ──────────────────────────────────────────
        try:
            hist=classified.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram(),
                geometry=aoi_geom,scale=scale,maxPixels=1e9,
                bestEffort=True
            ).getInfo()
            pix_ha=scale*scale/10000.0
            counts=hist.get("classification",{})
            for i,entry in enumerate(legend):
                entry["area_ha"]=round(counts.get(str(i),counts.get(i,0))*pix_ha,2)
        except Exception as ae: log.warning(f"gee area: {ae}")

        job_id=str(uuid.uuid4())
        # Stocker id_to_idx pour que le restyle re-mappe correctement
        _jobs[job_id]={"type":"gee","class_ids":idx_class_ids,"image_bounds":img_bounds,
                       "id_to_idx":id_to_idx,
                       "request":{"gee_params":req.gee_params,"aoi":req.aoi,
                                  "rois":[r.model_dump() for r in req.rois],
                                  "model":req.model,"model_params":req.model_params,
                                  "train_ratio":req.train_ratio}}
        return {"job_id":job_id,"tile_url":tile_url,"image_url":None,
                "image_bounds":img_bounds,"legend":legend,
                "metrics":{"overall_accuracy":round(oa,4),"kappa":round(kappa,4),
                            "per_class":per_class},
                "confusion_matrix":cm_list,
                "class_labels":[l["label"] for l in legend],
                "feature_importance":feat_imp,"backend":"gee","bands_used":bands,
                "cloud_cover_pct":img_meta.get("cloud_pct"),
                "image_count":img_meta.get("image_count"),
                "sensor":img_meta.get("sensor","S2_SR"),
                "landsat_fallback":img_meta.get("fallback",False),
                "fallback_reason":img_meta.get("fallback_reason")}
    except HTTPException: raise
    except Exception as e:
        log.error(f"GEE classify: {e}",exc_info=True); raise HTTPException(500,f"Erreur GEE : {e}")

# ─── Pipeline WMS / sklearn ──────────────────────────────────────────────────
def _run_wms(req,legend,class_ids,colors):
    try:
        from sklearn.metrics import confusion_matrix,accuracy_score,cohen_kappa_score,classification_report
        from sklearn.preprocessing import StandardScaler
    except ImportError: raise HTTPException(503,"scikit-learn requis")
    try:
        from PIL import Image as PILImage
    except ImportError: raise HTTPException(503,"Pillow requis")
    area=area_km2(req.aoi)
    if area>105:
        raise HTTPException(400,f"Zone trop grande ({area:.2f} km²). Limite 100 km² pour WMS/Tiles.")
    if not req.tile_url: raise HTTPException(400,"tile_url requis")
    bbox=bbox_from_geojson(req.aoi); zoom=15 if area<0.5 else 14
    try:
        mosaic,tx0,ty0,mw,mh=_build_mosaic(req.tile_url,bbox,zoom)
        X_list,y_list=[],[]
        for roi in req.rois:
            mask=_rasterize(roi.geometry,zoom,tx0,ty0,mw,mh)
            pix=mosaic[mask].astype(np.float32)
            if len(pix)==0: continue
            if len(pix)>5000:
                idx=np.random.choice(len(pix),5000,replace=False); pix=pix[idx]
            X_list.extend(pix.tolist()); y_list.extend([roi.class_id]*len(pix))
        if len(set(y_list))<2: raise HTTPException(400,"Pixels insuffisants — vérifiez les ROIs")
        X=np.array(X_list,dtype=np.float32); y=np.array(y_list)
        aoi_mask=_rasterize(req.aoi,zoom,tx0,ty0,mw,mh)
        X_predict=mosaic[aoi_mask].astype(np.float32)
        scaler=StandardScaler(); X_sc=scaler.fit_transform(X); Xp_sc=scaler.transform(X_predict)
        perm=np.random.permutation(len(X)); n_tr=int(len(X)*req.train_ratio)
        tr,te=perm[:n_tr],perm[n_tr:]
        clf=_sk_clf(req.model,req.model_params); clf.fit(X_sc[tr],y[tr])
        y_pred=clf.predict(X_sc[te]); y_true=y[te]
        oa=float(accuracy_score(y_true,y_pred)); kappa=float(cohen_kappa_score(y_true,y_pred))
        cm=confusion_matrix(y_true,y_pred,labels=class_ids).tolist()
        rep=classification_report(y_true,y_pred,labels=class_ids,output_dict=True,zero_division=0)
        per_class=[{"label":legend[i]["label"],"class_id":cid,
                    "precision":round(rep.get(str(cid),{}).get("precision",0),3),
                    "recall":round(rep.get(str(cid),{}).get("recall",0),3),
                    "f1":round(rep.get(str(cid),{}).get("f1-score",0),3),
                    "support":int(rep.get(str(cid),{}).get("support",0))}
                   for i,cid in enumerate(class_ids)]
        feat_imp=None
        if hasattr(clf,"feature_importances_"):
            fi=clf.feature_importances_; names=["R","G","B"]
            feat_imp={names[i]:round(float(fi[i]),4) for i in range(min(len(fi),3))}
        y_all=clf.predict(Xp_sc)
        # ── Surfaces par classe (estimation pixel) ────────────────────────────
        try:
            from collections import Counter
            lat_c=(bbox[1]+bbox[3])/2
            tile_m=40075016.686*math.cos(math.radians(lat_c))/(2**zoom)
            pix_ha=(tile_m/256)**2/10000
            cnt=Counter(y_all.tolist())
            for cid,entry in zip(class_ids,legend):
                entry["area_ha"]=round(cnt.get(cid,0)*pix_ha,2)
        except Exception as ae: log.warning(f"wms area: {ae}")
        rows,cols=np.where(aoi_mask)
        img_b64=_render_wms_image(y_all,rows,cols,mh,mw,class_ids,colors)
        nw_lon,nw_lat=_tile_nw(tx0,ty0,zoom)
        se_lon,se_lat=_tile_nw(tx0+(mw//256),ty0+(mh//256),zoom)
        img_bounds=[nw_lon,se_lat,se_lon,nw_lat]
        job_id=str(uuid.uuid4())
        _jobs[job_id]={"type":"wms","class_ids":class_ids,"mosaic_shape":(mh,mw),
                       "image_bounds":img_bounds,
                       "wms_predictions":{"predictions":y_all.tolist(),
                                          "rows":rows.tolist(),"cols":cols.tolist()}}
        return {"job_id":job_id,"tile_url":None,"image_url":img_b64,
                "image_bounds":img_bounds,"legend":legend,
                "metrics":{"overall_accuracy":round(oa,4),"kappa":round(kappa,4),"per_class":per_class},
                "confusion_matrix":cm,"class_labels":[l["label"] for l in legend],
                "feature_importance":feat_imp,"backend":"sklearn","bands_used":["R","G","B"]}
    except HTTPException: raise
    except Exception as e:
        log.error(f"WMS classify: {e}",exc_info=True); raise HTTPException(500,f"Erreur WMS : {e}")

# ─── Restyle WMS ─────────────────────────────────────────────────────────────
def _restyle_wms(job,new_colors):
    d=job.get("wms_predictions")
    if not d: raise HTTPException(404,"Données de prédiction non disponibles")
    mh,mw=job["mosaic_shape"]
    y_all=np.array(d["predictions"]); rows=np.array(d["rows"]); cols=np.array(d["cols"])
    img_b64=_render_wms_image(y_all,rows,cols,mh,mw,job["class_ids"],list(new_colors))
    return {"tile_url":None,"image_url":img_b64,"image_bounds":job.get("image_bounds")}

# ─── Restyle GEE supervisé / auto ────────────────────────────────────────────
def _restyle_gee(job,new_colors):
    from gee_routes import init_gee,geojson_to_ee_geometry
    import ee
    if not init_gee(): raise HTTPException(503,"GEE non disponible")
    try:
        req_data=job["request"]; class_ids=job["class_ids"]
        if job["type"]=="auto_gee":
            cfg=AUTO_CLASSIFIERS.get(req_data["classifier_id"])
            aoi_geom=geojson_to_ee_geometry(req_data["aoi"])
            if not cfg.get("needs_dates"):
                image=ee.ImageCollection(cfg["collection"]).first()
            elif cfg.get("year_only"):
                yr=req_data["date_start"][:4]
                image=ee.ImageCollection(cfg["collection"]).filter(
                    ee.Filter.date(f"{yr}-01-01",f"{yr}-12-31")).first()
            else:
                image=ee.ImageCollection(cfg["collection"]).filterDate(
                    req_data["date_start"],req_data["date_end"]).mode()
            image=image.select(cfg["band"]).clip(aoi_geom)
            # auto_gee utilise les class_ids originaux (ex 10,20,30...) → remap dans _gee_tile_url
            orig_class_ids=[c[0] for c in AUTO_CLASSIFIERS[req_data["classifier_id"]]["classes"]]
            tile_url=_gee_tile_url(image,orig_class_ids,list(new_colors))
        else:
            # Supervisé : re-entraîner avec les mêmes ROIs remappés 0..N-1
            id_to_idx=job.get("id_to_idx",{})
            image,bands,scale=_build_gee_image(req_data["gee_params"])
            aoi_geom=geojson_to_ee_geometry(req_data["aoi"])
            image=image.clip(aoi_geom)
            feats=[]
            for r in req_data["rois"]:
                mapped=id_to_idx.get(r["class_id"], r["class_id"])
                try: feats.append(ee.Feature(geojson_to_ee_geometry(r["geometry"]),{"class":mapped}))
                except Exception: pass
            roi_fc=ee.FeatureCollection(feats)
            samples=image.sampleRegions(collection=roi_fc,properties=["class"],
                                        scale=scale,geometries=False)
            samples=samples.randomColumn("_split",42)
            train_d=samples.filter(ee.Filter.lt("_split",req_data["train_ratio"]))
            clf=_gee_clf(req_data["model"],req_data["model_params"])
            trained=clf.train(features=train_d,classProperty="class",inputProperties=bands)
            classified=image.select(bands).classify(trained).clip(aoi_geom)
            # class_ids déjà 0..N-1
            tile_url=_gee_tile_url(classified,class_ids,list(new_colors))
        return {"tile_url":tile_url,"image_url":None,"image_bounds":job.get("image_bounds")}
    except Exception as e:
        raise HTTPException(500,f"Restyle GEE : {e}")

# ─── Restyle clustering ───────────────────────────────────────────────────────
def _restyle_cluster(job,new_colors):
    from gee_routes import init_gee,geojson_to_ee_geometry
    import ee
    if not init_gee(): raise HTTPException(503,"GEE non disponible")
    try:
        req_data=job["request"]; n=job["n_clusters"]
        image,bands,scale=_build_gee_image(req_data["gee_params"])
        aoi_geom=geojson_to_ee_geometry(req_data["aoi"])
        image=image.clip(aoi_geom)
        training=image.select(bands).sample(
            region=aoi_geom,scale=scale,
            numPixels=req_data["sample_size"],seed=42,geometries=False)
        if req_data["method"]=="xmeans":
            clusterer=ee.Clusterer.wekaXMeans(
                minClusters=2,maxClusters=req_data["max_clusters"],seed=42)
        else:
            clusterer=ee.Clusterer.wekaKMeans(nClusters=req_data["n_clusters"],seed=42)
        trained=clusterer.train(training)
        clustered=image.select(bands).cluster(trained).clip(aoi_geom)
        palette=[c.lstrip("#") for c in list(new_colors)[:n]]
        vis={"min":0,"max":max(n-1,1),"palette":palette}
        mid=clustered.getMapId(vis); fetcher=mid.get("tile_fetcher")
        tile_url=(fetcher.url_format if (fetcher and hasattr(fetcher,"url_format"))
                  else mid.get("urlFormat",""))
        return {"tile_url":tile_url,"image_url":None,"image_bounds":job.get("image_bounds")}
    except Exception as e:
        raise HTTPException(500,f"Restyle cluster : {e}")
