"""
gee_change_detection.py — Détection de changement GEE
Route : POST /api/gee/change-detection
Calcule la différence B - A entre deux périodes sur le même indice.

Intégration dans agent.py :
    from gee_change_detection import router as change_router
    app.include_router(change_router)
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter(prefix="/api/gee", tags=["gee-change"])

# ── Modèle de requête ─────────────────────────────────────────────────────────
class ChangeDetectionRequest(BaseModel):
    dataset:      str
    index:        str             = "NDVI"
    date_start_a: str             = "2018-06-01"
    date_end_a:   str             = "2018-09-30"
    date_start_b: str             = "2024-06-01"
    date_end_b:   str             = "2024-09-30"
    cloud_max:    float           = 20.0
    composite:    str             = "median"      # median | least_cloudy | mosaic
    threshold:    float           = 0.1           # |Δ| > threshold → changement
    bbox:         Optional[List[float]] = None
    roi_geojson:  Optional[dict]  = None


# ── Collections et bandes par dataset/indice ──────────────────────────────────
COLLECTION_CFG = {
    "sentinel2": {
        "collection": "COPERNICUS/S2_SR_HARMONIZED",
        "cloud_prop": "CLOUDY_PIXEL_PERCENTAGE",
        "bands": {
            "NDVI":        ("B8",  "B4"),
            "NDWI":        ("B3",  "B8"),
            "NDBI":        ("B11", "B8"),
            "EVI":         ("B8",  "B4", "B2"),
            "RGB":         ("B4",  "B3", "B2"),
        },
        "scale": 1.0,
    },
    "landsat8": {
        "collection": "LANDSAT/LC08/C02/T1_L2",
        "cloud_prop": "CLOUD_COVER",
        "bands": {
            "NDVI": ("SR_B5", "SR_B4"),
            "NDWI": ("SR_B3", "SR_B5"),
            "LST":  ("ST_B10",),
        },
        "scale": 0.0000275,
        "offset": -0.2,
        "lst_scale": 0.00341802,
        "lst_offset": 149.0,
    },
    "landsat9": {
        "collection": "LANDSAT/LC09/C02/T1_L2",
        "cloud_prop": "CLOUD_COVER",
        "bands": {
            "NDVI": ("SR_B5", "SR_B4"),
            "NDWI": ("SR_B3", "SR_B5"),
            "LST":  ("ST_B10",),
        },
        "scale": 0.0000275,
        "offset": -0.2,
        "lst_scale": 0.00341802,
        "lst_offset": 149.0,
    },
    "modis_ndvi": {
        "collection": "MODIS/061/MOD13A1",
        "cloud_prop": None,
        "bands": {
            "NDVI": ("NDVI",),
            "EVI":  ("EVI",),
        },
        "scale": 0.0001,
    },
    "modis_lst": {
        "collection": "MODIS/061/MOD11A1",
        "cloud_prop": None,
        "bands": {
            "LST Jour": ("LST_Day_1km",),
            "LST Nuit": ("LST_Night_1km",),
        },
        "lst_scale": 0.02,
        "kelvin_offset": -273.15,
    },
    "sentinel1": {
        "collection": "COPERNICUS/S1_GRD",
        "cloud_prop": None,
        "bands": {
            "VV": ("VV",),
            "VH": ("VH",),
        },
        "scale": 1.0,
    },
}

# ── Palettes différentielles ──────────────────────────────────────────────────
DIFF_VIS = {
    "NDVI":        {"palette":["a50026","d73027","f46d43","fdae61","ffffff","a6d96a","66bd63","1a9850","006837"], "min":-0.4, "max":0.4},
    "NDWI":        {"palette":["8B4513","DEB887","ffffff","AED6F1","1A5276"], "min":-0.4, "max":0.4},
    "NDBI":        {"palette":["1a9850","fee08b","d73027"], "min":-0.4, "max":0.4},
    "EVI":         {"palette":["a50026","fdae61","ffffff","a6d96a","006837"], "min":-0.4, "max":0.4},
    "LST":         {"palette":["2166ac","92c5de","ffffff","f4a582","b2182b"], "min":-5,   "max":5  },
    "LST Jour":    {"palette":["2166ac","92c5de","ffffff","f4a582","b2182b"], "min":-5,   "max":5  },
    "LST Nuit":    {"palette":["2166ac","92c5de","ffffff","f4a582","b2182b"], "min":-5,   "max":5  },
    "VV":          {"palette":["d73027","fee08b","ffffff","91bfdb","4575b4"], "min":-5,   "max":5  },
    "VH":          {"palette":["d73027","fee08b","ffffff","91bfdb","4575b4"], "min":-5,   "max":5  },
    "RGB":         {"palette":["d73027","fee08b","ffffff","91bfdb","4575b4"], "min":-1000,"max":1000},
}


# ── Init GEE — délégué à gee_auth (Windows + Linux) ─────────────────────────
from gee_auth import get_ee, init_gee


# ── Construire un composite pour une période ──────────────────────────────────
def build_composite(ee, cfg, d_start, d_end, cloud_max, composite_mode, region, index):
    """
    Retourne une ee.Image représentant l'indice calculé pour la période donnée.
    """
    col = (ee.ImageCollection(cfg["collection"])
           .filterDate(d_start, d_end)
           .filterBounds(region))

    # Filtre nuages
    cloud_prop = cfg.get("cloud_prop")
    if cloud_prop and cloud_max < 100:
        col = col.filter(ee.Filter.And(
            ee.Filter.notNull([cloud_prop]),
            ee.Filter.lte(cloud_prop, cloud_max),
        ))

    count = col.size().getInfo()
    if count == 0:
        # Retry sans filtre nuages
        col = (ee.ImageCollection(cfg["collection"])
               .filterDate(d_start, d_end)
               .filterBounds(region))
        count = col.size().getInfo()
        if count == 0:
            return None

    # Composite
    if composite_mode == "least_cloudy" and cloud_prop:
        img = col.sort(cloud_prop).first()
    elif composite_mode == "mosaic":
        img = col.mosaic()
    else:
        img = col.median()

    sf  = cfg.get("scale", 1.0)
    off = cfg.get("offset", 0.0)
    bands = cfg["bands"].get(index)
    if not bands:
        return None

    # Calcul de l'indice
    if index == "RGB":
        r, g, b = bands
        out = img.select([r, g, b])
        if sf != 1.0:
            out = out.multiply(sf).add(off)
        # Pour diff RGB : utiliser la moyenne des 3 bandes
        return out.reduce(ee.Reducer.mean()).rename("RGB_mean")

    elif index in ("NDVI", "NDWI", "NDBI"):
        b1, b2 = bands[:2]
        band1 = img.select(b1)
        band2 = img.select(b2)
        if sf != 1.0:
            band1 = band1.multiply(sf).add(off)
            band2 = band2.multiply(sf).add(off)
        return band1.subtract(band2).divide(band1.add(band2)).rename(index)

    elif index == "EVI":
        if len(bands) >= 3:
            nir, red, blue = bands[:3]
            if sf != 1.0:
                nir_b  = img.select(nir).multiply(sf).add(off)
                red_b  = img.select(red).multiply(sf).add(off)
                blue_b = img.select(blue).multiply(sf).add(off)
            else:
                nir_b = img.select(nir)
                red_b = img.select(red)
                blue_b = img.select(blue)
            evi = nir_b.subtract(red_b).divide(
                nir_b.add(red_b.multiply(6)).subtract(blue_b.multiply(7.5)).add(1)
            ).multiply(2.5).rename("EVI")
            return evi
        else:
            b1, b2 = bands[:2]
            return img.select(b1).subtract(img.select(b2)).divide(
                img.select(b1).add(img.select(b2))
            ).rename("EVI")

    elif index in ("LST", "LST Jour", "LST Nuit"):
        b = bands[0]
        lst_sf  = cfg.get("lst_scale", 0.02)
        lst_off = cfg.get("lst_offset", 149.0) if "lst_offset" in cfg else 0
        kelvin  = cfg.get("kelvin_offset", 0)
        if lst_off:
            return img.select(b).multiply(lst_sf).add(lst_off).add(kelvin).rename("LST")
        else:
            return img.select(b).multiply(lst_sf).add(kelvin).rename("LST")

    elif index in ("VV", "VH"):
        b = bands[0]
        return img.select(b).rename(index)

    elif index in ("NDVI", "EVI") and cfg.get("scale") == 0.0001:
        b = bands[0]
        return img.select(b).multiply(cfg["scale"]).rename(index)

    return None


# ── Route principale ──────────────────────────────────────────────────────────
@router.post("/change-detection")
def gee_change_detection(req: ChangeDetectionRequest):
    """
    Calcule la différence B - A entre deux périodes sur le même indice.
    Retourne une tuile WMS de la carte différentielle + statistiques.
    """
    if not init_gee():
        raise HTTPException(503, "GEE non disponible — vérifiez les credentials")
    try:
        ee = get_ee()
    except Exception as e:
        raise HTTPException(503, f"GEE non initialisé : {e}")

    cfg = COLLECTION_CFG.get(req.dataset)
    if not cfg:
        raise HTTPException(400, f"Dataset inconnu : {req.dataset}")

    if req.index not in cfg["bands"]:
        raise HTTPException(400, f"Indice '{req.index}' non disponible pour {req.dataset}")

    # ── ROI ───────────────────────────────────────────────────
    if req.roi_geojson:
        region = ee.Geometry(req.roi_geojson)
    elif req.bbox:
        w, s, e, n = req.bbox
        region = ee.Geometry.BBox(w, s, e, n)
    else:
        raise HTTPException(400, "Fournissez une bbox ou un roi_geojson")

    # ── Composite A (référence) ───────────────────────────────
    img_a = build_composite(ee, cfg, req.date_start_a, req.date_end_a,
                            req.cloud_max, req.composite, region, req.index)
    if img_a is None:
        raise HTTPException(404, f"Aucune image disponible pour la période A ({req.date_start_a} → {req.date_end_a})")

    # ── Composite B (comparaison) ─────────────────────────────
    img_b = build_composite(ee, cfg, req.date_start_b, req.date_end_b,
                            req.cloud_max, req.composite, region, req.index)
    if img_b is None:
        raise HTTPException(404, f"Aucune image disponible pour la période B ({req.date_start_b} → {req.date_end_b})")

    # ── Différence B - A ──────────────────────────────────────
    diff = img_b.subtract(img_a).rename("diff").clip(region)

    # ── Paramètres de visualisation ───────────────────────────
    vis_key = req.index
    vis     = DIFF_VIS.get(vis_key, DIFF_VIS["NDVI"])

    # ── URL tuiles ────────────────────────────────────────────
    tile_url = diff.visualize(**{
        "min":     vis["min"],
        "max":     vis["max"],
        "palette": vis["palette"],
    }).getMapId()["tile_fetcher"].url_format

    # ── Statistiques de changement ────────────────────────────
    try:
        # Réduction sur la région à 100m de résolution pour aller vite
        stats_raw = diff.reduceRegion(
            reducer=ee.Reducer.mean()
                      .combine(ee.Reducer.stdDev(), sharedInputs=True)
                      .combine(ee.Reducer.percentile([10, 25, 75, 90]), sharedInputs=True),
            geometry=region,
            scale=100,
            maxPixels=1e8,
        ).getInfo()

        band_name = "diff"
        mean_val  = stats_raw.get(f"{band_name}_mean", 0) or 0
        std_val   = stats_raw.get(f"{band_name}_stdDev", 0) or 0

        # Pixels gain / perte / stable via seuil
        gain_mask  = diff.gt(req.threshold)
        loss_mask  = diff.lt(-req.threshold)
        stable_mask= diff.abs().lte(req.threshold)

        gain_area  = gain_mask.multiply(ee.Image.pixelArea()).reduceRegion(ee.Reducer.sum(), region, 100, maxPixels=1e8).getInfo().get("diff", 0) or 0
        loss_area  = loss_mask.multiply(ee.Image.pixelArea()).reduceRegion(ee.Reducer.sum(), region, 100, maxPixels=1e8).getInfo().get("diff", 0) or 0
        total_area = ee.Image.pixelArea().reduceRegion(ee.Reducer.sum(), region, 100, maxPixels=1e8).getInfo().get("area", 1) or 1

        pct_gain   = gain_area  / total_area * 100
        pct_loss   = loss_area  / total_area * 100
        pct_stable = max(0, 100 - pct_gain - pct_loss)

        stats = {
            "mean_change":   round(mean_val, 4),
            "std_change":    round(std_val,  4),
            "pct_gain":      round(pct_gain,  2),
            "pct_loss":      round(pct_loss,  2),
            "pct_stable":    round(pct_stable, 2),
            "area_gain_km2": round(gain_area  / 1e6, 2),
            "area_loss_km2": round(loss_area  / 1e6, 2),
        }
    except Exception as ex:
        print(f"Stats error: {ex}")
        stats = None

    # ── Bbox de la couche ─────────────────────────────────────
    try:
        bounds = region.bounds().getInfo()["coordinates"][0]
        clip_bbox = [
            min(c[0] for c in bounds),
            min(c[1] for c in bounds),
            max(c[0] for c in bounds),
            max(c[1] for c in bounds),
        ]
    except Exception:
        clip_bbox = req.bbox

    name = f"Δ {req.index} · {req.date_start_a[:7]} → {req.date_end_b[:7]}"

    return {
        "tile_url":  tile_url,
        "name":      name,
        "vis_params": { "palette": vis["palette"], "min": vis["min"], "max": vis["max"] },
        "stats":     stats,
        "clip_bbox": clip_bbox,
        "dataset":   req.dataset,
        "index":     req.index,
    }
