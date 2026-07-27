"""
gee_timelapse.py — Route FastAPI Timelapse GEE v2
Landsat unifié (5/7/8/9 auto selon l'année), composite, annotations Pillow.

Annotations sur chaque frame :
  - Titre + description (coin haut-gauche)
  - Timestamp lisible "Juin 2023" adapté à la fréquence
  - Flèche Nord (coin bas-droit)
  - Barre d'échelle en km (coin bas-droit)
  - Source satellite auto en tout petit (ex : "Landsat 8 · GEE")
  - Barre de progression temporelle (bas de frame)
  - Légende palette mini (bas-gauche, si indice non-RGB)

Intégration dans agent.py :
    from gee_timelapse import router as tl_router
    app.include_router(tl_router)

Dépendances :
    pip install Pillow imageio requests
"""

import io
import os
import uuid
import tempfile
import calendar as cal_module
from typing import Optional, List
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import ee 
import json
from ee import oauth as ee_oauth
# google.oauth2 géré par gee_auth

router = APIRouter(prefix="/api/gee", tags=["gee-timelapse"])

TMP_DIR = os.path.join(tempfile.gettempdir(), "ome_timelapse")
os.makedirs(TMP_DIR, exist_ok=True)

# ── Initialisation GEE (réutilise le même service account) ───────────────────
# ── Init GEE — délégué à gee_auth (Windows + Linux) ─────────────────────────
from gee_auth import get_ee, init_gee

    
# ── Routing Landsat par année ─────────────────────────────────────────────────
# Chaque segment = (année_début_incluse, année_fin_incluse, collection_id, label_court)
LANDSAT_SEGMENTS = [
    (1984, 1999, "LANDSAT/LT05/C02/T1_L2", "Landsat 5"),
    (1999, 2013, "LANDSAT/LE07/C02/T1_L2", "Landsat 7"),
    (2013, 2021, "LANDSAT/LC08/C02/T1_L2", "Landsat 8"),
    (2021, 2099, "LANDSAT/LC09/C02/T1_L2", "Landsat 9"),
]

# Bandes RGB / NDVI / NDWI / LST par collection Landsat
LANDSAT_BANDS = {
    "LANDSAT/LT05/C02/T1_L2": {
        "RGB":  ("SR_B3", "SR_B2", "SR_B1"),
        "NDVI": ("SR_B4", "SR_B3"),
        "NDWI": ("SR_B2", "SR_B4"),
        "LST":  None,
        "scale": 0.0000275, "offset": -0.2,
        "rgb_min": 0.0, "rgb_max": 0.35,
    },
    "LANDSAT/LE07/C02/T1_L2": {
        "RGB":  ("SR_B3", "SR_B2", "SR_B1"),
        "NDVI": ("SR_B4", "SR_B3"),
        "NDWI": ("SR_B2", "SR_B4"),
        "LST":  None,
        "scale": 0.0000275, "offset": -0.2,
        "rgb_min": 0.0, "rgb_max": 0.35,
    },
    "LANDSAT/LC08/C02/T1_L2": {
        "RGB":  ("SR_B4", "SR_B3", "SR_B2"),
        "NDVI": ("SR_B5", "SR_B4"),
        "NDWI": ("SR_B3", "SR_B5"),
        "LST":  ("ST_B10", 0.00341802, 149.0),
        "scale": 0.0000275, "offset": -0.2,
        "rgb_min": 0.0, "rgb_max": 0.35,
    },
    "LANDSAT/LC09/C02/T1_L2": {
        "RGB":  ("SR_B4", "SR_B3", "SR_B2"),
        "NDVI": ("SR_B5", "SR_B4"),
        "NDWI": ("SR_B3", "SR_B5"),
        "LST":  ("ST_B10", 0.00341802, 149.0),
        "scale": 0.0000275, "offset": -0.2,
        "rgb_min": 0.0, "rgb_max": 0.35,
    },
}

# ── Paramètres visuels par (dataset_générique, indice) ───────────────────────
VIS_CFG = {
    # Landsat RGB → géré dynamiquement selon la collection
    ("landsat", "NDVI"): {
        "min": -0.2, "max": 0.8,
        "palette": ["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"],
        "legend_label": "NDVI",
    },
    ("landsat", "NDWI"): {
        "min": -0.5, "max": 0.5,
        "palette": ["#8B4513","#DEB887","#ffffff","#AED6F1","#1A5276"],
        "legend_label": "NDWI",
    },
    ("landsat", "LST"): {
        "min": 0, "max": 50,
        "palette": ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],
        "legend_label": "LST (°C)",
    },
    ("sentinel2", "RGB"): {"min": 0, "max": 3000, "gamma": 1.4},
    ("sentinel2", "NDVI"): {
        "min": -0.2, "max": 0.8,
        "palette": ["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"],
        "legend_label": "NDVI",
    },
    ("sentinel2", "NDWI"): {
        "min": -0.5, "max": 0.5,
        "palette": ["#8B4513","#DEB887","#ffffff","#AED6F1","#1A5276"],
        "legend_label": "NDWI",
    },
    ("sentinel2", "NDBI"): {
        "min": -0.5, "max": 0.5,
        "palette": ["#1a9850","#fee08b","#d73027"],
        "legend_label": "NDBI",
    },
    ("sentinel2", "False Color"): {"min": 0, "max": 5000},
    ("modis_ndvi", "NDVI"): {
        "min": -2000, "max": 10000,
        "palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"],
        "legend_label": "NDVI",
    },
    ("modis_ndvi", "EVI"): {
        "min": -2000, "max": 10000,
        "palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"],
        "legend_label": "EVI",
    },
    # MODIS LST
    ("modis_lst", "LST Jour"): {
        "min": -10, "max": 55,
        "palette": ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],
        "legend_label": "LST Jour (°C)",
    },
    ("modis_lst", "LST Nuit"): {
        "min": -25, "max": 35,
        "palette": ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],
        "legend_label": "LST Nuit (°C)",
    },
    # ERA5
    ("era5", "Température"): {
        "min": 250, "max": 310,
        "palette": ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],
        "legend_label": "Temp (K)",
    },
    ("era5", "Précipitations"): {
        "min": 0, "max": 0.3,
        "palette": ["#ffffff","#c6dbef","#6baed6","#2171b5","#08306b"],
        "legend_label": "Préc (m/j)",
    },
}

# ── Modèle de requête ─────────────────────────────────────────────────────────
class TimelapseRequest(BaseModel):
    dataset:          str            = "landsat"
    index:            str            = "RGB"
    year_start:       int            = 2000
    year_end:         int            = 2024
    month_start:      int            = 1
    month_end:        int            = 12
    frequency:        str            = "annual"
    composite:        str            = "least_cloudy"
    cloud_max:        float          = 30.0
    fps:              int            = 3
    gif_width:        int            = 512
    # Annotations
    ann_title:        str            = ""
    ann_desc:         str            = ""
    ann_credits:      str            = "OpenMapAgents · GEE"
    show_north:       bool           = True
    show_scale:       bool           = True
    show_progress:    bool           = True
    show_legend:      bool           = True
    timestamp_format: str            = "readable"  # "readable" → "Juin 2023"
    # Géographie
    bbox:             Optional[List[float]] = None
    roi_geojson:      Optional[dict] = None


# ── Découpe temporelle ───────────────────────────────────────────────────────
def build_date_ranges(year_start, year_end, month_start, month_end, frequency):
    """Retourne liste de (date_start_str, date_end_str, label_lisible)."""
    ranges = []
    SEASONS = [
        (1, 3, "Hiver"), (4, 6, "Printemps"),
        (7, 9, "Été"),   (10, 12, "Automne"),
    ]
    MONTHS_FR_LONG = [
        "Janvier","Février","Mars","Avril","Mai","Juin",
        "Juillet","Août","Septembre","Octobre","Novembre","Décembre"
    ]

    for year in range(year_start, year_end + 1):
        months = list(range(month_start, month_end + 1))

        if frequency == "annual":
            ds = f"{year}-{month_start:02d}-01"
            last = cal_module.monthrange(year, month_end)[1]
            de = f"{year}-{month_end:02d}-{last:02d}"
            ranges.append((ds, de, str(year)))

        elif frequency == "seasonal":
            for s_start, s_end, s_name in SEASONS:
                ms = max(s_start, month_start)
                me = min(s_end, month_end)
                if ms > me:
                    continue
                ds = f"{year}-{ms:02d}-01"
                last = cal_module.monthrange(year, me)[1]
                de = f"{year}-{me:02d}-{last:02d}"
                ranges.append((ds, de, f"{s_name} {year}"))

        elif frequency == "monthly":
            for month in months:
                ds = f"{year}-{month:02d}-01"
                last = cal_module.monthrange(year, month)[1]
                de = f"{year}-{month:02d}-{last:02d}"
                label = f"{MONTHS_FR_LONG[month - 1]} {year}"
                ranges.append((ds, de, label))

        elif frequency == "biweekly":
            for month in months:
                last = cal_module.monthrange(year, month)[1]
                label_a = f"{MONTHS_FR_LONG[month - 1][:3]}-A {year}"
                label_b = f"{MONTHS_FR_LONG[month - 1][:3]}-B {year}"
                ranges.append((f"{year}-{month:02d}-01", f"{year}-{month:02d}-15", label_a))
                ranges.append((f"{year}-{month:02d}-16", f"{year}-{month:02d}-{last:02d}", label_b))

    return ranges


# ── Choisir la collection Landsat pour une année donnée ──────────────────────
def landsat_collection_for_year(year):
    for y_min, y_max, coll, label in LANDSAT_SEGMENTS:
        if y_min <= year <= y_max:
            return coll, label
    return LANDSAT_SEGMENTS[-1][2], LANDSAT_SEGMENTS[-1][3]


# ── Construire une image GEE pour une plage donnée ───────────────────────────
def build_gee_image(ee, dataset, index, d_start, d_end, cloud_max, composite_mode, region):
    """
    Retourne (ee.Image, vis_params_dict, source_label_str) prête pour getThumbURL.
    """
    year = int(d_start[:4])

    # ── Landsat — sélection dynamique de la collection ──────────
    if dataset == "landsat":
        coll_id, src_label = landsat_collection_for_year(year)
        cfg = LANDSAT_BANDS[coll_id]
        col = (ee.ImageCollection(coll_id)
               .filterDate(d_start, d_end)
               .filterBounds(region)
               .filter(ee.Filter.lt("CLOUD_COVER", cloud_max)))

        count = col.size().getInfo()
        if count == 0:
            return None, None, src_label

        if composite_mode == "least_cloudy":
            img = col.sort("CLOUD_COVER").first()
        elif composite_mode == "mosaic":
            img = col.mosaic()
        else:
            img = col.median()

        sf  = cfg["scale"]
        off = cfg["offset"]

        if index == "RGB":
            r, g, b = cfg["RGB"]
            out = img.select([r, g, b]).multiply(sf).add(off)
            vis = {"min": cfg["rgb_min"], "max": cfg["rgb_max"], "gamma": 1.4}

        elif index in ("NDVI", "NDWI"):
            b1, b2 = cfg[index]
            band1 = img.select(b1).multiply(sf).add(off)
            band2 = img.select(b2).multiply(sf).add(off)
            out = band1.subtract(band2).divide(band1.add(band2)).rename(index)
            vis = VIS_CFG[("landsat", index)]

        elif index == "LST" and cfg["LST"] is not None:
            b, lst_sf, lst_off = cfg["LST"]
            out = img.select(b).multiply(lst_sf).add(lst_off).subtract(273.15).rename("LST_C")
            vis = VIS_CFG[("landsat", "LST")]

        else:
            # Fallback RGB si LST non dispo sur Landsat 5/7
            r, g, b = cfg["RGB"]
            out = img.select([r, g, b]).multiply(sf).add(off)
            vis = {"min": cfg["rgb_min"], "max": cfg["rgb_max"], "gamma": 1.4}
            index = "RGB"

    # ── Sentinel-2 ───────────────────────────────────────────────
    elif dataset == "sentinel2":
        src_label = "Sentinel-2"
        col = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
               .filterDate(d_start, d_end)
               .filterBounds(region)
               .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", cloud_max)))

        count = col.size().getInfo()
        if count == 0:
            return None, None, src_label

        if composite_mode == "least_cloudy":
            img = col.sort("CLOUDY_PIXEL_PERCENTAGE").first()
        elif composite_mode == "mosaic":
            img = col.mosaic()
        else:
            img = col.median()

        if index == "RGB":
            out = img.select(["B4", "B3", "B2"])
            vis = {"min": 0, "max": 3000, "gamma": 1.4}
        elif index == "False Color":
            out = img.select(["B8", "B4", "B3"])
            vis = {"min": 0, "max": 5000}
        elif index in ("NDVI", "NDWI", "NDBI"):
            bands = {"NDVI": ("B8","B4"), "NDWI": ("B3","B8"), "NDBI": ("B11","B8")}
            b1, b2 = bands[index]
            band1 = img.select(b1)
            band2 = img.select(b2)
            out = band1.subtract(band2).divide(band1.add(band2)).rename(index)
            vis = VIS_CFG[("sentinel2", index)]
        else:
            out = img.select(["B4", "B3", "B2"])
            vis = {"min": 0, "max": 3000, "gamma": 1.4}

    # ── MODIS NDVI ───────────────────────────────────────────────
    elif dataset == "modis_ndvi":
        src_label = "MODIS MOD13A1"
        col = (ee.ImageCollection("MODIS/061/MOD13A1")
               .filterDate(d_start, d_end)
               .filterBounds(region))

        count = col.size().getInfo()
        if count == 0:
            return None, None, src_label

        img = col.median()
        band = "NDVI" if index == "NDVI" else "EVI"
        out = img.select(band).multiply(0.0001).rename(band)
        vis = VIS_CFG[("modis_ndvi", index)]

    # ── MODIS LST (température de surface) ───────────────────────
    elif dataset == "modis_lst":
        src_label = "MODIS MOD11A1"
        col = (ee.ImageCollection("MODIS/061/MOD11A1")
               .filterDate(d_start, d_end)
               .filterBounds(region))

        count = col.size().getInfo()
        if count == 0:
            return None, None, src_label

        if composite_mode == "least_cloudy":
            img = col.first()
        else:
            img = col.mean()

        if index == "LST Nuit":
            out = img.select("LST_Night_1km").multiply(0.02).subtract(273.15).rename("LST_Nuit")
        else:  # LST Jour (défaut)
            out = img.select("LST_Day_1km").multiply(0.02).subtract(273.15).rename("LST_Jour")

        vis = VIS_CFG[("modis_lst", index if index in ("LST Jour","LST Nuit") else "LST Jour")]

    # ── ERA5 Climat mensuel ───────────────────────────────────────
    elif dataset == "era5":
        src_label = "ERA5 ECMWF"
        col = (ee.ImageCollection("ECMWF/ERA5_LAND/MONTHLY_AGGR")
               .filterDate(d_start, d_end)
               .filterBounds(region))

        count = col.size().getInfo()
        if count == 0:
            return None, None, src_label

        img = col.mean()

        if index == "Précipitations":
            out = img.select("total_precipitation_sum").rename("Precip")
            vis = VIS_CFG[("era5", "Précipitations")]
        else:  # Température (défaut)
            out = img.select("temperature_2m").rename("Temp")
            vis = VIS_CFG[("era5", "Température")]

    else:
        raise ValueError(f"Dataset inconnu : {dataset}")

    return out.clip(region), vis, src_label


# ── Charger une police ───────────────────────────────────────────────────────
def load_font(size=14, bold=False):
    from PIL import ImageFont
    font_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/TTF/DejaVuSans.ttf",
    ]
    for p in font_paths:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                pass
    return ImageFont.load_default()


def draw_text_with_bg(draw, text, xy, font, text_color=(255,255,255), bg_color=(0,0,0,160), padding=3):
    """Dessine un texte avec fond semi-transparent."""
    from PIL import ImageDraw
    bbox = draw.textbbox(xy, text, font=font)
    draw.rectangle(
        [bbox[0]-padding, bbox[1]-padding, bbox[2]+padding, bbox[3]+padding],
        fill=bg_color,
    )
    draw.text(xy, text, fill=text_color, font=font)


# ── Hex → RGB ────────────────────────────────────────────────────────────────
def hex_to_rgb(h):
    h = h.lstrip("#")
    if len(h) == 3:
        h = h[0]*2 + h[1]*2 + h[2]*2
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


# ── Annotation d'une frame PIL ───────────────────────────────────────────────
def annotate_frame(img, label, frame_idx, total_frames, src_label,
                   ann_title, ann_desc, ann_credits,
                   show_north, show_scale, show_progress, show_legend,
                   bbox, vis_params, index):
    """
    Ajoute toutes les annotations sur une frame PIL et retourne l'image annotée.
    Coin haut-gauche : titre + description + timestamp
    Coin bas-droit   : flèche Nord + barre d'échelle
    Bas centre       : barre de progression
    Bas gauche       : légende palette + source
    """
    from PIL import Image as PILImage, ImageDraw

    # Agrandir légèrement pour la barre de progression en bas
    prog_h = 6 if show_progress else 0
    W, H = img.size

    # Créer canvas avec éventuelle bande progression
    canvas = PILImage.new("RGB", (W, H + prog_h), (20, 20, 20))
    canvas.paste(img, (0, 0))
    draw = ImageDraw.Draw(canvas, "RGBA")

    font_title  = load_font(14, bold=True)
    font_normal = load_font(12)
    font_small  = load_font(10)
    font_tiny   = load_font(8)

    PAD = 8

    # ── Coin haut-gauche : titre + description + timestamp ───────
    y_cur = PAD
    if ann_title:
        draw_text_with_bg(draw, ann_title, (PAD, y_cur), font_title, padding=3)
        y_cur += font_title.size + 6
    if ann_desc:
        draw_text_with_bg(draw, ann_desc, (PAD, y_cur), font_small, text_color=(220,220,220), padding=2)
        y_cur += font_small.size + 4
    # Timestamp — toujours affiché
    draw_text_with_bg(draw, label, (PAD, y_cur), font_normal,
                      text_color=(255, 220, 80), bg_color=(0,0,0,150), padding=3)

    # ── Coin haut-droit : flèche Nord ───────────────────────────
    if show_north:
        arrow_h = 28
        ax = W - PAD - 14
        ay = PAD
        # Corps flèche
        draw.line([(ax, ay + arrow_h), (ax, ay + 4)], fill=(255,255,255), width=2)
        # Pointe
        draw.polygon([(ax-5, ay+8), (ax+5, ay+8), (ax, ay)], fill=(255,255,255))
        # Lettre N
        nb = draw.textbbox((0,0), "N", font=font_small)
        nw = nb[2] - nb[0]
        draw.text((ax - nw//2, ay + arrow_h + 2), "N", fill=(255,255,255), font=font_small)

    # ── Bas de frame — ligne unique ──────────────────────────────
    # Gauche : source · OpenMapAgents · GEE · crédits perso
    # Droite : |___| X km  (échelle petite, font_tiny, pas de fond)
    # Les deux sur la même ligne y = H - PAD - font_tiny.size - 1

    line_y = H - PAD - font_tiny.size - 1

    # Source + crédits — bas-gauche
    suffix   = f" · {ann_credits}" if ann_credits.strip() else ""
    src_text = f"{src_label} · OpenMapAgents · GEE{suffix}"
    draw.text((PAD, line_y), src_text, fill=(170,170,170), font=font_tiny)

    # Barre d'échelle — bas-droit, petit, font_tiny, pas de fond
    if show_scale and bbox is not None:
        try:
            import math
            lng_span   = abs(bbox[2] - bbox[0])
            lat_mid    = (bbox[1] + bbox[3]) / 2
            km_per_deg = math.cos(math.radians(lat_mid)) * 111.32
            total_km   = lng_span * km_per_deg
            # Viser ~8% de la largeur image — petit
            target_km  = total_km * 0.08
            magnitude  = 10 ** int(math.log10(max(target_km, 0.001))) if target_km > 0 else 1
            nice_km    = max(1, int(round(target_km / magnitude) * magnitude))
            bar_px     = max(12, min(int((nice_km / total_km) * W) if total_km > 0 else 20, W // 6))

            lbl_scale  = f"{nice_km} km"
            tb         = draw.textbbox((0,0), lbl_scale, font=font_tiny)
            lbl_w      = tb[2] - tb[0]
            tick_h     = 3

            # La barre est à droite, le label juste après à droite aligné
            bx2  = W - PAD
            bx1  = bx2 - bar_px
            # Centre vertical sur line_y
            bar_y = line_y + font_tiny.size // 2

            # Trait |___| fin
            draw.line([(bx1, bar_y), (bx2, bar_y)], fill=(200,200,200), width=1)
            draw.line([(bx1, bar_y - tick_h), (bx1, bar_y + tick_h)], fill=(200,200,200), width=1)
            draw.line([(bx2, bar_y - tick_h), (bx2, bar_y + tick_h)], fill=(200,200,200), width=1)
            # Label km à droite de la barre, aligné verticalement
            draw.text((bx2 - lbl_w, line_y - font_tiny.size - 2), lbl_scale, fill=(200,200,200), font=font_tiny)
        except Exception:
            pass

    # ── Légende palette (ligne au-dessus de la ligne de bas) ─────
    if show_legend and index not in ("RGB", "False Color"):
        vis = vis_params or {}
        palette = vis.get("palette")
        if palette:
            leg_w  = min(160, W // 4)
            leg_h  = 8
            lx     = PAD
            ly     = line_y - leg_h - font_tiny.size - 4
            n = len(palette)
            for pi, col_hex in enumerate(palette):
                try:
                    r, g, b = hex_to_rgb(col_hex)
                    seg_w = max(1, leg_w // n)
                    draw.rectangle([lx + pi*seg_w, ly, lx + (pi+1)*seg_w, ly+leg_h], fill=(r,g,b,220))
                except Exception:
                    pass
            draw.rectangle([lx, ly, lx+leg_w, ly+leg_h], outline=(200,200,200,150), width=1)
            vmin      = vis.get("min", 0)
            vmax      = vis.get("max", 1)
            leg_label = vis.get("legend_label", index)
            draw.text((lx, ly + leg_h + 2), f"{leg_label}  {vmin}", fill=(200,200,200), font=font_tiny)
            rm = draw.textbbox((0,0), str(vmax), font=font_tiny)
            draw.text((lx + leg_w - (rm[2]-rm[0]), ly + leg_h + 2), str(vmax), fill=(200,200,200), font=font_tiny)

    # ── Barre de progression (bande en bas) ──────────────────────
    if show_progress and prog_h > 0:
        total = max(1, total_frames)
        progress_w = int((frame_idx + 1) / total * W)
        draw.rectangle([0, H, W, H + prog_h], fill=(40,40,40))
        draw.rectangle([0, H, progress_w, H + prog_h], fill=(29, 158, 117))  # C.acc vert

    return canvas


# ── Route principale ─────────────────────────────────────────────────────────
@router.post("/timelapse")
async def generate_timelapse(req: TimelapseRequest):
    """Génère un GIF timelapse GEE annoté."""
    try:
        ee = get_ee()
    except Exception as e:
        raise HTTPException(503, f"GEE non initialisé : {e}")

    # ── ROI ───────────────────────────────────────────────────────
    if req.roi_geojson:
        region = ee.Geometry(req.roi_geojson)
    elif req.bbox:
        mn_lng, mn_lat, mx_lng, mx_lat = req.bbox
        region = ee.Geometry.BBox(mn_lng, mn_lat, mx_lng, mx_lat)
    else:
        raise HTTPException(400, "Fournissez une bbox ou un roi_geojson")

    # ── Plages temporelles ────────────────────────────────────────
    date_ranges = build_date_ranges(
        req.year_start, req.year_end,
        req.month_start, req.month_end,
        req.frequency,
    )
    if not date_ranges:
        raise HTTPException(400, "Aucune plage temporelle générée — vérifiez les paramètres")

    # ── Construction des frames ───────────────────────────────────
    try:
        from PIL import Image as PILImage
        import imageio
        import requests as req_lib

        frames_pil   = []
        labels_used  = []
        sources_used = set()

        total = len(date_ranges)

        for i, (d_start, d_end, label) in enumerate(date_ranges):
            gee_img, vis, src_label = build_gee_image(
                ee, req.dataset, req.index,
                d_start, d_end,
                req.cloud_max, req.composite, region,
            )
            if gee_img is None:
                continue  # Pas d'image disponible pour cette période

            sources_used.add(src_label)

            # Paramètres thumb
            vis_params = {
                "dimensions": req.gif_width,
                "region": region.getInfo(),
                "format": "png",
            }
            # Fusionner vis avec vis_params
            for k, v in vis.items():
                if k not in ("legend_label",):
                    vis_params[k] = v

            # Récupérer la frame PNG depuis GEE
            thumb_url = gee_img.getThumbURL(vis_params)
            r = req_lib.get(thumb_url, timeout=90)
            r.raise_for_status()

            img = PILImage.open(io.BytesIO(r.content)).convert("RGB")

            # Annoter la frame
            annotated = annotate_frame(
                img            = img,
                label          = label,
                frame_idx      = len(frames_pil),
                total_frames   = total,
                src_label      = src_label,
                ann_title      = req.ann_title,
                ann_desc       = req.ann_desc,
                ann_credits    = req.ann_credits,
                show_north     = req.show_north,
                show_scale     = req.show_scale,
                show_progress  = req.show_progress,
                show_legend    = req.show_legend,
                bbox           = req.bbox,
                vis_params     = vis,
                index          = req.index,
            )

            frames_pil.append(annotated)
            labels_used.append(label)

        if not frames_pil:
            raise HTTPException(404, "Aucune image disponible sur la période et la zone choisies. Essayez d'élargir la période ou d'augmenter le filtre nuages.")

        # ── Assemblage GIF ────────────────────────────────────────
        gif_id   = uuid.uuid4().hex[:10]
        gif_name = f"timelapse_{req.dataset}_{req.year_start}-{req.year_end}_{req.index}_{gif_id}.gif"
        gif_path = os.path.join(TMP_DIR, gif_name)

        duration_ms = max(100, int(1000 / req.fps))

        imageio.mimsave(
            gif_path,
            frames_pil,
            format="GIF",
            duration=duration_ms,
            loop=0,
        )

        size_mb = round(os.path.getsize(gif_path) / (1024 * 1024), 2)

        # Source lisible (ex : "Landsat 5, Landsat 8, Landsat 9")
        source_str = " · ".join(sorted(sources_used))

        period_str = ""
        if labels_used:
            period_str = f"{labels_used[0]} → {labels_used[-1]}"

        return {
            "gif_url":  f"/api/gee/timelapse/file/{gif_name}",
            "frames":   len(frames_pil),
            "period":   period_str,
            "size_mb":  size_mb,
            "dataset":  req.dataset,
            "index":    req.index,
            "source":   source_str,
        }

    except ImportError as ie:
        raise HTTPException(500, f"Dépendance manquante : {ie} — pip install Pillow imageio requests")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erreur génération GIF : {e}")


# ── Servir le GIF généré ─────────────────────────────────────────────────────
@router.get("/timelapse/file/{filename}")
async def serve_timelapse_gif(filename: str):
    if ".." in filename or "/" in filename:
        raise HTTPException(400, "Nom de fichier invalide")
    path = os.path.join(TMP_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(404, "Fichier GIF introuvable — il a peut-être expiré")
    return FileResponse(path, media_type="image/gif", filename=filename)


# ── Nettoyage ────────────────────────────────────────────────────────────────
def cleanup_old_gifs(max_age_hours: int = 2):
    import time
    now = time.time()
    for fname in os.listdir(TMP_DIR):
        fpath = os.path.join(TMP_DIR, fname)
        if os.path.isfile(fpath) and fname.endswith(".gif"):
            if (now - os.path.getmtime(fpath)) / 3600 > max_age_hours:
                try:
                    os.remove(fpath)
                except Exception:
                    pass
