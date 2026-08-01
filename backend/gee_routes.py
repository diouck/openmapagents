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
    "sentinel2": {"label":"Sentinel-2 (10m)","collection":"COPERNICUS/S2_SR_HARMONIZED","cloud_property":"CLOUDY_PIXEL_PERCENTAGE","indices":["RGB","NDVI","EVI","SAVI","GNDVI","NDRE","NDWI","MNDWI","NDMI","NDCI","NDBI","BSI","NBR","NDSI","False Color (NIR)","SWIR (feux)"],"temporal":"5 jours"},
    "landsat9":  {"label":"Landsat 9 (30m)","collection":"LANDSAT/LC09/C02/T1_L2","cloud_property":"CLOUD_COVER","indices":["RGB","NDVI","GNDVI","NDWI","MNDWI","NDMI","BSI","NBR","NDSI","LST (température)","SWIR (feux)","False Color (NIR)"],"temporal":"16 jours"},
    "landsat8":  {"label":"Landsat 8 (30m)","collection":"LANDSAT/LC08/C02/T1_L2","cloud_property":"CLOUD_COVER","indices":["RGB","NDVI","GNDVI","NDWI","MNDWI","NDMI","BSI","NBR","NDSI","LST (température)","SWIR (feux)","False Color (NIR)"],"temporal":"16 jours"},
    "landsat":   {"label":"Landsat auto 4/5/7/8/9 (30m)","collection":"LANDSAT_MERGED","cloud_property":"CLOUD_COVER","indices":["RGB","NDVI","GNDVI","NDWI","MNDWI","NDMI","BSI","NBR","NDSI","LST (température)","SWIR (feux)","False Color (NIR)"],"temporal":"16 jours","auto":True},
    "modis_lst": {"label":"MODIS LST Temp. (1km)","collection":"MODIS/061/MOD11A1","cloud_property":None,"indices":["LST Jour","LST Nuit"],"temporal":"1 jour"},
    "modis_ndvi":{"label":"MODIS NDVI (500m)","collection":"MODIS/061/MOD13A1","cloud_property":None,"indices":["NDVI","EVI"],"temporal":"16 jours"},
    "modis_rgb": {"label":"MODIS Vraies couleurs (500m)","collection":"MODIS/061/MCD43A4","cloud_property":None,"indices":["RGB","False Color (NIR)"],"temporal":"Quotidien (BRDF 16j)"},
    # Réflectance de surface JOURNALIÈRE (MOD09GA) : faible latence (~2-3 j) et
    # montre les nuages/panaches (contrairement au MCD43A4 « nettoyé » et retardé).
    "modis_daily": {"label":"MODIS Vraies couleurs journalier (500m)","collection":"MODIS/061/MOD09GA","cloud_property":None,"indices":["RGB","False Color (NIR)"],"temporal":"Quotidien"},
    "worldcover":{"label":"ESA WorldCover 2021 (10m)","collection":"ESA/WorldCover/v200","cloud_property":None,"indices":["Occupation du sol"],"temporal":"Annuel","static":True},
    "sentinel1": {"label":"Sentinel-1 SAR (10m)","collection":"COPERNICUS/S1_GRD","cloud_property":None,"indices":["VV","VH","VV/VH"],"temporal":"6-12 jours"},
    "hansen":    {"label":"Global Forest Watch (30m)","collection":None,"cloud_property":None,"indices":["Couverture forêt 2000","Perte forêt","Gain forêt"],"temporal":"Annuel","static":True},
    "era5":      {"label":"ERA5 Climat mensuel (11km)","collection":"ECMWF/ERA5_LAND/MONTHLY_AGGR","cloud_property":None,"indices":["Température air","Précipitations"],"temporal":"Mensuel"},
    "srtm":      {"label":"SRTM Relief (30m)","collection":None,"cloud_property":None,"indices":["Élévation","Pente","Ombrage"],"temporal":"Statique","static":True},
    "canopy_height": {"label":"Hauteur de Canopée WRI/Meta 2020 (~1m)","collection":None,"cloud_property":None,"indices":["Hauteur canopée"],"temporal":"Statique","static":True},
    # ── Lot 2 : qualité de l'air, eau, nuit, climat/hydro, relief ──
    "sentinel5p": {"label":"Sentinel-5P Qualité de l'air (~7km)","collection":None,"cloud_property":None,"indices":["NO₂","CO","SO₂","CH₄","O₃","Aérosols (AI)"],"temporal":"Quotidien"},
    "jrc_water":  {"label":"JRC Eaux de surface (30m)","collection":None,"cloud_property":None,"indices":["Occurrence","Saisonnalité","Changement"],"temporal":"1984-2021","static":True},
    "viirs":      {"label":"VIIRS Lumières nocturnes (500m)","collection":"NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG","cloud_property":None,"indices":["Radiance nocturne"],"temporal":"Mensuel"},
    "chirps":     {"label":"CHIRPS Précipitations (5km)","collection":"UCSB-CHG/CHIRPS/DAILY","cloud_property":None,"indices":["Précipitations (cumul)"],"temporal":"Quotidien"},
    "copdem":     {"label":"Copernicus DEM GLO-30 (30m)","collection":None,"cloud_property":None,"indices":["Élévation","Pente","Ombrage"],"temporal":"Statique","static":True},
    "modis_et":   {"label":"MODIS Évapotranspiration (500m)","collection":"MODIS/061/MOD16A2","cloud_property":None,"indices":["Évapotranspiration"],"temporal":"8 jours"},
    "smap":       {"label":"SMAP Humidité du sol (~10km)","collection":"NASA/SMAP/SPL4SMGP/007","cloud_property":None,"indices":["Humidité du sol"],"temporal":"3 heures"},
    # ── Population maillée (densité hab/km²) : époque la plus récente ──
    "gpw_pop":    {"label":"GPW v4.11 Densité pop. (~1km, CIESIN)","collection":None,"cloud_property":None,"indices":["Densité de population"],"temporal":"2000-2020","static":True},
    "ghsl_pop":   {"label":"GHSL Densité pop. (100m, JRC)","collection":"JRC/GHSL/P2023A/GHS_POP","cloud_property":None,"indices":["Densité de population"],"temporal":"5 ans (1975-2030)"},
    # ── Lot A — Urbain : bâti, urbanisation, OCS temps réel ──
    "ghsl_built": {"label":"GHSL Surface bâtie (100m, JRC)","collection":"JRC/GHSL/P2023A/GHS_BUILT_S","cloud_property":None,"indices":["Surface bâtie"],"temporal":"5 ans (1975-2030)"},
    "ghsl_smod":  {"label":"GHSL Degré d'urbanisation (1km, JRC)","collection":"JRC/GHSL/P2023A/GHS_SMOD_V2-0","cloud_property":None,"indices":["Degré d'urbanisation"],"temporal":"5 ans (1975-2030)"},
    "dynamicworld":{"label":"Dynamic World OCS (10m, Google)","collection":"GOOGLE/DYNAMICWORLD/V1","cloud_property":None,"indices":["Occupation du sol (DW)"],"temporal":"~5 jours"},
    # ── Lot B — Risques : feux ──
    "burned":  {"label":"MODIS Zones brûlées (500m)","collection":"MODIS/061/MCD64A1","cloud_property":None,"indices":["Zones brûlées"],"temporal":"Mensuel"},
    "firms":   {"label":"FIRMS Feux actifs (1km)","collection":"FIRMS","cloud_property":None,"indices":["Feux actifs"],"temporal":"Quotidien"},
    # ── Fumée d'incendie — GEOS-CF (NASA GMAO, prévision composition, ~27km) ──
    # PM2.5 et espèces carbonées = panache de fumée modélisé (le modèle ingère les
    # émissions de feux QFED). Couverture GEE ~2022→2026 → études de cas de feux
    # (pas de temps réel) ; à confirmer au 1er run (GEE non testable en local).
    "geos_cf": {"label":"GEOS-CF Fumée & PM2.5 (~27km, NASA)","collection":"NASA/GEOS-CF/v1/fcst/tavg1hr","cloud_property":None,"indices":["PM2.5 (fumée)","Carbone suie (BC)","Carbone organique (OC)"],"temporal":"Horaire (prévision 5j)"},
    # ── Fumée d'incendie TEMPS RÉEL — CAMS (Copernicus/ECMWF, ~44km) ──
    # Mis à jour quotidiennement (couvre les feux actuels), contrairement à GEOS-CF.
    # PM2.5 en kg/m³ (→ µg/m³) et épaisseurs optiques d'aérosols (AOD, sans dimension).
    "cams":    {"label":"CAMS Fumée & PM2.5 temps réel (~44km, ECMWF)","collection":"ECMWF/CAMS/NRT","cloud_property":None,"indices":["PM2.5 (fumée)","Fumée (carbone organique)","Aérosols (AOD)"],"temporal":"Quotidien (prévision 5j)"},
    # ── Lot C — Sols (OpenLandMap, statiques) ──
    "soil_soc":  {"label":"Carbone organique du sol (250m, OpenLandMap)","collection":None,"cloud_property":None,"indices":["Carbone organique"],"temporal":"Statique","static":True},
    "soil_ph":   {"label":"pH du sol (250m, OpenLandMap)","collection":None,"cloud_property":None,"indices":["pH du sol"],"temporal":"Statique","static":True},
    "soil_clay": {"label":"Argile du sol (250m, OpenLandMap)","collection":None,"cloud_property":None,"indices":["Teneur en argile"],"temporal":"Statique","static":True},
    # ── Lot D — Végétation : productivité ──
    "modis_gpp": {"label":"MODIS Productivité GPP (500m)","collection":"MODIS/061/MOD17A2H","cloud_property":None,"indices":["Productivité (GPP)"],"temporal":"8 jours"},
    "modis_lai": {"label":"MODIS LAI / FAPAR (500m)","collection":"MODIS/061/MCD15A3H","cloud_property":None,"indices":["LAI","FAPAR"],"temporal":"4 jours"},
    # ── Lot E — Climat / énergie ──
    "era5_solar":{"label":"ERA5 Rayonnement solaire (~11km)","collection":"ECMWF/ERA5_LAND/MONTHLY_AGGR","cloud_property":None,"indices":["Rayonnement solaire"],"temporal":"Mensuel"},
    "era5_wind": {"label":"ERA5 Vent 10m (~11km)","collection":"ECMWF/ERA5_LAND/MONTHLY_AGGR","cloud_property":None,"indices":["Vitesse du vent"],"temporal":"Mensuel"},
    "gpm":       {"label":"GPM IMERG Précipitations (~11km)","collection":"NASA/GPM_L3/IMERG_MONTHLY_V07","cloud_property":None,"indices":["Précipitations (GPM)"],"temporal":"Mensuel"},
    "modis_snow":{"label":"MODIS Couverture neigeuse (500m)","collection":"MODIS/061/MOD10A1","cloud_property":None,"indices":["Neige"],"temporal":"Quotidien"},
    # ── Lot F — Océan / carbone (statiques) ──
    "etopo":     {"label":"ETOPO1 Relief & bathymétrie (~1.8km)","collection":None,"cloud_property":None,"indices":["Relief & bathymétrie"],"temporal":"Statique","static":True},
    "gedi_agb":  {"label":"GEDI Biomasse aérienne (1km)","collection":None,"cloud_property":None,"indices":["Biomasse aérienne"],"temporal":"Statique","static":True},
    # ── Lot G — Océanographie ──
    # NB : noms de bandes et facteurs d'échelle issus de la doc des assets ; GEE
    # n'étant pas testable en local, à revérifier au déploiement.
    "modis_ocean": {"label":"MODIS-Aqua Couleur de l'océan (~4km)","collection":"NASA/OCEANDATA/MODIS-Aqua/L3SMI","cloud_property":None,"indices":["SST (MODIS)","Chlorophylle-a","Matières en suspension"],"temporal":"Quotidien"},
    "viirs_ocean": {"label":"VIIRS Couleur de l'océan (~4km)","collection":"NASA/OCEANDATA/VIIRS-SNPP/L3SMI","cloud_property":None,"indices":["Chlorophylle-a"],"temporal":"Quotidien"},
    "oisst":       {"label":"NOAA OISST v2.1 — SST & glace (~27km)","collection":"NOAA/CDR/OISST/V2_1","cloud_property":None,"indices":["SST (OISST)","Anomalie SST","Glace de mer"],"temporal":"Quotidien"},
    "hycom_vel":   {"label":"HYCOM Courants marins (~9km)","collection":"HYCOM/sea_water_velocity","cloud_property":None,"indices":["Courants marins"],"temporal":"Quotidien"},
    "hycom_sal":   {"label":"HYCOM Salinité de surface (~9km)","collection":"HYCOM/sea_temp_salinity","cloud_property":None,"indices":["Salinité de surface"],"temporal":"Quotidien"},
    "era5_ocean_wind": {"label":"ERA5 Vent océanique 10m (~31km)","collection":"ECMWF/ERA5/HOURLY","cloud_property":None,"indices":["Vent océanique"],"temporal":"Horaire"},
    "coral":     {"label":"Allen Coral Atlas — récifs (~5m)","collection":None,"cloud_property":None,"indices":["Récifs coralliens"],"temporal":"2018-2020","static":True},
    "mangrove":  {"label":"Global Mangrove Forests 2000 (30m)","collection":None,"cloud_property":None,"indices":["Mangroves"],"temporal":"2000","static":True},
    # ── Lot I — GEDI LiDAR spatial (NASA, 25m, ±51,6° lat) ──
    "gedi_l2a":  {"label":"GEDI L2A Hauteur & élévation (25m)","collection":"LARSE/GEDI/GEDI02_A_002_MONTHLY","cloud_property":None,"indices":["Hauteur canopée (RH98)","Élévation terrain"],"temporal":"Mensuel"},
    "gedi_l2b":  {"label":"GEDI L2B Structure de canopée (25m)","collection":"LARSE/GEDI/GEDI02_B_002_MONTHLY","cloud_property":None,"indices":["Couverture canopée","Indice foliaire (PAI)","Diversité verticale (FHD)"],"temporal":"Mensuel"},
    "gedi_l4a":  {"label":"GEDI L4A Biomasse footprint (25m)","collection":"LARSE/GEDI/GEDI04_A_002_MONTHLY","cloud_property":None,"indices":["Biomasse aérienne (25m)"],"temporal":"Mensuel"},
}

# Sentinel-5P : une collection L3 par polluant (index → collection, bande)
S5P_PRODUCTS = {
    "NO₂":           ("COPERNICUS/S5P/OFFL/L3_NO2", "tropospheric_NO2_column_number_density"),
    "CO":            ("COPERNICUS/S5P/OFFL/L3_CO",  "CO_column_number_density"),
    "SO₂":           ("COPERNICUS/S5P/OFFL/L3_SO2", "SO2_column_number_density"),
    "CH₄":           ("COPERNICUS/S5P/OFFL/L3_CH4", "CH4_column_volume_mixing_ratio_dry_air"),
    "O₃":            ("COPERNICUS/S5P/OFFL/L3_O3",  "O3_column_number_density"),
    "Aérosols (AI)": ("COPERNICUS/S5P/OFFL/L3_AER_AI", "absorbing_aerosol_index"),
}

# Palette par INDICE (repli quand (dataset,index) n'est pas dans VIS_PARAMS) :
# évite de dupliquer une entrée par (dataset, index) pour les nouveaux indices.
_INDEX_VIS = {
    "NDVI":  {"palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"], "min": -0.2, "max": 0.8},
    "EVI":   {"palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"], "min": -0.2, "max": 0.8},
    "SAVI":  {"palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"], "min": -0.2, "max": 0.8},
    "GNDVI": {"palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"], "min": 0.0,  "max": 0.8},
    "NDRE":  {"palette": ["#d73027","#fdae61","#d9ef8b","#1a9850"], "min": 0.0,  "max": 0.6},
    "NDWI":  {"palette": ["#8B4513","#DEB887","#ffffff","#AED6F1","#1A5276"], "min": -0.5, "max": 0.5},
    "MNDWI": {"palette": ["#8B4513","#DEB887","#ffffff","#AED6F1","#1A5276"], "min": -0.5, "max": 0.5},
    "NDMI":  {"palette": ["#a6611a","#dfc27d","#f5f5f5","#80cdc1","#018571"], "min": -0.5, "max": 0.5},
    "NDCI":  {"palette": ["#2c7bb6","#abd9e9","#ffffbf","#fdae61","#d7191c"], "min": -0.2, "max": 0.4},
    "NDBI":  {"palette": ["#1a9850","#fee08b","#d73027"], "min": -0.5, "max": 0.5},
    "BSI":   {"palette": ["#1a9850","#fee08b","#d73027"], "min": -0.5, "max": 0.5},
    "NBR":   {"palette": ["#d73027","#fee08b","#1a9850"], "min": -0.5, "max": 1.0},
    "NDSI":  {"palette": ["#08306b","#4292c6","#9ecae1","#deebf7","#ffffff"], "min": -0.2, "max": 0.6},
    # Densité de population (hab/km²) : fortement asymétrique → max 1000 (villes saturées) ;
    # utiliser la classification Jenks/quantiles pour une carte fine.
    "Densité de population": {"palette": ["#ffffcc","#fed976","#fd8d3c","#e31a1c","#800026"], "min": 0, "max": 1000},
    "Surface bâtie": {"palette": ["#ffffff","#f0e0c0","#d8a878","#b45f3c","#7a1f0f"], "min": 0, "max": 100},
    "Carbone organique":  {"palette": ["#ffffe5","#d9f0a3","#78c679","#238443","#004529"], "min": 0, "max": 120},
    "pH du sol":          {"palette": ["#d73027","#fdae61","#ffffbf","#abd9e9","#4575b4"], "min": 4, "max": 9},
    "Teneur en argile":   {"palette": ["#f6e8c3","#dfc27d","#bf812d","#8c510a","#543005"], "min": 0, "max": 60},
    "Productivité (GPP)": {"palette": ["#ffffe5","#d9f0a3","#78c679","#238443","#00441b"], "min": 0, "max": 0.1},
    "LAI":                {"palette": ["#ffffe5","#d9f0a3","#78c679","#238443","#00441b"], "min": 0, "max": 6},
    "FAPAR":              {"palette": ["#ffffe5","#d9f0a3","#78c679","#238443","#00441b"], "min": 0, "max": 1},
    "Rayonnement solaire":{"palette": ["#000033","#3b0f70","#8c2981","#de4968","#fe9f6d","#fcfdbf"], "min": 0, "max": 30},
    "Vitesse du vent":    {"palette": ["#ffffcc","#a1dab4","#41b6c4","#2c7fb8","#253494"], "min": 0, "max": 12},
    "Précipitations (GPM)": {"palette": ["#ffffff","#c6dbef","#4292c6","#08306b"], "min": 0, "max": 400},
    "Neige":              {"palette": ["#08306b","#4292c6","#9ecae1","#deebf7","#ffffff"], "min": 0, "max": 100},
    "Relief & bathymétrie": {"palette": ["#08306b","#2171b5","#6baed6","#c6dbef","#006633","#E5FFCC","#662A00","#D8D8D8","#F5F5F5"], "min": -6000, "max": 6000},
    "Biomasse aérienne":  {"palette": ["#ffffe5","#d9f0a3","#78c679","#238443","#00441b"], "min": 0, "max": 300},
    "Zones brûlées":      {"palette": ["#ffffb2","#fecc5c","#fd8d3c","#f03b20","#bd0026"], "min": 1, "max": 366},
    "Feux actifs":        {"palette": ["#ffffb2","#fd8d3c","#f03b20","#bd0026"], "min": 325, "max": 400},
    # ── Lot G — Océanographie ──
    "SST (MODIS)":  {"palette": ["#000080","#0000ff","#00ffff","#ffff00","#ff8000","#ff0000"], "min": 0,  "max": 32},
    "SST (OISST)":  {"palette": ["#000080","#0000ff","#00ffff","#ffff00","#ff8000","#ff0000"], "min": 0,  "max": 32},
    # Anomalie : palette DIVERGENTE centrée sur 0 (bleu = plus froid, rouge = canicule marine)
    "Anomalie SST": {"palette": ["#2166ac","#67a9cf","#d1e5f0","#ffffff","#fddbc7","#ef8a62","#b2182b"], "min": -4, "max": 4},
    # Chlorophylle : très étalée (0,01–20 mg/m³) → prévoir la classification quantile pour affiner
    "Chlorophylle-a": {"palette": ["#08306b","#2171b5","#41ab5d","#addd8e","#ffffcc","#fe9929","#cc4c02"], "min": 0, "max": 10},
    "Matières en suspension": {"palette": ["#f7fcf0","#ccebc5","#7bccc4","#2b8cbe","#084081"], "min": 0, "max": 800},
    "Courants marins":  {"palette": ["#f7fbff","#9ecae1","#4292c6","#08519c","#08306b"], "min": 0, "max": 1.5},
    "Salinité de surface": {"palette": ["#40004b","#762a83","#9970ab","#c2a5cf","#e7d4e8","#d9f0d3","#5aae61"], "min": 30, "max": 38},
    "Glace de mer":     {"palette": ["#08306b","#4292c6","#9ecae1","#deebf7","#ffffff"], "min": 0, "max": 100},
    "Vent océanique":   {"palette": ["#ffffcc","#a1dab4","#41b6c4","#2c7fb8","#253494"], "min": 0, "max": 20},
    "Récifs coralliens": {"palette": ["#ff6f91"], "min": 0, "max": 1},
    "Mangroves":        {"palette": ["#1b7837"], "min": 0, "max": 1},
    # ── Lot I — GEDI LiDAR ──
    "Hauteur canopée (RH98)": {"palette": ["#ffffcc","#c2e699","#78c679","#31a354","#006837"], "min": 0, "max": 40},
    "Élévation terrain":  {"palette": ["#006633","#E5FFCC","#662A00","#D8D8D8","#F5F5F5"], "min": 0, "max": 3000},
    "Couverture canopée": {"palette": ["#ffffe5","#d9f0a3","#78c679","#238443","#00441b"], "min": 0, "max": 100},
    "Indice foliaire (PAI)": {"palette": ["#ffffe5","#d9f0a3","#78c679","#238443","#00441b"], "min": 0, "max": 6},
    "Diversité verticale (FHD)": {"palette": ["#ffffe5","#d9f0a3","#78c679","#238443","#00441b"], "min": 0, "max": 3.5},
    "Biomasse aérienne (25m)": {"palette": ["#ffffe5","#d9f0a3","#78c679","#238443","#00441b"], "min": 0, "max": 300},
}

# Palette « pollution » commune Sentinel-5P (faible → fort)
_S5P_PAL = ["#000000", "#0000ff", "#800080", "#00ffff", "#008000", "#ffff00", "#ff0000"]

# Palette « AQI » PM2.5 / fumée (bon → dangereux) : vert · jaune · orange · rouge · violet · marron
_PM25_PAL = ["#00e400", "#ffff00", "#ff7e00", "#ff0000", "#8f3f97", "#7e0023"]

# Palettes des datasets Lot 2 (bandes spécifiques → clé (dataset, index))
_LOT2_VIS = {
    ("sentinel5p", "NO₂"):            {"min": 0,     "max": 0.0002, "palette": _S5P_PAL},
    ("sentinel5p", "CO"):             {"min": 0,     "max": 0.05,   "palette": _S5P_PAL},
    ("sentinel5p", "SO₂"):            {"min": 0,     "max": 0.0005, "palette": _S5P_PAL},
    ("sentinel5p", "CH₄"):            {"min": 1750,  "max": 1900,   "palette": _S5P_PAL},
    ("sentinel5p", "O₃"):             {"min": 0.12,  "max": 0.15,   "palette": _S5P_PAL},
    ("sentinel5p", "Aérosols (AI)"):  {"min": -1,    "max": 2,      "palette": _S5P_PAL},
    ("jrc_water", "Occurrence"):      {"min": 0, "max": 100, "palette": ["#ffffff", "#a5c9e3", "#0000ff"]},
    ("jrc_water", "Saisonnalité"):    {"min": 0, "max": 12,  "palette": ["#ffffff", "#99d8c9", "#2ca25f", "#006d2c"]},
    ("jrc_water", "Changement"):      {"min": -50, "max": 50, "palette": ["#ff0000", "#ffffff", "#0000ff"]},
    ("viirs", "Radiance nocturne"):   {"min": 0, "max": 60, "palette": ["#000000", "#ffc800", "#ffffff"]},
    ("chirps", "Précipitations (cumul)"): {"min": 0, "max": 400, "palette": ["#ffffff", "#c6dbef", "#4292c6", "#08306b"]},
    ("copdem", "Élévation"):          {"min": 0, "max": 3000, "palette": ["#006633", "#E5FFCC", "#662A00", "#D8D8D8", "#F5F5F5"]},
    ("copdem", "Pente"):              {"min": 0, "max": 60,  "palette": ["#ffffff", "#fdae61", "#d73027"]},
    ("copdem", "Ombrage"):            {"min": 0, "max": 255},
    ("modis_et", "Évapotranspiration"): {"min": 0, "max": 100, "palette": ["#ffffff", "#c6dbef", "#4292c6", "#08306b"]},
    ("smap", "Humidité du sol"):      {"min": 0, "max": 0.5, "palette": ["#8B4513", "#DEB887", "#ffffff", "#AED6F1", "#1A5276"]},
    # GEOS-CF fumée — PM2.5 en µg/m³ (échelle type AQI), espèces carbonées plus basses
    ("geos_cf", "PM2.5 (fumée)"):          {"min": 0, "max": 150, "palette": _PM25_PAL},
    ("geos_cf", "Carbone suie (BC)"):      {"min": 0, "max": 20,  "palette": _PM25_PAL},
    ("geos_cf", "Carbone organique (OC)"): {"min": 0, "max": 60,  "palette": _PM25_PAL},
    # CAMS temps réel — PM2.5 en µg/m³ (échelle AQI), AOD sans dimension
    ("cams", "PM2.5 (fumée)"):             {"min": 0, "max": 150, "palette": _PM25_PAL},
    ("cams", "Fumée (carbone organique)"): {"min": 0, "max": 1,   "palette": _PM25_PAL},
    ("cams", "Aérosols (AOD)"):            {"min": 0, "max": 2,   "palette": _PM25_PAL},
    # Lot A — catégoriels
    ("ghsl_smod", "Degré d'urbanisation"): {"min": 10, "max": 30, "palette": ["#7ab6f5", "#cdf57a", "#abcd66", "#375623", "#ffff00", "#a87000", "#732600", "#ff0000"]},
    ("dynamicworld", "Occupation du sol (DW)"): {"min": 0, "max": 8, "palette": ["#419BDF", "#397D49", "#88B053", "#7A87C6", "#E49635", "#DFC35A", "#C4281B", "#A59B8F", "#B39FE1"]},
}

VIS_PARAMS = {
    ("sentinel2","RGB"):                {"bands":["B4","B3","B2"],"min":0,"max":3000,"gamma":1.4},
    ("sentinel2","NDVI"):               {"palette":["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"],"min":-0.2,"max":0.8},
    ("sentinel2","NDWI"):               {"palette":["#8B4513","#DEB887","#ffffff","#AED6F1","#1A5276"],"min":-0.5,"max":0.5},
    ("sentinel2","NDBI"):               {"palette":["#1a9850","#fee08b","#d73027"],"min":-0.5,"max":0.5},
    ("sentinel2","EVI"):                {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-0.2,"max":0.8},
    ("sentinel2","False Color (NIR)"):  {"bands":["B8","B4","B3"],"min":0,"max":5000},
    # Composite SWIR (12-11-8A) : la référence pour les feux. Le moyen infrarouge
    # traverse la fumée et sature sur les fronts de flamme, qui ressortent en
    # orange vif ; les brûlis récents virent au brun-rouge et la végétation saine
    # au vert franc. Étirement plus large que le RVB, le SWIR étant peu réfléchi.
    ("sentinel2","SWIR (feux)"):        {"bands":["B12","B11","B8A"],"min":0,"max":6000},
    ("landsat9","False Color (NIR)"):   {"bands":["SR_B5","SR_B4","SR_B3"],"min":5000,"max":30000},
    ("landsat8","False Color (NIR)"):   {"bands":["SR_B5","SR_B4","SR_B3"],"min":5000,"max":30000},
    ("landsat","False Color (NIR)"):    {"bands":["NIR","RED","GREEN"],"min":5000,"max":30000},
    ("landsat9","SWIR (feux)"):         {"bands":["SR_B7","SR_B6","SR_B5"],"min":5000,"max":30000},
    ("landsat8","SWIR (feux)"):         {"bands":["SR_B7","SR_B6","SR_B5"],"min":5000,"max":30000},
    ("landsat","SWIR (feux)"):          {"bands":["SWIR2","SWIR1","NIR"],"min":5000,"max":30000},
    ("landsat9","RGB"):                 {"bands":["SR_B4","SR_B3","SR_B2"],"min":5000,"max":25000,"gamma":1.4},
    ("landsat8","RGB"):                 {"bands":["SR_B4","SR_B3","SR_B2"],"min":5000,"max":25000,"gamma":1.4},
    ("landsat9","NDVI"):                {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-0.2,"max":0.8},
    ("landsat8","NDVI"):                {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-0.2,"max":0.8},
    ("landsat","RGB"):                  {"bands":["RED","GREEN","BLUE"],"min":5000,"max":25000,"gamma":1.4},
    ("landsat","NDVI"):                 {"palette":["#d73027","#fdae61","#d9ef8b","#1a9850"],"min":-0.2,"max":0.8},
    # MODIS vraies couleurs (MCD43A4, réflectance ajustée BRDF, ×0,0001)
    ("modis_rgb","RGB"):                {"bands":["Nadir_Reflectance_Band1","Nadir_Reflectance_Band4","Nadir_Reflectance_Band3"],"min":0,"max":4000,"gamma":1.4},
    ("modis_rgb","False Color (NIR)"):  {"bands":["Nadir_Reflectance_Band2","Nadir_Reflectance_Band1","Nadir_Reflectance_Band4"],"min":0,"max":5000},
    ("modis_daily","RGB"):              {"bands":["sur_refl_b01","sur_refl_b04","sur_refl_b03"],"min":0,"max":3500,"gamma":1.4},
    ("modis_daily","False Color (NIR)"):{"bands":["sur_refl_b02","sur_refl_b01","sur_refl_b04"],"min":0,"max":5500},
    ("landsat","NDWI"):                 {"palette":["#8B4513","#ffffff","#1A5276"],"min":-0.5,"max":0.5},
    ("landsat","LST (température)"):    {"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":0,"max":45},
    ("landsat9","NDWI"):                {"palette":["#8B4513","#ffffff","#1A5276"],"min":-0.5,"max":0.5},
    ("landsat8","NDWI"):                {"palette":["#8B4513","#ffffff","#1A5276"],"min":-0.5,"max":0.5},
    ("landsat9","LST (température)"):   {"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":0,"max":45},   # °C après subtract(273.15)
    ("landsat8","LST (température)"):   {"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":0,"max":45},   # °C après subtract(273.15)
    ("modis_lst","LST Jour"):           {"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":0,"max":55},   # °C après subtract(273.15)
    ("modis_lst","LST Nuit"):           {"palette":["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],"min":-15,"max":35}, # °C après subtract(273.15)
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
        # ── Datasets Lot 2 : sélection (et mise à l'échelle) de la bande ──
        if dataset == "sentinel5p":
            prod = S5P_PRODUCTS.get(index)
            if prod:
                return image.select(prod[1]).rename("S5P")
        if dataset == "jrc_water":
            band = {"Occurrence": "occurrence", "Saisonnalité": "seasonality", "Changement": "change_abs"}.get(index)
            if band:
                return image.select(band).rename("water")
        if dataset == "viirs":
            return image.select("avg_rad").rename("radiance")
        if dataset == "chirps":
            return image.select("precipitation").rename("precip")   # image = somme sur la période
        if dataset == "modis_et":
            return image.select("ET").multiply(0.1).rename("ET")     # facteur d'échelle → mm/8j
        if dataset == "smap":
            return image.select("sm_surface").rename("sm")           # m³/m³
        if dataset == "geos_cf":
            # Panache de fumée : PM2.5 total ou espèce carbonée (déjà en µg/m³).
            band = {"PM2.5 (fumée)":          "PM25_RH35_GCC",
                    "Carbone suie (BC)":      "PM25bc_RH35_GCC",
                    "Carbone organique (OC)": "PM25oc_RH35_GCC"}.get(index, "PM25_RH35_GCC")
            return image.select(band).rename("pm25")                 # µg/m³
        if dataset == "cams":
            # CAMS temps réel : PM2.5 en kg/m³ → µg/m³ (×1e9) ; AOD sans dimension.
            if index == "PM2.5 (fumée)":
                return image.select("particulate_matter_d_less_than_25_um_surface").multiply(1e9).rename("pm25")
            band = {"Aérosols (AOD)":            "total_aerosol_optical_depth_at_550nm_surface",
                    "Fumée (carbone organique)": "organic_matter_aerosol_optical_depth_at_550nm_surface"}.get(
                        index, "total_aerosol_optical_depth_at_550nm_surface")
            return image.select(band).rename("aod")
        if dataset == "copdem" and index == "Élévation":
            return image.select("DEM").rename("elevation")
        if dataset == "gpw_pop":
            return image.select("population_density").rename("pop_density")   # déjà en hab/km²
        if dataset == "ghsl_pop":
            # population_count = habitants par cellule 100 m (0.01 km²) → ×100 = hab/km²
            return image.select("population_count").multiply(100).rename("pop_density")
        if dataset == "ghsl_built":
            # built_surface = m² bâti par cellule 100 m (10000 m²) → /100 = % bâti
            return image.select("built_surface").divide(100).rename("built_pct")
        if dataset == "ghsl_smod":
            return image.select("smod_code").rename("smod")
        if dataset == "dynamicworld":
            return image.select("label").rename("dw")
        # ── Lot B ──
        if dataset == "burned":
            bd = image.select("BurnDate"); return bd.updateMask(bd.gt(0)).rename("burndate")
        if dataset == "firms":
            return image.select("T21").rename("firetemp")
        # ── Lot C (OpenLandMap : bande de surface b0 ; scales v02) ──
        if dataset == "soil_soc":
            return image.select("b0").multiply(0.2).rename("soc")      # 5×g/kg → g/kg
        if dataset == "soil_ph":
            return image.select("b0").multiply(0.1).rename("ph")       # pH×10
        if dataset == "soil_clay":
            return image.select("b0").rename("clay")                   # % argile
        # ── Lot D ──
        if dataset == "modis_gpp":
            return image.select("Gpp").multiply(0.0001).rename("gpp")  # kgC/m²/8j
        if index == "LAI" and dataset == "modis_lai":
            return image.select("Lai").multiply(0.1).rename("LAI")
        if index == "FAPAR" and dataset == "modis_lai":
            return image.select("Fpar").multiply(0.01).rename("FAPAR")
        # ── Lot E ──
        if dataset == "era5_solar":
            return image.select("surface_solar_radiation_downwards_sum").divide(1e6).rename("ghi")  # J/m²→MJ/m²
        if dataset == "era5_wind":
            u = image.select("u_component_of_wind_10m"); v = image.select("v_component_of_wind_10m")
            return u.hypot(v).rename("wind")                           # vitesse m/s
        if dataset == "gpm":
            return image.select("precipitation").multiply(720).rename("precip_gpm")  # mm/h → mm/mois (~30j)
        if dataset == "modis_snow":
            snow = image.select("NDSI_Snow_Cover"); return snow.updateMask(snow.lte(100)).rename("snow")
        # ── Lot F ──
        if dataset == "etopo":
            return image.select("bedrock").rename("relief")
        if dataset == "gedi_agb":
            return image.select("MU").rename("agb")
        # ── Lot G — Océanographie ──
        # Bornage physique de la SST et de la salinité : les bords côte/terre, les
        # franges de nuages et les artefacts de modèle laissent des pixels hors de
        # toute valeur océanique réelle, que la classification par quantiles ramasse
        # dans sa classe haute (SST « 39 °C », salinité « 40 PSU »). On les masque.
        def _clamp(im, lo, hi):
            return im.updateMask(im.gte(lo).And(im.lte(hi)))
        if dataset == "modis_ocean":
            if index == "Chlorophylle-a":        return image.select("chlor_a").rename("chlor")   # mg/m³
            if index == "Matières en suspension": return image.select("poc").rename("poc")          # mg/m³ (proxy)
            return _clamp(image.select("sst"), -2, 32).rename("sst")                                # °C
        if dataset == "viirs_ocean":
            return image.select("chlor_a").rename("chlor")                                          # mg/m³
        if dataset == "oisst":
            if index == "Anomalie SST": return image.select("anom").rename("sst_anom")              # °C (écart, non borné)
            if index == "Glace de mer": return image.select("ice").multiply(100).rename("ice")      # fraction → %
            return _clamp(image.select("sst"), -2, 32).rename("sst")                                # °C
        if dataset == "hycom_vel":
            # composantes stockées en entiers, facteur 0,001 → m/s ; vitesse = ‖(u,v)‖
            u = image.select("velocity_u_0").multiply(0.001)
            v = image.select("velocity_v_0").multiply(0.001)
            return u.hypot(v).rename("current")
        if dataset == "hycom_sal":
            # salinity_0 : facteur 0,001, décalage +20 → PSU, borné à l'eau de mer réelle
            sal = image.select("salinity_0").multiply(0.001).add(20)
            return _clamp(sal, 30, 40).rename("salinity")
        if dataset == "era5_ocean_wind":
            u = image.select("u_component_of_wind_10m"); v = image.select("v_component_of_wind_10m")
            return u.hypot(v).rename("wind")                                                         # m/s
        if dataset == "coral":
            m = image.select("reef_mask")
            return m.updateMask(m.gt(0)).rename("reef")
        if dataset == "mangrove":
            return image.select(0).rename("mangrove")   # bande unique, valeur 1 = mangrove
        # ── Lot I — GEDI LiDAR (bornage physique, cf. SST/salinité) ──
        if dataset == "gedi_l2a":
            if index == "Élévation terrain":
                return _clamp(image.select("elev_lowestmode"), -500, 9000).rename("elev")   # m
            return _clamp(image.select("rh98"), 0, 60).rename("canopy_h")                     # hauteur canopée m
        if dataset == "gedi_l2b":
            if index == "Indice foliaire (PAI)":
                return _clamp(image.select("pai"), 0, 10).rename("pai")
            if index == "Diversité verticale (FHD)":
                return _clamp(image.select("fhd_normal"), 0, 5).rename("fhd")
            return _clamp(image.select("cover"), 0, 1).multiply(100).rename("cover")          # fraction → %
        if dataset == "gedi_l4a":
            return _clamp(image.select("agbd"), 0, 1000).rename("agbd")                       # Mg/ha

        if index == "NDVI":
            if dataset == "sentinel2":        return image.normalizedDifference(["B8","B4"]).rename("NDVI")
            if dataset in ("landsat8","landsat9"): return image.normalizedDifference(["SR_B5","SR_B4"]).rename("NDVI")
            if dataset == "landsat":           return image.normalizedDifference(["NIR","RED"]).rename("NDVI")  # bandes harmonisées
            if dataset == "modis_ndvi":        return image.select("NDVI").multiply(0.0001).rename("NDVI")  # FIX: facteur échelle DN→[-1,1]
        if index == "NDWI":
            if dataset == "sentinel2":        return image.normalizedDifference(["B3","B8"]).rename("NDWI")
            if dataset in ("landsat8","landsat9"): return image.normalizedDifference(["SR_B3","SR_B5"]).rename("NDWI")
            if dataset == "landsat":           return image.normalizedDifference(["GREEN","NIR"]).rename("NDWI")  # bandes harmonisées
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
            if dataset == "landsat":
                return image.select("THERMAL").multiply(0.00341802).add(149.0).subtract(273.15).rename("LST")  # bande thermique harmonisée
            if dataset == "modis_lst":
                band = "LST_Day_1km" if "Jour" in index or "température" in index else "LST_Night_1km"
                return image.select(band).multiply(0.02).subtract(273.15).rename("LST")  # FIX: Kelvin→°C
        # ── Indices spectraux supplémentaires ─────────────────
        # Différences normalisées : insensibles à l'échelle DN (pas de scaling).
        if index == "MNDWI":     # eau (Green, SWIR1) — mieux que NDWI en zone bâtie
            if dataset == "sentinel2":              return image.normalizedDifference(["B3", "B11"]).rename("MNDWI")
            if dataset in ("landsat8", "landsat9"): return image.normalizedDifference(["SR_B3", "SR_B6"]).rename("MNDWI")
            if dataset == "landsat":                return image.normalizedDifference(["GREEN", "SWIR1"]).rename("MNDWI")
        if index == "NDMI":      # humidité/stress hydrique végétation (NIR, SWIR1)
            if dataset == "sentinel2":              return image.normalizedDifference(["B8", "B11"]).rename("NDMI")
            if dataset in ("landsat8", "landsat9"): return image.normalizedDifference(["SR_B5", "SR_B6"]).rename("NDMI")
            if dataset == "landsat":                return image.normalizedDifference(["NIR", "SWIR1"]).rename("NDMI")
        if index == "NBR":       # ratio de brûlage (NIR, SWIR2)
            if dataset == "sentinel2":              return image.normalizedDifference(["B8", "B12"]).rename("NBR")
            if dataset in ("landsat8", "landsat9"): return image.normalizedDifference(["SR_B5", "SR_B7"]).rename("NBR")
            if dataset == "landsat":                return image.normalizedDifference(["NIR", "SWIR2"]).rename("NBR")
        if index == "NDSI":      # neige (Green, SWIR1)
            if dataset == "sentinel2":              return image.normalizedDifference(["B3", "B11"]).rename("NDSI")
            if dataset in ("landsat8", "landsat9"): return image.normalizedDifference(["SR_B3", "SR_B6"]).rename("NDSI")
            if dataset == "landsat":                return image.normalizedDifference(["GREEN", "SWIR1"]).rename("NDSI")
        if index == "GNDVI":     # végétation (NIR, Green) — sensible à la chlorophylle
            if dataset == "sentinel2":              return image.normalizedDifference(["B8", "B3"]).rename("GNDVI")
            if dataset in ("landsat8", "landsat9"): return image.normalizedDifference(["SR_B5", "SR_B3"]).rename("GNDVI")
            if dataset == "landsat":                return image.normalizedDifference(["NIR", "GREEN"]).rename("GNDVI")
        if index == "NDRE":      # red-edge (NIR, RedEdge) — Sentinel-2 uniquement
            if dataset == "sentinel2":              return image.normalizedDifference(["B8", "B5"]).rename("NDRE")
        if index == "NDCI":      # chlorophylle / qualité eau (RedEdge, Red) — S2 uniquement
            if dataset == "sentinel2":              return image.normalizedDifference(["B5", "B4"]).rename("NDCI")
        if index == "SAVI":      # végétation sol-ajustée (réflectance ×0.0001) — S2
            if dataset == "sentinel2":
                nir = image.select("B8").multiply(0.0001); red = image.select("B4").multiply(0.0001)
                return nir.subtract(red).multiply(1.5).divide(nir.add(red).add(0.5)).rename("SAVI")
        if index == "BSI":       # indice de sol nu ((SWIR1+Red)-(NIR+Blue)) / somme
            if dataset == "sentinel2":
                num = image.select("B11").add(image.select("B4")).subtract(image.select("B8").add(image.select("B2")))
                den = image.select("B11").add(image.select("B4")).add(image.select("B8").add(image.select("B2")))
                return num.divide(den).rename("BSI")
            if dataset in ("landsat8", "landsat9"):
                num = image.select("SR_B6").add(image.select("SR_B4")).subtract(image.select("SR_B5").add(image.select("SR_B2")))
                den = image.select("SR_B6").add(image.select("SR_B4")).add(image.select("SR_B5").add(image.select("SR_B2")))
                return num.divide(den).rename("BSI")
            if dataset == "landsat":
                num = image.select("SWIR1").add(image.select("RED")).subtract(image.select("NIR").add(image.select("BLUE")))
                den = image.select("SWIR1").add(image.select("RED")).add(image.select("NIR").add(image.select("BLUE")))
                return num.divide(den).rename("BSI")

        if index == "Pente":
            return ee.Terrain.products(image).select("slope")
        if index == "Ombrage":
            return ee.Terrain.products(image).select("hillshade")
        if index == "VV":  return image.select("VV")
        if index == "VH":  return image.select("VH")
    except Exception as e:
        print(f"compute_index error ({dataset}/{index}): {e}")
    return image


# ═══════════════════════════════════════════════════════════════════════════
# LANDSAT « AUTO » — sélection de la mission selon la date + bandes harmonisées
# Les bandes Collection 2 Niveau 2 diffèrent entre générations :
#   • L4/L5 (TM) & L7 (ETM+) : NIR=SR_B4, ROUGE=SR_B3, thermique=ST_B6
#   • L8/L9 (OLI/TIRS)       : NIR=SR_B5, ROUGE=SR_B4, thermique=ST_B10
# On renomme tout vers des bandes communes : BLUE GREEN RED NIR SWIR1 SWIR2 THERMAL
# ═══════════════════════════════════════════════════════════════════════════

# (collection, capteur, début mission, fin mission [None = actif])
LANDSAT_MISSIONS = [
    ("LANDSAT/LT04/C02/T1_L2", "tm",  "1982-08-22", "1993-12-14"),
    ("LANDSAT/LT05/C02/T1_L2", "tm",  "1984-03-16", "2012-05-05"),
    ("LANDSAT/LE07/C02/T1_L2", "etm", "1999-05-28", "2022-04-06"),
    ("LANDSAT/LC08/C02/T1_L2", "oli", "2013-03-18", None),
    ("LANDSAT/LC09/C02/T1_L2", "oli", "2021-10-31", None),
]
_COMMON_BANDS = ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2", "THERMAL", "QA_PIXEL"]
_LS_SRC_BANDS = {
    "tm":  ["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7", "ST_B6",  "QA_PIXEL"],
    "etm": ["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7", "ST_B6",  "QA_PIXEL"],
    "oli": ["SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7", "ST_B10", "QA_PIXEL"],
}


def _landsat_merged(ee, date_start, date_end, region, cloud_max):
    """Fusionne les missions Landsat actives sur la période, bandes harmonisées.

    Retourne une ImageCollection dont chaque image porte les bandes communes
    (BLUE..THERMAL) + la propriété CLOUD_COVER conservée pour le tri/composite.
    """
    cols = []
    for cid, sensor, m_start, m_end in LANDSAT_MISSIONS:
        # Garder uniquement les missions dont la période de vie chevauche [date_start, date_end]
        if date_end < m_start:
            continue
        if m_end and date_start > m_end:
            continue
        c = ee.ImageCollection(cid).filterDate(date_start, date_end)
        if region is not None:
            c = c.filterBounds(region)
        if cloud_max is not None and cloud_max < 100:
            c = c.filter(ee.Filter.And(
                ee.Filter.notNull(["CLOUD_COVER"]),
                ee.Filter.lte("CLOUD_COVER", cloud_max),
            ))
        c = c.select(_LS_SRC_BANDS[sensor], _COMMON_BANDS)  # select + rename
        cols.append(c)
    if not cols:
        return ee.ImageCollection([])
    merged = cols[0]
    for c in cols[1:]:
        merged = merged.merge(c)
    return merged


# ── Masquage nuages/ombres PAR PIXEL (QA) ─────────────────────
# Le filtre par % de nuages ne retire que des scènes entières : une scène à
# 15 % de nuages garde des pixels nuageux qui faussent les valeurs (ex. LST
# NÉGATIVE en été car les nuages hauts sont froids). On masque donc chaque
# pixel nuageux/ombre avant tout composite (médiane/mosaïque/moins nuageux),
# ce qui corrige LST + tous les indices et fiabilise les statistiques.
_OPTICAL_MASKABLE = {"sentinel2", "landsat", "landsat8", "landsat9", "modis_lst", "modis_ndvi"}

def _mask_optical_image(ee, dataset, image):
    if dataset == "sentinel2":
        # SCL : 1 défect, 3 ombre, 8/9 nuage, 10 cirrus, 11 neige
        scl = image.select("SCL")
        bad = (scl.eq(1).Or(scl.eq(3)).Or(scl.eq(8))
               .Or(scl.eq(9)).Or(scl.eq(10)).Or(scl.eq(11)))
        return image.updateMask(bad.Not())
    if dataset in ("landsat", "landsat8", "landsat9"):
        # QA_PIXEL (C2 L2) : bit1 dilated, bit2 cirrus, bit3 cloud, bit4 shadow
        qa = image.select("QA_PIXEL")
        bits = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4)
        return image.updateMask(qa.bitwiseAnd(bits).eq(0))
    if dataset == "modis_lst":
        # QC_Day bits 0-1 : 0 bonne qualité, 1 autre qualité (garde), 2/3 non produit
        qc = image.select("QC_Day")
        return image.updateMask(qc.bitwiseAnd(3).lte(1))
    if dataset == "modis_ndvi":
        return image.updateMask(image.select("SummaryQA").lte(1))
    return image

def _mask_clouds(ee, dataset, col):
    """Masque les nuages par pixel sur toute une collection optique (no-op sinon)."""
    if col is not None and dataset in _OPTICAL_MASKABLE:
        return col.map(lambda im: _mask_optical_image(ee, dataset, im))
    return col


# Plages de disponibilité par dataset (début, fin[None]) pour valider les dates
DATASET_DATE_RANGES = {
    "sentinel2":  ("2015-06-23", None),
    "sentinel1":  ("2014-10-03", None),
    "landsat8":   ("2013-03-18", None),
    "landsat9":   ("2021-10-31", None),
    "landsat":    ("1982-08-22", None),
    "modis_lst":  ("2000-02-24", None),
    "modis_ndvi": ("2000-02-18", None),
    "modis_rgb":  ("2000-02-24", None),
    "modis_daily": ("2000-02-24", None),
    "era5":       ("1950-01-01", None),
    "sentinel5p": ("2018-07-10", None),
    "viirs":      ("2012-04-01", None),
    "chirps":     ("1981-01-01", None),
    "modis_et":   ("2001-01-01", None),
    "smap":       ("2015-03-31", None),
    "geos_cf":    ("2022-10-01", None),
    "cams":       ("2016-06-22", None),
    # ── Lot G — Océanographie ──
    "modis_ocean":     ("2002-07-04", None),
    "viirs_ocean":     ("2012-01-02", None),
    "oisst":           ("1981-09-01", None),
    "hycom_vel":       ("1992-10-02", None),
    "hycom_sal":       ("1992-10-02", None),
    "era5_ocean_wind": ("1940-01-01", None),
    # GEDI : à bord de l'ISS depuis avril 2019, couverture ±51,6° de latitude
    "gedi_l2a":        ("2019-04-18", None),
    "gedi_l2b":        ("2019-04-18", None),
    "gedi_l4a":        ("2019-04-18", None),
}


def _check_date_availability(dataset, date_start, date_end):
    """Lève ValueError si la période demandée précède/dépasse la disponibilité."""
    import datetime as _dt
    today = _dt.date.today().isoformat()
    # Période entièrement dans le futur → aucune image n'existe encore
    if date_start and date_start > today:
        raise ValueError(
            f"Période dans le futur ({date_start} → {date_end}) : aucune image "
            f"satellite n'existe encore à ces dates (aujourd'hui : {today}). "
            f"Choisissez une période passée."
        )
    rng = DATASET_DATE_RANGES.get(dataset)
    if not rng:
        return
    avail_start, avail_end = rng
    if date_end < avail_start:
        raise ValueError(
            f"« {dataset} » n'a pas de données avant {avail_start} "
            f"(période demandée : {date_start} → {date_end}). "
            f"Choisissez un autre capteur ou une date postérieure."
        )
    if avail_end and date_start > avail_end:
        raise ValueError(
            f"« {dataset} » n'a plus de données après {avail_end}."
        )

# ── Models ────────────────────────────────────────────────────
class TileRequest(BaseModel):
    dataset:    str
    index:      str
    date_start: str = "2020-01-01"   # ignoré pour les datasets statiques (Hansen, SRTM, etc.)
    date_end:   str = "2024-12-31"   # ignoré pour les datasets statiques
    bbox:             Optional[List[float]] = None
    cloud_max:        Optional[float] = 20.0
    composite:        Optional[str]   = "least_cloudy"  # tuiles : image la moins nuageuse par défaut
    roi_geojson:      Optional[dict]  = None   # GeoJSON geometry pour masquer les tuiles
    vis_params_override: Optional[dict] = None  # Écrase palette/min/max depuis LayerPanel
    auto_stretch:     Optional[bool]  = False   # min/max auto (percentiles p2/p98 sur l'emprise)
    classify:         Optional[str]   = None    # "quantile" | "jenks" | "equal" (rampe continue si None)
    n_classes:        Optional[int]   = 5       # nombre de classes pour classify
    # ── Relief (ombrage / pente) : exagération verticale + position du soleil ──
    z_factor:         Optional[float] = 1.0     # exagération du relief (× élévation avant ombrage/pente)
    sun_azimuth:      Optional[float] = 315.0   # azimut du soleil (°) pour l'ombrage
    sun_altitude:     Optional[float] = 45.0    # hauteur du soleil au-dessus de l'horizon (°)
    # Timelapse : interdit l'élargissement automatique de la fenêtre quand la
    # période est vide. Sans ça une frame « juin 2023 » sans image afficherait
    # l'image d'avril et l'animation mentirait sur la date.
    no_widen:         Optional[bool]  = False
    # Export : renvoie une VIGNETTE PNG (getThumbURL) au lieu d'une URL de tuiles.
    # Permet de réutiliser tout le pipeline de construction d'image sans le
    # dupliquer — seule la dernière étape (rendu) diffère.
    thumb_px:         Optional[int]   = None
    # Composites colorés : ne PAS masquer les nuages pixel par pixel. Le masque
    # supprime aussi la fumée, les panaches et tout ce qui ressemble à un nuage —
    # c'est-à-dire précisément ce qu'on cherche à voir sur un feu de forêt ou un
    # changement d'état brutal. Force aussi la conservation des scènes nuageuses.
    no_cloud_mask:    Optional[bool]  = False

class DatesRequest(BaseModel):
    dataset:    str
    bbox:       Optional[List[float]] = None
    roi_geojson: Optional[dict] = None
    date_start: str = "2023-01-01"
    date_end:   str = "2025-12-31"
    cloud_max:  Optional[float] = 30.0

# ── Helper ROI GeoJSON → ee.Geometry ─────────────────────────
def _is_global_bbox(bbox):
    """True si l'emprise couvre ~le monde entier.

    À l'échelle globe, clipper une image en projection non géographique (ex.
    MODIS sinusoïdal SR-ORG:6974) sur une BBox monde fait échouer GEE :
    « Image.clip: Unable to transform edge … » — les bords de la géométrie
    (antiméridien, ±180°) ne se reprojettent pas. Le clip y est de toute façon
    inutile (jeux MODIS/globaux déjà mondiaux) : on le saute donc dans ce cas.
    """
    try:
        w, s, e, n = bbox
        return (float(e) - float(w)) >= 355.0 or (float(w) <= -179.0 and float(e) >= 179.0)
    except Exception:
        return False


def geojson_to_ee_geometry(geojson: dict):
    """Convertit un GeoJSON en une ee.Geometry unique pour un CLIP EXACT.

    Accepte Polygon / MultiPolygon mais aussi Feature / FeatureCollection /
    GeometryCollection : toutes les géométries polygonales sont fusionnées en un
    seul MultiPolygon (coordonnées brutes → pas de repli sur la bbox englobante,
    qui donnait un découpage rectangulaire au lieu du contour réel).
    """
    import ee
    t = geojson.get("type", "")
    if t == "Polygon":
        return ee.Geometry.Polygon(geojson["coordinates"])
    if t == "MultiPolygon":
        return ee.Geometry.MultiPolygon(geojson["coordinates"])

    # Collections → collecter les coordonnées de tous les polygones
    polys = []  # liste de [ [ring], ... ] (coordonnées d'un Polygon)
    def _collect(g):
        gt = (g or {}).get("type")
        if gt == "Polygon":
            polys.append(g["coordinates"])
        elif gt == "MultiPolygon":
            polys.extend(g["coordinates"])
        elif gt == "GeometryCollection":
            for sub in g.get("geometries", []):
                _collect(sub)

    if t == "FeatureCollection":
        for f in geojson.get("features", []):
            _collect((f or {}).get("geometry"))
    elif t == "Feature":
        _collect(geojson.get("geometry"))
    elif t == "GeometryCollection":
        _collect(geojson)

    if polys:
        return ee.Geometry.Polygon(polys[0]) if len(polys) == 1 else ee.Geometry.MultiPolygon(polys)

    raise ValueError(f"Type GeoJSON non supporté ou sans polygone: {t}")

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

        _check_date_availability(req.dataset, req.date_start, req.date_end)

        bounds_geom = None
        if req.roi_geojson:
            try: bounds_geom = geojson_to_ee_geometry(req.roi_geojson).bounds()
            except Exception: bounds_geom = None
        if bounds_geom is None and req.bbox:
            w,s,e,n = req.bbox
            bounds_geom = ee.Geometry.BBox(w,s,e,n)

        # Landsat « auto » : fusion des missions selon la date
        if req.dataset == "landsat":
            col = _landsat_merged(ee, req.date_start, req.date_end, bounds_geom, req.cloud_max)
        else:
            col = ee.ImageCollection(ds["collection"]).filterDate(req.date_start, req.date_end)
            if bounds_geom is not None:
                col = col.filterBounds(bounds_geom)
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
    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(422, str(ve))
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Auto-stretch & classification raster (min/max auto, quantile/jenks/égaux) ──
_STRETCH_SCALE = {
    "sentinel2": 30, "landsat": 30, "landsat8": 30, "landsat9": 30,
    "worldcover": 30, "canopy_height": 30, "modis_ndvi": 500, "modis_lst": 1000, "modis_rgb": 500, "modis_daily": 500,
    "era5": 11000, "srtm": 90, "copdem": 90, "viirs": 500, "chirps": 5000,
    "modis_et": 500, "smap": 11000, "sentinel5p": 7000, "geos_cf": 27750, "cams": 44528, "jrc_water": 90, "hansen": 90,
    "gpw_pop": 1000, "ghsl_pop": 100,
    "ghsl_built": 100, "ghsl_smod": 1000, "dynamicworld": 10,
    "burned": 500, "firms": 1000, "soil_soc": 250, "soil_ph": 250, "soil_clay": 250,
    "modis_gpp": 500, "modis_lai": 500, "era5_solar": 11000, "era5_wind": 11000,
    "gpm": 11000, "modis_snow": 500, "etopo": 1800, "gedi_agb": 1000,
    # ── Lot G — Océanographie ──
    "modis_ocean": 4638, "viirs_ocean": 4638, "oisst": 27750,
    "hycom_vel": 8900, "hycom_sal": 8900, "era5_ocean_wind": 27750,
    "coral": 30, "mangrove": 30,
    "gedi_l2a": 25, "gedi_l2b": 25, "gedi_l4a": 25,
}

def _fmt_num(v):
    if v is None: return "?"
    av = abs(v)
    if av >= 100: return f"{v:.0f}"
    if av >= 1:   return f"{v:.1f}"
    return f"{v:.2f}"

def _hex_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))

def _palette_ramp(colors, n):
    """Échantillonne une palette (liste hex) en n couleurs (interpolation RGB)."""
    cols = [c.lstrip("#") for c in (colors or []) if c]
    if not cols: cols = ["440154", "31688e", "35b779", "fde725"]
    if n <= 1 or len(cols) == 1:
        return [cols[0]] * max(n, 1)
    rgb = [_hex_rgb(c) for c in cols]
    out = []
    for i in range(n):
        pos = i / (n - 1) * (len(rgb) - 1)
        lo = int(pos); hi = min(lo + 1, len(rgb) - 1); f = pos - lo
        c = tuple(rgb[lo][k] + (rgb[hi][k] - rgb[lo][k]) * f for k in range(3))
        out.append("%02x%02x%02x" % tuple(int(round(x)) for x in c))
    return out

def _percentiles(ee, single, region, scale, pcts):
    """Percentiles (dans l'ordre demandé) de la 1re bande sur la région.

    GEE nomme les clés avec des entiers (`<band>_p20`) → on arrondit les
    percentiles demandés pour retrouver les clés de façon fiable.
    """
    ipcts = [max(1, min(99, int(round(p)))) for p in pcts]
    red = ee.Reducer.percentile(ipcts)
    r = single.reduceRegion(reducer=red, geometry=region, scale=scale,
                            maxPixels=1e9, bestEffort=True).getInfo() or {}
    out = []
    for pk in ipcts:
        v = None
        for k, val in r.items():
            if k.endswith(f"_p{pk}") or k == f"p{pk}":
                v = val; break
        out.append(v)
    return out

def _minmax(ee, single, region, scale):
    r = single.reduceRegion(
        reducer=ee.Reducer.min().combine(ee.Reducer.max(), sharedInputs=True),
        geometry=region, scale=scale, maxPixels=1e9, bestEffort=True).getInfo() or {}
    mn = mx = None
    for k, v in r.items():
        if v is None: continue
        if k.endswith("_min") or k == "min": mn = v
        elif k.endswith("_max") or k == "max": mx = v
    return mn, mx

def _jenks(data, n_classes):
    """Ruptures naturelles de Fisher-Jenks (bornes internes) sur un échantillon."""
    data = sorted(float(x) for x in data)
    n = len(data)
    if n <= n_classes:
        return data[1:] if n > 1 else []
    mat1 = [[0] * (n_classes + 1) for _ in range(n + 1)]
    mat2 = [[0.0] * (n_classes + 1) for _ in range(n + 1)]
    for j in range(1, n_classes + 1):
        mat1[1][j] = 1
        mat2[1][j] = 0.0
        for i in range(2, n + 1):
            mat2[i][j] = float("inf")
    for l in range(2, n + 1):
        s1 = s2 = w = 0.0
        v = 0.0
        for m in range(1, l + 1):
            i3 = l - m + 1
            val = data[i3 - 1]
            s2 += val * val; s1 += val; w += 1
            v = s2 - (s1 * s1) / w
            i4 = i3 - 1
            if i4 != 0:
                for j in range(2, n_classes + 1):
                    if mat2[l][j] >= (v + mat2[i4][j - 1]):
                        mat1[l][j] = i3
                        mat2[l][j] = v + mat2[i4][j - 1]
        mat1[l][1] = 1
        mat2[l][1] = v
    k = n
    breaks = [0.0] * (n_classes + 1)
    cid = n_classes
    while cid >= 1:
        idx = int(mat1[k][cid]) - 1
        breaks[cid] = data[idx]
        k = int(mat1[k][cid]) - 1
        cid -= 1
    return breaks[1:n_classes]  # bornes internes (n_classes - 1)

def _class_breaks(ee, single, region, scale, method, n, mn, mx):
    if method == "equal":
        return [mn + (mx - mn) * i / n for i in range(1, n)]
    if method == "quantile":
        pcts = [100.0 * i / n for i in range(1, n)]
        return [v for v in _percentiles(ee, single, region, scale, pcts) if v is not None]
    if method == "jenks":
        bn = single.bandNames().getInfo()[0]
        vals = (single.sample(region=region, scale=scale, numPixels=2500,
                              dropNulls=True, geometries=False)
                      .aggregate_array(bn).getInfo()) or []
        vals = [v for v in vals if v is not None]
        if len(vals) < n + 1:  # échantillon trop petit → repli quantile
            pcts = [100.0 * i / n for i in range(1, n)]
            return [v for v in _percentiles(ee, single, region, scale, pcts) if v is not None]
        return _jenks(vals, n)
    return [mn + (mx - mn) * i / n for i in range(1, n)]


@router.post("/tiles")
def gee_tiles(req: TileRequest):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    try:
        import ee
        ds = DATASETS.get(req.dataset)
        if not ds:
            raise HTTPException(404, f"Dataset inconnu: {req.dataset}")

        # Validation de la disponibilité temporelle (Sentinel-2 pas avant 2015…)
        try:
            _check_date_availability(req.dataset, req.date_start, req.date_end)
        except ValueError as ve:
            raise HTTPException(422, str(ve))

        # Sans masque nuages, filtrer les scènes sur leur taux de nuages n'aurait
        # aucun sens : un incendie sous panache est justement une scène « nuageuse »
        # au sens des métadonnées. On garde tout.
        if req.no_cloud_mask:
            req.cloud_max = 100

        # Géométrie englobante commune (filterBounds Landsat + retry)
        bounds_geom = None
        if req.roi_geojson:
            try: bounds_geom = geojson_to_ee_geometry(req.roi_geojson).bounds()
            except Exception: bounds_geom = None
        if bounds_geom is None and req.bbox:
            w, s, e, n = req.bbox
            bounds_geom = ee.Geometry.BBox(w, s, e, n)

        # ── Landsat « auto » : fusion des missions selon la date ──
        if req.dataset == "landsat":
            col = _landsat_merged(ee, req.date_start, req.date_end, bounds_geom, req.cloud_max)

        # ── Sentinel-5P : une collection L3 par polluant (selon l'indice) ──
        elif req.dataset == "sentinel5p":
            prod = S5P_PRODUCTS.get(req.index)
            if not prod:
                raise HTTPException(422, f"Produit Sentinel-5P inconnu : {req.index}")
            col = ee.ImageCollection(prod[0]).filterDate(req.date_start, req.date_end).select(prod[1])
            if bounds_geom is not None:
                col = col.filterBounds(bounds_geom)

        # Collections temporelles seulement (SRTM/Hansen/WorldCover sont statiques)
        elif ds.get("collection") and not ds.get("static"):
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
        STATIC_DATASETS = {"srtm", "hansen", "worldcover", "canopy_height", "jrc_water", "copdem", "gpw_pop",
                           "soil_soc", "soil_ph", "soil_clay", "etopo", "gedi_agb", "coral", "mangrove"}

        if req.dataset not in STATIC_DATASETS:
            # ── Vérifier que la collection n'est pas vide ─────
            size = col.size().getInfo()
            if size == 0:
                # Sentinel-5P (collection dépend de l'indice) : pas de retry générique
                if req.dataset == "sentinel5p":
                    raise HTTPException(422,
                        f"Aucune donnée Sentinel-5P ({req.index}) entre {req.date_start} "
                        f"et {req.date_end} sur cette zone. Élargissez la période.")
                # Retry SANS filtre nuages mais AVEC même zone
                if req.dataset == "landsat":
                    col_retry = _landsat_merged(ee, req.date_start, req.date_end, bounds_geom, 100)
                else:
                    col_retry = ee.ImageCollection(ds["collection"]).filterDate(req.date_start, req.date_end)
                    if bounds_geom is not None:
                        col_retry = col_retry.filterBounds(bounds_geom)
                size_retry = col_retry.size().getInfo()
                if size_retry == 0 and req.no_widen:
                    # Frame de timelapse : période réellement vide → on le dit,
                    # l'appelant marquera la frame comme absente et la sautera.
                    raise HTTPException(422,
                        f"Aucune image {ds['label']} entre {req.date_start} et {req.date_end}.")
                if size_retry == 0:
                    # ── Élargissement automatique de la fenêtre vers le passé ──
                    # Produits à latence (composites 8/16 j, near-real-time) : la
                    # période demandée peut précéder toute image publiée (ex. mois
                    # courant pour MODIS 16 j). On borne la fin à aujourd'hui (jamais
                    # de date future) et on recule le début pour attraper le dernier
                    # composite disponible, plutôt que de renvoyer une erreur.
                    import datetime as _dt
                    _today = _dt.date.today().isoformat()
                    _clamp_end = min(req.date_end, _today) if req.date_end else _today
                    _temporal = (ds.get("temporal") or "").lower()
                    if req.dataset.startswith("ghsl_"):                _lookback = 2200   # époques tous les 5 ans
                    elif "16" in _temporal:                            _lookback = 96
                    elif "8" in _temporal:                             _lookback = 48
                    elif "mensuel" in _temporal or "month" in _temporal: _lookback = 150
                    elif "annuel" in _temporal:                        _lookback = 730
                    else:                                              _lookback = 60
                    try:
                        _end_d = _dt.date.fromisoformat(_clamp_end)
                    except Exception:
                        _end_d = _dt.date.today()
                    _wide_start = (_end_d - _dt.timedelta(days=_lookback)).isoformat()
                    if req.dataset == "landsat":
                        col_wide = _landsat_merged(ee, _wide_start, _clamp_end, bounds_geom, 100)
                    else:
                        col_wide = ee.ImageCollection(ds["collection"]).filterDate(_wide_start, _clamp_end)
                        if bounds_geom is not None:
                            col_wide = col_wide.filterBounds(bounds_geom)
                    # Dernier recours pour les jeux OCÉAN à forte latence ou figés
                    # (modèles HYCOM, couleur de l'océan…) : la fenêtre élargie de
                    # quelques semaines ne rattrape pas un gel de plusieurs mois. On
                    # prend alors les DERNIÈRES images publiées, quelle que soit la
                    # date demandée. img_date renverra leur date réelle.
                    _LATENCY_LATEST = {"modis_ocean", "viirs_ocean", "oisst",
                                       "hycom_vel", "hycom_sal", "era5_ocean_wind"}
                    if col_wide.size().getInfo() == 0 and req.dataset in _LATENCY_LATEST:
                        col_latest = ee.ImageCollection(ds["collection"])
                        if bounds_geom is not None:
                            col_latest = col_latest.filterBounds(bounds_geom)
                        col_latest = col_latest.sort("system:time_start", False).limit(8)
                        if col_latest.size().getInfo() > 0:
                            col_wide = col_latest
                    if col_wide.size().getInfo() == 0:
                        raise HTTPException(422,
                            f"Images {ds['label']} non disponibles pour l'analyse "
                            f"entre {req.date_start} et {req.date_end} sur cette zone "
                            f"(couverture incomplète ou nuages trop denses). "
                            f"Essayez une période plus large ou une autre zone."
                        )
                    # Le composite/1re image sera pris sur la fenêtre élargie.
                    col = col_wide
                    size = col_wide.size().getInfo()
                    _period_note = (
                        f"Aucune image {ds['label']} pour la période demandée "
                        f"({req.date_start} → {req.date_end}). Fenêtre élargie "
                        f"automatiquement à {_wide_start} → {_clamp_end}."
                    )
                else:
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
            "jrc_water":     "JRC/GSW1_4/GlobalSurfaceWater",
        }
        STATIC_COLLECTIONS = {
            "worldcover": "ESA/WorldCover/v200",
        }

        # Masque nuages par pixel AVANT tout composite (corrige LST négative, etc.).
        # Désactivable : sur un composite coloré, le masque efface fumées et
        # panaches, donc l'information même que l'on veut observer.
        if not req.no_cloud_mask:
            col = _mask_clouds(ee, req.dataset, col)

        if req.dataset in STATIC_IMAGES:
            asset_id = STATIC_IMAGES[req.dataset]
            if req.dataset == "canopy_height":
                image = ee.ImageCollection(asset_id).mosaic()
            else:
                image = ee.Image(asset_id)
        elif req.dataset in STATIC_COLLECTIONS:
            image = ee.ImageCollection(STATIC_COLLECTIONS[req.dataset]).first()
        elif req.dataset == "copdem":
            image = ee.ImageCollection("COPERNICUS/DEM/GLO30").select("DEM").mosaic()
        elif req.dataset == "gpw_pop":
            image = ee.ImageCollection("CIESIN/GPWv411/GPW_Population_Density").sort("system:time_start", False).first()
        elif req.dataset == "dynamicworld":
            image = col.select("label").mode()     # classe la plus fréquente sur la période
        elif req.dataset == "burned":
            image = col.select("BurnDate").max()   # jour de brûlage le + récent (toute la période)
        elif req.dataset == "firms":
            image = col.select("T21").max()        # foyer le plus chaud
        elif req.dataset == "modis_snow":
            image = col.select("NDSI_Snow_Cover").mean()
        elif req.dataset in ("era5_solar", "era5_wind", "gpm"):
            image = col.mean()
        elif req.dataset == "soil_soc":
            image = ee.Image("OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02")
        elif req.dataset == "soil_ph":
            image = ee.Image("OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02")
        elif req.dataset == "soil_clay":
            image = ee.Image("OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02")
        elif req.dataset == "etopo":
            image = ee.Image("NOAA/NGDC/ETOPO1")
        elif req.dataset == "gedi_agb":
            image = ee.Image("LARSE/GEDI/GEDI04_B_002")
        elif req.dataset == "coral":
            image = ee.Image("ACA/reef_habitat/v2_0")               # asset = Image, pas une collection
        elif req.dataset == "mangrove":
            image = ee.ImageCollection("LANDSAT/MANGROVE_FORESTS").mosaic()   # asset = ImageCollection
        elif req.dataset in ("modis_ocean", "viirs_ocean", "oisst", "hycom_vel", "hycom_sal", "era5_ocean_wind",
                             "gedi_l2a", "gedi_l2b", "gedi_l4a"):
            image = col.mean()                     # moyenne des empreintes/valeurs sur la période
        elif req.dataset == "sentinel5p":
            image = col.mean()                     # concentration moyenne sur la période
        elif req.dataset == "chirps":
            image = col.sum()                      # cumul de précipitations sur la période
        elif req.dataset in ("modis_et", "smap", "geos_cf", "cams"):
            image = col.mean()                     # ET / humidité / PM2.5 / AOD moyen sur la période
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

        # ── Relief : exagération verticale + soleil (Élévation / Ombrage / Pente) ──
        # z_factor multiplie l'élévation → « monte/baisse » le relief modélisé.
        shaded_rgb = False   # True = image RGB déjà visualisée (relief ombré)
        if req.dataset in ("srtm", "copdem") and req.index in ("Ombrage", "Pente", "Élévation"):
            dem  = image.select(0)
            z    = req.z_factor if (req.z_factor and req.z_factor > 0) else 1.0
            demz = dem.multiply(z) if z != 1.0 else dem
            az   = req.sun_azimuth  if req.sun_azimuth  is not None else 315.0
            alt  = req.sun_altitude if req.sun_altitude is not None else 45.0
            if req.index == "Ombrage":
                image = ee.Terrain.hillshade(demz, az, alt).rename("hillshade")
            elif req.index == "Pente":
                image = ee.Terrain.slope(demz).rename("slope")
            else:   # Élévation
                if z != 1.0:
                    # RELIEF OMBRÉ : teinte d'altitude modulée par l'ombrage exagéré.
                    base = VIS_PARAMS.get((req.dataset, "Élévation")) or _LOT2_VIS.get((req.dataset, "Élévation")) or {}
                    pal  = base.get("palette") or ["#313695", "#74add1", "#e0f3f8", "#fee090", "#f46d43", "#a50026"]
                    mn, mx = base.get("min", 0), base.get("max", 3000)
                    hs = ee.Terrain.hillshade(demz, az, alt).divide(255.0)
                    # 0.5→1.1 : ombre marquée sans noircir la carte
                    image = dem.visualize(min=mn, max=mx, palette=pal).multiply(hs.multiply(0.6).add(0.5)).uint8()
                    shaded_rgb = True
                else:
                    image = dem.rename("elevation")
        else:
            # ── Calcul de l'indice ────────────────────────────
            image = compute_index(image, req.dataset, req.index)
            # Fumée : masquer l'air « propre » → seul le panache s'affiche, en
            # surimpression translucide sur le fond (rendu type Windy). Seuil par
            # (dataset, indice) : µg/m³ pour PM2.5, sans dimension pour l'AOD.
            _smoke_floor = {
                ("geos_cf", "PM2.5 (fumée)"): 5, ("geos_cf", "Carbone suie (BC)"): 1,
                ("geos_cf", "Carbone organique (OC)"): 1,
                ("cams", "PM2.5 (fumée)"): 5, ("cams", "Aérosols (AOD)"): 0.1,
                ("cams", "Fumée (carbone organique)"): 0.05,
            }.get((req.dataset, req.index))
            if _smoke_floor is not None:
                image = image.updateMask(image.gt(_smoke_floor))

        # ── Clip ROI : masquer l'image au contour exact du polygone ──
        # Sans ce clip, GEE retourne des tuiles mondiales même en mode "couche"
        roi_geom = None
        if req.roi_geojson:
            try:
                roi_geom = geojson_to_ee_geometry(req.roi_geojson)
                image = image.clip(roi_geom)
            except Exception as clip_err:
                print(f"ROI clip warning: {clip_err}")
                # Fallback : clip via bbox si le clip exact échoue (sauf emprise monde)
                if req.bbox and not _is_global_bbox(req.bbox):
                    w, s, e, n = req.bbox
                    image = image.clip(ee.Geometry.BBox(w, s, e, n))
        elif req.bbox and not _is_global_bbox(req.bbox):
            # Même sans ROI polygon, clipper sur la bbox améliore les perfs.
            # Emprise ~monde entier → pas de clip (inutile + casse la reprojection
            # sinusoïdale MODIS à l'antiméridien).
            w, s, e, n = req.bbox
            image = image.clip(ee.Geometry.BBox(w, s, e, n))
        if req.dataset == "canopy_height":
            image = image.updateMask(image.gte(1))

        # ── Génération URL tuiles ─────────────────────────────
        # Palette : (dataset,index) explicite, sinon repli par indice (_INDEX_VIS).
        # vis_params_override permet de styler depuis le LayerPanel sans recharger.
        vis_default = (VIS_PARAMS.get((req.dataset, req.index))
                       or _LOT2_VIS.get((req.dataset, req.index))
                       or _INDEX_VIS.get(req.index, {}))
        if req.vis_params_override:
            vis = {**vis_default, **req.vis_params_override}
        else:
            vis = dict(vis_default)

        # ── Auto-stretch / classification (rasters mono-bande continus) ──
        # Relief ombré : l'image est déjà RGB visualisée → pas de palette ni d'étirement.
        if shaded_rgb:
            vis = {}
        legend = None
        needs_region = (req.auto_stretch or req.classify) and not shaded_rgb
        _region = roi_geom
        # Emprise ~monde : pas de reduceRegion planétaire (lent/timeout + reprojection
        # sinusoïdale MODIS impossible) → l'auto-stretch/classif garde la palette par défaut.
        if needs_region and _region is None and req.bbox and not _is_global_bbox(req.bbox):
            w, s, e, n = req.bbox
            _region = ee.Geometry.BBox(w, s, e, n)
        if needs_region and _region is not None and not vis.get("bands"):
            single = image.select([0])
            sscale = req.scale if getattr(req, "scale", None) else _STRETCH_SCALE.get(req.dataset, 100)
            try:
                if req.classify:
                    mn, mx = _minmax(ee, single, _region, sscale)
                    if mn is None or mx is None or mn >= mx:
                        raise ValueError("données constantes ou vides")
                    nreq = max(2, min(int(req.n_classes or 5), 12))
                    brks = _class_breaks(ee, single, _region, sscale, req.classify, nreq, mn, mx)
                    brks = sorted({round(float(b), 6) for b in brks if b is not None and mn < b < mx})
                    if not brks:
                        raise ValueError("ruptures de classes introuvables")
                    ncl = len(brks) + 1
                    classified = ee.Image(0)
                    for i, b in enumerate(brks):
                        classified = classified.where(single.gte(b), i + 1)
                    image = classified.updateMask(single.mask())
                    pal = _palette_ramp(vis.get("palette"), ncl)
                    vis = {"min": 0, "max": ncl - 1, "palette": pal}
                    edges = [mn] + brks + [mx]
                    legend = [{"class_id": i, "color": "#" + pal[i],
                               "label": f"{_fmt_num(edges[i])} – {_fmt_num(edges[i + 1])}"}
                              for i in range(ncl)]
                elif req.auto_stretch:
                    p2, p98 = _percentiles(ee, single, _region, sscale, [2, 98])
                    if p2 is not None and p98 is not None and p98 > p2:
                        vis = {**vis, "min": round(float(p2), 4), "max": round(float(p98), 4)}
            except HTTPException:
                raise
            except Exception as stretch_err:
                print(f"[tiles] auto_stretch/classify ignoré : {stretch_err}")

        # ── Mode vignette (export animation) ──────────────────
        if req.thumb_px:
            if roi_geom is not None:
                _region = roi_geom
            elif req.bbox:
                w, s, e, n = req.bbox
                _region = ee.Geometry.BBox(w, s, e, n)
            else:
                raise HTTPException(422, "Emprise requise pour l'export.")
            tp = {k: v for k, v in vis.items() if k != "legend_label"}
            tp.update({"dimensions": int(req.thumb_px), "region": _region, "format": "png"})
            return {"thumb_url": image.getThumbURL(tp), "vis_params": vis,
                    "dataset": req.dataset, "index": req.index}

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
            "legend":     legend,
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


# ══════════════════════════════════════════════════════════════
#  TIMELAPSE SUR CARTE — animation de tuiles GEE dans MapLibre
# ══════════════════════════════════════════════════════════════
# Principe : on NE duplique PAS la logique de /tiles (350 lignes de cas
# particuliers par dataset). Chaque frame est un appel gee_tiles() normal, avec
# deux différences :
#   • no_widen=True      → une période vide reste vide (pas de repli sur une
#                          autre date, sinon l'animation mentirait) ;
#   • vis_params_override → l'échelle de couleurs est calculée UNE FOIS sur toute
#                          la période puis imposée à chaque frame. Sans ça chaque
#                          image se re-normalise sur ses propres p2/p98 et on ne
#                          voit AUCUNE évolution (l'hiver a la couleur de l'été).

_TL_MONTHS = ["janv.", "févr.", "mars", "avr.", "mai", "juin",
              "juil.", "août", "sept.", "oct.", "nov.", "déc."]

# Datasets SANS dimension temporelle dans /tiles (asset unique ou .first()).
# À ne pas confondre avec _STATIC_FOR_TS, bien plus large : celui-là exclut aussi
# des jeux parfaitement animables (GHSL, Dynamic World, GPM, neige…) au seul
# motif que la série temporelle ne leur est pas câblée.
_TL_STATIC = {"srtm", "copdem", "etopo", "canopy_height", "gedi_agb",
              "soil_soc", "soil_ph", "soil_clay",
              "hansen", "worldcover", "jrc_water", "gpw_pop",
              "coral", "mangrove"}

# Pas de temps proposables. Les pas courts n'ont de sens que si la revisite du
# capteur suit — le front restreint la liste selon le dataset.
_TL_STEPS = {"day": 1, "5day": 5, "8day": 8, "16day": 16,
             "month": None, "quarter": None, "year": None}


def _tl_label(d0, step):
    if step == "year":    return str(d0.year)
    if step == "quarter": return f"T{(d0.month - 1) // 3 + 1} {d0.year}"
    if step == "month":   return f"{_TL_MONTHS[d0.month - 1]} {d0.year}"
    return f"{d0.day:02d} {_TL_MONTHS[d0.month - 1]} {d0.year}"


def _tl_periods(date_start, date_end, step, max_frames=48):
    """Découpe [date_start, date_end] en périodes successives.

    Bornes de fin EXCLUSIVES : c'est la convention de ee.filterDate, donc un pas
    mensuel donne exactement [1er du mois, 1er du mois suivant[.
    """
    import datetime as _dt
    if step not in _TL_STEPS:
        raise HTTPException(422, f"Pas de temps inconnu : {step}")
    try:
        d0 = _dt.date.fromisoformat(date_start)
        d1 = _dt.date.fromisoformat(date_end)
    except Exception:
        raise HTTPException(422, "Dates invalides (attendu AAAA-MM-JJ).")
    today = _dt.date.today()
    if d1 > today:
        d1 = today                      # jamais de frame dans le futur
    if d1 <= d0:
        raise HTTPException(422, "La date de fin doit être postérieure à la date de début.")
    cap = max(2, min(int(max_frames or 48), 60))

    out = []
    if step in ("month", "quarter", "year"):
        nmon = {"month": 1, "quarter": 3, "year": 12}[step]
        m = d0.month
        if step == "quarter": m = ((m - 1) // 3) * 3 + 1
        if step == "year":    m = 1
        cur = _dt.date(d0.year, m, 1)
        while cur < d1 and len(out) < cap:
            tot = (cur.month - 1) + nmon
            nxt = _dt.date(cur.year + tot // 12, tot % 12 + 1, 1)
            out.append({"date_start": cur.isoformat(),
                        "date_end":   min(nxt, d1).isoformat(),
                        "label":      _tl_label(cur, step)})
            cur = nxt
    else:
        ndays = _TL_STEPS[step]
        cur = d0
        while cur < d1 and len(out) < cap:
            nxt = cur + _dt.timedelta(days=ndays)
            out.append({"date_start": cur.isoformat(),
                        "date_end":   min(nxt, d1).isoformat(),
                        "label":      _tl_label(cur, step)})
            cur = nxt
    if not out:
        raise HTTPException(422, "Aucune période à animer sur cet intervalle.")
    return out


class TimelapseRequest(TileRequest):
    step:       Optional[str]  = "month"
    max_frames: Optional[int]  = 48
    # Périodes explicites (millésimes GHSL, dates réelles d'images…) ; sinon
    # elles sont calculées à partir de step.
    periods:    Optional[List[dict]] = None
    # Sous-ensemble à calculer : permet le chargement progressif côté front
    # (les 8 premières frames, puis le reste en tâche de fond).
    offset:     Optional[int]  = 0
    limit:      Optional[int]  = 0      # 0 = toutes les périodes restantes


def _tl_child(req: "TimelapseRequest", p: dict, vis: Optional[dict],
              thumb_px: Optional[int] = None) -> TileRequest:
    """TileRequest d'une frame : même requête, période resserrée, échelle figée.

    `thumb_px` bascule le rendu en vignette PNG (export fichier) au lieu d'une
    URL de tuiles (aperçu sur carte).
    """
    return TileRequest(
        dataset=req.dataset, index=req.index,
        date_start=p["date_start"], date_end=p["date_end"],
        bbox=req.bbox, cloud_max=req.cloud_max, composite=req.composite,
        roi_geojson=req.roi_geojson,
        vis_params_override=vis or req.vis_params_override,
        auto_stretch=False, classify=None,   # l'échelle vient du plan, pas de la frame
        z_factor=req.z_factor, sun_azimuth=req.sun_azimuth, sun_altitude=req.sun_altitude,
        no_widen=True, thumb_px=thumb_px,
    )


def _tl_resolve_periods(req: "TimelapseRequest"):
    return req.periods if req.periods else _tl_periods(
        req.date_start, req.date_end, req.step or "month", req.max_frames)


@router.post("/timelapse/plan")
def gee_timelapse_plan(req: TimelapseRequest):
    """Prépare une animation : liste des périodes + échelle de couleurs commune.

    Un seul appel GEE (sur la période entière, en composite médian pour que
    l'étirement reflète des valeurs typiques et non une image isolée).
    """
    # Contrôle local d'abord : inutile de solliciter GEE pour dire qu'un MNT n'a
    # pas de dimension temporelle.
    if req.dataset in _TL_STATIC:
        raise HTTPException(422,
            f"{DATASETS.get(req.dataset, {}).get('label', req.dataset)} est un jeu de données "
            f"statique : il n'y a rien à animer dans le temps.")
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")

    periods = _tl_resolve_periods(req)

    # Échelle commune : composite médian sur TOUTE la période animée.
    whole = TileRequest(
        dataset=req.dataset, index=req.index,
        date_start=periods[0]["date_start"], date_end=periods[-1]["date_end"],
        bbox=req.bbox, cloud_max=req.cloud_max,
        composite="median",
        roi_geojson=req.roi_geojson,
        vis_params_override=req.vis_params_override,
        auto_stretch=True if not req.vis_params_override else False,
        z_factor=req.z_factor, sun_azimuth=req.sun_azimuth, sun_altitude=req.sun_altitude,
    )
    ref = gee_tiles(whole)

    return {
        "periods":    periods,
        "count":      len(periods),
        "step":       req.step,
        "vis_params": ref.get("vis_params") or {},
        "legend":     ref.get("legend"),
        "name":       ref.get("name"),
        "preview_tile_url": ref.get("tile_url"),   # aperçu affichable pendant le chargement
    }


@router.post("/timelapse/frames")
def gee_timelapse_frames(req: TimelapseRequest):
    """Calcule les URLs de tuiles d'une tranche de frames, en parallèle.

    getMapId est I/O-bound (aller-retour GEE) : un pool de threads ramène une
    vingtaine de frames de ~20 s à ~4 s. On reste à 6 threads pour ne pas
    déclencher le quota de requêtes simultanées d'Earth Engine.
    """
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    from concurrent.futures import ThreadPoolExecutor

    periods = _tl_resolve_periods(req)
    off = max(0, int(req.offset or 0))
    lim = int(req.limit or 0)
    sl  = periods[off: off + lim] if lim > 0 else periods[off:]
    vis = req.vis_params_override

    def one(idx_period):
        i, p = idx_period
        base = {"index": off + i, "label": p.get("label"),
                "date_start": p["date_start"], "date_end": p["date_end"]}
        try:
            d = gee_tiles(_tl_child(req, p, vis))
            return {**base, "tile_url": d.get("tile_url"), "date": d.get("date"), "empty": False}
        except HTTPException as he:
            # Période sans image (nuages, hors couverture) → frame sautée côté front
            return {**base, "tile_url": None, "empty": True, "reason": str(he.detail)}
        except Exception as e:
            return {**base, "tile_url": None, "empty": True, "reason": str(e)}

    with ThreadPoolExecutor(max_workers=6) as ex:
        frames = list(ex.map(one, enumerate(sl)))

    return {"frames": frames, "offset": off, "total": len(periods)}


# ══════════════════════════════════════════════════════════════
#  SÉVÉRITÉ D'INCENDIE — dNBR (méthodologie UN-SPIDER / USGS)
# ══════════════════════════════════════════════════════════════
# Vérifié contre l'implémentation de référence UN-SPIDER
# (github.com/UN-SPIDER/burn-severity-mapping-EO, burn_severity1.py) :
#   • NBR  = (B8A − B12) / (B8A + B12)  ← B8A, PAS B8. B8A est la bande PIR
#     étroite à 20 m, résolution native de B12 ; B8 est à 10 m et plus large.
#     C'est la formule du code de référence.
#   • dNBR = NBR_avant − NBR_après  (positif = perte de végétation = brûlé)
#   • les seuils s'appliquent au ratio BRUT, sans multiplication par 1000
#     (la table USGS souvent citée en 100/270/440/660 est la même, ×1000).
#   • seuils du code de référence : 0.1 / 0.27 / 0.44 / 0.66
# La classe « repousse » (dNBR < −0.1) vient de la table USGS complète : le code
# de référence la fond dans « non brûlé », on la distingue pour obtenir 6 classes.

_BURN_CLASSES = [
    ("Repousse / rehaussement", "#7a8b3c"),
    ("Non brûlé",               "#4bd44b"),
    ("Sévérité faible",         "#ffff3f"),
    ("Faible à modérée",        "#ff9a1f"),
    ("Modérée à forte",         "#f01e1e"),
    ("Sévérité forte",          "#e01ee0"),
]
_BURN_DEFAULT_THRESHOLDS = [-0.1, 0.1, 0.27, 0.44, 0.66]   # 5 bornes → 6 classes

# Table USGS intégrale : elle distingue DEUX niveaux de repousse, que la variante
# à 6 classes fusionne. Borne supplémentaire à −0,251.
_BURN_CLASSES_7 = [
    ("Repousse forte",   "#6e7b36"),
    ("Repousse faible",  "#a5c249"),
    ("Non brûlé",        "#4bd44b"),
    ("Sévérité faible",  "#ffff3f"),
    ("Faible à modérée", "#ff9a1f"),
    ("Modérée à forte",  "#f01e1e"),
    ("Sévérité forte",   "#e01ee0"),
]
_BURN_THRESHOLDS_7 = [-0.251, -0.101, 0.1, 0.27, 0.44, 0.66]

# NOTE sur la table publiée : sa colonne « non mise à l'échelle » indique
# « Unburned : −0.100 to +0.99 ». C'est une coquille — la colonne ×10³ donne
# −100 à +99, soit +0,099. Recopier 0,99 ferait déborder « non brûlé » sur les
# sévérités faible, modérée-basse et modérée-haute, qui disparaîtraient.


class BurnSeverityRequest(BaseModel):
    dataset:     str   = "sentinel2"           # sentinel2 | landsat | landsat8 | landsat9
    pre_start:   str
    pre_end:     str
    post_start:  str
    post_end:    str
    cloud_max:   Optional[float] = 40.0
    bbox:        Optional[List[float]] = None
    roi_geojson: Optional[dict]  = None
    thresholds:  Optional[List[float]] = None  # bornes croissantes (n → n+1 classes)
    scale:       Optional[int]   = 20          # résolution native de B12
    vectorize:   Optional[bool]  = True        # périmètre de l'incendie en vecteur
    split_regrowth: Optional[bool] = False     # True → table USGS intégrale, 7 classes


def _nbr_of(ee, dataset, col):
    """Composite médian → NBR, selon la formule de référence du dataset."""
    img = col.median()
    if dataset == "sentinel2":
        return img.normalizedDifference(["B8A", "B12"]).rename("NBR")
    if dataset in ("landsat8", "landsat9"):
        return img.normalizedDifference(["SR_B5", "SR_B7"]).rename("NBR")
    return img.normalizedDifference(["NIR", "SWIR2"]).rename("NBR")


@router.post("/burn-severity")
def gee_burn_severity(req: BurnSeverityRequest):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    import ee

    ds = DATASETS.get(req.dataset)
    if not ds:
        raise HTTPException(404, f"Dataset inconnu : {req.dataset}")

    if req.roi_geojson:
        try: region = geojson_to_ee_geometry(req.roi_geojson)
        except Exception: region = None
    else:
        region = None
    if region is None:
        if not req.bbox:
            raise HTTPException(422, "Emprise requise (bbox ou ROI).")
        w, s, e, n = req.bbox
        region = ee.Geometry.BBox(w, s, e, n)

    classes = _BURN_CLASSES_7 if req.split_regrowth else _BURN_CLASSES
    thr = list(req.thresholds or (_BURN_THRESHOLDS_7 if req.split_regrowth else _BURN_DEFAULT_THRESHOLDS))
    if len(thr) != len(classes) - 1 or any(thr[i] >= thr[i + 1] for i in range(len(thr) - 1)):
        raise HTTPException(422,
            f"{len(classes) - 1} seuils strictement croissants sont attendus.")

    def window(d0, d1):
        if req.dataset == "landsat":
            c = _landsat_merged(ee, d0, d1, region, req.cloud_max)
        else:
            c = ee.ImageCollection(ds["collection"]).filterDate(d0, d1).filterBounds(region)
            cp = ds.get("cloud_property")
            if cp and req.cloud_max is not None and req.cloud_max < 100:
                c = c.filter(ee.Filter.And(ee.Filter.notNull([cp]), ee.Filter.lte(cp, req.cloud_max)))
        # Masque nuages ACTIF, contrairement aux composites d'observation : un
        # nuage sur l'image post-incendie fait chuter le NBR et se traduirait
        # par une fausse « sévérité forte ». C'est l'erreur classique du dNBR.
        return _mask_clouds(ee, req.dataset, c)

    # Dates hors couverture du capteur : le dire avant d'interroger GEE, et
    # nommer la fenêtre fautive — « 0 image » ne guide personne.
    for lib, d0, d1 in (("avant", req.pre_start, req.pre_end),
                        ("après", req.post_start, req.post_end)):
        try:
            _check_date_availability(req.dataset, d0, d1)
        except ValueError as ve:
            raise HTTPException(422, f"Fenêtre « {lib} » indisponible — {ve}")

    col_pre, col_post = window(req.pre_start, req.pre_end), window(req.post_start, req.post_end)
    n_pre, n_post = col_pre.size().getInfo(), col_post.size().getInfo()
    if n_pre == 0 or n_post == 0:
        manque = ("avant et après" if n_pre == 0 and n_post == 0
                  else "avant" if n_pre == 0 else "après")
        raise HTTPException(422,
            f"Aucune image disponible {manque} sur cette zone "
            f"(avant {req.pre_start}→{req.pre_end} : {n_pre} image(s) ; "
            f"après {req.post_start}→{req.post_end} : {n_post}). "
            f"Élargissez la ou les fenêtres de dates, ou augmentez le filtre nuages.")

    dnbr = _nbr_of(ee, req.dataset, col_pre) \
        .subtract(_nbr_of(ee, req.dataset, col_post)).rename("dNBR").clip(region)

    # Classement 0..5 : on empile les seuils du plus bas au plus haut
    classified = ee.Image(0)
    for i, t in enumerate(thr):
        classified = classified.where(dnbr.gte(t), i + 1)
    classified = classified.updateMask(dnbr.mask()).rename("severite")

    pal = [c for _, c in classes]
    vis = {"min": 0, "max": len(classes) - 1, "palette": pal}
    sc = max(10, int(req.scale or 20))

    # ── Surfaces par classe ────────────────────────────────────
    areas = {}
    try:
        grouped = ee.Image.pixelArea().divide(1e4).addBands(classified).reduceRegion(
            reducer=ee.Reducer.sum().group(groupField=1, groupName="cls"),
            geometry=region, scale=sc, maxPixels=1e10, bestEffort=True,
        ).getInfo() or {}
        for g in grouped.get("groups", []):
            areas[int(g["cls"])] = round(float(g["sum"]), 1)
    except Exception as ex:
        print(f"[burn-severity] surfaces indisponibles : {ex}")

    legend = [{
        "class_id": i, "label": lab, "color": col,
        "range": ("< %.3f" % thr[0]) if i == 0
                 else ("≥ %.3f" % thr[-1]) if i == len(classes) - 1
                 else "%.3f – %.3f" % (thr[i - 1], thr[i]),
        "area_ha": areas.get(i, 0.0),
    } for i, (lab, col) in enumerate(classes)]

    # Total brûlé = toutes les classes au-delà du seuil de brûlage (0,1 par
    # défaut), quel que soit le nombre de classes de repousse en amont.
    first_burn = next((i for i, t in enumerate(thr) if t >= 0.1), len(thr) - 1) + 1
    burned_ha = round(sum(areas.get(i, 0.0) for i in range(first_burn, len(classes))), 1)

    def tiles(image, v):
        mid = image.getMapId(v)
        f = mid.get("tile_fetcher")
        return f.url_format if (f and hasattr(f, "url_format")) else mid.get("urlFormat", "")

    out = {
        "tile_url":      tiles(classified, vis),
        "dnbr_tile_url": tiles(dnbr, {"min": -0.5, "max": 1.0,
                                      "palette": ["#2b83ba", "#ffffbf", "#fdae61", "#d7191c"]}),
        "vis_params":    vis,
        "legend":        legend,
        "burned_ha":     burned_ha,
        "images":        {"pre": n_pre, "post": n_post},
        "thresholds":    thr,
        "dataset":       req.dataset,
        "scale":         sc,
    }

    # ── Périmètre vectoriel ────────────────────────────────────
    if req.vectorize:
        try:
            # Résolution volontairement plus grossière : reduceToVectors au pas
            # natif sur un grand incendie dépasse les limites de calcul GEE.
            vsc = max(sc, 40)
            fc = classified.gte(2).selfMask().reduceToVectors(
                geometry=region, scale=vsc, geometryType="polygon",
                eightConnected=True, maxPixels=1e9, bestEffort=True,
            )
            out["perimeter"] = fc.geometry().dissolve(maxError=vsc).getInfo()
        except Exception as ex:
            print(f"[burn-severity] vectorisation ignorée : {ex}")
            out["perimeter"] = None

    return out


# ══════════════════════════════════════════════════════════════════════════════
#  CARTOGRAPHIE DES INONDATIONS — détection SAR + modèle HAND
# ══════════════════════════════════════════════════════════════════════════════
# Deux voies (+ impacts population/bâti) :
#   • sar  : Sentinel-1 VV avant/après, chute de rétrodiffusion = eau nouvelle
#            (méthode Copernicus EMS / UN-SPIDER, traverse les nuages). Voie
#            unique de détection : l'optique a été retirée (trop dépendante des
#            nuages, dilution de la crue par le compositing, eau turbide invisible).
#   • hand : modèle par hauteur d'eau — inondé là où HAND ≤ hauteur (MERIT Hydro).
# Affinage SAR : retrait de l'eau permanente (JRC), des pentes fortes (le DEM :
# l'eau ne stagne pas), lissage morphologique, et pixels isolés (connectivité).
# Impacts : surface (ha), population exposée (GHSL), surface bâtie inondée (GHSL).

class FloodRequest(BaseModel):
    mode:         str = "sar"                    # "sar" | "hand"
    bbox:         Optional[List[float]] = None
    roi_geojson:  Optional[dict]  = None
    pre_start:    Optional[str]   = None
    pre_end:      Optional[str]   = None
    post_start:   Optional[str]   = None
    post_end:     Optional[str]   = None
    water_height: Optional[float] = 2.0          # modèle HAND (m)
    sensitivity:  Optional[float] = 50.0         # 0 (strict) → 100 (large) — SAR
    vectorize:    Optional[bool]  = True
    context:      Optional[bool]  = True          # fond satellite fausse couleur (contexte de la crue)


@router.post("/flood")
def gee_flood(req: FloodRequest):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    import ee

    if req.roi_geojson:
        try: region = geojson_to_ee_geometry(req.roi_geojson)
        except Exception: region = None
    else:
        region = None
    if region is None:
        if not req.bbox:
            raise HTTPException(422, "Emprise requise (bbox ou ROI).")
        w, s, e, n = req.bbox
        region = ee.Geometry.BBox(w, s, e, n)

    notes = []
    dem = ee.ImageCollection("COPERNICUS/DEM/GLO30").select("DEM").mosaic()

    # Fond satellite « fausse couleur SWIR » (contexte de la crue, comme
    # Copernicus EMS) : végétation verte, sols nus rose-gris, eau sombre —
    # la nappe inondée ressort en rouge par-dessus.
    _FC_VIS = {"bands": ["B12", "B8", "B4"], "min": 0, "max": 3500}
    context_img = None

    # ── 1. Masque d'inondation selon le mode ──────────────────────
    if req.mode == "hand":
        try:
            hnd = ee.Image("MERIT/Hydro/v1_0_1").select("hnd")
        except Exception as ex:
            raise HTTPException(502, f"MNT hydrologique (MERIT Hydro) indisponible : {ex}")
        h = max(0.1, float(req.water_height or 2.0))
        flood = hnd.lte(h).rename("flood")
        scale = 90
        notes.append(f"Modèle HAND : inondé là où la hauteur au-dessus du cours d'eau ≤ {h:g} m.")
    else:
        for lbl, d0, d1 in (("avant", req.pre_start, req.pre_end), ("après", req.post_start, req.post_end)):
            if not d0 or not d1:
                raise HTTPException(422, f"Fenêtre « {lbl} » requise (dates début/fin).")
        import datetime as _dt
        # Sensibilité 0..100 → seuils : plus haut = détecte plus (marges turbides,
        # eau peu profonde) au prix de faux positifs. L'utilisateur ajuste au visuel.
        _s = max(0.0, min(float(req.sensitivity if req.sensitivity is not None else 50), 100)) / 100.0
        # Fenêtre à fin INCLUSIVE (filterDate est exclusif : [03,05) rate le 05) +
        # élargissement progressif (Sentinel-1 repasse tous les ~6-12 j → une
        # fenêtre de 2-3 j tombe souvent à vide).
        def _win(d0, d1, extra):
            a = (_dt.date.fromisoformat(d0) - _dt.timedelta(days=extra)).isoformat()
            b = (_dt.date.fromisoformat(d1) + _dt.timedelta(days=1 + extra)).isoformat()
            return a, b

        # ── SAR Sentinel-1 (seule voie de détection : traverse les nuages) ──
        base = (ee.ImageCollection("COPERNICUS/S1_GRD")
                .filter(ee.Filter.eq("instrumentMode", "IW"))
                .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
                .select("VV").filterBounds(region))
        def _vv(which, d0, d1):
            for extra in (0, 12, 36):
                a, b = _win(d0, d1, extra)
                col = base.filterDate(a, b)
                if int(col.size().getInfo()) > 0:
                    if extra:
                        notes.append(f"Fenêtre « {which} » élargie de ±{extra} j — Sentinel-1 repasse tous les ~6-12 j.")
                    return col.median().focal_median(50, "circle", "meters")   # composite + anti-speckle
            raise HTTPException(422,
                f"Aucune image Sentinel-1 (VV) « {which} » même en élargissant à ±36 j sur cette zone. "
                f"Choisissez une autre période ou une autre zone.")
        before, after = _vv("avant", req.pre_start, req.pre_end), _vv("après", req.post_start, req.post_end)
        drop     = -5.0 + _s * 3.5        # −5 (strict) → −1,5 dB (large)
        darkgate = -16.0 + _s * 4.0       # −16 → −12 dB : tolère l'eau moins sombre (turbide/peu profonde)
        flood = after.subtract(before).lt(drop).And(after.lt(darkgate)).rename("flood")  # chute + sombre = eau
        scale = 30
        notes.append(f"SAR Sentinel-1 : chute VV ≤ {drop:.1f} dB et signal < {darkgate:.0f} dB. Traverse les nuages.")
        # Fond optique de contexte (au mieux) : une image Sentinel-2 claire proche
        # aide à lire la carte (le SAR, lui, traverse les nuages de l'événement).
        if req.context:
            try:
                for extra in (5, 25, 90):
                    a, b = _win(req.post_start, req.post_end, extra)
                    cc = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                          .filterDate(a, b).filterBounds(region)
                          .filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", 60)))
                    if int(cc.size().getInfo()) > 0:
                        context_img = _mask_clouds(ee, "sentinel2", cc).median()
                        break
            except Exception as ex:
                print(f"[flood] fond optique indisponible : {ex}")

        # ── Affinage SAR : nettoyage + lissage morphologique ──
        # Binaire 0/1 non masqué + résolution FIGÉE (reproject) : les opérateurs
        # focaux deviennent déterministes (mêmes rayons quel que soit le zoom).
        _msc  = max(scale, 30)
        flood = flood.unmask(0).reproject(crs="EPSG:3857", scale=_msc)
        try:
            jrc = ee.Image("JRC/GSW1_4/GlobalSurfaceWater").select("seasonality")
            flood = flood.where(jrc.unmask(0).gte(10), 0)            # retire l'eau permanente
        except Exception as ex:
            print(f"[flood] JRC indisponible : {ex}")
        try:
            slope = ee.Terrain.slope(dem)
            flood = flood.where(slope.gte(5), 0)                     # pas d'eau sur pente > 5°
        except Exception as ex:
            print(f"[flood] pente indisponible : {ex}")
        # EMS produit une nappe CONTINUE ; une classif pixel brute est en
        # sel-et-poivre où les pixels épars sont de la VRAIE eau. On RECOLLE
        # (sans effacer) : dilatation généreuse → fusionne les fragments voisins
        # en nappe ; érosion un peu MOINS forte → nappe pleine, légèrement
        # étendue (comme EMS). Puis retrait des taches restées isolées.
        _dil = _msc * (1.5 + _s * 2.0)                              # dilatation 1,5 → 3,5 px
        _ero = _msc * (1.0 + _s * 1.5)                              # érosion   1,0 → 2,5 px (< dilatation)
        flood = flood.focal_max(radius=_dil, kernelType="circle", units="meters")
        flood = flood.focal_min(radius=_ero, kernelType="circle", units="meters")
        _minpix = max(3, int(round(8 - _s * 4)))                    # 8 (strict) → 4 (large)
        flood = flood.updateMask(flood).connectedPixelCount(100, True).gte(_minpix).unmask(0)
        flood = flood.gt(0).rename("flood")
        notes.append("Nappe recollée par fermeture morphologique (dilatation + érosion) pour une emprise continue.")

    flood = flood.selfMask().clip(region)

    # ── 2. Impacts : surface, population, bâti ────────────────────
    def _sum(image, sc):
        try:
            return float((image.updateMask(flood).reduceRegion(
                reducer=ee.Reducer.sum(), geometry=region, scale=sc,
                maxPixels=1e10, bestEffort=True).values().get(0)).getInfo() or 0)
        except Exception as ex:
            print(f"[flood] somme indisponible : {ex}")
            return 0.0

    area_ha = round(_sum(ee.Image.pixelArea().divide(1e4), max(20, scale)), 1)
    population, built_ha = None, None
    try:
        pop = ee.ImageCollection("JRC/GHSL/P2023A/GHS_POP").filterDate("2020-01-01", "2021-01-01") \
            .first().select("population_count")
        population = int(round(_sum(pop, 100)))
    except Exception as ex:
        print(f"[flood] population GHSL indisponible : {ex}"); notes.append("Population GHSL indisponible.")
    try:
        built = ee.ImageCollection("JRC/GHSL/P2023A/GHS_BUILT_S").filterDate("2020-01-01", "2021-01-01") \
            .first().select("built_surface")                          # m² bâti / cellule 100 m
        built_ha = round(_sum(built, 100) / 1e4, 1)
    except Exception as ex:
        print(f"[flood] bâti GHSL indisponible : {ex}"); notes.append("Surface bâtie GHSL indisponible.")

    # ── 3. Tuiles + périmètre vectoriel ───────────────────────────
    vis = {"palette": ["#e11d1d"], "min": 0, "max": 1}   # nappe inondée en rouge (convention EMS)
    def _tiles(image, v):
        mid = image.getMapId(v); f = mid.get("tile_fetcher")
        return f.url_format if (f and hasattr(f, "url_format")) else mid.get("urlFormat", "")

    context_url = None
    if context_img is not None:
        try:
            context_url = _tiles(context_img.clip(region), _FC_VIS)
        except Exception as ex:
            print(f"[flood] tuiles de contexte indisponibles : {ex}")

    out = {
        "mode": req.mode,
        "tile_url": _tiles(flood, vis),
        "vis_params": vis,
        "context_url": context_url,
        "context_vis": _FC_VIS,
        "surface_ha": area_ha,
        "surface_km2": round(area_ha / 100, 2),
        "population": population,
        "bati_ha": built_ha,
        "notes": notes,
    }
    if req.vectorize:
        try:
            vsc = max(scale, 60)
            fc = flood.reduceToVectors(geometry=region, scale=vsc, geometryType="polygon",
                                       eightConnected=True, maxPixels=1e9, bestEffort=True)
            out["perimeter"] = fc.geometry().dissolve(maxError=vsc).getInfo()
        except Exception as ex:
            print(f"[flood] vectorisation ignorée : {ex}"); out["perimeter"] = None
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  MÉTÉO — Prévision GFS (partie « futur » de la frise du module Météo)
# ══════════════════════════════════════════════════════════════════════════════
# Le radar et le satellite IR (passé + nowcast) viennent de RainViewer CÔTÉ CLIENT
# (sans clé, CORS ok) — rien à faire ici. Ce endpoint fournit uniquement la
# PRÉVISION du modèle GFS (NOAA/GFS0P25) : température 2 m, précipitations ou vent
# 10 m, par échéance jusqu'à +48 h — la partie « futur » de la frise temporelle.
# Réponse mise en cache par (variable, cycle) : un cycle GFS ne change pas.

_GFS_STEPS = list(range(0, 49, 3)) + [54, 60, 66, 72]   # 0→48 h par 3 h, puis →72 h par 6 h

# Palettes de précipitation façon radar météo. « storm » = orage éclatant,
# « blue » = bleu classique. Choix côté client (champ `palette`).
_PRECIP_PAL = {
    "storm": ["#9be7ff", "#4aa8ff", "#1f6fff", "#00e400", "#7fdb00",
              "#ffd400", "#ff9000", "#ff2a00", "#c2005a"],
    "blue":  ["#eef6ff", "#c6dbef", "#9ecae1", "#4292c6", "#2171b5", "#08519c", "#08306b"],
}

_GFS_VARS = {
    "temp":   {"band": "temperature_2m_above_ground", "unit": "°C", "mask": None,
               "vis": {"min": -30, "max": 45,
                       "palette": ["#08306b", "#2171b5", "#6baed6", "#c6dbef", "#ffffcc",
                                   "#fed976", "#fd8d3c", "#e31a1c", "#800026"]},
               "legend": "Température de l'air à 2 m (°C)"},
    "precip": {"band": "total_precipitation_surface", "unit": "mm", "mask": 0.2,
               "min": 0.2, "max": 25, "palettes": _PRECIP_PAL,
               "legend": "Précipitations prévues (mm)"},
    "wind":   {"band": None, "unit": "m/s", "mask": None,
               "vis": {"min": 0, "max": 25,
                       "palette": ["#edf8fb", "#b2e2e2", "#66c2a4", "#2ca25f", "#dfc27d",
                                   "#bf812d", "#8c510a"]},
               "legend": "Vitesse du vent à 10 m (m/s)"},
}

_GFS_CACHE = {}   # (variable, palette, run) → réponse


class WeatherGfsRequest(BaseModel):
    variable: str = "temp"          # "temp" | "precip" | "wind"
    palette:  str = "storm"         # précip : "storm" | "blue"


@router.post("/weather/gfs")
def gee_weather_gfs(req: WeatherGfsRequest):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    import ee, datetime as _dt, time

    var = req.variable if req.variable in _GFS_VARS else "temp"
    meta = _GFS_VARS[var]
    pal = req.palette if req.palette in _PRECIP_PAL else "storm"
    out_vis = ({"min": meta["min"], "max": meta["max"], "palette": meta["palettes"][pal]}
               if var == "precip" else meta["vis"])

    # Dernier cycle GFS, de façon ROBUSTE aux unités de `creation_time` (dont je ne
    # présume rien) :
    #   • fenêtre autour de maintenant via `filterDate` (system:time_start, en ms,
    #     fiable) — capte le dernier run quelle que soit la sémantique du champ ;
    #   • run = max(creation_time) : simple comparaison de nombres, sans hypothèse ;
    #   • instant de validité déduit de system:time_start (ms) d'UNE échéance, puis
    #     par arithmétique — robuste que system:time_start soit l'échéance ou le run.
    now = _dt.datetime.utcnow()
    col = (ee.ImageCollection("NOAA/GFS0P25")
           .filterDate((now - _dt.timedelta(days=1)).strftime("%Y-%m-%d"),
                       (now + _dt.timedelta(days=4)).strftime("%Y-%m-%d")))
    try:
        run = ee.Number(col.aggregate_max("creation_time")).getInfo()
    except Exception as ex:
        raise HTTPException(502, f"GFS indisponible : {ex}")
    if run is None:
        raise HTTPException(502, "Aucun cycle GFS récent disponible.")

    cache_key = (var, pal, int(run))
    if cache_key in _GFS_CACHE:
        return _GFS_CACHE[cache_key]

    run_col = col.filter(ee.Filter.eq("creation_time", run))
    try:
        avail = run_col.aggregate_array("forecast_hours").getInfo() or []
    except Exception as ex:
        raise HTTPException(502, f"GFS : échéances illisibles ({ex}).")
    avail = sorted({int(h) for h in avail if h is not None and int(h) <= 48})
    hours = [h for h in _GFS_STEPS if h in avail] or avail[:9]
    if not hours:
        raise HTTPException(502, "Aucune échéance GFS ≤ 48 h dans le dernier cycle.")

    # Ancre temporelle FIABLE (ms) via system:time_start d'une échéance → on en
    # déduit l'instant du cycle, puis chaque échéance par arithmétique.
    try:
        h0 = hours[0]
        t0_ms = ee.Image(run_col.filter(ee.Filter.eq("forecast_hours", h0)).first()) \
            .get("system:time_start").getInfo()
        anchor = int(round(t0_ms / 1000)) - h0 * 3600
    except Exception:
        anchor = int(time.time())
    run_unix = anchor
    run_iso = _dt.datetime.utcfromtimestamp(anchor).strftime("%Y-%m-%dT%H:%M")

    def _tile(image, vis):
        mid = image.getMapId(vis); f = mid.get("tile_fetcher")
        return f.url_format if (f and hasattr(f, "url_format")) else mid.get("urlFormat", "")

    steps = []
    for h in hours:
        try:
            img = ee.Image(run_col.filter(ee.Filter.eq("forecast_hours", h)).first())
            if var == "wind":
                band = img.select("u_component_of_wind_10m_above_ground").hypot(
                       img.select("v_component_of_wind_10m_above_ground")).rename("wind")
            elif var == "precip":
                band = img.select(meta["band"])
                band = band.updateMask(band.gt(meta["mask"]))   # transparent hors précipitation
            else:
                band = img.select(meta["band"])
            steps.append({"hours": h, "valid_unix": anchor + h * 3600,
                          "tile_url": _tile(band, out_vis)})
        except Exception as ex:
            print(f"[weather/gfs] échéance +{h} h ignorée : {ex}")

    if not steps:
        raise HTTPException(502, "Aucune échéance GFS exploitable.")

    out = {"variable": var, "unit": meta["unit"], "run_iso": run_iso, "run_unix": run_unix,
           "vis_params": out_vis, "legend": meta["legend"], "steps": steps}
    _GFS_CACHE[cache_key] = out
    return out


class WeatherPointRequest(BaseModel):
    lon: float
    lat: float
    variable: str = "temp"          # "temp" | "precip" | "wind"
    hours: int = 0                  # échéance (h) — celle affichée sur la frise


@router.post("/weather/point")
def gee_weather_point(req: WeatherPointRequest):
    """Valeur du modèle GFS au point cliqué (« touchez pour inspecter »), pour
    la variable et l'échéance courantes. Radar/IR n'exposent pas de valeur ponctuelle."""
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    import ee, datetime as _dt

    var = req.variable if req.variable in _GFS_VARS else "temp"
    meta = _GFS_VARS[var]

    now = _dt.datetime.utcnow()
    col = (ee.ImageCollection("NOAA/GFS0P25")
           .filterDate((now - _dt.timedelta(days=1)).strftime("%Y-%m-%d"),
                       (now + _dt.timedelta(days=4)).strftime("%Y-%m-%d")))
    try:
        run = ee.Number(col.aggregate_max("creation_time")).getInfo()
    except Exception as ex:
        raise HTTPException(502, f"GFS indisponible : {ex}")
    if run is None:
        raise HTTPException(502, "Aucun cycle GFS récent disponible.")
    run_col = col.filter(ee.Filter.eq("creation_time", run))

    h = int(req.hours or 0)
    try:
        img = ee.Image(run_col.filter(ee.Filter.eq("forecast_hours", h)).first())
        if var == "wind":
            band = img.select("u_component_of_wind_10m_above_ground").hypot(
                   img.select("v_component_of_wind_10m_above_ground"))
        else:
            band = img.select(meta["band"])
        pt = ee.Geometry.Point([float(req.lon), float(req.lat)])
        val = band.reduceRegion(reducer=ee.Reducer.first(), geometry=pt,
                                scale=27830, bestEffort=True).values().get(0).getInfo()
    except Exception as ex:
        raise HTTPException(502, f"Valeur GFS indisponible : {ex}")

    return {"variable": var, "unit": meta["unit"], "hours": h,
            "value": (round(float(val), 1) if val is not None else None)}


# ══════════════════════════════════════════════════════════════════════════════
#  LOT H — BASSINS VERSANTS (HydroSHEDS)
# ══════════════════════════════════════════════════════════════════════════════
# Délimitation d'un bassin versant à partir d'un exutoire cliqué :
#   1. le sous-bassin HydroBASINS (niveau Pfafstetter) contenant le point ;
#   2. remontée de TOUT l'amont par la topologie d'écoulement `NEXT_DOWN` ;
#   3. dissolution → le bassin qui draine réellement vers l'exutoire.
# Assets HydroSHEDS (à revérifier au déploiement, GEE non testable en local) :
#   WWF/HydroSHEDS/v1/Basins/hybas_{lvl}      basins Pfafstetter niveaux 1..12
#   WWF/HydroSHEDS/v1/FreeFlowingRivers       réseau hydrographique
# Attributs hors GEE traités « au mieux » (proxies documentés, choix utilisateur) :
#   nappe → GLDAS-CLSM GWS_tavg (~28 km) ; pérennité → ordre de rivière ;
#   unité aquifère → reste indisponible (GLiM/GLHYMPS absents de GEE).

_WORLDCOVER_CLASSES = {
    10: "Forêt", 20: "Arbustes", 30: "Prairie", 40: "Cultures", 50: "Bâti",
    60: "Sol nu / clairsemé", 70: "Neige / glace", 80: "Eau", 90: "Zone humide",
    95: "Mangrove", 100: "Mousses / lichens",
}


class WatershedRequest(BaseModel):
    lat: float
    lon: float
    level:            Optional[int]  = 12    # niveau HydroBASINS (7..12 ; 12 = plus fin)
    include_upstream: Optional[bool] = True  # False → seulement le sous-bassin local


@router.post("/watershed")
def gee_watershed(req: WatershedRequest):
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    import ee

    lvl = max(7, min(int(req.level or 12), 12))
    pt = ee.Geometry.Point([req.lon, req.lat])

    try:
        basins = ee.FeatureCollection(f"WWF/HydroSHEDS/v1/Basins/hybas_{lvl}")
        seed = basins.filterBounds(pt).first()
        seed_info = seed.getInfo()
    except Exception as e:
        raise HTTPException(502, f"Couche HydroBASINS indisponible : {e}")
    if not seed_info:
        raise HTTPException(422,
            "Aucun bassin versant sous ce point : il est probablement en mer ou "
            "hors de la couverture continentale HydroSHEDS. Cliquez sur une terre émergée.")

    sp = seed_info.get("properties", {})
    seed_id = sp.get("HYBAS_ID"); main_bas = sp.get("MAIN_BAS")
    sub = basins.filter(ee.Filter.eq("MAIN_BAS", main_bas))
    notes, unavailable = [], []
    upstream_count = 1

    # ── 1. Emprise du bassin : remontée d'amont par PARCOURS DE GRAPHE déterministe.
    # Topologie (HYBAS_ID, NEXT_DOWN) de tout le réseau MAIN_BAS en deux listes,
    # puis BFS Python. Garde-fou de CONTINUITÉ (composante contenant l'exutoire,
    # à défaut la plus grande) — un bassin versant est d'un seul tenant.
    if req.include_upstream:
        try:
            hyb = sub.aggregate_array("HYBAS_ID").getInfo()
            nxt = sub.aggregate_array("NEXT_DOWN").getInfo()
            children = {}                       # aval → [sous-bassins qui s'y déversent]
            for h, n in zip(hyb, nxt):
                children.setdefault(n, []).append(h)
            upstream, frontier = {seed_id}, [seed_id]
            while frontier:                     # BFS strictement en remontant
                nf = []
                for hid in frontier:
                    for c in children.get(hid, []):
                        if c not in upstream:
                            upstream.add(c); nf.append(c)
                frontier = nf
            up_ids = list(upstream)
            ws_fc = sub.filter(ee.Filter.inList("HYBAS_ID", up_ids))
            upstream_count = len(up_ids)
        except Exception as e:
            print(f"[watershed] trace amont échouée ({e}) → sous-bassin local")
            notes.append("Remontée de l'amont impossible : seul le sous-bassin local est montré.")
            ws_fc = ee.FeatureCollection([seed]); upstream_count = 1
    else:
        ws_fc = ee.FeatureCollection([seed])

    try:
        raw = ws_fc.geometry().dissolve(maxError=90)
        try:
            comps = ee.FeatureCollection(
                ee.List(raw.geometries()).map(lambda g: ee.Feature(ee.Geometry(g))))
            near = comps.filterBounds(pt.buffer(1500))
            largest = (comps.map(lambda f: f.set("__a", f.geometry().area(maxError=200)))
                            .sort("__a", False).first().geometry())
            geom = ee.Geometry(ee.Algorithms.If(near.size().gt(0),
                                                near.geometry().dissolve(maxError=90), largest))
        except Exception:
            geom = raw
        area_km2 = round(float(ee.Number(geom.area(maxError=90)).divide(1e6).getInfo()), 2)
        perim_km = round(float(ee.Number(geom.perimeter(maxError=90)).divide(1e3).getInfo()), 2)
        boundary = geom.simplify(maxError=120).getInfo()
    except Exception as e:
        raise HTTPException(500, f"Assemblage du bassin impossible : {e}")

    attrs = {"surface_km2": area_km2, "perimetre_km": perim_km,
             "sous_bassins": upstream_count, "niveau_hydrobasins": lvl}

    # ── 2. Réseau hydrographique + proxy de pérennité (ordre de rivière) ──
    rivers_gj = None
    try:
        riv = ee.FeatureCollection("WWF/HydroSHEDS/v1/FreeFlowingRivers").filterBounds(geom)
        total_km = ee.Number(riv.geometry().length(maxError=90)).divide(1e3)
        attrs["reseau_km"] = round(float(total_km.getInfo()), 1)
        try:
            omax = riv.aggregate_min("RIV_ORD")   # RIV_ORD : 1 = grand fleuve … 10 = ruisseau
            attrs["ordre_min"] = int(omax.getInfo())   # plus l'ordre est bas, plus le cours est majeur
        except Exception:
            pass
        # Proxy pérennité : cours majeurs (RIV_ORD ≤ 6, débit fort) ~ permanents ;
        # ordres élevés (petits cours) ~ souvent temporaires. Approximation grossière.
        try:
            perm = riv.filter(ee.Filter.lte("RIV_ORD", 6))
            temp = riv.filter(ee.Filter.gt("RIV_ORD", 6))
            attrs["reseau_permanent_km"] = round(float(ee.Number(perm.geometry().length(maxError=90)).divide(1e3).getInfo()), 1)
            attrs["reseau_temporaire_km"] = round(float(ee.Number(temp.geometry().length(maxError=90)).divide(1e3).getInfo()), 1)
            notes.append("Permanent / temporaire = PROXY par ordre de rivière (RIV_ORD ≤ 6 supposé "
                         "permanent), non une observation de pérennité.")
        except Exception:
            unavailable.append("Cours d'eau temporaires / permanents (pas de couche de pérennité dans GEE)")
        # Payload plafonné (getInfo limite à ~5000 entités) MAIS trié par ordre de
        # rivière d'abord : on garde les cours MAJEURS de tout le bassin plutôt
        # qu'un cluster de petits affluents d'une seule zone dense. Sans ce tri,
        # un grand bassin très ramifié au sud tronquait le réseau au nord.
        # Géométries simplifiées + une seule propriété → payload raisonnable
        # (ee.Feature n'a pas de .simplify() → on reconstruit chaque Feature).
        def _slim(f):
            return ee.Feature(f.geometry().simplify(120), {"RIV_ORD": f.get("RIV_ORD")})
        try:
            rivers_gj = riv.sort("RIV_ORD").limit(4500).map(_slim).getInfo()
        except Exception:
            rivers_gj = riv.limit(4500).map(_slim).getInfo()   # RIV_ORD absent → tri ignoré
    except Exception as e:
        print(f"[watershed] réseau hydro indisponible : {e}")
        notes.append("Réseau hydrographique HydroRIVERS indisponible sur ce bassin.")

    # Étape 1 finie : géométrie + réseau. L'échantillonnage des indicateurs
    # (relief, sol, climat, nappe) est déporté sur /watershed/attributes pour
    # que le front affiche le bassin immédiatement, puis complète le tableau.
    return {
        "outlet": {"lat": req.lat, "lon": req.lon},
        "boundary": boundary,
        "rivers": rivers_gj,
        "attributes": attrs,
        "unavailable": unavailable,
        "notes": notes,
    }


class WatershedAttrsRequest(BaseModel):
    boundary: dict   # géométrie du bassin (Polygon / MultiPolygon) renvoyée par /watershed


@router.post("/watershed/attributes")
def gee_watershed_attributes(req: WatershedAttrsRequest):
    """Étape 2 : échantillonne les indicateurs thématiques sur un bassin déjà
    délimité. Chaque source est indépendante (try/except) → une couche qui tombe
    ne coule pas le tableau, ce qui compte d'autant plus que GEE n'est pas
    testable en local."""
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    import ee
    try:
        geom = geojson_to_ee_geometry(req.boundary)
    except Exception as e:
        raise HTTPException(422, f"Contour de bassin invalide : {e}")

    attrs, notes, unavailable = {}, [], []

    def _rr(image, reducer, scale=1000):   # échelle grossière + bestEffort → pas de timeout
        return image.reduceRegion(reducer=reducer, geometry=geom, scale=scale,
                                  maxPixels=1e10, bestEffort=True).getInfo() or {}

    try:  # Relief (SRTM)
        dem = ee.Image("USGS/SRTMGL1_003")
        st = _rr(dem, ee.Reducer.min().combine(ee.Reducer.max(), sharedInputs=True)
                        .combine(ee.Reducer.mean(), sharedInputs=True), scale=200)
        slope = _rr(ee.Terrain.slope(dem), ee.Reducer.mean(), scale=200)
        attrs["altitude_min"] = round(float(st.get("elevation_min", 0)), 0)
        attrs["altitude_max"] = round(float(st.get("elevation_max", 0)), 0)
        attrs["altitude_moy"] = round(float(st.get("elevation_mean", 0)), 0)
        attrs["pente_moy_deg"] = round(float(slope.get("slope", 0)), 1)
    except Exception as e:
        print(f"[watershed] relief : {e}")

    try:  # Sol (OpenLandMap)
        clay = _rr(ee.Image("OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02").select("b0"), ee.Reducer.mean(), 250)
        soc  = _rr(ee.Image("OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02").select("b0"), ee.Reducer.mean(), 250)
        ph   = _rr(ee.Image("OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02").select("b0"), ee.Reducer.mean(), 250)
        attrs["sol"] = {
            "argile_pct":  round(float(clay.get("b0", 0)), 1),
            "carbone_gkg": round(float(soc.get("b0", 0)) * 0.2, 1),   # 5×g/kg → g/kg
            "ph":          round(float(ph.get("b0", 0)) * 0.1, 1),    # pH×10
        }
    except Exception as e:
        print(f"[watershed] sol : {e}")

    try:  # Précipitation annuelle (CHIRPS, année de référence récente)
        chirps = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").filterDate("2023-01-01", "2024-01-01").sum()
        pr = _rr(chirps, ee.Reducer.mean(), 5000)
        attrs["precip_mm_an"] = round(float(pr.get("precipitation", 0)), 0)
    except Exception as e:
        print(f"[watershed] précip : {e}")

    try:  # Occupation du sol dominante (ESA WorldCover)
        wc = ee.ImageCollection("ESA/WorldCover/v200").first().select("Map")
        mode = _rr(wc, ee.Reducer.mode(), 100)
        code = int(mode.get("Map", 0) or 0)
        attrs["occupation_dominante"] = _WORLDCOVER_CLASSES.get(code, f"classe {code}")
    except Exception as e:
        print(f"[watershed] occupation du sol : {e}")

    try:  # Nappe phréatique — PROXY GLDAS-CLSM (stockage souterrain, ~28 km)
        gws = ee.ImageCollection("NASA/GLDAS/V022/CLSM/G025/DA1D") \
            .filterDate("2022-01-01", "2023-01-01").select("GWS_tavg").mean()
        gw = _rr(gws, ee.Reducer.mean(), 25000)
        val = gw.get("GWS_tavg")
        if val is not None:
            attrs["nappe_proxy_mm"] = round(float(val), 1)
            notes.append("Nappe = PROXY : stockage d'eau souterraine GLDAS-CLSM moyen (~28 km), "
                         "non une profondeur de nappe mesurée.")
        else:
            unavailable.append("Nappe phréatique (proxy GLDAS indisponible sur ce bassin)")
    except Exception as e:
        print(f"[watershed] nappe : {e}")
        unavailable.append("Nappe phréatique (proxy GLDAS indisponible)")

    # Aquifère : aucune couche lithologique/hydrogéologique dans GEE public
    unavailable.append("Unité aquifère (GLiM / GLHYMPS absents du catalogue GEE)")

    return {"attributes": attrs, "notes": notes, "unavailable": unavailable}


# ══════════════════════════════════════════════════════════════════════════════
#  CATALOGUE VECTORIEL — charge une FeatureCollection GEE en GeoJSON
# ══════════════════════════════════════════════════════════════════════════════
class VectorRequest(BaseModel):
    asset:      str
    bbox:       Optional[List[float]] = None   # [w, s, e, n] — découpe sur l'emprise
    limit:      Optional[int]  = 2000          # plafond d'entités (getInfo ~5000 max)
    simplify_m: Optional[float] = None         # tolérance de simplification (m), lignes/polygones


@router.post("/vector")
def gee_vector(req: VectorRequest):
    """Charge un jeu vectoriel du catalogue Earth Engine (FeatureCollection),
    découpé sur l'emprise carte et plafonné en nombre d'entités — sinon un jeu
    mondial (routes, districts…) dépasserait tout budget de téléchargement."""
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    import ee
    try:
        fc = ee.FeatureCollection(req.asset)
    except Exception as e:
        raise HTTPException(502, f"Jeu vectoriel indisponible : {e}")

    if req.bbox:
        w, s, e, n = req.bbox
        fc = fc.filterBounds(ee.Geometry.BBox(w, s, e, n))
    lim = max(1, min(int(req.limit or 2000), 5000))
    fc = fc.limit(lim)
    if req.simplify_m:
        tol = float(req.simplify_m)
        fc = fc.map(lambda f: ee.Feature(f.geometry().simplify(tol), f.toDictionary()))

    try:
        gj = fc.getInfo()
    except Exception as e:
        raise HTTPException(422,
            f"Chargement impossible ({e}). Réduisez l'emprise (zoomez) ou la limite d'entités.")
    feats = gj.get("features", []) if isinstance(gj, dict) else []
    return {"geojson": {"type": "FeatureCollection", "features": feats},
            "count": len(feats), "truncated": len(feats) >= lim}


# Proxy pour les flux open-data SANS CORS (ex. GDACS). Liste blanche d'hôtes
# stricte → pas de SSRF : on ne relaie que des sources publiques identifiées.
_PROXY_ALLOW = {"www.gdacs.org", "gdacs.org"}


@router.get("/vector/proxy")
def gee_vector_proxy(url: str):
    """Relaie un GeoJSON public d'un hôte autorisé (contourne l'absence de CORS)."""
    from urllib.parse import urlparse
    u = urlparse(url)
    if u.scheme != "https" or u.hostname not in _PROXY_ALLOW:
        raise HTTPException(400, f"Hôte non autorisé : {u.hostname}")
    try:
        import requests as _req
        r = _req.get(url, timeout=30, headers={"User-Agent": "OpenMapAgents/1.0"})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(502, f"Flux indisponible : {e}")


class TimelapseExportRequest(TimelapseRequest):
    fps:       Optional[int] = 3
    width:     Optional[int] = 720     # largeur des vignettes GEE
    title:     Optional[str] = None
    credits:   Optional[str] = "OpenMapAgents · GEE"
    fmt:       Optional[str] = "gif"   # "gif" | "mp4" (mp4 nécessite imageio-ffmpeg)


@router.post("/timelapse/export")
def gee_timelapse_export(req: TimelapseExportRequest):
    """Assemble l'animation en FICHIER, côté serveur.

    L'enregistrement du canvas dans le navigateur (MediaRecorder) produit un
    conteneur fragmenté sans durée : beaucoup de lecteurs n'affichent alors que
    la première image. On reprend donc la méthode éprouvée du module GIF :
    une vignette PNG par date, annotée avec Pillow, assemblée par imageio.

    Différence avec l'aperçu sur carte : pas de fond de plan, uniquement la
    couche GEE — c'est aussi le cas de /api/gee/timelapse.
    """
    if req.dataset in _TL_STATIC:
        raise HTTPException(422, "Jeu de données statique : rien à animer.")
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")

    try:
        import io as _io, os as _os, uuid as _uuid, tempfile as _tf
        from PIL import Image as PILImage
        import imageio
        import requests as _req
        from gee_timelapse import annotate_frame   # même habillage que le module GIF
    except ImportError as ie:
        raise HTTPException(500, f"Dépendance manquante : {ie} — pip install Pillow imageio requests")

    periods = _tl_resolve_periods(req)

    # Échelle de couleurs commune, calculée une seule fois (cf. /timelapse/plan)
    vis = req.vis_params_override
    if not vis:
        ref = gee_tiles(TileRequest(
            dataset=req.dataset, index=req.index,
            date_start=periods[0]["date_start"], date_end=periods[-1]["date_end"],
            bbox=req.bbox, cloud_max=req.cloud_max, composite="median",
            roi_geojson=req.roi_geojson, auto_stretch=True,
        ))
        vis = ref.get("vis_params") or {}

    px = max(240, min(int(req.width or 720), 1600))

    def one(p):
        """Vignette PNG d'une période (None si aucune image)."""
        try:
            d = gee_tiles(_tl_child(req, p, vis, thumb_px=px))
            r = _req.get(d["thumb_url"], timeout=90)
            r.raise_for_status()
            return p["label"], r.content
        except Exception as e:
            print(f"[timelapse/export] {p.get('label')} ignorée : {e}")
            return p["label"], None

    # Les vignettes se téléchargent en parallèle (I/O), l'assemblage reste ordonné.
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as ex:
        raw = list(ex.map(one, periods))

    frames, labels = [], []
    for label, content in raw:
        if not content:
            continue                      # période sans image → sautée
        try:
            img = PILImage.open(_io.BytesIO(content)).convert("RGB")
        except Exception:
            continue
        frames.append(annotate_frame(
            img=img, label=label, frame_idx=len(frames), total_frames=len(periods),
            src_label=DATASETS.get(req.dataset, {}).get("label", req.dataset),
            ann_title=req.title or f"{req.index} de {periods[0]['label']} à {periods[-1]['label']}",
            ann_desc=None, ann_credits=req.credits,
            show_north=True, show_scale=True, show_progress=True, show_legend=True,
            bbox=req.bbox, vis_params=vis, index=req.index,
        ))
        labels.append(label)

    if not frames:
        raise HTTPException(404, "Aucune image disponible sur la période et la zone choisies. "
                                 "Élargissez la période ou augmentez le filtre nuages.")

    # Toutes les frames doivent partager la même taille, sinon l'encodage échoue.
    w0, h0 = frames[0].size
    frames = [f if f.size == (w0, h0) else f.resize((w0, h0)) for f in frames]

    tmp = _os.path.join(_tf.gettempdir(), "ome_timelapse")
    _os.makedirs(tmp, exist_ok=True)
    fps  = max(1, min(int(req.fps or 3), 15))
    name = f"anim_{req.dataset}_{req.index}_{_uuid.uuid4().hex[:8]}".replace(" ", "_")

    fmt = (req.fmt or "gif").lower()
    if fmt == "mp4":
        try:
            path = _os.path.join(tmp, name + ".mp4")
            # macro_block_size=1 : évite le redimensionnement silencieux de ffmpeg
            imageio.mimsave(path, frames, format="FFMPEG", fps=fps, macro_block_size=1)
        except Exception as e:
            print(f"[timelapse/export] MP4 indisponible ({e}) → repli GIF")
            fmt = "gif"
    if fmt != "mp4":
        path = _os.path.join(tmp, name + ".gif")
        imageio.mimsave(path, frames, format="GIF",
                        duration=max(60, int(1000 / fps)), loop=0)

    fname = _os.path.basename(path)
    return {
        "url":     f"/api/gee/timelapse/export/file/{fname}",
        "frames":  len(frames),
        "skipped": len(periods) - len(frames),
        "period":  f"{labels[0]} → {labels[-1]}" if labels else "",
        "size_mb": round(_os.path.getsize(path) / (1024 * 1024), 2),
        "format":  "mp4" if fname.endswith(".mp4") else "gif",
    }


# Chemin distinct de /timelapse/file/{filename} du module GIF : même préfixe et
# même dossier temporaire, deux routes identiques se masqueraient l'une l'autre.
@router.get("/timelapse/export/file/{filename}")
def gee_timelapse_export_file(filename: str):
    """Sert un fichier d'animation généré. Nom vérifié : pas de remontée de chemin."""
    import os as _os, re as _re, tempfile as _tf
    from fastapi.responses import FileResponse
    if not _re.fullmatch(r"[A-Za-z0-9_.\-]+\.(gif|mp4)", filename):
        raise HTTPException(400, "Nom de fichier invalide")
    path = _os.path.join(_os.path.join(_tf.gettempdir(), "ome_timelapse"), filename)
    if not _os.path.isfile(path):
        raise HTTPException(404, "Fichier expiré ou introuvable")
    media = "video/mp4" if filename.endswith(".mp4") else "image/gif"
    return FileResponse(path, media_type=media, filename=filename)


def compute_tiles_internal(
    dataset: str,
    index: str,
    xmin: float, ymin: float,
    xmax: float, ymax: float,
    date_start: str = "2020-01-01",
    date_end: str = "2024-12-31",
    cloud_cover: float = 20.0,
    composite: str = "median",
    vis_params_override: dict = None,
) -> dict:
    """Wrapper direct pour mcp_gee.py — appelle gee_tiles() sans HTTP."""
    req = TileRequest(
        dataset=dataset,
        index=index,
        date_start=date_start,
        date_end=date_end,
        bbox=[xmin, ymin, xmax, ymax],
        cloud_max=cloud_cover,
        composite=composite,
        vis_params_override=vis_params_override,
    )
    result = gee_tiles(req)
    if not result.get("bbox") and result.get("clip_bbox"):
        result["bbox"] = result["clip_bbox"]
    elif not result.get("bbox"):
        result["bbox"] = [xmin, ymin, xmax, ymax]
    return result

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
    baseline_start: Optional[str]      = None   # anomalie : période de référence
    baseline_end:   Optional[str]      = None
    agg:         Optional[str]         = "monthly"  # série temporelle : "monthly" | "yearly"


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

def _stats_raw_image(ee, dataset, ds, date_start, date_end, region):
    """Image brute (avant compute_index) pour les stats des nouveaux rasters."""
    STATIC = {
        "soil_soc":  "OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02",
        "soil_ph":   "OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02",
        "soil_clay": "OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02",
        "etopo":     "NOAA/NGDC/ETOPO1",
        "gedi_agb":  "LARSE/GEDI/GEDI04_B_002",
    }
    if dataset in STATIC:
        return ee.Image(STATIC[dataset])
    col = ee.ImageCollection(ds["collection"]).filterDate(date_start, date_end).filterBounds(region)
    if col.size().getInfo() == 0:   # fenêtre vide → dernières images publiées
        col = ee.ImageCollection(ds["collection"]).filterBounds(region).sort("system:time_start", False).limit(8)
    if dataset == "burned":       return col.select("BurnDate").max()
    if dataset == "firms":        return col.select("T21").max()
    if dataset == "modis_snow":   return col.select("NDSI_Snow_Cover").mean()
    if dataset in ("era5_solar", "era5_wind", "gpm",
                   "modis_ocean", "viirs_ocean", "oisst", "hycom_vel", "hycom_sal", "era5_ocean_wind",
                   "gedi_l2a", "gedi_l2b", "gedi_l4a"):
        return col.mean()
    return col.median()


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
    _check_date_availability(dataset, date_start, date_end)

    # ── Landsat « auto » : fusion des missions disponibles sur la période ──
    if dataset == "landsat":
        col = _landsat_merged(ee, date_start, date_end, region, cloud_max)
        if col.size().getInfo() == 0:
            # NE PAS retomber sur des images nuageuses : erreur claire.
            n_all = _landsat_merged(ee, date_start, date_end, region, 100).size().getInfo()
            if n_all == 0:
                raise ValueError(
                    f"Aucune image Landsat entre {date_start} et {date_end} sur cette zone. "
                    f"Élargissez la période ou la zone.")
            raise ValueError(
                f"Aucune image Landsat avec ≤ {int(cloud_max)}% de nuages entre {date_start} et "
                f"{date_end} sur cette zone ({n_all} image(s) disponible(s) mais plus nuageuse(s)). "
                f"Augmentez le seuil de nuages, élargissez la période ou choisissez une autre zone.")
        col = _mask_clouds(ee, "landsat", col)   # masque nuages par pixel
        if composite == "median":   img = col.median()
        elif composite == "mosaic": img = col.mosaic()
        else:                       img = col.sort("CLOUD_COVER").first()
        return img, 30

    COLLS = {
        "sentinel2":  ("COPERNICUS/S2_SR_HARMONIZED",   10,    "CLOUDY_PIXEL_PERCENTAGE"),
        "landsat9":   ("LANDSAT/LC09/C02/T1_L2",        30,    "CLOUD_COVER"),
        "landsat8":   ("LANDSAT/LC08/C02/T1_L2",        30,    "CLOUD_COVER"),
        "modis_ndvi": ("MODIS/061/MOD13A1",              500,   None),
        "modis_lst":  ("MODIS/061/MOD11A1",              1000,  None),
        "era5":       ("ECMWF/ERA5_LAND/MONTHLY_AGGR",  11000, None),
    }
    cid, scale, cp = COLLS[dataset]
    sensor = {"sentinel2": "Sentinel-2", "sentinel1": "Sentinel-1",
              "landsat8": "Landsat 8", "landsat9": "Landsat 9",
              "modis_ndvi": "MODIS", "modis_lst": "MODIS",
              "era5": "ERA5"}.get(dataset, dataset)
    base = ee.ImageCollection(cid).filterDate(date_start, date_end).filterBounds(region)
    col = base
    if cp and cloud_max is not None and cloud_max < 100:
        col = base.filter(ee.Filter.And(ee.Filter.notNull([cp]), ee.Filter.lte(cp, cloud_max)))
        # Le filtre nuages a tout retiré → NE PAS générer une carte nuageuse :
        # on renvoie une erreur claire (respect strict du seuil demandé).
        if col.size().getInfo() == 0:
            n_all = base.size().getInfo()
            if n_all == 0:
                raise ValueError(
                    f"Aucune image {sensor} entre {date_start} et {date_end} sur cette zone. "
                    f"Élargissez la période ou la zone.")
            raise ValueError(
                f"Aucune image {sensor} avec ≤ {int(cloud_max)}% de nuages entre {date_start} et "
                f"{date_end} sur cette zone ({n_all} image(s) disponible(s) mais plus nuageuse(s)). "
                f"Augmentez le seuil de nuages, élargissez la période ou choisissez une autre zone.")
    # Aucune image du tout (sans filtre nuages) → erreur claire.
    if col.size().getInfo() == 0:
        raise ValueError(
            f"Images {sensor} non disponibles pour l'analyse entre {date_start} et "
            f"{date_end} sur cette zone (couverture incomplète). "
            f"Essayez une période plus large ou une autre zone.")
    col = _mask_clouds(ee, dataset, col)   # masque nuages par pixel (optique)
    if composite == "median":   img = col.median()
    elif composite == "mosaic": img = col.mosaic()
    else: img = col.sort(cp or "system:time_start", not bool(cp)).first()
    return img, scale

def _compute_band(ee, img, dataset, index):
    if index == "NDVI":
        if dataset == "sentinel2":              return img.normalizedDifference(["B8","B4"]).rename("NDVI"), "NDVI"
        if dataset in ("landsat8","landsat9"):  return img.normalizedDifference(["SR_B5","SR_B4"]).rename("NDVI"), "NDVI"
        if dataset == "landsat":                return img.normalizedDifference(["NIR","RED"]).rename("NDVI"), "NDVI"
        if dataset == "modis_ndvi":             return img.select("NDVI").multiply(0.0001), "NDVI"
    if index == "EVI":
        if dataset == "sentinel2":
            nir=img.select("B8").multiply(0.0001); red=img.select("B4").multiply(0.0001); blue=img.select("B2").multiply(0.0001)
            return nir.subtract(red).multiply(2.5).divide(nir.add(red.multiply(6)).subtract(blue.multiply(7.5)).add(1)).rename("EVI"), "EVI"
        if dataset == "modis_ndvi": return img.select("EVI").multiply(0.0001), "EVI"
    if index == "NDWI":
        if dataset == "sentinel2":             return img.normalizedDifference(["B3","B8"]).rename("NDWI"), "NDWI"
        if dataset in ("landsat8","landsat9"): return img.normalizedDifference(["SR_B3","SR_B5"]).rename("NDWI"), "NDWI"
        if dataset == "landsat":               return img.normalizedDifference(["GREEN","NIR"]).rename("NDWI"), "NDWI"
    if index == "NDBI":
        if dataset == "sentinel2": return img.normalizedDifference(["B11","B8"]).rename("NDBI"), "NDBI"
        if dataset == "landsat":   return img.normalizedDifference(["SWIR1","NIR"]).rename("NDBI"), "NDBI"
    if index in ("LST Jour","LST (température)"):
        if dataset in ("landsat8","landsat9"): return img.select("ST_B10").multiply(0.00341802).add(149.0).subtract(273.15).rename("LST"), "LST"
        if dataset == "landsat":               return img.select("THERMAL").multiply(0.00341802).add(149.0).subtract(273.15).rename("LST"), "LST"
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

        # ── SRTM / Copernicus DEM (relief) ───────────────────
        if req.dataset in ("srtm", "copdem"):
            scale = req.scale or 30
            if req.dataset == "copdem":
                srtm = ee.ImageCollection("COPERNICUS/DEM/GLO30").select("DEM").mosaic().rename("elevation")
            else:
                srtm = ee.Image("USGS/SRTMGL1_003")

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

        # ── Population maillée (GPW / GHSL) : densité hab/km² ──
        if req.dataset in ("gpw_pop", "ghsl_pop"):
            scale = req.scale or (1000 if req.dataset == "gpw_pop" else 100)
            if req.dataset == "gpw_pop":
                img = (ee.ImageCollection("CIESIN/GPWv411/GPW_Population_Density")
                       .sort("system:time_start", False).first()
                       .select("population_density").rename("pop_density"))
            else:
                # GHSL temporel : époque sélectionnée (repli 2020 si la fenêtre est vide)
                _gc = ee.ImageCollection("JRC/GHSL/P2023A/GHS_POP").filterDate(date_start, date_end)
                _gimg = ee.Image(ee.Algorithms.If(_gc.size().gt(0),
                            _gc.sort("system:time_start", False).first(),
                            ee.Image("JRC/GHSL/P2023A/GHS_POP/2020")))
                img = _gimg.select("population_count").multiply(100).rename("pop_density")
            band = "pop_density"
            mean, mn, mx, std, n = _global_stats(ee, img, band, region, scale)
            total_km2 = round(n * _pixel_km2(scale), 3)
            POP_DIST = [
                ("Inhabité (< 1)",        (0, 1),         "#ffffcc"),
                ("Rural (1–50)",          (1, 50),        "#fed976"),
                ("Périurbain (50–500)",   (50, 500),      "#fd8d3c"),
                ("Urbain (500–5000)",     (500, 5000),    "#e31a1c"),
                ("Très dense (> 5000)",   (5000, 1e9),    "#800026"),
            ]
            classes  = _build_classes(ee, img, band, POP_DIST, region, scale, n)
            surfaces = _build_classes(ee, img, band, POP_DIST, region, scale, n)
            return {"mean": round(mean, 1), "min": round(mn, 1), "max": round(mx, 1),
                    "total_km2": total_km2, "valid_pixels": n, "scale": scale,
                    "classes": classes, "surfaces": surfaces}

        # ══════════════════════════════════════════════════════
        # CAS GÉNÉRAL : collections temporelles
        # ══════════════════════════════════════════════════════
        # ── Fallback générique : nouveaux rasters continus (bâti, sols, GPP/LAI,
        #    solaire/vent/GPM/neige, bathymétrie, biomasse, feux) → stats de base
        #    (moyenne/min/max + classes quantiles auto). Réutilise compute_index.
        _GENERIC_CONT = {"ghsl_built", "soil_soc", "soil_ph", "soil_clay", "modis_gpp", "modis_lai",
                         "era5_solar", "era5_wind", "gpm", "modis_snow", "etopo", "gedi_agb", "burned", "firms",
                         "modis_ocean", "viirs_ocean", "oisst", "hycom_vel", "hycom_sal", "era5_ocean_wind",
                         "gedi_l2a", "gedi_l2b", "gedi_l4a"}
        if req.dataset in _GENERIC_CONT:
            ds = DATASETS.get(req.dataset, {})
            scale = req.scale or _STRETCH_SCALE.get(req.dataset, 500)
            raw = _stats_raw_image(ee, req.dataset, ds, date_start, date_end, region)
            img = compute_index(raw, req.dataset, req.index)
            band = img.bandNames().get(0).getInfo()
            mean, mn, mx, std, n = _global_stats(ee, img, band, region, scale)
            total_km2 = round(n * _pixel_km2(scale), 3)
            classes = []
            try:
                if mn is not None and mx is not None and mx > mn:
                    brks = _class_breaks(ee, img.select(band), region, scale, "quantile", 5, mn, mx)
                    edges = [mn] + sorted({round(float(b), 4) for b in brks if mn < b < mx}) + [mx]
                    pal = (_INDEX_VIS.get(req.index, {}) or VIS_PARAMS.get((req.dataset, req.index), {})).get("palette") \
                        or ["#ffffcc", "#a1dab4", "#41b6c4", "#2c7fb8", "#253494"]
                    pick = lambda i, k: pal[min(len(pal) - 1, int(i * len(pal) / max(k, 1)))]
                    defs = [(f"{_fmt_num(edges[i])} – {_fmt_num(edges[i + 1])}", (edges[i], edges[i + 1]), pick(i, len(edges) - 1))
                            for i in range(len(edges) - 1)]
                    classes = _build_classes(ee, img, band, defs, region, scale, n)
            except Exception as _ce:
                print(f"[stats generic] classes ignoré: {_ce}")
            return {"mean": round(float(mean), 3), "min": round(float(mn), 3), "max": round(float(mx), 3),
                    "std": round(float(std), 3), "valid_pixels": n, "total_km2": total_km2,
                    "scale": scale, "classes": classes, "surfaces": classes}

        _STATS_GENERIC = {"sentinel2", "landsat", "landsat8", "landsat9", "modis_ndvi", "modis_lst", "era5"}
        if req.dataset not in _STATS_GENERIC:
            raise HTTPException(422,
                f"Statistiques détaillées non disponibles pour « {DATASETS.get(req.dataset, {}).get('label', req.dataset)} ». "
                f"Disponibles pour : Sentinel-2, Landsat, MODIS, ERA5, WorldCover, Global Forest Watch, "
                f"SRTM/Copernicus DEM et Hauteur de canopée.")
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
    except ValueError as ve:
        raise HTTPException(422, str(ve))
    except Exception as e:
        raise HTTPException(500, f"Erreur stats index GEE : {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  LOT 3 — ANALYTIQUE : série temporelle + carte d'anomalie
# ══════════════════════════════════════════════════════════════════════════════
_STATIC_FOR_TS = {"srtm", "hansen", "worldcover", "canopy_height", "jrc_water", "copdem", "gpw_pop", "ghsl_pop",
                  "ghsl_built", "ghsl_smod", "dynamicworld", "burned", "firms", "soil_soc", "soil_ph",
                  "soil_clay", "modis_gpp", "modis_lai", "era5_solar", "era5_wind", "gpm", "modis_snow",
                  "etopo", "gedi_agb", "coral", "mangrove"}


def _ts_region(ee, req):
    if req.roi_geojson:
        try: return geojson_to_ee_geometry(req.roi_geojson)
        except Exception: pass
    if req.bbox:
        w, s, e, n = req.bbox
        return ee.Geometry.BBox(w, s, e, n)
    raise HTTPException(422, "Fournir 'bbox' ou 'roi_geojson'")


def _index_collection(ee, dataset, index, date_start, date_end, bounds, cloud_max):
    """Collection filtrée + masque nuages par pixel (avant composite)."""
    ds = DATASETS.get(dataset)
    if dataset == "landsat":
        col = _landsat_merged(ee, date_start, date_end, bounds, cloud_max)
        return _mask_clouds(ee, "landsat", col)
    if dataset == "sentinel5p":
        prod = S5P_PRODUCTS.get(index)
        if not prod:
            raise HTTPException(422, f"Produit Sentinel-5P inconnu : {index}")
        col = ee.ImageCollection(prod[0]).filterDate(date_start, date_end).select(prod[1])
        return col.filterBounds(bounds) if bounds is not None else col
    col = ee.ImageCollection(ds["collection"]).filterDate(date_start, date_end)
    if bounds is not None:
        col = col.filterBounds(bounds)
    if ds.get("cloud_property") and cloud_max is not None and cloud_max < 100:
        cp = ds["cloud_property"]
        col = col.filter(ee.Filter.And(ee.Filter.notNull([cp]), ee.Filter.lte(cp, cloud_max)))
    return _mask_clouds(ee, dataset, col)   # masque nuages par pixel (optique)


def _composite_index(ee, dataset, index, date_start, date_end, bounds, cloud_max):
    """Image mono-bande = indice composité sur la période."""
    col = _index_collection(ee, dataset, index, date_start, date_end, bounds, cloud_max)
    if dataset == "chirps":
        img = col.sum()
    elif dataset in ("sentinel5p", "modis_et", "smap", "geos_cf", "cams"):
        img = col.mean()
    else:
        img = col.median()
    return compute_index(img, dataset, index)


@router.post("/index/timeseries")
def gee_index_timeseries(req: IndexStatsRequest):
    """Série temporelle MENSUELLE d'un indice sur la zone (valeur moyenne / mois)."""
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    if req.dataset in _STATIC_FOR_TS:
        raise HTTPException(422, "Dataset statique : pas de série temporelle.")
    try:
        import ee
        import datetime as _dt
        region = _ts_region(ee, req)
        bounds = region.bounds()
        date_start = req.date_start or "2020-01-01"
        date_end   = req.date_end   or _dt.date.today().isoformat()
        try:
            _check_date_availability(req.dataset, date_start, date_end)
        except ValueError as ve:
            raise HTTPException(422, str(ve))

        d0 = _dt.date.fromisoformat(date_start); d1 = _dt.date.fromisoformat(date_end)
        # Granularité : une image composite par MOIS (défaut) ou par ANNÉE.
        yearly = (req.agg or "monthly").lower().startswith("year")
        unit   = "year" if yearly else "month"
        fmt    = "YYYY" if yearly else "YYYY-MM"
        if yearly:
            n_steps = max(1, min(d1.year - d0.year + 1, 60))       # borne à 60 ans
        else:
            n_steps = (d1.year - d0.year) * 12 + (d1.month - d0.month) + 1
            n_steps = max(1, min(n_steps, 120))                    # borne à 10 ans

        # Le masque nuages par pixel est déjà appliqué dans _index_collection ;
        # la médiane mensuelle/annuelle sur pixels clairs donne une valeur fiable.
        col = _index_collection(ee, req.dataset, req.index, date_start, date_end, bounds, None)
        # Échelle ADAPTATIVE : résolution native pour les petites zones, mais on
        # grossit la maille sur les grandes emprises pour borner le nb de pixels
        # (sinon la moyenne sur toute la vue × N périodes → timeout « Failed to fetch »).
        _NATIVE = {"sentinel2": 20, "landsat": 30, "landsat8": 30, "landsat9": 30,
                   "modis_ndvi": 500, "modis_lst": 1000, "era5": 11132, "sentinel5p": 1113,
                   "viirs": 500, "chirps": 5566, "modis_et": 500, "smap": 11000, "geos_cf": 27750, "cams": 44528}
        native = _NATIVE.get(req.dataset, 100)
        if req.scale:
            scale = req.scale
        else:
            try:
                area_m2 = region.area(maxError=100).getInfo()   # 1 appel léger
                # cible ≈ 6000 px pour la moyenne régionale → rapide et suffisant
                scale = max(native, (area_m2 / 6000.0) ** 0.5)
            except Exception:
                scale = native
        dataset, index = req.dataset, req.index
        period_comp = (lambda sub: sub.sum()) if dataset == "chirps" else (lambda sub: sub.median())

        def step(m):
            start = ee.Date(date_start).advance(ee.Number(m), unit)
            end   = start.advance(1, unit)
            sub   = col.filterDate(start, end)
            idx   = compute_index(period_comp(sub), dataset, index)
            reduced = idx.reduceRegion(reducer=ee.Reducer.mean(), geometry=region,
                                       scale=scale, maxPixels=1e9, bestEffort=True, tileScale=4)
            # Période SANS image → None (sinon compute_index sur un composite vide
            # fait échouer toute l'agrégation). If ne calcule que la branche retenue.
            val = ee.Algorithms.If(sub.size().gt(0), reduced.values().get(0), None)
            return ee.Feature(None, {"date": start.format(fmt), "value": val})

        fc = ee.FeatureCollection(ee.List.sequence(0, n_steps - 1).map(step))
        dates = fc.aggregate_array("date").getInfo()
        vals  = fc.aggregate_array("value").getInfo()
        series = [{"date": d, "value": (round(float(v), 4) if v is not None else None)}
                  for d, v in zip(dates, vals)]
        return {"dataset": req.dataset, "index": req.index, "scale": scale,
                "agg": ("yearly" if yearly else "monthly"), "series": series}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erreur série temporelle GEE : {e}")


@router.post("/index/anomaly")
def gee_index_anomaly(req: IndexStatsRequest):
    """Carte d'anomalie : indice(période) − moyenne(période de référence). → tuiles."""
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    if req.dataset in _STATIC_FOR_TS:
        raise HTTPException(422, "Dataset statique : pas d'anomalie.")
    try:
        import ee
        import datetime as _dt
        region = _ts_region(ee, req)
        bounds = region.bounds()
        date_start = req.date_start or "2024-01-01"
        date_end   = req.date_end   or _dt.date.today().isoformat()
        d0 = _dt.date.fromisoformat(date_start)
        base_start = req.baseline_start or f"{d0.year - 5}-{date_start[5:10]}"   # 5 ans avant
        base_end   = req.baseline_end   or date_start

        cur  = _composite_index(ee, req.dataset, req.index, date_start, date_end, bounds, req.cloud_max)
        base = _composite_index(ee, req.dataset, req.index, base_start, base_end, bounds, req.cloud_max)
        anomaly = cur.subtract(base).rename("anomaly").clip(region)

        # Palette divergente + amplitude selon le type d'indice
        warm = req.index in ("LST Jour", "LST Nuit", "LST (température)", "Température air",
                             "NO₂", "CO", "SO₂", "O₃")
        pal = ["#2166ac", "#f7f7f7", "#b2182b"] if warm else ["#8B4513", "#f5f5f5", "#1a9850"]
        amp = {"LST Jour": 5, "LST Nuit": 5, "LST (température)": 5, "Température air": 5,
               "Précipitations (cumul)": 100}.get(req.index, 0.3)
        vis = {"palette": pal, "min": -amp, "max": amp}

        map_id = anomaly.getMapId(vis)
        fetcher = map_id.get("tile_fetcher")
        tile_url = fetcher.url_format if (fetcher and hasattr(fetcher, "url_format")) else map_id.get("urlFormat", "")
        return {"tile_url": tile_url, "vis_params": vis,
                "baseline": [base_start, base_end], "period": [date_start, date_end]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erreur anomalie GEE : {e}")






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


# ═══════════════════════════════════════════════════════════════════════════
# CARTE BIVARIÉE — sémiologie croisant deux variables raster
# Ex. : Température de surface (LST) × NDVI
# Chaque variable est classée en tertiles (Faible / Moyen / Élevé) puis croisée
# dans une matrice 3×3 → code 0..8 → palette bivariée.
# ═══════════════════════════════════════════════════════════════════════════

# Palettes bivariées 3×3.  Index = clA*3 + clB  (clA = variable A en lignes
# Faible→Élevé ; clB = variable B en colonnes Faible→Élevé).
BIVARIATE_PALETTES = {
    # A = Température (froid→chaud), B = NDVI (faible→fort).
    # chaud + faible végétation = rouge (îlots de chaleur) ; froid + fort = vert.
    "temp_ndvi": [
        "#fffccc", "#c2e699", "#1a9850",   # A faible (froid)
        "#fdae61", "#b9c46a", "#4d9970",   # A moyen
        "#d7191c", "#a8674a", "#6e6e3b",   # A élevé (chaud)
    ],
    # Stevens classique bleu/violet
    "violet_bleu": [
        "#e8e8e8", "#ace4e4", "#5ac8c8",
        "#dfb0d6", "#a5add3", "#5698b9",
        "#be64ac", "#8c62aa", "#3b4994",
    ],
    # Rouge / Bleu
    "rouge_bleu": [
        "#e8e8e8", "#b5c0da", "#6c83b5",
        "#e4acac", "#ad9ea5", "#5b6c9e",
        "#c85a5a", "#985356", "#574249",
    ],
    # Vert / Magenta
    "vert_magenta": [
        "#e8e8e8", "#e4acac", "#c85a5a",
        "#b0d5c0", "#ad9ea5", "#985356",
        "#5ac8a0", "#739f8e", "#574249",
    ],
}

# Unité affichée par index (pour les seuils de la légende)
_UNIT_BY_INDEX = {
    "NDVI": "", "EVI": "", "NDWI": "", "NDBI": "",
    "LST Jour": "°C", "LST Nuit": "°C", "LST (température)": "°C",
    "Température air": "°C", "Précipitations": "mm", "Humidité": "%",
    "Élévation": "m", "Pente": "°", "Hauteur canopée": "m",
}


def _resolve_bivar_variable(ee, dataset, index, date_start, date_end,
                            cloud_max, composite, region):
    """Retourne (image_mono_bande, nom_bande, scale_m, unité) pour une variable.

    Gère les datasets dynamiques (S2, Landsat, MODIS, ERA5) via le pipeline
    composite existant, ainsi que les datasets statiques (SRTM, canopée).
    """
    # ── Datasets statiques ───────────────────────────────────────────────
    if dataset == "srtm":
        dem = ee.Image("USGS/SRTMGL1_003")
        if index == "Pente":
            return ee.Terrain.slope(dem).rename("SLOPE"), "SLOPE", 30, "°"
        return dem.rename("ELEV"), "ELEV", 30, "m"
    if dataset == "canopy_height":
        img = ee.ImageCollection(
            "projects/meta-forest-monitoring-okw37/assets/CanopyHeight"
        ).mosaic().updateMask(
            ee.ImageCollection(
                "projects/meta-forest-monitoring-okw37/assets/CanopyHeight"
            ).mosaic().gte(1)
        )
        return img.rename("CANOPY"), "CANOPY", 10, "m"

    # ── Datasets dynamiques (collection + composite) ─────────────────────
    img, scale = _get_composite(ee, dataset, date_start, date_end,
                                cloud_max, composite, region)
    band_img, band = _compute_band(ee, img, dataset, index)
    return band_img.rename(band), band, scale, _UNIT_BY_INDEX.get(index, "")


def _tertile_classes(ee, img, band, region, scale, thresholds=None):
    """Classe une image mono-bande en 3 classes (0/1/2) par tertiles.

    Si ``thresholds`` ([t1, t2]) est fourni, on l'utilise tel quel ; sinon on
    estime les percentiles 33 et 66 sur la région.  Renvoie (image_classée,
    [t1, t2]).
    """
    if thresholds and len(thresholds) == 2 and None not in thresholds:
        t1, t2 = float(thresholds[0]), float(thresholds[1])
    else:
        pct_scale = max(scale, 100)  # échantillonnage allégé pour les percentiles
        pct = img.reduceRegion(
            reducer=ee.Reducer.percentile([33, 66]),
            geometry=region, scale=pct_scale, maxPixels=1e9, bestEffort=True,
        ).getInfo()
        t1 = pct.get(f"{band}_p33")
        t2 = pct.get(f"{band}_p66")
        if t1 is None or t2 is None or t1 == t2:
            mm = img.reduceRegion(
                reducer=ee.Reducer.min().combine(ee.Reducer.max(), sharedInputs=True),
                geometry=region, scale=pct_scale, maxPixels=1e9, bestEffort=True,
            ).getInfo()
            lo = mm.get(f"{band}_min", 0) or 0
            hi = mm.get(f"{band}_max", 1) or 1
            if hi == lo:
                hi = lo + 1
            t1 = lo + (hi - lo) / 3.0
            t2 = lo + 2.0 * (hi - lo) / 3.0
    cls = img.gte(t1).add(img.gte(t2)).rename("CLS")  # 0, 1, 2
    return cls, [round(float(t1), 3), round(float(t2), 3)]


class BivariateVar(BaseModel):
    dataset: str
    index:   str
    label:   Optional[str] = None
    thresholds: Optional[List[float]] = None   # [t1, t2] manuels, sinon auto


class BivariateRequest(BaseModel):
    var_a:       BivariateVar
    var_b:       BivariateVar
    date_start:  Optional[str]   = "2024-01-01"
    date_end:    Optional[str]   = "2025-01-01"
    cloud_max:   Optional[float] = 20.0
    composite:   Optional[str]   = "median"
    bbox:        Optional[List[float]] = None
    roi_geojson: Optional[dict]  = None
    palette:     Optional[str]   = "temp_ndvi"


@router.post("/bivariate/tiles")
def gee_bivariate_tiles(req: BivariateRequest):
    """Carte de sémiologie bivariée croisant deux variables raster.

    Chaque variable est classée en tertiles (Faible/Moyen/Élevé) puis les deux
    classes sont croisées dans une matrice 3×3 (code 0..8) visualisée avec une
    palette bivariée.
    """
    if not init_gee():
        raise HTTPException(503, "GEE non disponible")
    if not req.bbox and not req.roi_geojson:
        raise HTTPException(422, "Fournir 'bbox' ou 'roi_geojson'")

    try:
        from gee_auth import get_ee
        ee = get_ee()

        # ── Région d'analyse ─────────────────────────────────────────────
        if req.roi_geojson:
            geojson = req.roi_geojson
            if geojson.get("type") == "Feature":
                geojson = geojson["geometry"]
            try:
                region = geojson_to_ee_geometry(geojson)
            except Exception:
                if req.bbox:
                    w, s, e, n = req.bbox
                    region = ee.Geometry.BBox(w, s, e, n)
                else:
                    raise HTTPException(422, "ROI invalide")
        else:
            w, s, e, n = req.bbox
            region = ee.Geometry.BBox(w, s, e, n)

        ds, de = req.date_start or "2024-01-01", req.date_end or "2025-01-01"

        # ── Résolution des deux variables ────────────────────────────────
        try:
            imgA, bandA, scaleA, unitA = _resolve_bivar_variable(
                ee, req.var_a.dataset, req.var_a.index, ds, de,
                req.cloud_max, req.composite, region)
            imgB, bandB, scaleB, unitB = _resolve_bivar_variable(
                ee, req.var_b.dataset, req.var_b.index, ds, de,
                req.cloud_max, req.composite, region)
        except ValueError as ve:
            # Filtre nuages non satisfait / collection vide / index non supporté.
            print(f"[bivariate] refus de génération : {ve} "
                  f"(var_a={req.var_a.dataset}/{req.var_a.index}, "
                  f"var_b={req.var_b.dataset}/{req.var_b.index}, "
                  f"dates={ds}→{de}, cloud_max={req.cloud_max}%)")
            raise HTTPException(422, str(ve))

        scale = max(scaleA, scaleB)

        # ── Classification en tertiles ───────────────────────────────────
        clsA, thrA = _tertile_classes(ee, imgA, bandA, region, scaleA,
                                      req.var_a.thresholds)
        clsB, thrB = _tertile_classes(ee, imgB, bandB, region, scaleB,
                                      req.var_b.thresholds)

        # ── Croisement 3×3 → code 0..8 ───────────────────────────────────
        code = clsA.multiply(3).add(clsB).rename("BIV").clip(region)

        palette = BIVARIATE_PALETTES.get(req.palette) or BIVARIATE_PALETTES["temp_ndvi"]

        map_id   = code.getMapId({"min": 0, "max": 8, "palette": palette})
        fetcher  = map_id.get("tile_fetcher")
        tile_url = (fetcher.url_format
                    if fetcher and hasattr(fetcher, "url_format")
                    else map_id.get("urlFormat", ""))
        if not tile_url:
            raise HTTPException(500, "Impossible de générer les tuiles bivariées")

        # ── Bbox du clip pour fitBounds / cadrage légende ────────────────
        try:
            bb = region.bounds().getInfo()["coordinates"][0]
            xs = [c[0] for c in bb]; ys = [c[1] for c in bb]
            clip_bbox = [min(xs), min(ys), max(xs), max(ys)]
        except Exception:
            clip_bbox = req.bbox

        label_a = req.var_a.label or req.var_a.index
        label_b = req.var_b.label or req.var_b.index
        lvl = ["Faible", "Moyen", "Élevé"]

        classes = []
        for a in range(3):
            for b in range(3):
                c = a * 3 + b
                classes.append({"code": c, "color": palette[c],
                                "a": lvl[a], "b": lvl[b]})

        return {
            "tile_url":  tile_url,
            "name":      f"Bivarié — {label_a} × {label_b}",
            "clip_bbox": clip_bbox,
            "bivariate": {
                "palette":      palette,
                "label_a":      label_a,
                "label_b":      label_b,
                "unit_a":       unitA,
                "unit_b":       unitB,
                "thresholds_a": thrA,
                "thresholds_b": thrB,
                "levels":       lvl,
                "classes":      classes,
            },
        }

    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(422, str(ve))
    except Exception as e:
        msg = str(e)
        if "Permission" in msg or "403" in msg:
            raise HTTPException(403, "Accès refusé GEE.")
        # Collection vide (dates futures, zone non couverte, période trop courte)
        low = msg.lower()
        if any(k in low for k in ("empty date range", "reducecolumns", "no bands",
                                  "empty collection", "no features")):
            raise HTTPException(422,
                "Images Landsat ou Sentinel non disponibles pour l'analyse sur cette "
                "période/zone (dates futures, couverture incomplète ou nuages trop "
                "denses). Élargissez la période ou changez de zone.")
        raise HTTPException(500, f"Erreur carte bivariée GEE : {msg}")