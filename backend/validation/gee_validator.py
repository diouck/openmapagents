"""
validation/gee_validator.py — Validateur GEE
=============================================
Valide dates, datasets et vis_params avant tout appel GEE.
"""

import re
import logging
from datetime import datetime, date
from typing import Optional

log = logging.getLogger("gee_validator")

# Collections GEE autorisées (whitelist)
GEE_DATASETS = {
    # Optique
    "sentinel2":      "COPERNICUS/S2_SR_HARMONIZED",
    "sentinel2_l1":   "COPERNICUS/S2",
    "landsat8":       "LANDSAT/LC08/C02/T1_L2",
    "landsat9":       "LANDSAT/LC09/C02/T1_L2",
    "landsat7":       "LANDSAT/LE07/C02/T1_L2",
    # SAR
    "sentinel1":      "COPERNICUS/S1_GRD",
    # Climat
    "modis_lst":      "MODIS/061/MOD11A1",
    "modis_ndvi":     "MODIS/061/MOD13Q1",
    "era5":           "ECMWF/ERA5_LAND/DAILY_AGGR",
    # Végétation / Couverture
    "esa_worldcover": "ESA/WorldCover/v200",
    "forest_watch":   "UMD/hansen/global_forest_change_2023_v1_11",
    "srtm":           "USGS/SRTMGL1_003",
    # IDs complets acceptés
    "COPERNICUS/S2_SR_HARMONIZED": "COPERNICUS/S2_SR_HARMONIZED",
    "COPERNICUS/S1_GRD":           "COPERNICUS/S1_GRD",
    "MODIS/061/MOD11A1":           "MODIS/061/MOD11A1",
    "ECMWF/ERA5_LAND/DAILY_AGGR":  "ECMWF/ERA5_LAND/DAILY_AGGR",
    "USGS/SRTMGL1_003":            "USGS/SRTMGL1_003",
}

_HEX_RE = re.compile(r'^#?[0-9a-fA-F]{6}$')


class GEEValidationError(ValueError):
    pass


def validate_dates(
    start:       str,
    end:         str,
    raise_error: bool = True,
) -> tuple[bool, str]:
    """
    Valide start_date / end_date.
    - Format ISO YYYY-MM-DD
    - start < end
    - end ne dépasse pas aujourd'hui
    """
    today = date.today()

    for label, val in [("start_date", start), ("end_date", end)]:
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except (ValueError, TypeError):
            msg = f"{label} invalide: '{val}' (format requis: YYYY-MM-DD)"
            if raise_error: raise GEEValidationError(msg)
            return False, msg

    dt_start = datetime.strptime(start, "%Y-%m-%d").date()
    dt_end   = datetime.strptime(end,   "%Y-%m-%d").date()

    if dt_start >= dt_end:
        msg = f"start_date ({start}) doit être < end_date ({end})"
        if raise_error: raise GEEValidationError(msg)
        return False, msg

    if dt_end > today:
        msg = (
            f"end_date ({end}) est dans le futur. "
            f"Date max: {today.isoformat()}"
        )
        if raise_error: raise GEEValidationError(msg)
        return False, msg

    duration_days = (dt_end - dt_start).days
    if duration_days > 365 * 10:
        log.warning(
            f"Période très longue: {duration_days} jours "
            f"({start} → {end}) — le timelapse pourrait être lent"
        )

    return True, f"dates valides ({start} → {end}, {duration_days}j)"


def validate_dataset(
    name:        str,
    raise_error: bool = True,
) -> tuple[bool, str]:
    """
    Vérifie que le dataset est dans la whitelist GEE_DATASETS.
    """
    if not name:
        msg = "dataset vide"
        if raise_error: raise GEEValidationError(msg)
        return False, msg

    if name in GEE_DATASETS:
        return True, f"dataset valide: {name} → {GEE_DATASETS[name]}"

    # Recherche case-insensitive
    name_lower = name.lower()
    for k in GEE_DATASETS:
        if k.lower() == name_lower:
            return True, f"dataset valide (case-insensitive): {k}"

    msg = (
        f"Dataset GEE non autorisé: '{name}'. "
        f"Datasets valides: {list(GEE_DATASETS.keys())}"
    )
    if raise_error: raise GEEValidationError(msg)
    return False, msg


def validate_vis_params(
    params:      dict,
    raise_error: bool = True,
) -> tuple[bool, str]:
    """
    Valide les paramètres de visualisation GEE.
    - min < max
    - palette : liste de hex valides, 2-8 couleurs
    """
    if not params:
        return True, "vis_params vides (ok)"

    vmin = params.get("min")
    vmax = params.get("max")

    if vmin is not None and vmax is not None:
        try:
            if float(vmin) >= float(vmax):
                msg = f"vis_params: min ({vmin}) doit être < max ({vmax})"
                if raise_error: raise GEEValidationError(msg)
                return False, msg
        except (TypeError, ValueError):
            msg = f"vis_params: min/max non numériques: min={vmin}, max={vmax}"
            if raise_error: raise GEEValidationError(msg)
            return False, msg

    palette = params.get("palette")
    if palette is not None:
        if not isinstance(palette, list):
            msg = "vis_params: palette doit être une liste de couleurs hex"
            if raise_error: raise GEEValidationError(msg)
            return False, msg

        if len(palette) < 2:
            msg = f"vis_params: palette trop courte ({len(palette)} couleur). Min 2."
            if raise_error: raise GEEValidationError(msg)
            return False, msg

        if len(palette) > 8:
            log.warning(f"vis_params: palette longue ({len(palette)} couleurs), max recommandé 8")

        for color in palette:
            color_str = str(color).strip()
            if not _HEX_RE.match(color_str):
                msg = (
                    f"vis_params: couleur hex invalide '{color_str}'. "
                    f"Format requis: #RRGGBB ou RRGGBB"
                )
                if raise_error: raise GEEValidationError(msg)
                return False, msg

    return True, "vis_params valides"


def fix_dates(start: str, end: str) -> tuple[str, str]:
    """
    Corrige automatiquement des dates invalides.
    Retourne (start_corrigé, end_corrigé).
    """
    today = date.today().isoformat()

    # Corriger end dans le futur
    if end > today:
        log.info(f"end_date corrigé: {end} → {today}")
        end = today

    # Corriger start >= end
    if start >= end:
        from datetime import timedelta
        dt_end   = datetime.strptime(end, "%Y-%m-%d").date()
        dt_start = dt_end - timedelta(days=180)
        start    = dt_start.isoformat()
        log.info(f"start_date corrigé: → {start}")

    return start, end
