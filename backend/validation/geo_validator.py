"""
validation/geo_validator.py — Validateur géographique
======================================================
Valide bbox, radius, et géocode des toponymes via mcp_nominatim.
"""

import os
import math
import logging
from typing import Optional

log = logging.getLogger("geo_validator")

# Limites sécurité
GEE_MAX_KM2      = float(os.getenv("GEE_MAX_BBOX_KM2",      "500"))
OVERTURE_MAX_KM2 = float(os.getenv("OVERTURE_MAX_BBOX_KM2", "5000"))
OSM_MAX_KM2      = float(os.getenv("OSM_MAX_BBOX_KM2",      "2000"))
MAX_RADIUS_M     = float(os.getenv("MAX_RADIUS_M",           "200000"))  # 200 km


class GeoValidationError(ValueError):
    pass


def _bbox_area_km2(bbox: list) -> float:
    xmin, ymin, xmax, ymax = bbox
    lat_mid = (ymin + ymax) / 2
    dx = (xmax - xmin) * 111.32 * math.cos(math.radians(lat_mid))
    dy = (ymax - ymin) * 110.57
    return abs(dx * dy)


def validate_bbox(
    bbox:        list,
    source:      str  = "default",
    raise_error: bool = True,
) -> tuple[bool, str]:
    """
    Valide une bbox [xmin, ymin, xmax, ymax].

    Args:
        bbox:    [xmin, ymin, xmax, ymax]
        source:  "gee" | "overture" | "osm" | "default"
        raise_error: lève GeoValidationError si True

    Returns:
        (valid: bool, message: str)
    """
    if not bbox or len(bbox) != 4:
        msg = "bbox manquant ou invalide (attendu: [xmin,ymin,xmax,ymax])"
        if raise_error: raise GeoValidationError(msg)
        return False, msg

    try:
        xmin, ymin, xmax, ymax = [float(v) for v in bbox]
    except (TypeError, ValueError):
        msg = f"bbox contient des valeurs non numériques: {bbox}"
        if raise_error: raise GeoValidationError(msg)
        return False, msg

    # Bornes mondiales
    if not (-180 <= xmin < xmax <= 180):
        msg = f"bbox longitude invalide: xmin={xmin}, xmax={xmax} (limites ±180)"
        if raise_error: raise GeoValidationError(msg)
        return False, msg

    if not (-90 <= ymin < ymax <= 90):
        msg = f"bbox latitude invalide: ymin={ymin}, ymax={ymax} (limites ±90)"
        if raise_error: raise GeoValidationError(msg)
        return False, msg

    # Cohérence
    if xmax - xmin < 1e-6:
        msg = f"bbox trop petite (largeur={xmax-xmin:.8f}°)"
        if raise_error: raise GeoValidationError(msg)
        return False, msg

    # Superficie max selon la source
    area = _bbox_area_km2([xmin, ymin, xmax, ymax])
    limits = {
        "gee":      GEE_MAX_KM2,
        "overture": OVERTURE_MAX_KM2,
        "osm":      OSM_MAX_KM2,
        "default":  OVERTURE_MAX_KM2,
    }
    max_area = limits.get(source, OVERTURE_MAX_KM2)

    if area > max_area:
        msg = (
            f"bbox trop grande: {area:.0f} km² "
            f"(max {max_area:.0f} km² pour {source}). "
            f"Zoomez davantage sur la carte."
        )
        if raise_error: raise GeoValidationError(msg)
        return False, msg

    return True, f"bbox valide ({area:.1f} km²)"


def validate_radius(
    radius_m:    float,
    raise_error: bool = True,
) -> tuple[bool, str]:
    """
    Valide un rayon en mètres (max 200 km).
    """
    try:
        r = float(radius_m)
    except (TypeError, ValueError):
        msg = f"radius invalide: {radius_m}"
        if raise_error: raise GeoValidationError(msg)
        return False, msg

    if r <= 0:
        msg = f"radius doit être positif: {r}"
        if raise_error: raise GeoValidationError(msg)
        return False, msg

    if r > MAX_RADIUS_M:
        msg = (
            f"radius trop grand: {r/1000:.0f} km "
            f"(max {MAX_RADIUS_M/1000:.0f} km)"
        )
        if raise_error: raise GeoValidationError(msg)
        return False, msg

    return True, f"radius valide ({r:.0f} m)"


async def geocode_to_bbox(
    toponym: str,
    country_code: str = "",
) -> Optional[list]:
    """
    Géocode un toponyme → bbox via mcp_nominatim + cache PG.

    Returns:
        [xmin, ymin, xmax, ymax] ou None si non trouvé
    """
    if not toponym or not toponym.strip():
        return None

    try:
        from mcp_client import get_mcp_client
        client = get_mcp_client()
        args   = {"place": toponym.strip()}
        if country_code:
            args["country_code"] = country_code

        r = await client.call_tool(
            "get_bbox_for_place", args,
            server_name="nominatim",
            use_cache=True,
        )
        if "error" not in r and r.get("bbox"):
            bbox = r["bbox"]
            valid, _ = validate_bbox(bbox, raise_error=False)
            if valid:
                return bbox
            log.warning(f"geocode_to_bbox: bbox invalide pour '{toponym}': {bbox}")
    except Exception as e:
        log.warning(f"geocode_to_bbox '{toponym}': {e}")

    return None


def clip_bbox_to_max(bbox: list, source: str = "gee") -> list:
    """
    Réduit une bbox trop grande en gardant le centre.
    Retourne la bbox clippée.
    """
    limits = {"gee": GEE_MAX_KM2, "overture": OVERTURE_MAX_KM2, "osm": OSM_MAX_KM2}
    max_km2 = limits.get(source, OVERTURE_MAX_KM2)
    area    = _bbox_area_km2(bbox)

    if area <= max_km2:
        return bbox

    xmin, ymin, xmax, ymax = bbox
    cx  = (xmin + xmax) / 2
    cy  = (ymin + ymax) / 2
    # Calculer le delta pour atteindre max_km2
    # area ≈ (2*d)² * 111² → d = sqrt(max_km2) / 111 / 2
    d = math.sqrt(max_km2) / 111 / 2
    clipped = [cx-d, cy-d/2, cx+d, cy+d/2]
    log.info(
        f"clip_bbox_to_max ({source}): "
        f"{area:.0f}km² → {_bbox_area_km2(clipped):.0f}km²"
    )
    return clipped
