"""
validation/mcp_validator.py — Validateur MCP
=============================================
Valide les arguments avant chaque appel MCP
et la structure des réponses retournées.
"""

import re
import logging
from typing import Any

log = logging.getLogger("mcp_validator")


class MCPValidationError(ValueError):
    """Erreur de validation MCP avec message clair pour l'utilisateur."""
    def __init__(self, server: str, tool: str, message: str):
        self.server  = server
        self.tool    = tool
        super().__init__(f"[{server}.{tool}] {message}")


# ── Schémas d'arguments par server/tool ──────────────────────
# Format : {server: {tool: {arg: (type, required, validator_fn|None)}}}

def _is_bbox(v) -> bool:
    return (isinstance(v, list) and len(v) == 4
            and all(isinstance(x, (int,float)) for x in v)
            and -180<=v[0]<v[2]<=180 and -90<=v[1]<v[3]<=90)

def _is_date(v) -> bool:
    try:
        from datetime import datetime
        datetime.strptime(str(v), "%Y-%m-%d")
        return True
    except Exception:
        return False

def _is_coords(v) -> bool:
    return (isinstance(v, list) and len(v) == 2
            and isinstance(v[0], (int,float))
            and isinstance(v[1], (int,float))
            and -180<=v[0]<=180 and -90<=v[1]<=90)

def _is_positive_int(v) -> bool:
    return isinstance(v, int) and v > 0

def _is_pct(v) -> bool:
    return isinstance(v, (int,float)) and 0 <= v <= 100

_HEX = re.compile(r'^#?[0-9a-fA-F]{6}$')

TOOL_SCHEMAS: dict[str, dict[str, dict]] = {
    "gee": {
        "compute_ndvi": {
            "bbox":       (list, False, _is_bbox),
            "start_date": (str,  False, _is_date),
            "end_date":   (str,  False, _is_date),
            "collection": (str,  False, None),
            "cloud_cover":(int,  False, _is_pct),
        },
        "compute_timelapse": {
            "bbox":       (list, False, _is_bbox),
            "start_date": (str,  True,  _is_date),
            "end_date":   (str,  True,  _is_date),
            "interval":   (str,  False, lambda v: v in ("month","quarter","year")),
            "index":      (str,  False, lambda v: v in ("ndvi","rgb","evi","ndwi","sar_vv")),
        },
        "compute_lst_modis": {
            "bbox":       (list, False, _is_bbox),
            "start_date": (str,  False, _is_date),
            "end_date":   (str,  False, _is_date),
            "mode":       (str,  False, lambda v: v in ("day","night","both")),
        },
        "compute_sar_vv": {
            "bbox":       (list, False, _is_bbox),
            "start_date": (str,  False, _is_date),
            "end_date":   (str,  False, _is_date),
        },
    },
    "ors": {
        "compute_isochrone": {
            "center":       (list, True,  _is_coords),
            "time_minutes": (int,  True,  lambda v: 1 <= v <= 120),
            "profile":      (str,  False, lambda v: v in ("foot","bike","car","wheelchair")),
        },
        "compute_isochrones_multi": {
            "center":    (list, True,  _is_coords),
            "intervals": (list, True,  lambda v: all(isinstance(x,int) and 1<=x<=120 for x in v)),
            "profile":   (str,  False, lambda v: v in ("foot","bike","car")),
        },
        "compute_route": {
            "waypoints": (list, True,  lambda v: len(v) >= 2 and all(_is_coords(p) for p in v)),
            "profile":   (str,  False, lambda v: v in ("foot","bike","car","hike","wheelchair")),
        },
        "compute_matrix": {
            "locations": (list, True,  lambda v: 2 <= len(v) <= 25),
            "profile":   (str,  False, None),
        },
    },
    "worldbank": {
        "get_indicator": {
            "indicator": (str,  False, None),
            "year":      (int,  False, lambda v: 1960 <= v <= 2030),
            "keyword":   (str,  False, None),
        },
        "get_country_profile": {
            "country": (str, True, lambda v: len(v) >= 2),
        },
        "compare_countries": {
            "countries": (list, True,  lambda v: 2 <= len(v) <= 20),
            "indicator": (str,  False, None),
        },
    },
    "overture": {
        "query_places": {
            "bbox":           (list, False, _is_bbox),
            "category":       (str,  False, None),
            "limit":          (int,  False, lambda v: 1 <= v <= 10000),
            "min_confidence": (float,False, lambda v: 0 <= v <= 1),
        },
        "query_buildings": {
            "bbox":       (list, False, _is_bbox),
            "min_height": (float,False, lambda v: v >= 0),
            "max_height": (float,False, lambda v: v >= 0),
        },
        "query_roads": {
            "bbox":       (list, False, _is_bbox),
            "road_class": (str,  False, None),
        },
    },
    "ors": {
        "compute_isochrone": {
            "center":       (list, True,  _is_coords),
            "time_minutes": (int,  True,  lambda v: 1 <= v <= 120),
        },
    },
    "nominatim": {
        "geocode": {
            "query": (str, True, lambda v: len(v.strip()) >= 2),
        },
        "reverse_geocode": {
            "lng": ((int,float), True, lambda v: -180 <= v <= 180),
            "lat": ((int,float), True, lambda v: -90  <= v <= 90),
        },
        "get_bbox_for_place": {
            "place": (str, True, lambda v: len(v.strip()) >= 2),
        },
    },
    "postgis": {
        "query_table": {
            "table": (str, True, lambda v: len(v) >= 2 and v.isidentifier()),
            "bbox":  (list,False, _is_bbox),
            "limit": (int, False, lambda v: 1 <= v <= 100000),
        },
        "spatial_buffer": {
            "radius_m": ((int,float), True, lambda v: 0 < v <= 200000),
        },
    },
    "maptiler": {
        "get_elevation_profile": {
            "coordinates": (list, True, lambda v: len(v) >= 2),
            "n_points":    (int,  False, lambda v: 1 <= v <= 500),
        },
        "get_contours": {
            "bbox":        (list, True,  _is_bbox),
            "interval_m":  (int,  False, lambda v: v in (5,10,20,50,100)),
        },
    },
}

# ── Schémas de réponse ────────────────────────────────────────
RESPONSE_SCHEMAS: dict[str, dict[str, list[str]]] = {
    "gee": {
        "compute_ndvi":    ["action","tile_url"],
        "compute_rgb":     ["action","tile_url"],
        "compute_timelapse":["action","frames","dates"],
        "compute_lst_modis":["action"],
    },
    "ors": {
        "compute_isochrone":      ["action","geojson"],
        "compute_isochrones_multi":["action","geojson"],
        "compute_route":          ["action","geojson"],
    },
    "worldbank": {
        "get_indicator": ["action"],
    },
    "overture": {
        "query_places":   ["type","features"],
        "query_buildings":["type","features"],
    },
    "nominatim": {
        "geocode":         ["action","latitude","longitude"],
        "reverse_geocode": ["action","display_name"],
        "get_bbox_for_place":["action","bbox"],
    },
}


def validate_tool_args(
    server: str,
    tool:   str,
    args:   dict,
    strict: bool = False,
) -> tuple[bool, list[str]]:
    """
    Valide les arguments d'un appel MCP.

    Args:
        server: nom du MCP server
        tool:   nom du tool
        args:   arguments à valider
        strict: True = lève MCPValidationError, False = retourne erreurs

    Returns:
        (valid: bool, errors: list[str])
    """
    schema = TOOL_SCHEMAS.get(server, {}).get(tool)
    if not schema:
        # Pas de schéma défini → accepté (permissif)
        return True, []

    errors = []
    for arg_name, (expected_type, required, validator) in schema.items():
        value = args.get(arg_name)

        # Requis
        if required and (value is None or value == ""):
            errors.append(f"Argument requis manquant: '{arg_name}'")
            continue

        if value is None:
            continue

        # Type
        if not isinstance(value, expected_type):
            # Tentative de coercition légère
            try:
                if expected_type in (int, float) and isinstance(value, (int,float)):
                    pass  # ok
                elif expected_type == list and isinstance(value, (list,tuple)):
                    pass
                else:
                    errors.append(
                        f"'{arg_name}': type invalide "
                        f"(reçu {type(value).__name__}, "
                        f"attendu {expected_type if isinstance(expected_type,str) else expected_type.__name__})"
                    )
                    continue
            except Exception:
                errors.append(f"'{arg_name}': erreur de type")
                continue

        # Validator custom
        if validator and not validator(value):
            errors.append(f"'{arg_name}': valeur invalide: {value!r}")

    if errors and strict:
        raise MCPValidationError(server, tool, "; ".join(errors))

    return len(errors) == 0, errors


def validate_mcp_response(
    server:   str,
    tool:     str,
    response: Any,
    strict:   bool = False,
) -> tuple[bool, list[str]]:
    """
    Valide la structure de la réponse d'un tool MCP.

    Returns:
        (valid: bool, warnings: list[str])
    """
    if not isinstance(response, dict):
        msg = f"réponse non-dict: {type(response).__name__}"
        if strict: raise MCPValidationError(server, tool, msg)
        return False, [msg]

    if "error" in response:
        # Erreur explicite → pas de validation structurelle
        return False, [f"tool error: {response['error']}"]

    required_fields = RESPONSE_SCHEMAS.get(server, {}).get(tool, [])
    warnings = []

    for field in required_fields:
        if field not in response:
            warnings.append(f"champ manquant dans la réponse: '{field}'")

    # Validation tile_url si présente
    tile_url = response.get("tile_url","")
    if tile_url:
        if not tile_url.startswith("http"):
            warnings.append(f"tile_url ne commence pas par https: {tile_url[:60]}")
        if "{z}" not in tile_url or "{x}" not in tile_url or "{y}" not in tile_url:
            warnings.append(f"tile_url manque {{z}}/{{x}}/{{y}}: {tile_url[:60]}")

    # Validation GeoJSON si présent
    geojson = response.get("geojson")
    if geojson:
        if geojson.get("type") not in ("FeatureCollection","Feature","Polygon","MultiPolygon"):
            warnings.append(f"geojson.type invalide: {geojson.get('type')}")
        features = geojson.get("features",[])
        if isinstance(features, list) and len(features) == 0:
            warnings.append("geojson vide (0 features)")

    # Validation frames timelapse
    frames = response.get("frames")
    if frames is not None:
        if not isinstance(frames, list) or len(frames) < 2:
            warnings.append(f"timelapse: frames insuffisants ({len(frames) if isinstance(frames,list) else 0})")

    if warnings:
        log.warning(f"[mcp_validator] {server}.{tool}: {'; '.join(warnings)}")

    if warnings and strict:
        raise MCPValidationError(server, tool, "; ".join(warnings))

    return len(warnings) == 0, warnings
