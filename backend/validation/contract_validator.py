"""
validation/contract_validator.py — Contrats Pydantic par action
================================================================
Valide chaque réponse agent avant de l'envoyer au frontend.
Compatible avec le dispatcher ChatPanel.jsx.
"""

import re
import logging
from typing import Any, Optional, List
from pydantic import BaseModel, field_validator, model_validator

log = logging.getLogger("contract_validator")

_HEX = re.compile(r'^#?[0-9a-fA-F]{6}$')

# ── Helpers ───────────────────────────────────────────────────

def _valid_hex(color: str) -> bool:
    return bool(_HEX.match(str(color).strip()))


def _valid_geojson(gj: dict) -> bool:
    if not isinstance(gj, dict):
        return False
    valid_types = {
        "FeatureCollection","Feature","Point","MultiPoint",
        "LineString","MultiLineString","Polygon","MultiPolygon",
    }
    return gj.get("type") in valid_types


def _features_non_empty(gj: dict) -> bool:
    if gj.get("type") == "FeatureCollection":
        return len(gj.get("features",[])) > 0
    return True   # Feature simple → ok


def _coords_in_bounds(gj: dict) -> bool:
    """Vérifie que les premières coordonnées sont dans les limites mondiales."""
    try:
        features = gj.get("features",[]) if gj.get("type")=="FeatureCollection" else [gj]
        for f in features[:3]:
            geom = f.get("geometry",{}) if f.get("type")=="Feature" else f
            coords = geom.get("coordinates",[])
            # Extraire le premier point
            while isinstance(coords, list) and coords and isinstance(coords[0], list):
                coords = coords[0]
            if isinstance(coords, list) and len(coords) >= 2:
                lng, lat = float(coords[0]), float(coords[1])
                if not (-180 <= lng <= 180) or not (-90 <= lat <= 90):
                    return False
    except Exception:
        pass
    return True


# ═══════════════════════════════════════════════════════════════
# MODÈLES PYDANTIC
# ═══════════════════════════════════════════════════════════════

class AddLayerPayload(BaseModel):
    action:     str
    tile_url:   str
    layer_name: str
    message:    str = ""
    min:        Optional[float] = None
    max:        Optional[float] = None
    palette:    Optional[List[str]] = None
    opacity:    float = 0.8
    attribution:str = ""

    @field_validator("action")
    @classmethod
    def check_action(cls, v):
        if v != "add_layer":
            raise ValueError(f"action doit être 'add_layer', reçu '{v}'")
        return v

    @field_validator("tile_url")
    @classmethod
    def check_tile_url(cls, v):
        if not v:
            raise ValueError("tile_url vide")
        if not v.startswith("http"):
            raise ValueError(f"tile_url doit commencer par https: {v[:80]}")
        if "{z}" not in v or "{x}" not in v or "{y}" not in v:
            raise ValueError(f"tile_url manque {{z}}/{{x}}/{{y}}: {v[:80]}")
        return v

    @field_validator("palette")
    @classmethod
    def check_palette(cls, v):
        if v is None:
            return v
        if len(v) < 2:
            raise ValueError(f"palette trop courte ({len(v)}). Min 2 couleurs.")
        if len(v) > 8:
            log.warning(f"palette longue ({len(v)} couleurs), max recommandé 8")
        for c in v:
            if not _valid_hex(c):
                raise ValueError(f"couleur hex invalide: '{c}'")
        return [c.lstrip("#") for c in v]   # normaliser sans #

    @field_validator("opacity")
    @classmethod
    def check_opacity(cls, v):
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"opacity doit être entre 0 et 1: {v}")
        return v

    @model_validator(mode="after")
    def check_min_max(self):
        if self.min is not None and self.max is not None:
            if self.min >= self.max:
                raise ValueError(f"min ({self.min}) doit être < max ({self.max})")
        return self


class AddIsochronePayload(BaseModel):
    action:     str
    geojson:    dict
    layer_name: str
    center:     List[float]
    duration:   Optional[int]  = None
    profile:    str            = "foot"
    color:      str            = "3b82f6"
    opacity:    float          = 0.35
    message:    str            = ""

    @field_validator("action")
    @classmethod
    def check_action(cls, v):
        if v != "add_isochrone":
            raise ValueError(f"action doit être 'add_isochrone', reçu '{v}'")
        return v

    @field_validator("geojson")
    @classmethod
    def check_geojson(cls, v):
        if not _valid_geojson(v):
            raise ValueError(f"geojson invalide: type={v.get('type','?')}")
        if not _features_non_empty(v):
            raise ValueError("geojson vide (0 features)")
        if not _coords_in_bounds(v):
            raise ValueError("geojson: coordonnées hors limites mondiales")
        return v

    @field_validator("center")
    @classmethod
    def check_center(cls, v):
        if len(v) != 2:
            raise ValueError(f"center doit être [lng, lat]: {v}")
        lng, lat = v
        if not (-180 <= lng <= 180) or not (-90 <= lat <= 90):
            raise ValueError(f"center hors limites: [{lng}, {lat}]")
        return v

    @field_validator("profile")
    @classmethod
    def check_profile(cls, v):
        valid = ("foot","bike","car","wheelchair","hike")
        if v not in valid:
            raise ValueError(f"profile invalide: '{v}'. Valides: {valid}")
        return v


class AddChoroplethPayload(BaseModel):
    action:       str
    geojson:      dict
    layer_name:   str
    message:      str   = ""
    property_name:str   = "value"
    palette:      Optional[List[str]] = None
    min_value:    Optional[float] = None
    max_value:    Optional[float] = None
    unit:         str   = ""
    year:         Optional[int] = None

    @field_validator("action")
    @classmethod
    def check_action(cls, v):
        if v != "add_choropleth":
            raise ValueError(f"action doit être 'add_choropleth', reçu '{v}'")
        return v

    @field_validator("geojson")
    @classmethod
    def check_geojson(cls, v):
        if not _valid_geojson(v):
            raise ValueError(f"geojson invalide")
        if not _features_non_empty(v):
            raise ValueError("geojson vide (0 features)")
        return v

    @field_validator("palette")
    @classmethod
    def check_palette(cls, v):
        if v is None:
            return v
        if len(v) < 2:
            raise ValueError(f"palette trop courte ({len(v)})")
        for c in v:
            if not _valid_hex(c):
                raise ValueError(f"couleur hex invalide: '{c}'")
        return [c.lstrip("#") for c in v]

    @model_validator(mode="after")
    def check_min_max(self):
        if self.min_value is not None and self.max_value is not None:
            if self.min_value >= self.max_value:
                raise ValueError(
                    f"min_value ({self.min_value}) doit être < max_value ({self.max_value})"
                )
        return self


class AddTimelapsePayload(BaseModel):
    action:      str
    frames:      List[str]
    dates:       List[str]
    layer_name:  str
    message:     str = ""
    fps:         int = 2
    index:       str = "ndvi"
    interval:    str = "month"
    palette:     Optional[List[str]] = None
    frame_count: Optional[int]       = None

    @field_validator("action")
    @classmethod
    def check_action(cls, v):
        if v != "add_timelapse":
            raise ValueError(f"action doit être 'add_timelapse', reçu '{v}'")
        return v

    @field_validator("frames")
    @classmethod
    def check_frames(cls, v):
        if len(v) < 2:
            raise ValueError(f"Timelapse nécessite ≥ 2 frames (reçu {len(v)})")
        for url in v[:3]:
            if not url.startswith("http"):
                raise ValueError(f"frame URL invalide: {url[:60]}")
        return v

    @model_validator(mode="after")
    def check_dates_frames(self):
        if len(self.dates) != len(self.frames):
            # Auto-corriger
            min_len = min(len(self.dates), len(self.frames))
            self.frames = self.frames[:min_len]
            self.dates  = self.dates[:min_len]
            log.warning(
                f"AddTimelapsePayload: dates/frames désalignés → tronqué à {min_len}"
            )
        if self.frame_count is None:
            self.frame_count = len(self.frames)
        return self

    @field_validator("fps")
    @classmethod
    def check_fps(cls, v):
        if not (1 <= v <= 10):
            return 2
        return v


class AddMarkersPayload(BaseModel):
    action:     str
    geojson:    dict
    layer_name: str
    message:    str = ""

    @field_validator("action")
    @classmethod
    def check_action(cls, v):
        if v != "add_markers":
            raise ValueError(f"action doit être 'add_markers', reçu '{v}'")
        return v

    @field_validator("geojson")
    @classmethod
    def check_geojson(cls, v):
        if not _valid_geojson(v):
            raise ValueError("geojson invalide")
        if not _features_non_empty(v):
            raise ValueError("geojson vide (0 features)")
        return v


class AddRoutePayload(BaseModel):
    action:       str
    geojson:      dict
    layer_name:   str
    distance_km:  Optional[float] = None
    duration_min: Optional[float] = None
    profile:      str             = "foot"
    message:      str             = ""

    @field_validator("action")
    @classmethod
    def check_action(cls, v):
        if v != "add_route":
            raise ValueError(f"action doit être 'add_route', reçu '{v}'")
        return v

    @field_validator("geojson")
    @classmethod
    def check_geojson(cls, v):
        if not _valid_geojson(v):
            raise ValueError("geojson invalide pour route")
        return v


# ═══════════════════════════════════════════════════════════════
# REGISTRE DES MODÈLES
# ═══════════════════════════════════════════════════════════════

ACTION_MODELS = {
    "add_layer":      AddLayerPayload,
    "add_isochrone":  AddIsochronePayload,
    "add_choropleth": AddChoroplethPayload,
    "add_timelapse":  AddTimelapsePayload,
    "add_markers":    AddMarkersPayload,
    "add_route":      AddRoutePayload,
}


# ═══════════════════════════════════════════════════════════════
# VALIDATE RESPONSE — point d'entrée principal
# ═══════════════════════════════════════════════════════════════

def validate_agent_response(
    response:    dict,
    strict:      bool = False,
) -> tuple[bool, Any, list[str]]:
    """
    Valide la réponse d'un agent via le modèle Pydantic correspondant.

    Args:
        response: dict retourné par un tool MCP
        strict:   True = lève une exception si invalide

    Returns:
        (valid: bool, validated_payload: dict|None, errors: list[str])
    """
    if not isinstance(response, dict):
        return False, None, ["réponse non-dict"]

    if "error" in response:
        return False, None, [f"tool error: {response['error']}"]

    action = response.get("action","")
    model_cls = ACTION_MODELS.get(action)

    if not model_cls:
        # Action inconnue → passer tel quel (actions spéciales comme fly_to, etc.)
        log.debug(f"validate_agent_response: action '{action}' sans modèle Pydantic")
        return True, response, []

    try:
        validated = model_cls(**response)
        return True, validated.model_dump(), []

    except Exception as e:
        errors = [str(e)]
        log.warning(f"validate_agent_response ({action}): {e!s:.120}")
        if strict:
            raise
        return False, None, errors


def sanitize_response(response: dict) -> dict:
    """
    Tente de corriger automatiquement une réponse invalide.
    Retourne la réponse corrigée ou originale.
    """
    if not isinstance(response, dict):
        return response

    action = response.get("action","")

    # Corriger tile_url manquant {z}/{x}/{y}
    tile_url = response.get("tile_url","")
    if tile_url and "{z}" not in tile_url:
        log.warning(f"sanitize: tile_url sans {{z}} → ignorée")
        response.pop("tile_url", None)
        if action == "add_layer":
            response["action"] = "error"
            response["error"]  = "tile_url invalide retournée par GEE"

    # Corriger palette
    palette = response.get("palette")
    if palette and isinstance(palette, list):
        valid_palette = [c.lstrip("#") for c in palette if _valid_hex(str(c))]
        if len(valid_palette) < 2:
            response.pop("palette", None)
        else:
            response["palette"] = valid_palette

    # Corriger frames timelapse
    if action == "add_timelapse":
        frames = response.get("frames",[])
        dates  = response.get("dates",[])
        if len(frames) != len(dates):
            n = min(len(frames), len(dates))
            response["frames"] = frames[:n]
            response["dates"]  = dates[:n]

    return response
