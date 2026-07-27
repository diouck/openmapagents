"""
geo_validator.py — Validation géospatiale déterministe
Utilisé par : GeoDataAgent, RoutingAgent, SatelliteAgent (bbox)

Fonctions :
  validate_bbox()      → bornes WGS84, superficie max, auto-correction
  validate_radius()    → max 200km, min 1m
  validate_coords()    → couple [lon, lat] valide
  geocode_to_bbox()    → Nominatim + cache SQLite local
  bbox_from_center()   → centre + rayon → bbox
  bbox_is_coherent()   → détection bbox absurdes (océan, pôle, trop petite)
"""

import os
import re
import math
import json
import time
import sqlite3
import logging
import hashlib
from pathlib import Path
from typing import Optional

import requests

log = logging.getLogger("geo_validator")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
MAX_BBOX_AREA_DEG2    = float(os.getenv("GEO_MAX_BBOX_AREA",   "25.0"))   # ~550×550km
MAX_GEE_BBOX_AREA     = float(os.getenv("GEO_MAX_GEE_BBOX",    "100.0"))  # GEE tolère plus large
MAX_RADIUS_M          = float(os.getenv("GEO_MAX_RADIUS_M",    "200000")) # 200km
MIN_RADIUS_M          = float(os.getenv("GEO_MIN_RADIUS_M",    "1"))
DEFAULT_RADIUS_M      = float(os.getenv("GEO_DEFAULT_RADIUS_M","1000"))
NOMINATIM_UA          = os.getenv("NOMINATIM_UA", "OpenMapAgents/1.0")
NOMINATIM_URL         = "https://nominatim.openstreetmap.org/search"
GEOCODE_TIMEOUT       = int(os.getenv("GEOCODE_TIMEOUT", "10"))
GEOCODE_CACHE_TTL_SEC = int(os.getenv("GEOCODE_CACHE_TTL", str(60 * 60 * 24 * 30)))  # 30 jours
CACHE_DB_PATH         = Path(os.getenv("GEOCODE_CACHE_DB", "./data/geocode_cache.db"))

# Villes fréquentes — évite un appel Nominatim pour les cas les plus courants
_CITY_CACHE: dict[str, dict] = {
    "nantes": {
        "lat": 47.2184, "lon": -1.5536,
        "bbox": [-1.72, 47.15, -1.42, 47.32],
        "display_name": "Nantes, Loire-Atlantique, France",
    },
    "gare de nantes": {
        "lat": 47.2173, "lon": -1.5419,
        "bbox": [-1.548, 47.213, -1.536, 47.222],
        "display_name": "Gare de Nantes, Nantes, Loire-Atlantique, France",
    },
    "gare nantes": {
        "lat": 47.2173, "lon": -1.5419,
        "bbox": [-1.548, 47.213, -1.536, 47.222],
        "display_name": "Gare de Nantes, Nantes, Loire-Atlantique, France",
    },
    "château des ducs": {
        "lat": 47.2184, "lon": -1.5534,
        "bbox": [-1.558, 47.214, -1.548, 47.223],
        "display_name": "Château des Ducs de Bretagne, Nantes, France",
    },
    "château des ducs de bretagne": {
        "lat": 47.2184, "lon": -1.5534,
        "bbox": [-1.558, 47.214, -1.548, 47.223],
        "display_name": "Château des Ducs de Bretagne, Nantes, France",
    },
    "paris": {
        "lat": 48.8566, "lon": 2.3522,
        "bbox": [2.22, 48.81, 2.47, 48.90],
        "display_name": "Paris, Île-de-France, France",
    },
    "dakar": {
        "lat": 14.7167, "lon": -17.4677,
        "bbox": [-17.55, 14.63, -17.33, 14.82],
        "display_name": "Dakar, Région de Dakar, Sénégal",
    },
    "lyon": {
        "lat": 45.7640, "lon": 4.8357,
        "bbox": [4.77, 45.70, 4.90, 45.81],
        "display_name": "Lyon, Auvergne-Rhône-Alpes, France",
    },
    "marseille": {
        "lat": 43.2965, "lon": 5.3698,
        "bbox": [5.27, 43.21, 5.42, 43.37],
        "display_name": "Marseille, Provence-Alpes-Côte d'Azur, France",
    },
    "bordeaux": {
        "lat": 44.8378, "lon": -0.5792,
        "bbox": [-0.68, 44.78, -0.49, 44.90],
        "display_name": "Bordeaux, Nouvelle-Aquitaine, France",
    },
    "toulouse": {
        "lat": 43.6047, "lon": 1.4442,
        "bbox": [1.34, 43.53, 1.55, 43.68],
        "display_name": "Toulouse, Occitanie, France",
    },
    "saint-louis": {
        "lat": 16.0179, "lon": -16.4897,
        "bbox": [-16.55, 15.95, -16.42, 16.08],
        "display_name": "Saint-Louis, Sénégal",
    },
    "abidjan": {
        "lat": 5.3600, "lon": -4.0083,
        "bbox": [-4.10, 5.26, -3.89, 5.45],
        "display_name": "Abidjan, Côte d'Ivoire",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# CACHE NOMINATIM (SQLite local)
# ═══════════════════════════════════════════════════════════════════════════════

def _init_cache() -> sqlite3.Connection:
    """Initialise la base SQLite de cache géocodage."""
    CACHE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(CACHE_DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS geocode_cache (
            key        TEXT PRIMARY KEY,
            query      TEXT NOT NULL,
            result     TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_key ON geocode_cache(key)")
    conn.commit()
    return conn


def _cache_key(query: str) -> str:
    return hashlib.md5(query.lower().strip().encode()).hexdigest()


def _get_cached(query: str) -> Optional[dict]:
    try:
        conn = _init_cache()
        key = _cache_key(query)
        row = conn.execute(
            "SELECT result, created_at FROM geocode_cache WHERE key = ?", (key,)
        ).fetchone()
        conn.close()
        if row:
            result, created_at = row
            if time.time() - created_at < GEOCODE_CACHE_TTL_SEC:
                return json.loads(result)
            # TTL expiré → supprimer
            conn = _init_cache()
            conn.execute("DELETE FROM geocode_cache WHERE key = ?", (key,))
            conn.commit()
            conn.close()
    except Exception as e:
        log.debug(f"Cache read error: {e}")
    return None


def _set_cached(query: str, result: dict):
    try:
        conn = _init_cache()
        key = _cache_key(query)
        conn.execute(
            "INSERT OR REPLACE INTO geocode_cache(key, query, result, created_at) VALUES(?,?,?,?)",
            (key, query, json.dumps(result), int(time.time()))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug(f"Cache write error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION BBOX
# ═══════════════════════════════════════════════════════════════════════════════

class BboxValidationError(ValueError):
    """Erreur de validation bbox — bloquante."""
    pass


class BboxWarning(UserWarning):
    """Avertissement bbox — corrigé automatiquement."""
    pass


def validate_bbox(
    args: dict,
    max_area: float = MAX_BBOX_AREA_DEG2,
    auto_fix: bool = True,
) -> dict:
    """
    Valide et corrige une bbox WGS84.

    Vérifie :
      - Présence des 4 valeurs (xmin, ymin, xmax, ymax)
      - Bornes WGS84 (-180..180, -90..90)
      - Cohérence (xmin < xmax, ymin < ymax)
      - Bbox inversée (lon/lat échangés) → correction automatique
      - Superficie max → recadrage sur le centroïde
      - Bbox dégénérée (trop petite, < 0.0001°)

    Args:
        args     : dict avec xmin/ymin/xmax/ymax
        max_area : superficie max en degrés carrés
        auto_fix : True = corriger silencieusement, False = lever une exception

    Returns:
        args avec bbox éventuellement corrigée
    """
    xmin = args.get("xmin")
    ymin = args.get("ymin")
    xmax = args.get("xmax")
    ymax = args.get("ymax")

    # ── Présence ──────────────────────────────────────────────────────────────
    missing = [k for k, v in {"xmin":xmin,"ymin":ymin,"xmax":xmax,"ymax":ymax}.items() if v is None]
    if missing:
        # Si center+radius présent → sera converti ailleurs
        if args.get("center_lon") is not None or args.get("center_lat") is not None:
            return args
        raise BboxValidationError(
            f"Paramètres bbox manquants : {missing}. "
            "Fournis xmin/ymin/xmax/ymax ou center_lon/center_lat/radius_m"
        )

    xmin, ymin, xmax, ymax = float(xmin), float(ymin), float(xmax), float(ymax)

    # ── Détection bbox lon/lat échangés ───────────────────────────────────────
    # Cas : le LLM a mis les latitudes en xmin/xmax et vice-versa
    # Heuristique : si abs(xmin) <= 90 et abs(ymin) > 90 → probablement inversé
    if abs(xmin) <= 90 and abs(ymin) > 90:
        log.warning("BBox lon/lat probablement échangés — correction automatique")
        xmin, ymin = ymin, xmin
        xmax, ymax = ymax, xmax

    # ── Bornes WGS84 ──────────────────────────────────────────────────────────
    if not (-180 <= xmin <= 180) or not (-180 <= xmax <= 180):
        if auto_fix:
            xmin = max(-180, min(180, xmin))
            xmax = max(-180, min(180, xmax))
            log.warning(f"Longitude corrigée : [{xmin:.4f},{xmax:.4f}]")
        else:
            raise BboxValidationError(
                f"Longitude hors limites WGS84 : xmin={xmin}, xmax={xmax}"
            )

    if not (-90 <= ymin <= 90) or not (-90 <= ymax <= 90):
        if auto_fix:
            ymin = max(-90, min(90, ymin))
            ymax = max(-90, min(90, ymax))
            log.warning(f"Latitude corrigée : [{ymin:.4f},{ymax:.4f}]")
        else:
            raise BboxValidationError(
                f"Latitude hors limites WGS84 : ymin={ymin}, ymax={ymax}"
            )

    # ── Cohérence min/max ─────────────────────────────────────────────────────
    if xmin >= xmax:
        if auto_fix:
            xmin, xmax = min(xmin, xmax) - 0.01, max(xmin, xmax) + 0.01
            log.warning(f"xmin >= xmax corrigé : [{xmin:.4f},{xmax:.4f}]")
        else:
            raise BboxValidationError(f"xmin ({xmin}) >= xmax ({xmax})")

    if ymin >= ymax:
        if auto_fix:
            ymin, ymax = min(ymin, ymax) - 0.01, max(ymin, ymax) + 0.01
            log.warning(f"ymin >= ymax corrigé : [{ymin:.4f},{ymax:.4f}]")
        else:
            raise BboxValidationError(f"ymin ({ymin}) >= ymax ({ymax})")

    # ── Superficie max ────────────────────────────────────────────────────────
    area = (xmax - xmin) * (ymax - ymin)
    if area > max_area:
        cx = (xmin + xmax) / 2
        cy = (ymin + ymax) / 2
        half = math.sqrt(max_area) / 2
        log.warning(
            f"BBox trop large ({area:.1f}°²  > max {max_area}°²), "
            f"recadrée autour de ({cx:.4f},{cy:.4f})"
        )
        xmin = round(cx - half, 6)
        xmax = round(cx + half, 6)
        ymin = round(cy - half, 6)
        ymax = round(cy + half, 6)
        # Re-vérifier les bornes après recadrage
        xmin = max(-180, xmin); xmax = min(180, xmax)
        ymin = max(-90,  ymin); ymax = min(90,  ymax)

    # ── Bbox dégénérée (trop petite) ──────────────────────────────────────────
    if (xmax - xmin) < 0.0001 or (ymax - ymin) < 0.0001:
        # Élargir à ~10m minimum
        cx = (xmin + xmax) / 2
        cy = (ymin + ymax) / 2
        xmin, xmax = cx - 0.0001, cx + 0.0001
        ymin, ymax = cy - 0.0001, cy + 0.0001
        log.warning("BBox dégénérée élargie à minimum 10m")

    args.update({"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax})
    return args


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION RAYON
# ═══════════════════════════════════════════════════════════════════════════════

def validate_radius(args: dict) -> dict:
    """
    Valide le rayon en mètres pour une recherche centre + radius.
    Corrige automatiquement les valeurs hors limites.
    """
    radius = args.get("radius_m")
    if radius is None:
        return args

    radius = float(radius)

    if radius <= 0:
        log.warning(f"radius_m={radius} invalide, défaut {DEFAULT_RADIUS_M}m")
        args["radius_m"] = DEFAULT_RADIUS_M
        return args

    if radius < MIN_RADIUS_M:
        log.warning(f"radius_m={radius} trop petit, corrigé à {MIN_RADIUS_M}m")
        args["radius_m"] = MIN_RADIUS_M
        return args

    if radius > MAX_RADIUS_M:
        log.warning(f"radius_m={radius}m trop grand, réduit à {MAX_RADIUS_M}m")
        args["radius_m"] = MAX_RADIUS_M
        return args

    # Vérifier que center_lon/center_lat sont présents
    if args.get("center_lon") is None or args.get("center_lat") is None:
        raise BboxValidationError(
            "center_lon et center_lat requis avec radius_m"
        )

    # Vérifier les coordonnées du centre
    lon = float(args["center_lon"])
    lat = float(args["center_lat"])
    if not (-180 <= lon <= 180):
        raise BboxValidationError(f"center_lon invalide : {lon}")
    if not (-90 <= lat <= 90):
        raise BboxValidationError(f"center_lat invalide : {lat}")

    args["radius_m"] = radius
    return args


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION COORDONNÉES
# ═══════════════════════════════════════════════════════════════════════════════

def validate_coords(coord: list, label: str = "") -> list[float]:
    """
    Valide un couple [lon, lat] ou [lat, lon].
    Détecte automatiquement l'ordre si ambiguïté.

    Returns: [lon, lat] normalisé
    """
    if not coord or len(coord) < 2:
        raise BboxValidationError(
            f"Coordonnée {label} invalide : {coord} (attendu [lon, lat])"
        )

    a, b = float(coord[0]), float(coord[1])

    # Détection ordre lon/lat vs lat/lon
    # Heuristique : lat ∈ [-90,90], lon ∈ [-180,180]
    # Si a > 90 ou a < -90 → c'est forcément une longitude
    if abs(a) > 90 and abs(b) <= 90:
        lon, lat = a, b
    elif abs(b) > 90 and abs(a) <= 90:
        # Ordre inversé (lat, lon) → corriger
        log.warning(f"Coordonnée {label} : ordre lat/lon détecté, correction automatique")
        lon, lat = b, a
    else:
        # Ambiguïté : les deux dans [-90,90] → supposer [lon, lat] standard
        lon, lat = a, b

    if not (-180 <= lon <= 180):
        raise BboxValidationError(f"Longitude {label} invalide : {lon}")
    if not (-90 <= lat <= 90):
        raise BboxValidationError(f"Latitude {label} invalide : {lat}")

    return [round(lon, 6), round(lat, 6)]


# ═══════════════════════════════════════════════════════════════════════════════
# BBOX DEPUIS CENTRE + RAYON
# ═══════════════════════════════════════════════════════════════════════════════

def bbox_from_center(lon: float, lat: float, radius_m: float) -> dict:
    """
    Convertit un centre + rayon en mètres → bbox WGS84.
    Utilise la formule haversine inversée pour précision.

    Returns: dict avec xmin, ymin, xmax, ymax
    """
    # 1 degré de latitude ≈ 111320m (constant)
    lat_deg = radius_m / 111320.0
    # 1 degré de longitude varie selon la latitude
    lon_deg = radius_m / (111320.0 * math.cos(math.radians(lat)))

    return {
        "xmin": round(lon - lon_deg, 6),
        "xmax": round(lon + lon_deg, 6),
        "ymin": round(lat - lat_deg, 6),
        "ymax": round(lat + lat_deg, 6),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# GÉOCODAGE NOMINATIM + CACHE
# ═══════════════════════════════════════════════════════════════════════════════

def geocode_to_bbox(query: str) -> Optional[dict]:
    """
    Géocode un toponyme → bbox + coordonnées centre.
    Stratégie :
      1. Cache mémoire (villes fréquentes)
      2. Cache SQLite local (TTL 30 jours)
      3. Nominatim API

    Returns:
        {
            "lat": float, "lon": float,
            "bbox": [xmin, ymin, xmax, ymax],
            "display_name": str,
            "source": "memory_cache" | "sqlite_cache" | "nominatim"
        }
        ou None si introuvable
    """
    query_clean = query.strip()
    query_lower = query_clean.lower()

    # ── 1. Cache mémoire (villes fréquentes) ──────────────────────────────────
    for city_key, city_data in _CITY_CACHE.items():
        if city_key in query_lower:
            log.info(f"Géocodage cache mémoire : '{query}' → {city_key}")
            return {**city_data, "source": "memory_cache"}

    # ── 2. Cache SQLite ────────────────────────────────────────────────────────
    cached = _get_cached(query_clean)
    if cached:
        log.info(f"Géocodage cache SQLite : '{query}'")
        return {**cached, "source": "sqlite_cache"}

    # ── 3. Nominatim ───────────────────────────────────────────────────────────
    log.info(f"Géocodage Nominatim : '{query}'")
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={
                "q":              query_clean,
                "format":         "json",
                "limit":          1,
                "addressdetails": 1,
            },
            headers={"User-Agent": NOMINATIM_UA},
            timeout=GEOCODE_TIMEOUT,
        )
        resp.raise_for_status()
        results = resp.json()

        if not results:
            log.warning(f"Nominatim : aucun résultat pour '{query}'")
            return None

        r = results[0]
        bb = r.get("boundingbox", [])

        # boundingbox Nominatim = [lat_min, lat_max, lon_min, lon_max]
        if len(bb) == 4:
            bbox = [float(bb[2]), float(bb[0]), float(bb[3]), float(bb[1])]
        else:
            lat = float(r["lat"])
            lon = float(r["lon"])
            bbox_dict = bbox_from_center(lon, lat, 2000)
            bbox = [bbox_dict["xmin"], bbox_dict["ymin"], bbox_dict["xmax"], bbox_dict["ymax"]]

        result = {
            "lat":          round(float(r["lat"]), 6),
            "lon":          round(float(r["lon"]), 6),
            "bbox":         [round(v, 6) for v in bbox],
            "display_name": r.get("display_name", query),
        }

        # Mettre en cache SQLite
        _set_cached(query_clean, result)

        return {**result, "source": "nominatim"}

    except requests.exceptions.Timeout:
        log.error(f"Nominatim timeout pour '{query}'")
        return None
    except Exception as e:
        log.error(f"Erreur géocodage '{query}': {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# COHÉRENCE BBOX
# ═══════════════════════════════════════════════════════════════════════════════

def bbox_is_coherent(bbox: dict, context: str = "") -> tuple[bool, str]:
    """
    Vérifie la cohérence sémantique d'une bbox.
    Détecte les cas suspects sans bloquer.

    Returns: (is_ok, warning_message)
    """
    xmin = bbox.get("xmin", 0)
    ymin = bbox.get("ymin", 0)
    xmax = bbox.get("xmax", 0)
    ymax = bbox.get("ymax", 0)

    warnings = []

    # Bbox au milieu de l'océan (pas de données Overture)
    cx = (xmin + xmax) / 2
    cy = (ymin + ymax) / 2
    # Zones océaniques connues (heuristique grossière)
    if (-40 < cx < -10) and (-30 < cy < 0):
        warnings.append("Zone probablement océanique (Atlantique sud) — peu de données attendues")

    # Bbox au pôle
    if ymax > 85 or ymin < -85:
        warnings.append("Zone polaire — données limitées")

    # Bbox minuscule (< 10m)
    area = (xmax - xmin) * (ymax - ymin)
    if area < 1e-8:
        warnings.append(f"BBox très petite ({area:.2e}°²) — probablement 0 résultats")

    if warnings:
        msg = f"[{context}] " + "; ".join(warnings) if context else "; ".join(warnings)
        return False, msg

    return True, ""


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION COMPLÈTE — POINT D'ENTRÉE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def validate_geo_args(args: dict, max_area: float = MAX_BBOX_AREA_DEG2) -> dict:
    """
    Validation complète pour un appel geo_data ou routing.
    Pipeline : radius → bbox → cohérence
    """
    # 1. Valider le rayon si présent
    if args.get("radius_m") is not None or args.get("center_lon") is not None:
        args = validate_radius(args)

        # Convertir center+radius → bbox si pas déjà de bbox
        if args.get("center_lon") is not None and args.get("xmin") is None:
            lon = float(args["center_lon"])
            lat = float(args["center_lat"])
            radius = float(args.get("radius_m", DEFAULT_RADIUS_M))
            bbox = bbox_from_center(lon, lat, radius)
            args.update(bbox)

    # 2. Valider la bbox résultante
    if args.get("xmin") is not None:
        args = validate_bbox(args, max_area=max_area)

        # 3. Vérification de cohérence (non-bloquante)
        ok, warning = bbox_is_coherent(args)
        if not ok:
            log.warning(f"BBox suspecte : {warning}")

    return args
