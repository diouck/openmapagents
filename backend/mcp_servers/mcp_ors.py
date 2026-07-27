"""
mcp_servers/mcp_ors.py — MCP Server OpenRouteService
=====================================================
Remplace compute_isochrone et compute_route de l'agent.py existant.
Utilise ORS en priorité, fallback Mapbox si ORS_API_KEY absent.

Tools exposés :
    compute_isochrone   → polygones isochrone depuis un point
    compute_route       → itinéraire entre waypoints
    compute_matrix      → matrice distances/durées N×N
    compute_isochrones_multi → isochrones multi-intervalles (5,10,15min)
"""

import os
import logging
import asyncio
import math
from typing import Optional

import httpx

log = logging.getLogger("mcp_ors")

# ── Config ────────────────────────────────────────────────────
ORS_API_KEY   = os.getenv("ORS_API_KEY", "")
ORS_BASE_URL  = os.getenv("ORS_BASE_URL", "https://api.openrouteservice.org")
MAPBOX_TOKEN  = os.getenv("MAPBOX_ACCESS_TOKEN", "")
TIMEOUT       = 10  # secondes

# ── Profils ORS → Mapbox ──────────────────────────────────────
PROFILE_ORS = {
    "foot":  "foot-walking",
    "bike":  "cycling-regular",
    "car":   "driving-car",
    "hike":  "foot-hiking",
    "wheelchair": "wheelchair",
}
PROFILE_MAPBOX = {
    "foot":  "walking",
    "bike":  "cycling",
    "car":   "driving",
    "hike":  "walking",
    "wheelchair": "walking",
}

# ── Palettes isochrones ───────────────────────────────────────
ISOCHRONE_COLORS = [
    "#1a9850",  # 1er intervalle (plus proche)
    "#91cf60",
    "#d9ef8b",
    "#fee08b",
    "#fc8d59",
    "#d73027",  # dernier intervalle (plus loin)
]


def _validate_coords(lng: float, lat: float):
    if not (-180 <= lng <= 180):
        raise ValueError(f"Longitude invalide: {lng}")
    if not (-90 <= lat <= 90):
        raise ValueError(f"Latitude invalide: {lat}")


def _validate_duration(minutes: int):
    if minutes <= 0 or minutes > 120:
        raise ValueError(f"Durée invalide: {minutes}min (max 120min)")


# ═══════════════════════════════════════════════════════════════
# ORS SERVER
# ═══════════════════════════════════════════════════════════════

class OrsServer:
    """
    MCP Server ORS/Mapbox.
    Priorité ORS si ORS_API_KEY présent, sinon Mapbox.
    """

    def __init__(self):
        self._use_ors = bool(ORS_API_KEY)
        log.info(
            f"OrsServer init — "
            f"{'ORS' if self._use_ors else 'Mapbox (fallback)'}"
        )

    async def call(self, tool: str, args: dict) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._run, tool, args)

    def _run(self, tool: str, args: dict) -> dict:
        fn = {
            "compute_isochrone":        self.isochrone,
            "compute_route":            self.route,
            "compute_matrix":           self.matrix,
            "compute_isochrones_multi": self.isochrones_multi,
        }.get(tool)
        if not fn:
            return {"error": f"ORS tool inconnu: '{tool}'"}
        try:
            return fn(args)
        except ValueError as e:
            return {"error": str(e), "tool": tool}
        except Exception as e:
            log.error(f"ORS {tool}: {e}")
            return {"error": f"Erreur ORS: {e}", "tool": tool}

    # ─── ISOCHRONE ────────────────────────────────────────────

    def isochrone(self, a: dict) -> dict:
        """
        Isochrone depuis un point.

        Args:
            center:       [lng, lat]
            time_minutes: durée max en minutes (défaut 10, max 120)
            profile:      foot | bike | car | hike | wheelchair
            intervals:    liste de durées [5,10,15] — si fourni,
                          génère plusieurs polygones
        """
        center       = a.get("center", [])
        time_minutes = a.get("time_minutes", 10)
        profile      = a.get("profile", "foot")
        intervals    = a.get("intervals")

        if not center or len(center) != 2:
            return {"error": "center requis: [longitude, latitude]"}

        lng, lat = float(center[0]), float(center[1])
        _validate_coords(lng, lat)
        _validate_duration(time_minutes)

        # Si intervals fournis → déléguer à isochrones_multi
        if intervals:
            return self.isochrones_multi({
                "center": center,
                "intervals": intervals,
                "profile": profile,
            })

        if self._use_ors:
            return self._isochrone_ors(lng, lat, time_minutes, profile)
        else:
            return self._isochrone_mapbox(lng, lat, time_minutes, profile)

    def _isochrone_ors(self, lng, lat, minutes, profile) -> dict:
        """Isochrone via ORS API."""
        prof = PROFILE_ORS.get(profile, "foot-walking")
        url  = f"{ORS_BASE_URL}/v2/isochrones/{prof}"
        headers = {
            "Authorization": ORS_API_KEY,
            "Content-Type":  "application/json",
        }
        body = {
            "locations":   [[lng, lat]],
            "range":       [minutes * 60],  # secondes
            "range_type":  "time",
            "attributes":  ["area", "reachfactor"],
            "smoothing":   0.5,
        }
        resp = httpx.post(url, json=body, headers=headers, timeout=TIMEOUT)
        if resp.status_code != 200:
            return {
                "error": f"ORS isochrone error {resp.status_code}: {resp.text[:200]}"
            }
        data = resp.json()
        features = data.get("features", [])
        if not features:
            return {"error": "ORS n'a retourné aucune feature."}

        # Enrichir les propriétés
        for f in features:
            props = f.get("properties", {})
            props["duration_min"] = minutes
            props["profile"]      = profile
            props["center"]       = [lng, lat]
            f["properties"] = props

        geojson = {
            "type":     "FeatureCollection",
            "features": features,
        }
        return {
            "action":      "add_isochrone",
            "geojson":     geojson,
            "layer_name":  f"Isochrone {minutes}min {profile}",
            "center":      [lng, lat],
            "duration":    minutes,
            "profile":     profile,
            "color":       ISOCHRONE_COLORS[0],
            "opacity":     0.35,
            "provider":    "ORS",
        }

    def _isochrone_mapbox(self, lng, lat, minutes, profile) -> dict:
        """Isochrone via Mapbox Isochrone API (fallback)."""
        if not MAPBOX_TOKEN:
            return {
                "error": "Ni ORS_API_KEY ni MAPBOX_ACCESS_TOKEN configurés. "
                         "Ajoutez l'un des deux dans .env"
            }
        prof = PROFILE_MAPBOX.get(profile, "walking")
        url  = (
            f"https://api.mapbox.com/isochrone/v1/mapbox/{prof}"
            f"/{lng},{lat}"
            f"?contours_minutes={minutes}"
            f"&polygons=true"
            f"&access_token={MAPBOX_TOKEN}"
        )
        resp = httpx.get(url, timeout=TIMEOUT)
        if resp.status_code != 200:
            return {
                "error": f"Mapbox isochrone error {resp.status_code}: {resp.text[:200]}"
            }
        geojson = resp.json()
        return {
            "action":     "add_isochrone",
            "geojson":    geojson,
            "layer_name": f"Isochrone {minutes}min {profile}",
            "center":     [lng, lat],
            "duration":   minutes,
            "profile":    profile,
            "color":      ISOCHRONE_COLORS[0],
            "opacity":    0.35,
            "provider":   "Mapbox",
        }

    # ─── ISOCHRONES MULTI-INTERVALLES ─────────────────────────

    def isochrones_multi(self, a: dict) -> dict:
        """
        Isochrones multi-intervalles depuis un point.
        Retourne N polygones concentriques (un par intervalle).

        Args:
            center:    [lng, lat]
            intervals: liste de durées en minutes ex: [5, 10, 15]
            profile:   foot | bike | car
        """
        center    = a.get("center", [])
        intervals = a.get("intervals", [5, 10, 15])
        profile   = a.get("profile", "foot")

        if not center or len(center) != 2:
            return {"error": "center requis: [longitude, latitude]"}

        lng, lat = float(center[0]), float(center[1])
        _validate_coords(lng, lat)

        # Valider chaque intervalle
        for t in intervals:
            _validate_duration(t)

        intervals_sorted = sorted(intervals)

        if self._use_ors:
            return self._isochrones_multi_ors(lng, lat, intervals_sorted, profile)
        else:
            return self._isochrones_multi_mapbox(lng, lat, intervals_sorted, profile)

    def _isochrones_multi_ors(self, lng, lat, intervals, profile) -> dict:
        prof = PROFILE_ORS.get(profile, "foot-walking")
        url  = f"{ORS_BASE_URL}/v2/isochrones/{prof}"
        headers = {
            "Authorization": ORS_API_KEY,
            "Content-Type":  "application/json",
        }
        body = {
            "locations":  [[lng, lat]],
            "range":      [t * 60 for t in intervals],
            "range_type": "time",
            "attributes": ["area"],
            "smoothing":  0.5,
        }
        resp = httpx.post(url, json=body, headers=headers, timeout=TIMEOUT)
        if resp.status_code != 200:
            return {
                "error": f"ORS error {resp.status_code}: {resp.text[:200]}"
            }
        data     = resp.json()
        features = data.get("features", [])
        if not features:
            return {"error": "ORS n'a retourné aucune feature."}

        # Associer couleurs aux intervalles
        colors = ISOCHRONE_COLORS[:len(features)]
        for i, f in enumerate(features):
            f["properties"]["color"]   = colors[i % len(colors)]
            f["properties"]["profile"] = profile
            f["properties"]["center"]  = [lng, lat]

        geojson = {"type": "FeatureCollection", "features": features}
        return {
            "action":     "add_isochrone",
            "geojson":    geojson,
            "layer_name": f"Isochrones {intervals[0]}–{intervals[-1]}min {profile}",
            "center":     [lng, lat],
            "intervals":  intervals,
            "profile":    profile,
            "colors":     colors,
            "opacity":    0.35,
            "provider":   "ORS",
        }

    def _isochrones_multi_mapbox(self, lng, lat, intervals, profile) -> dict:
        if not MAPBOX_TOKEN:
            return {"error": "MAPBOX_ACCESS_TOKEN non configuré."}
        prof          = PROFILE_MAPBOX.get(profile, "walking")
        minutes_str   = ",".join(str(t) for t in intervals)
        colors_str    = ",".join(
            c.lstrip("#") for c in ISOCHRONE_COLORS[:len(intervals)]
        )
        url = (
            f"https://api.mapbox.com/isochrone/v1/mapbox/{prof}"
            f"/{lng},{lat}"
            f"?contours_minutes={minutes_str}"
            f"&contours_colors={colors_str}"
            f"&polygons=true"
            f"&access_token={MAPBOX_TOKEN}"
        )
        resp = httpx.get(url, timeout=TIMEOUT)
        if resp.status_code != 200:
            return {
                "error": f"Mapbox error {resp.status_code}: {resp.text[:200]}"
            }
        geojson = resp.json()
        return {
            "action":     "add_isochrone",
            "geojson":    geojson,
            "layer_name": f"Isochrones {intervals[0]}–{intervals[-1]}min {profile}",
            "center":     [lng, lat],
            "intervals":  intervals,
            "profile":    profile,
            "colors":     ISOCHRONE_COLORS[:len(intervals)],
            "opacity":    0.35,
            "provider":   "Mapbox",
        }

    # ─── ROUTE ────────────────────────────────────────────────

    def route(self, a: dict) -> dict:
        """
        Itinéraire entre waypoints.

        Args:
            waypoints: liste de [lng, lat] — min 2 points
            profile:   foot | bike | car | hike | wheelchair
            instructions: bool (défaut True) — inclure virages

        Returns:
            GeoJSON LineString + distance (km) + durée (min) + instructions
        """
        waypoints    = a.get("waypoints", [])
        profile      = a.get("profile", "foot")
        instructions = a.get("instructions", True)

        if len(waypoints) < 2:
            return {"error": "waypoints requis: minimum 2 points [[lng,lat],[lng,lat]]"}

        for wp in waypoints:
            if len(wp) != 2:
                return {"error": f"Waypoint invalide: {wp} — format [lng, lat]"}
            _validate_coords(float(wp[0]), float(wp[1]))

        if self._use_ors:
            return self._route_ors(waypoints, profile, instructions)
        else:
            return self._route_mapbox(waypoints, profile)

    def _route_ors(self, waypoints, profile, instructions) -> dict:
        prof = PROFILE_ORS.get(profile, "foot-walking")
        url  = f"{ORS_BASE_URL}/v2/directions/{prof}/geojson"
        headers = {
            "Authorization": ORS_API_KEY,
            "Content-Type":  "application/json",
        }
        body = {
            "coordinates":        [[float(wp[0]), float(wp[1])] for wp in waypoints],
            "instructions":       instructions,
            "instructions_format":"text",
            "language":           "fr",
            "units":              "km",
        }
        resp = httpx.post(url, json=body, headers=headers, timeout=TIMEOUT)
        if resp.status_code != 200:
            return {
                "error": f"ORS route error {resp.status_code}: {resp.text[:200]}"
            }
        data     = resp.json()
        features = data.get("features", [])
        if not features:
            return {"error": "ORS n'a retourné aucune route."}

        props    = features[0].get("properties", {})
        summary  = props.get("summary", {})
        segments = props.get("segments", [])

        # Extraire les étapes de navigation
        steps = []
        for seg in segments:
            for step in seg.get("steps", []):
                steps.append({
                    "instruction": step.get("instruction", ""),
                    "distance_km": round(step.get("distance", 0) / 1000, 2),
                    "duration_min": round(step.get("duration", 0) / 60, 1),
                    "name": step.get("name", ""),
                })

        return {
            "action":       "add_route",
            "geojson":      data,
            "layer_name":   f"Itinéraire {profile} ({summary.get('distance',0)/1000:.1f}km)",
            "distance_km":  round(summary.get("distance", 0) / 1000, 2),
            "duration_min": round(summary.get("duration", 0) / 60, 1),
            "profile":      profile,
            "waypoints":    waypoints,
            "steps":        steps[:50],   # max 50 étapes
            "provider":     "ORS",
        }

    def _route_mapbox(self, waypoints, profile) -> dict:
        if not MAPBOX_TOKEN:
            return {"error": "MAPBOX_ACCESS_TOKEN non configuré."}
        prof       = PROFILE_MAPBOX.get(profile, "walking")
        coords_str = ";".join(f"{wp[0]},{wp[1]}" for wp in waypoints)
        url = (
            f"https://api.mapbox.com/directions/v5/mapbox/{prof}"
            f"/{coords_str}"
            f"?steps=true&geometries=geojson&language=fr"
            f"&access_token={MAPBOX_TOKEN}"
        )
        resp = httpx.get(url, timeout=TIMEOUT)
        if resp.status_code != 200:
            return {
                "error": f"Mapbox route error {resp.status_code}: {resp.text[:200]}"
            }
        data   = resp.json()
        routes = data.get("routes", [])
        if not routes:
            return {"error": "Mapbox n'a retourné aucune route."}

        route = routes[0]
        geom  = route.get("geometry", {})

        # Convertir en GeoJSON FeatureCollection
        geojson = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "distance_km":  round(route.get("distance", 0) / 1000, 2),
                    "duration_min": round(route.get("duration", 0) / 60, 1),
                    "profile":      profile,
                }
            }]
        }

        # Extraire les étapes
        steps = []
        for leg in route.get("legs", []):
            for step in leg.get("steps", []):
                instr = step.get("maneuver", {}).get("instruction", "")
                steps.append({
                    "instruction":  instr,
                    "distance_km":  round(step.get("distance", 0) / 1000, 2),
                    "duration_min": round(step.get("duration", 0) / 60, 1),
                    "name":         step.get("name", ""),
                })

        return {
            "action":       "add_route",
            "geojson":      geojson,
            "layer_name":   f"Itinéraire {profile} ({route.get('distance',0)/1000:.1f}km)",
            "distance_km":  round(route.get("distance", 0) / 1000, 2),
            "duration_min": round(route.get("duration", 0) / 60, 1),
            "profile":      profile,
            "waypoints":    waypoints,
            "steps":        steps[:50],
            "provider":     "Mapbox",
        }

    # ─── MATRICE DISTANCES ────────────────────────────────────

    def matrix(self, a: dict) -> dict:
        """
        Matrice distances/durées entre N points.

        Args:
            locations: liste de [lng, lat] — min 2, max 25
            profile:   foot | bike | car
            metric:    "duration" | "distance" | "both" (défaut: both)

        Returns:
            matrice NxN durées (min) et distances (km)
        """
        locations = a.get("locations", [])
        profile   = a.get("profile", "car")
        metric    = a.get("metric", "both")

        if len(locations) < 2:
            return {"error": "locations requis: minimum 2 points"}
        if len(locations) > 25:
            return {"error": "Maximum 25 points pour la matrice ORS"}

        for loc in locations:
            _validate_coords(float(loc[0]), float(loc[1]))

        if not self._use_ors:
            return {
                "error": "La matrice nécessite ORS_API_KEY. "
                         "Mapbox matrix non implémenté en fallback."
            }

        prof    = PROFILE_ORS.get(profile, "driving-car")
        url     = f"{ORS_BASE_URL}/v2/matrix/{prof}"
        headers = {
            "Authorization": ORS_API_KEY,
            "Content-Type":  "application/json",
        }
        metrics = []
        if metric in ("duration", "both"): metrics.append("duration")
        if metric in ("distance", "both"): metrics.append("distance")

        body = {
            "locations": [[float(l[0]), float(l[1])] for l in locations],
            "metrics":   metrics,
            "units":     "km",
        }
        resp = httpx.post(url, json=body, headers=headers, timeout=TIMEOUT)
        if resp.status_code != 200:
            return {
                "error": f"ORS matrix error {resp.status_code}: {resp.text[:200]}"
            }
        data = resp.json()

        # Convertir durées secondes → minutes, distances m → km
        result = {"locations": locations, "profile": profile}

        if "durations" in data:
            result["durations_min"] = [
                [round(v / 60, 1) if v is not None else None for v in row]
                for row in data["durations"]
            ]
        if "distances" in data:
            result["distances_km"] = [
                [round(v / 1000, 2) if v is not None else None for v in row]
                for row in data["distances"]
            ]

        result.update({
            "action":     "show_matrix",
            "layer_name": f"Matrice {profile} ({len(locations)} points)",
            "n_points":   len(locations),
            "provider":   "ORS",
        })
        return result
