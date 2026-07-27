"""
mcp_servers/mcp_maptiler.py — MCP Server MapTiler / Élévation
=============================================================
Sources d'élévation par priorité :
  1. IGN RGE Alti 1m (France métropolitaine — ton elevation_routes.py existant)
  2. MapTiler Elevation API (si MAPTILER_API_KEY configuré)
  3. Open-Elevation API (fallback gratuit, mondial)
  4. SRTM via GEE (dernier recours — voir mcp_gee.py)

Tools exposés :
    get_elevation_profile   → profil altimétrique le long d'une ligne
    get_elevation_point     → altitude d'un point unique
    get_elevation_grid      → grille d'altitudes sur une bbox
    get_contours            → courbes de niveau (GeoJSON)
    get_hillshade_url       → URL tuiles hillshade MapTiler
    get_slope_analysis      → analyse de pente sur une ligne/bbox
"""

import os
import json
import math
import hashlib
import logging
import asyncio
from pathlib import Path
from typing import Optional

import httpx

log = logging.getLogger("mcp_maptiler")

# ── Config ────────────────────────────────────────────────────
MAPTILER_KEY     = os.getenv("MAPTILER_API_KEY", "")
MAPTILER_BASE    = "https://api.maptiler.com"
OPEN_ELEV_BASE   = "https://api.open-elevation.com/api/v1"
IGN_ALTI_BASE    = os.getenv(
    "IGN_ALTI_URL",
    "https://wxs.ign.fr/altimetrie/geoportail/r/wms"
)
TIMEOUT          = int(os.getenv("MAPTILER_TIMEOUT", "15"))
CACHE_DIR        = Path(os.getenv("CACHE_DIR", "./data/cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── Limites ───────────────────────────────────────────────────
MAX_PROFILE_POINTS = int(os.getenv("MAX_PROFILE_POINTS", "500"))
MAX_GRID_POINTS    = int(os.getenv("MAX_GRID_POINTS",    "2500"))
FRANCE_BBOX        = (-5.5, 41.2, 9.7, 51.2)   # métropole + Corse


def _in_france(lng: float, lat: float) -> bool:
    """Vérifie si un point est en France métropolitaine."""
    xmin, ymin, xmax, ymax = FRANCE_BBOX
    return xmin <= lng <= xmax and ymin <= lat <= ymax


def _bbox_in_france(bbox: list) -> bool:
    xmin, ymin, xmax, ymax = bbox
    cx = (xmin + xmax) / 2
    cy = (ymin + ymax) / 2
    return _in_france(cx, cy)


def _cache_key(params: dict) -> str:
    return hashlib.md5(
        json.dumps(params, sort_keys=True, default=str).encode()
    ).hexdigest()


def _get_cached(key: str) -> Optional[dict]:
    path = CACHE_DIR / f"elev_{key}.json"
    if path.exists():
        try:
            data = json.loads(path.read_text())
            log.debug(f"[Elev Cache hit] {key[:8]}")
            return data
        except Exception:
            pass
    return None


def _set_cached(key: str, data: dict):
    path = CACHE_DIR / f"elev_{key}.json"
    try:
        path.write_text(json.dumps(data, default=str))
    except Exception as e:
        log.warning(f"Elevation cache write error: {e}")


def _haversine(p1: list, p2: list) -> float:
    """Distance haversine en mètres entre deux points [lng, lat]."""
    R = 6_371_000
    lat1, lat2 = math.radians(p1[1]), math.radians(p2[1])
    dlat = math.radians(p2[1] - p1[1])
    dlng = math.radians(p2[0] - p1[0])
    a = (math.sin(dlat/2)**2
         + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _interpolate_line(coords: list, n_points: int) -> list:
    """
    Génère n_points équidistants le long d'une polyligne.
    coords = [[lng, lat], ...]
    """
    if len(coords) < 2:
        return coords

    # Calculer la longueur totale
    total_len = sum(
        _haversine(coords[i], coords[i+1])
        for i in range(len(coords)-1)
    )
    if total_len == 0:
        return coords

    step = total_len / (n_points - 1)
    pts  = [coords[0]]
    accumulated = 0.0
    seg_idx = 0

    for i in range(1, n_points - 1):
        target = i * step
        while seg_idx < len(coords) - 2:
            seg_len = _haversine(coords[seg_idx], coords[seg_idx+1])
            if accumulated + seg_len >= target:
                break
            accumulated += seg_len
            seg_idx += 1
        if seg_idx >= len(coords) - 1:
            break
        ratio = (target - accumulated) / max(
            _haversine(coords[seg_idx], coords[seg_idx+1]), 1e-9
        )
        ratio = max(0.0, min(1.0, ratio))
        p0, p1 = coords[seg_idx], coords[seg_idx+1]
        pts.append([
            p0[0] + ratio * (p1[0] - p0[0]),
            p0[1] + ratio * (p1[1] - p0[1]),
        ])

    pts.append(coords[-1])
    return pts


def _elevations_open_elevation(points: list) -> list:
    """
    Récupère les altitudes via Open-Elevation API (gratuit, mondial).
    points = [[lng, lat], ...]
    Retourne [altitude, ...] dans le même ordre.
    """
    batch_size = 100
    elevations = []

    for i in range(0, len(points), batch_size):
        batch = points[i:i+batch_size]
        body  = {
            "locations": [
                {"latitude": p[1], "longitude": p[0]}
                for p in batch
            ]
        }
        try:
            resp = httpx.post(
                f"{OPEN_ELEV_BASE}/lookup",
                json=body,
                timeout=TIMEOUT,
            )
            if resp.status_code == 200:
                data = resp.json()
                elevations.extend(
                    r.get("elevation", 0)
                    for r in data.get("results", [])
                )
            else:
                elevations.extend([None] * len(batch))
        except Exception as e:
            log.warning(f"Open-Elevation error: {e}")
            elevations.extend([None] * len(batch))

    return elevations


def _elevations_maptiler(points: list) -> list:
    """
    Récupère les altitudes via MapTiler Elevation API.
    Requiert MAPTILER_API_KEY.
    """
    if not MAPTILER_KEY:
        return [None] * len(points)

    elevations = []
    batch_size = 50

    for i in range(0, len(points), batch_size):
        batch = points[i:i+batch_size]
        try:
            # MapTiler Elevation endpoint
            url = f"{MAPTILER_BASE}/elevation/at.json"
            params = {"key": MAPTILER_KEY}
            coords_str = "|".join(
                f"{p[1]},{p[0]}" for p in batch
            )
            params["latlng"] = coords_str
            resp = httpx.get(url, params=params, timeout=TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                elevations.extend(
                    r.get("ele", 0)
                    for r in data.get("results", [])
                )
            else:
                # Fallback open-elevation pour ce batch
                elevations.extend(_elevations_open_elevation(batch))
        except Exception as e:
            log.warning(f"MapTiler elevation error: {e}")
            elevations.extend(_elevations_open_elevation(batch))

    return elevations


def _get_elevations(points: list) -> list:
    """
    Récupère les altitudes — sélectionne automatiquement la source.
    Priorité : IGN (France) → MapTiler → Open-Elevation
    """
    if not points:
        return []

    # Vérifier si tous les points sont en France
    if all(_in_france(p[0], p[1]) for p in points):
        try:
            return _elevations_ign(points)
        except Exception as e:
            log.warning(f"IGN elevation failed: {e} → fallback")

    # MapTiler si clé disponible
    if MAPTILER_KEY:
        return _elevations_maptiler(points)

    # Open-Elevation fallback gratuit
    return _elevations_open_elevation(points)


def _elevations_ign(points: list) -> list:
    """
    Élévation via IGN RGE Alti (France uniquement).
    Réutilise la logique de elevation_routes.py existant.
    """
    try:
        from elevation_routes import get_elevations_ign
        return get_elevations_ign(points)
    except ImportError:
        pass

    # Fallback : appel direct API IGN alti
    elevations = []
    batch_size = 50
    for i in range(0, len(points), batch_size):
        batch = points[i:i+batch_size]
        lon_str = "|".join(str(p[0]) for p in batch)
        lat_str = "|".join(str(p[1]) for p in batch)
        try:
            resp = httpx.get(
                "https://wxs.ign.fr/altimetrie/geoportail/r/wms",
                params={
                    "SERVICE": "WMS", "VERSION": "1.3.0",
                    "REQUEST": "GetFeatureInfo",
                    "lon": lon_str, "lat": lat_str,
                    "zonly": "true", "indent": "false",
                },
                timeout=TIMEOUT,
            )
            if resp.status_code == 200:
                vals = resp.text.strip().split("|")
                elevations.extend(
                    float(v) if v not in ("", "null", "nan") else None
                    for v in vals
                )
            else:
                elevations.extend([None] * len(batch))
        except Exception as e:
            log.warning(f"IGN alti batch error: {e}")
            elevations.extend([None] * len(batch))

    return elevations


def _compute_slope_pct(elev: list, dist_m: list) -> list:
    """Calcule la pente en % entre points consécutifs."""
    slopes = [0.0]
    for i in range(1, len(elev)):
        if (elev[i] is not None and elev[i-1] is not None
                and dist_m[i] > dist_m[i-1]):
            dh = elev[i] - elev[i-1]
            dd = dist_m[i] - dist_m[i-1]
            slopes.append(round(dh / dd * 100, 2) if dd > 0 else 0.0)
        else:
            slopes.append(None)
    return slopes


# ═══════════════════════════════════════════════════════════════
# MAPTILER SERVER
# ═══════════════════════════════════════════════════════════════

class MaptilerServer:

    async def call(self, tool: str, args: dict) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._run, tool, args)

    def _run(self, tool: str, args: dict) -> dict:
        fn = {
            "get_elevation_profile": self.elevation_profile,
            "get_elevation_point":   self.elevation_point,
            "get_elevation_grid":    self.elevation_grid,
            "get_contours":          self.contours,
            "get_hillshade_url":     self.hillshade_url,
            "get_slope_analysis":    self.slope_analysis,
        }.get(tool)
        if not fn:
            return {"error": f"MapTiler tool inconnu: '{tool}'"}
        try:
            return fn(args)
        except ValueError as e:
            return {"error": str(e), "tool": tool}
        except Exception as e:
            log.error(f"MapTiler {tool}: {e}")
            return {"error": f"Erreur élévation: {e}", "tool": tool}

    # ─── ELEVATION PROFILE ────────────────────────────────────

    def elevation_profile(self, a: dict) -> dict:
        """
        Profil altimétrique le long d'une ligne.
        Réutilise la logique de elevation_routes.py existant.

        Args:
            coordinates: [[lng,lat], [lng,lat], ...] — polyligne
            n_points:    nb de points du profil (défaut 100, max 500)
            smoothing:   bool — lissage du profil (défaut True)

        Returns:
            profil altimétrique avec distances cumulées,
            dénivelés +/-, pente moyenne/max
        """
        coords    = a.get("coordinates") or a.get("coords", [])
        n_points  = min(int(a.get("n_points", 100)), MAX_PROFILE_POINTS)
        smoothing = a.get("smoothing", True)

        if len(coords) < 2:
            return {"error": "coordinates requis: minimum 2 points [[lng,lat],...]"}

        for p in coords:
            if len(p) < 2:
                return {"error": f"Point invalide: {p} — format [lng, lat]"}
            if not (-180 <= p[0] <= 180) or not (-90 <= p[1] <= 90):
                return {"error": f"Coordonnées hors limites: {p}"}

        # Vérifier cache
        ck = _cache_key({
            "tool": "profile", "coords": coords,
            "n": n_points, "smooth": smoothing
        })
        cached = _get_cached(ck)
        if cached:
            return {**cached, "_cache_hit": True}

        # Interpoler les points le long de la ligne
        interp = _interpolate_line(coords, n_points)

        # Récupérer les altitudes
        elevations = _get_elevations(interp)

        # Distances cumulées en mètres
        distances = [0.0]
        for i in range(1, len(interp)):
            distances.append(
                distances[-1] + _haversine(interp[i-1], interp[i])
            )

        # Lissage simple (moyenne mobile 3 points)
        if smoothing and len(elevations) > 4:
            smoothed = [elevations[0]]
            for i in range(1, len(elevations)-1):
                e0 = elevations[i-1] or 0
                e1 = elevations[i]   or 0
                e2 = elevations[i+1] or 0
                smoothed.append(round((e0 + e1 + e2) / 3, 1))
            smoothed.append(elevations[-1])
            elevations = smoothed

        # Statistiques
        valid_elev = [e for e in elevations if e is not None]
        elev_min   = round(min(valid_elev), 1) if valid_elev else None
        elev_max   = round(max(valid_elev), 1) if valid_elev else None
        total_dist = round(distances[-1])

        # Dénivelés positif / négatif
        deniv_pos = 0.0
        deniv_neg = 0.0
        for i in range(1, len(elevations)):
            if elevations[i] is not None and elevations[i-1] is not None:
                diff = elevations[i] - elevations[i-1]
                if diff > 0: deniv_pos += diff
                else:        deniv_neg += abs(diff)

        # Pentes
        slopes = _compute_slope_pct(elevations, distances)
        valid_slopes = [abs(s) for s in slopes if s is not None]
        slope_avg  = round(sum(valid_slopes)/len(valid_slopes), 1) if valid_slopes else 0
        slope_max  = round(max(valid_slopes), 1) if valid_slopes else 0

        # Construire le profil point par point
        profile = [
            {
                "distance_m": round(distances[i]),
                "elevation_m": round(elevations[i], 1) if elevations[i] is not None else None,
                "slope_pct":   slopes[i],
                "lng":         round(interp[i][0], 6),
                "lat":         round(interp[i][1], 6),
            }
            for i in range(len(interp))
        ]

        # Détecter source
        in_fr = _in_france(coords[0][0], coords[0][1])
        source = "IGN RGE Alti 1m" if in_fr else (
            "MapTiler" if MAPTILER_KEY else "Open-Elevation (SRTM 30m)"
        )

        result = {
            "action":       "show_elevation_profile",
            "profile":      profile,
            "stats": {
                "total_distance_m": total_dist,
                "total_distance_km": round(total_dist/1000, 2),
                "elevation_min_m":  elev_min,
                "elevation_max_m":  elev_max,
                "elevation_range_m": round(elev_max - elev_min, 1) if (elev_min and elev_max) else None,
                "denivele_positif_m": round(deniv_pos, 1),
                "denivele_negatif_m": round(deniv_neg, 1),
                "slope_avg_pct":    slope_avg,
                "slope_max_pct":    slope_max,
            },
            "n_points":     len(profile),
            "source":       source,
            "smoothed":     smoothing,
        }

        _set_cached(ck, result)
        return result

    # ─── ELEVATION POINT ──────────────────────────────────────

    def elevation_point(self, a: dict) -> dict:
        """
        Altitude d'un point unique.

        Args:
            lng / longitude
            lat / latitude

        Returns:
            elevation_m + source
        """
        lng = float(a.get("lng") or a.get("longitude", 0))
        lat = float(a.get("lat") or a.get("latitude",  0))

        if not (-180 <= lng <= 180) or not (-90 <= lat <= 90):
            return {"error": f"Coordonnées invalides: [{lng}, {lat}]"}

        ck = _cache_key({"tool": "point", "lng": round(lng,5), "lat": round(lat,5)})
        cached = _get_cached(ck)
        if cached:
            return {**cached, "_cache_hit": True}

        elevs  = _get_elevations([[lng, lat]])
        elev   = elevs[0] if elevs else None
        source = "IGN RGE Alti 1m" if _in_france(lng, lat) else (
            "MapTiler" if MAPTILER_KEY else "Open-Elevation (SRTM 30m)"
        )

        result = {
            "action":      "show_elevation_point",
            "elevation_m": round(float(elev), 1) if elev is not None else None,
            "lng":         lng,
            "lat":         lat,
            "source":      source,
        }

        _set_cached(ck, result)
        return result

    # ─── ELEVATION GRID ───────────────────────────────────────

    def elevation_grid(self, a: dict) -> dict:
        """
        Grille régulière d'altitudes sur une bbox.
        Utile pour visualiser le relief ou calculer des stats.

        Args:
            bbox:       [xmin, ymin, xmax, ymax]
            resolution: nb de cellules par côté (défaut 25, max 50)

        Returns:
            grille NxN d'altitudes + stats
        """
        bbox       = a.get("bbox")
        resolution = min(int(a.get("resolution", 25)), 50)

        if not bbox or len(bbox) != 4:
            return {"error": "bbox requis: [xmin, ymin, xmax, ymax]"}

        xmin, ymin, xmax, ymax = [float(v) for v in bbox]
        if not (-180 <= xmin < xmax <= 180) or not (-90 <= ymin < ymax <= 90):
            return {"error": f"bbox invalide: {bbox}"}

        # Générer la grille
        n = resolution
        lngs = [xmin + (xmax-xmin)*i/(n-1) for i in range(n)]
        lats = [ymin + (ymax-ymin)*j/(n-1) for j in range(n)]
        points = [[lng, lat] for lat in lats for lng in lngs]

        if len(points) > MAX_GRID_POINTS:
            return {"error": f"Grille trop grande ({len(points)} points). Max {MAX_GRID_POINTS}."}

        ck = _cache_key({"tool":"grid","bbox":bbox,"res":resolution})
        cached = _get_cached(ck)
        if cached:
            return {**cached, "_cache_hit": True}

        elevs = _get_elevations(points)

        # Restructurer en grille
        grid = []
        for j, lat in enumerate(lats):
            row = []
            for i, lng in enumerate(lngs):
                idx  = j * n + i
                elev = elevs[idx] if idx < len(elevs) else None
                row.append(round(float(elev), 1) if elev is not None else None)
            grid.append(row)

        valid = [e for e in elevs if e is not None]
        source = "IGN RGE Alti 1m" if _bbox_in_france(bbox) else (
            "MapTiler" if MAPTILER_KEY else "Open-Elevation (SRTM 30m)"
        )

        result = {
            "action":     "show_elevation_grid",
            "grid":       grid,
            "lngs":       [round(v, 6) for v in lngs],
            "lats":       [round(v, 6) for v in lats],
            "bbox":       bbox,
            "resolution": n,
            "stats": {
                "min_m":  round(min(valid), 1) if valid else None,
                "max_m":  round(max(valid), 1) if valid else None,
                "avg_m":  round(sum(valid)/len(valid), 1) if valid else None,
            },
            "source": source,
        }

        _set_cached(ck, result)
        return result

    # ─── CONTOURS ─────────────────────────────────────────────

    def contours(self, a: dict) -> dict:
        """
        Courbes de niveau GeoJSON pour une bbox.

        Stratégie :
          - MapTiler Contour Tiles si MAPTILER_API_KEY disponible
          - Sinon : tile URL SRTM contours MapTiler public
          - Ou tile URL via GEE (via mcp_gee.compute_slope)

        Args:
            bbox:       [xmin, ymin, xmax, ymax]
            interval_m: intervalle entre courbes en mètres
                        (5 | 10 | 20 | 50 | 100, défaut: 20)
            major_every: toutes les N courbes = courbe maîtresse (défaut: 5)

        Returns:
            tile_url des tuiles contours + layer_name
        """
        bbox       = a.get("bbox")
        interval   = int(a.get("interval_m", 20))
        major_every = int(a.get("major_every", 5))

        if not bbox:
            return {"error": "bbox requis"}

        valid_intervals = [5, 10, 20, 50, 100]
        if interval not in valid_intervals:
            interval = min(valid_intervals, key=lambda x: abs(x - interval))
            log.info(f"Interval ajusté à {interval}m")

        # MapTiler contour tiles
        if MAPTILER_KEY:
            # MapTiler Contour source
            tile_url = (
                f"https://api.maptiler.com/tiles/contours/{{z}}/{{x}}/{{y}}.pbf"
                f"?key={MAPTILER_KEY}"
            )
            return {
                "action":      "add_vector_layer",
                "tile_url":    tile_url,
                "layer_type":  "vector",
                "source_type": "vector",
                "layer_name":  f"Courbes de niveau {interval}m",
                "interval_m":  interval,
                "major_every": major_every,
                "style": {
                    "minor": {"color": "#8B7355", "width": 0.5, "opacity": 0.6},
                    "major": {"color": "#5C4033", "width": 1.2, "opacity": 0.9},
                },
                "provider":   "MapTiler",
            }

        # Fallback : tile URL raster SRTM via TF
        # (Terrain Tiles OpenFreeMap / Terrarium format)
        tile_url = (
            "https://s3.amazonaws.com/elevation-tiles-prod/terrarium"
            "/{z}/{x}/{y}.png"
        )
        return {
            "action":      "add_contour_layer",
            "tile_url":    tile_url,
            "layer_type":  "terrarium_raster",
            "layer_name":  f"Courbes de niveau {interval}m (SRTM)",
            "interval_m":  interval,
            "major_every": major_every,
            "bbox":        bbox,
            "provider":    "Terrarium/SRTM (libre)",
            "note":        (
                "Configurez MAPTILER_API_KEY pour des tuiles vectorielles "
                "de meilleure qualité."
            ),
        }

    # ─── HILLSHADE URL ────────────────────────────────────────

    def hillshade_url(self, a: dict) -> dict:
        """
        URL de tuiles hillshade (ombrage) pour MapLibre.

        Args:
            style:    standard | satellite_overlay | terrain (défaut: standard)
            opacity:  opacité de la couche 0-1 (défaut: 0.4)
            exaggeration: facteur d'exagération verticale (défaut: 1.5)

        Returns:
            tile_url + config MapLibre pour ajouter la couche hillshade
        """
        style         = a.get("style", "standard")
        opacity       = float(a.get("opacity", 0.4))
        exaggeration  = float(a.get("exaggeration", 1.5))

        if MAPTILER_KEY:
            # MapTiler hillshade de haute qualité
            tile_url = (
                f"https://api.maptiler.com/tiles/hillshade/{{z}}/{{x}}/{{y}}.webp"
                f"?key={MAPTILER_KEY}"
            )
            provider = "MapTiler"
        else:
            # Terrarium public (SRTM) — décodage côté client MapLibre
            tile_url = (
                "https://s3.amazonaws.com/elevation-tiles-prod/terrarium"
                "/{z}/{x}/{y}.png"
            )
            provider = "Terrarium/SRTM (libre)"

        return {
            "action":       "add_hillshade_layer",
            "tile_url":     tile_url,
            "layer_name":   f"Ombrage {style}",
            "opacity":      opacity,
            "exaggeration": exaggeration,
            "provider":     provider,
            # Config MapLibre GL terrain
            "maplibre_config": {
                "source_type":    "raster-dem" if MAPTILER_KEY else "raster-dem",
                "encoding":       "mapbox" if MAPTILER_KEY else "terrarium",
                "tile_size":      512 if MAPTILER_KEY else 256,
                "max_zoom":       13,
                "exaggeration":   exaggeration,
            },
        }

    # ─── SLOPE ANALYSIS ───────────────────────────────────────

    def slope_analysis(self, a: dict) -> dict:
        """
        Analyse de pente le long d'une ligne ou sur une grille bbox.

        Args:
            coordinates: [[lng,lat],...] — ligne (optionnel)
            bbox:        [xmin,ymin,xmax,ymax] — grille (optionnel)
            n_points:    résolution (défaut 50)
            classify:    bool — classifier les pentes par catégorie

        Returns:
            pentes + classification par segment/cellule
        """
        coords   = a.get("coordinates", [])
        bbox     = a.get("bbox")
        n_points = min(int(a.get("n_points", 50)), MAX_PROFILE_POINTS)
        classify = a.get("classify", True)

        if not coords and not bbox:
            return {"error": "coordinates ou bbox requis"}

        if coords and len(coords) >= 2:
            # Analyse sur une ligne
            profile_result = self.elevation_profile({
                "coordinates": coords,
                "n_points":    n_points,
                "smoothing":   False,
            })
            if "error" in profile_result:
                return profile_result

            profile = profile_result["profile"]
            slopes  = [p["slope_pct"] for p in profile]

            classified = []
            if classify:
                for i, s in enumerate(slopes):
                    if s is None:
                        cat = "inconnu"
                    elif abs(s) < 3:   cat = "plat"
                    elif abs(s) < 10:  cat = "doux"
                    elif abs(s) < 20:  cat = "modéré"
                    elif abs(s) < 35:  cat = "fort"
                    else:              cat = "très fort"
                    classified.append({
                        "distance_m":  profile[i]["distance_m"],
                        "slope_pct":   s,
                        "category":    cat,
                        "lng":         profile[i]["lng"],
                        "lat":         profile[i]["lat"],
                    })

            valid_s = [abs(s) for s in slopes if s is not None]
            return {
                "action":     "show_slope_analysis",
                "type":       "line",
                "segments":   classified or [
                    {"distance_m": p["distance_m"], "slope_pct": p["slope_pct"]}
                    for p in profile
                ],
                "stats": {
                    "slope_avg_pct": round(sum(valid_s)/len(valid_s), 1) if valid_s else 0,
                    "slope_max_pct": round(max(valid_s), 1) if valid_s else 0,
                    "slope_min_pct": round(min(valid_s), 1) if valid_s else 0,
                },
                "source":     profile_result.get("source"),
            }

        else:
            # Analyse sur une bbox → grille
            grid_result = self.elevation_grid({
                "bbox":       bbox,
                "resolution": min(n_points, 25),
            })
            if "error" in grid_result:
                return grid_result

            # Calculer pentes depuis la grille
            grid  = grid_result["grid"]
            lngs  = grid_result["lngs"]
            lats  = grid_result["lats"]
            n     = len(lngs)

            slope_grid = []
            for j in range(len(lats)):
                row = []
                for i in range(len(lngs)):
                    elev = grid[j][i]
                    if elev is None:
                        row.append(None)
                        continue
                    # Gradient central si voisins disponibles
                    dz_dx, dz_dy = 0.0, 0.0
                    if i > 0 and i < n-1 and grid[j][i-1] and grid[j][i+1]:
                        dx = _haversine([lngs[i-1], lats[j]], [lngs[i+1], lats[j]])
                        dz_dx = (grid[j][i+1] - grid[j][i-1]) / max(dx, 1)
                    if j > 0 and j < len(lats)-1 and grid[j-1][i] and grid[j+1][i]:
                        dy = _haversine([lngs[i], lats[j-1]], [lngs[i], lats[j+1]])
                        dz_dy = (grid[j+1][i] - grid[j-1][i]) / max(dy, 1)
                    slope_pct = round(
                        math.sqrt(dz_dx**2 + dz_dy**2) * 100, 1
                    )
                    row.append(slope_pct)
                slope_grid.append(row)

            flat = [v for row in slope_grid for v in row if v is not None]
            return {
                "action":      "show_slope_grid",
                "type":        "grid",
                "slope_grid":  slope_grid,
                "lngs":        lngs,
                "lats":        lats,
                "bbox":        bbox,
                "stats": {
                    "slope_avg_pct": round(sum(flat)/len(flat), 1) if flat else 0,
                    "slope_max_pct": round(max(flat), 1) if flat else 0,
                },
                "source": grid_result.get("source"),
            }
