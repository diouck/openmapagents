"""
mcp_servers/mcp_osm.py — MCP Server OpenStreetMap / Overpass API
================================================================
Réutilise la logique de osm_routes.py existant.
Endpoint Overpass : https://overpass-api.de/api/interpreter

Tools exposés :
    query_overpass          → requête Overpass QL libre (validée)
    get_amenities           → équipements par type (école, hôpital, etc.)
    get_street_network      → réseau routier OSM dans une bbox
    get_water_features      → cours d'eau, lacs, zones humides
    get_green_spaces        → parcs, forêts, espaces verts
    get_public_transport    → arrêts bus/tram/métro, lignes
    get_landuse             → occupation du sol OSM
    get_buildings_osm       → bâtiments OSM (complément Overture)
"""

import os
import json
import hashlib
import logging
import asyncio
import time
from pathlib import Path
from typing import Optional

import httpx

log = logging.getLogger("mcp_osm")

# ── Config ────────────────────────────────────────────────────
OVERPASS_URL   = os.getenv("OVERPASS_URL", "https://overpass-api.de/api/interpreter")
OVERPASS_TIMEOUT = int(os.getenv("OVERPASS_TIMEOUT", "30"))
MAX_FEATURES   = int(os.getenv("OSM_MAX_FEATURES", "5000"))
CACHE_DIR      = Path(os.getenv("CACHE_DIR", "./data/cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── Mapping amenity types fr → OSM tags ──────────────────────
AMENITY_MAP = {
    # Santé
    "hôpital":       {"amenity": "hospital"},
    "hopital":       {"amenity": "hospital"},
    "pharmacie":     {"amenity": "pharmacy"},
    "médecin":       {"amenity": "doctors"},
    "clinique":      {"amenity": "clinic"},
    "dentiste":      {"amenity": "dentist"},
    # Éducation
    "école":         {"amenity": "school"},
    "ecole":         {"amenity": "school"},
    "université":    {"amenity": "university"},
    "universite":    {"amenity": "university"},
    "maternelle":    {"amenity": "kindergarten"},
    "bibliothèque":  {"amenity": "library"},
    "bibliotheque":  {"amenity": "library"},
    # Restauration
    "restaurant":    {"amenity": "restaurant"},
    "café":          {"amenity": "cafe"},
    "cafe":          {"amenity": "cafe"},
    "bar":           {"amenity": "bar"},
    "fast_food":     {"amenity": "fast_food"},
    # Services
    "banque":        {"amenity": "bank"},
    "distributeur":  {"amenity": "atm"},
    "poste":         {"amenity": "post_office"},
    "mairie":        {"amenity": "townhall"},
    "police":        {"amenity": "police"},
    "pompiers":      {"amenity": "fire_station"},
    # Transport
    "parking":       {"amenity": "parking"},
    "vélo":          {"amenity": "bicycle_parking"},
    "velo":          {"amenity": "bicycle_parking"},
    "station_service":{"amenity": "fuel"},
    # Loisirs
    "cinema":        {"amenity": "cinema"},
    "cinéma":        {"amenity": "cinema"},
    "théâtre":       {"amenity": "theatre"},
    "theatre":       {"amenity": "theatre"},
    "musée":         {"amenity": "museum"},
    "musee":         {"amenity": "museum"},
    # Transports publics
    "arrêt_bus":     {"public_transport": "stop_position", "bus": "yes"},
    "arret_bus":     {"public_transport": "stop_position", "bus": "yes"},
    "station_métro": {"station": "subway"},
    "station_metro": {"station": "subway"},
    "gare":          {"railway": "station"},
    "tram":          {"railway": "tram_stop"},
}

# ── Types de réseau routier OSM ───────────────────────────────
HIGHWAY_TYPES = {
    "motorway":   "Autoroute",
    "trunk":      "Voie rapide",
    "primary":    "Route principale",
    "secondary":  "Route secondaire",
    "tertiary":   "Route tertiaire",
    "residential":"Voie résidentielle",
    "cycleway":   "Piste cyclable",
    "footway":    "Sentier piéton",
    "path":       "Chemin",
    "service":    "Voie de service",
}

# ── Occupation du sol OSM ─────────────────────────────────────
LANDUSE_TYPES = {
    "residential":   "Résidentiel",
    "commercial":    "Commercial",
    "industrial":    "Industriel",
    "retail":        "Commerce",
    "forest":        "Forêt",
    "farmland":      "Terres agricoles",
    "grass":         "Pelouse",
    "meadow":        "Prairie",
    "cemetery":      "Cimetière",
    "recreation_ground": "Aire de loisirs",
    "military":      "Militaire",
    "education":     "Éducation",
    "religious":     "Religieux",
}


def _validate_bbox(bbox: list) -> tuple:
    if len(bbox) != 4:
        raise ValueError("bbox doit être [xmin, ymin, xmax, ymax]")
    xmin, ymin, xmax, ymax = [float(v) for v in bbox]
    if not (-180 <= xmin < xmax <= 180) or not (-90 <= ymin < ymax <= 90):
        raise ValueError(f"bbox invalide: {bbox}")
    area = (xmax - xmin) * (ymax - ymin) * 111 * 111
    if area > 2000:
        raise ValueError(
            f"bbox trop grande ({area:.0f} km²). Max ~2000 km² pour Overpass."
        )
    # Overpass bbox = (S, W, N, E)
    return ymin, xmin, ymax, xmax


def _overpass_bbox(bbox: list) -> str:
    """Convertit [xmin,ymin,xmax,ymax] → '(S,W,N,E)' pour Overpass."""
    s, w, n, e = _validate_bbox(bbox)
    return f"({s},{w},{n},{e})"


def _run_overpass(query: str) -> Optional[dict]:
    """Exécute une requête Overpass et retourne le JSON brut."""
    cache_key  = hashlib.md5(query.encode()).hexdigest()
    cache_path = CACHE_DIR / f"osm_{cache_key}.json"

    if cache_path.exists():
        log.debug(f"[OSM Cache hit] {cache_key[:8]}")
        cached = json.loads(cache_path.read_text())
        cached["_cache_hit"] = True
        return cached

    try:
        resp = httpx.post(
            OVERPASS_URL,
            data={"data": query},
            timeout=OVERPASS_TIMEOUT,
            headers={"User-Agent": "OpenMapAgents/1.0"},
        )
        if resp.status_code != 200:
            log.warning(f"Overpass {resp.status_code}: {resp.text[:200]}")
            return None
        data = resp.json()
        cache_path.write_text(json.dumps(data))
        return data
    except Exception as e:
        log.error(f"Overpass request error: {e}")
        return None


def _overpass_to_geojson(data: dict, label: str = "") -> dict:
    """
    Convertit le résultat Overpass en GeoJSON FeatureCollection.
    Gère nodes (points), ways (lignes/polygones), relations.
    """
    if not data or "elements" not in data:
        return {"type": "FeatureCollection", "features": []}

    elements = data["elements"]
    # Index des nodes pour reconstruire les ways
    nodes = {
        el["id"]: (el["lon"], el["lat"])
        for el in elements if el["type"] == "node"
    }

    features = []
    for el in elements:
        tags = el.get("tags", {})
        props = {
            "osm_id":   el["id"],
            "osm_type": el["type"],
            **tags,
        }

        if el["type"] == "node" and "lat" in el:
            geom = {
                "type":        "Point",
                "coordinates": [el["lon"], el["lat"]],
            }
            features.append({
                "type":       "Feature",
                "geometry":   geom,
                "properties": props,
            })

        elif el["type"] == "way" and "nodes" in el:
            coords = [nodes[nid] for nid in el["nodes"] if nid in nodes]
            if len(coords) < 2:
                continue
            # Polygone si fermé (premier = dernier node)
            if (el["nodes"][0] == el["nodes"][-1]
                    and len(coords) >= 4):
                geom = {
                    "type":        "Polygon",
                    "coordinates": [coords],
                }
            else:
                geom = {
                    "type":        "LineString",
                    "coordinates": coords,
                }
            features.append({
                "type":       "Feature",
                "geometry":   geom,
                "properties": props,
            })

    # Limiter le nombre de features
    if len(features) > MAX_FEATURES:
        log.warning(
            f"OSM: {len(features)} features → tronqué à {MAX_FEATURES}"
        )
        features = features[:MAX_FEATURES]

    return {
        "type":     "FeatureCollection",
        "features": features,
        "metadata": {
            "total":      len(features),
            "truncated":  len(data["elements"]) > MAX_FEATURES,
            "label":      label,
        },
    }


def _validate_overpass_query(query: str) -> str:
    """Validation basique d'une requête Overpass QL."""
    # Bloquer les requêtes trop larges sans bbox
    if "bbox" not in query and "({{bbox}})" not in query:
        dangerous = ["area", "rel", "way", "node"]
        for kw in dangerous:
            if f"{kw}(" in query.lower() and "bbox" not in query.lower():
                log.warning(f"Overpass query sans bbox détectée: {query[:100]}")
    # Bloquer les injections basiques
    forbidden = ["<script", "javascript:", "eval("]
    for f in forbidden:
        if f.lower() in query.lower():
            raise ValueError(f"Requête Overpass invalide: '{f}' interdit")
    return query


# ═══════════════════════════════════════════════════════════════
# OSM SERVER
# ═══════════════════════════════════════════════════════════════

class OsmServer:

    async def call(self, tool: str, args: dict) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._run, tool, args)

    def _run(self, tool: str, args: dict) -> dict:
        fn = {
            "query_overpass":       self.query_overpass,
            "get_amenities":        self.get_amenities,
            "get_street_network":   self.get_street_network,
            "get_water_features":   self.get_water_features,
            "get_green_spaces":     self.get_green_spaces,
            "get_public_transport": self.get_public_transport,
            "get_landuse":          self.get_landuse,
            "get_buildings_osm":    self.get_buildings_osm,
        }.get(tool)
        if not fn:
            return {"error": f"OSM tool inconnu: '{tool}'"}
        try:
            return fn(args)
        except ValueError as e:
            return {"error": str(e), "tool": tool}
        except Exception as e:
            log.error(f"OSM {tool}: {e}")
            return {"error": f"Erreur OSM: {e}", "tool": tool}

    # ─── QUERY OVERPASS ───────────────────────────────────────

    def query_overpass(self, a: dict) -> dict:
        """
        Requête Overpass QL libre (validée et mise en cache).

        Args:
            query:      requête Overpass QL complète
            bbox:       [xmin,ymin,xmax,ymax] — injecté dans {{bbox}} si présent
            layer_name: nom de la couche résultante

        Returns:
            GeoJSON FeatureCollection
        """
        query      = a.get("query", "")
        bbox       = a.get("bbox")
        layer_name = a.get("layer_name", "OSM Query")

        if not query:
            return {"error": "query Overpass QL requis"}

        try:
            _validate_overpass_query(query)
        except ValueError as e:
            return {"error": str(e)}

        # Injecter bbox si {{bbox}} présent dans la query
        if bbox and "{{bbox}}" in query:
            try:
                s, w, n, e = _validate_bbox(bbox)
                query = query.replace("{{bbox}}", f"{s},{w},{n},{e}")
            except ValueError as e:
                return {"error": str(e)}

        data    = _run_overpass(query)
        if not data:
            return {"error": "Overpass API indisponible ou requête invalide"}

        geojson = _overpass_to_geojson(data, layer_name)
        n       = len(geojson["features"])

        return {
            "action":        "add_layer",
            "geojson":       geojson,
            "layer_name":    f"{layer_name} ({n})",
            "feature_count": n,
            "provider":      "OSM/Overpass",
            "_cache_hit":    data.get("_cache_hit", False),
        }

    # ─── GET AMENITIES ────────────────────────────────────────

    def get_amenities(self, a: dict) -> dict:
        """
        Équipements OSM par type dans une bbox.

        Args:
            bbox:          [xmin, ymin, xmax, ymax]
            amenity_type:  pharmacie | hôpital | école | restaurant | ...
                           ou code OSM direct (ex: "amenity=pharmacy")
            limit:         max features (défaut 500)

        Returns:
            GeoJSON points des équipements
        """
        bbox         = a.get("bbox")
        amenity_type = a.get("amenity_type", a.get("category", "")).lower()
        limit        = min(int(a.get("limit", 500)), MAX_FEATURES)

        if not bbox:
            return {"error": "bbox requis: [xmin, ymin, xmax, ymax]"}

        ob = _overpass_bbox(bbox)

        # Résoudre le type
        osm_tags = AMENITY_MAP.get(amenity_type, {})

        if osm_tags:
            # Tag connu
            tag_filters = "".join(
                f'["{k}"="{v}"]' for k, v in osm_tags.items()
            )
            query = f"""
                [out:json][timeout:{OVERPASS_TIMEOUT}];
                (
                  node{tag_filters}{ob};
                  way{tag_filters}{ob};
                );
                out body geom {limit};
            """
        elif "=" in amenity_type:
            # Format direct "amenity=hospital"
            key, val = amenity_type.split("=", 1)
            query = f"""
                [out:json][timeout:{OVERPASS_TIMEOUT}];
                (
                  node["{key}"="{val}"]{ob};
                  way["{key}"="{val}"]{ob};
                );
                out body geom {limit};
            """
        else:
            # Recherche générique amenity
            query = f"""
                [out:json][timeout:{OVERPASS_TIMEOUT}];
                (
                  node["amenity"="{amenity_type}"]{ob};
                  way["amenity"="{amenity_type}"]{ob};
                );
                out body geom {limit};
            """

        data    = _run_overpass(query)
        if not data:
            return {"error": "Overpass indisponible"}

        geojson = _overpass_to_geojson(data, amenity_type)
        n       = len(geojson["features"])

        if n == 0:
            return {
                "error":       f"Aucun équipement '{amenity_type}' dans cette zone",
                "amenity_type": amenity_type,
                "bbox":        bbox,
                "suggestion":  (
                    f"Types disponibles: {list(AMENITY_MAP.keys())[:10]}"
                ),
            }

        return {
            "action":        "add_markers",
            "geojson":       geojson,
            "layer_name":    f"{amenity_type.capitalize()} ({n})",
            "amenity_type":  amenity_type,
            "feature_count": n,
            "provider":      "OSM/Overpass",
        }

    # ─── GET STREET NETWORK ───────────────────────────────────

    def get_street_network(self, a: dict) -> dict:
        """
        Réseau routier OSM dans une bbox.

        Args:
            bbox:          [xmin, ymin, xmax, ymax]
            network_type:  all | drive | walk | bike (défaut: all)
                           drive = motorway→tertiary
                           walk  = footway, pedestrian, steps
                           bike  = cycleway + vélo autorisé
            road_class:    filtre sur un type précis (ex: "primary")

        Returns:
            GeoJSON LineString du réseau
        """
        bbox         = a.get("bbox")
        network_type = a.get("network_type", "all")
        road_class   = a.get("road_class", "")

        if not bbox:
            return {"error": "bbox requis"}

        ob = _overpass_bbox(bbox)

        # Filtres par type de réseau
        if road_class:
            highway_filter = f'["highway"="{road_class}"]'
            ways_query = f'way{highway_filter}{ob};'
        elif network_type == "drive":
            types = "|".join([
                "motorway","trunk","primary","secondary",
                "tertiary","motorway_link","trunk_link",
                "primary_link","secondary_link","tertiary_link",
            ])
            ways_query = f'way["highway"~"^({types})$"]{ob};'
        elif network_type == "walk":
            types = "|".join([
                "footway","pedestrian","steps","path",
                "living_street","residential",
            ])
            ways_query = f'way["highway"~"^({types})$"]{ob};'
        elif network_type == "bike":
            ways_query = (
                f'way["highway"="cycleway"]{ob};\n'
                f'way["cycleway"]{ob};\n'
                f'way["bicycle"~"yes|designated"]{ob};'
            )
        else:  # all
            ways_query = f'way["highway"]{ob};'

        query = f"""
            [out:json][timeout:{OVERPASS_TIMEOUT}];
            (
              {ways_query}
            );
            out body geom;
        """

        data    = _run_overpass(query)
        if not data:
            return {"error": "Overpass indisponible"}

        geojson = _overpass_to_geojson(data, f"Routes {network_type}")
        n       = len(geojson["features"])

        return {
            "action":        "add_layer",
            "geojson":       geojson,
            "layer_name":    f"Réseau {network_type} ({n})",
            "network_type":  network_type,
            "road_class":    road_class or None,
            "feature_count": n,
            "provider":      "OSM/Overpass",
        }

    # ─── GET WATER FEATURES ───────────────────────────────────

    def get_water_features(self, a: dict) -> dict:
        """
        Cours d'eau, lacs, étangs, zones humides OSM.

        Args:
            bbox:         [xmin, ymin, xmax, ymax]
            water_type:   all | river | lake | stream | wetland (défaut: all)

        Returns:
            GeoJSON points + lignes + polygones eau
        """
        bbox       = a.get("bbox")
        water_type = a.get("water_type", "all")

        if not bbox:
            return {"error": "bbox requis"}

        ob = _overpass_bbox(bbox)

        if water_type == "river":
            q = (f'way["waterway"~"river|canal"]{ob};\n'
                 f'relation["waterway"~"river|canal"]{ob};')
        elif water_type == "lake":
            q = (f'way["natural"="water"]["water"~"lake|reservoir"]{ob};\n'
                 f'relation["natural"="water"]["water"~"lake|reservoir"]{ob};')
        elif water_type == "stream":
            q = f'way["waterway"~"stream|ditch|drain"]{ob};'
        elif water_type == "wetland":
            q = (f'way["natural"="wetland"]{ob};\n'
                 f'way["wetland"]{ob};')
        else:  # all
            q = (f'way["waterway"]{ob};\n'
                 f'way["natural"="water"]{ob};\n'
                 f'way["natural"="wetland"]{ob};\n'
                 f'relation["natural"="water"]{ob};')

        query = f"""
            [out:json][timeout:{OVERPASS_TIMEOUT}];
            (
              {q}
            );
            out body geom;
        """

        data    = _run_overpass(query)
        if not data:
            return {"error": "Overpass indisponible"}

        geojson = _overpass_to_geojson(data, f"Eau {water_type}")
        n       = len(geojson["features"])

        return {
            "action":        "add_layer",
            "geojson":       geojson,
            "layer_name":    f"Eau {water_type} ({n})",
            "water_type":    water_type,
            "feature_count": n,
            "provider":      "OSM/Overpass",
        }

    # ─── GET GREEN SPACES ─────────────────────────────────────

    def get_green_spaces(self, a: dict) -> dict:
        """
        Parcs, forêts, espaces verts OSM.

        Args:
            bbox:       [xmin, ymin, xmax, ymax]
            green_type: all | park | forest | garden | playground (défaut: all)

        Returns:
            GeoJSON polygones espaces verts
        """
        bbox       = a.get("bbox")
        green_type = a.get("green_type", "all")

        if not bbox:
            return {"error": "bbox requis"}

        ob = _overpass_bbox(bbox)

        if green_type == "park":
            q = (f'way["leisure"="park"]{ob};\n'
                 f'relation["leisure"="park"]{ob};')
        elif green_type == "forest":
            q = (f'way["landuse"="forest"]{ob};\n'
                 f'way["natural"="wood"]{ob};')
        elif green_type == "garden":
            q = (f'way["leisure"="garden"]{ob};\n'
                 f'way["leisure"="botanical_garden"]{ob};')
        elif green_type == "playground":
            q = f'way["leisure"="playground"]{ob};\nnode["leisure"="playground"]{ob};'
        else:  # all
            q = (f'way["leisure"~"park|garden|nature_reserve|playground"]{ob};\n'
                 f'way["landuse"~"forest|grass|meadow|village_green"]{ob};\n'
                 f'way["natural"~"wood|scrub|heath"]{ob};\n'
                 f'relation["leisure"~"park|garden|nature_reserve"]{ob};')

        query = f"""
            [out:json][timeout:{OVERPASS_TIMEOUT}];
            (
              {q}
            );
            out body geom;
        """

        data    = _run_overpass(query)
        if not data:
            return {"error": "Overpass indisponible"}

        geojson = _overpass_to_geojson(data, f"Espaces verts {green_type}")
        n       = len(geojson["features"])

        return {
            "action":        "add_layer",
            "geojson":       geojson,
            "layer_name":    f"Espaces verts {green_type} ({n})",
            "green_type":    green_type,
            "feature_count": n,
            "provider":      "OSM/Overpass",
        }

    # ─── GET PUBLIC TRANSPORT ─────────────────────────────────

    def get_public_transport(self, a: dict) -> dict:
        """
        Réseau de transport en commun OSM.

        Args:
            bbox:       [xmin, ymin, xmax, ymax]
            mode:       all | bus | tram | metro | train | ferry (défaut: all)

        Returns:
            GeoJSON arrêts + lignes transport
        """
        bbox = a.get("bbox")
        mode = a.get("mode", "all")

        if not bbox:
            return {"error": "bbox requis"}

        ob = _overpass_bbox(bbox)

        if mode == "bus":
            q = (f'node["highway"="bus_stop"]{ob};\n'
                 f'node["public_transport"="stop_position"]["bus"="yes"]{ob};\n'
                 f'way["route"="bus"]{ob};')
        elif mode == "tram":
            q = (f'node["railway"="tram_stop"]{ob};\n'
                 f'way["railway"="tram"]{ob};')
        elif mode == "metro":
            q = (f'node["station"="subway"]{ob};\n'
                 f'node["railway"="subway_entrance"]{ob};\n'
                 f'way["railway"="subway"]{ob};')
        elif mode == "train":
            q = (f'node["railway"~"station|halt"]{ob};\n'
                 f'way["railway"~"rail|narrow_gauge"]{ob};')
        elif mode == "ferry":
            q = (f'node["amenity"="ferry_terminal"]{ob};\n'
                 f'way["route"="ferry"]{ob};')
        else:  # all
            q = (f'node["highway"="bus_stop"]{ob};\n'
                 f'node["railway"~"station|tram_stop|halt"]{ob};\n'
                 f'node["station"="subway"]{ob};\n'
                 f'node["amenity"="ferry_terminal"]{ob};\n'
                 f'way["railway"~"rail|tram|subway"]{ob};')

        query = f"""
            [out:json][timeout:{OVERPASS_TIMEOUT}];
            (
              {q}
            );
            out body geom;
        """

        data    = _run_overpass(query)
        if not data:
            return {"error": "Overpass indisponible"}

        geojson = _overpass_to_geojson(data, f"Transport {mode}")
        n       = len(geojson["features"])

        return {
            "action":        "add_layer",
            "geojson":       geojson,
            "layer_name":    f"Transport {mode} ({n})",
            "mode":          mode,
            "feature_count": n,
            "provider":      "OSM/Overpass",
        }

    # ─── GET LANDUSE ──────────────────────────────────────────

    def get_landuse(self, a: dict) -> dict:
        """
        Occupation du sol OSM.

        Args:
            bbox:         [xmin, ymin, xmax, ymax]
            landuse_type: all | residential | commercial | industrial |
                          forest | farmland | ... (défaut: all)

        Returns:
            GeoJSON polygones occupation du sol
        """
        bbox         = a.get("bbox")
        landuse_type = a.get("landuse_type", "all")

        if not bbox:
            return {"error": "bbox requis"}

        ob = _overpass_bbox(bbox)

        if landuse_type == "all":
            q = (f'way["landuse"]{ob};\n'
                 f'relation["landuse"]{ob};')
        else:
            lu = LANDUSE_TYPES.get(landuse_type, landuse_type)
            q  = (f'way["landuse"="{landuse_type}"]{ob};\n'
                  f'relation["landuse"="{landuse_type}"]{ob};')

        query = f"""
            [out:json][timeout:{OVERPASS_TIMEOUT}];
            (
              {q}
            );
            out body geom;
        """

        data    = _run_overpass(query)
        if not data:
            return {"error": "Overpass indisponible"}

        geojson = _overpass_to_geojson(data, f"Landuse {landuse_type}")
        n       = len(geojson["features"])

        return {
            "action":        "add_layer",
            "geojson":       geojson,
            "layer_name":    f"Occupation sol {landuse_type} ({n})",
            "landuse_type":  landuse_type,
            "feature_count": n,
            "available_types": list(LANDUSE_TYPES.keys()),
            "provider":      "OSM/Overpass",
        }

    # ─── GET BUILDINGS OSM ────────────────────────────────────

    def get_buildings_osm(self, a: dict) -> dict:
        """
        Bâtiments OSM (complément à Overture pour détails tag OSM).

        Args:
            bbox:           [xmin, ymin, xmax, ymax]
            building_type:  all | residential | commercial | church |
                            school | hospital | ... (défaut: all)
            with_height:    bool — filtrer seulement ceux avec hauteur

        Returns:
            GeoJSON polygones bâtiments avec tags OSM
        """
        bbox          = a.get("bbox")
        building_type = a.get("building_type", "all")
        with_height   = a.get("with_height", False)

        if not bbox:
            return {"error": "bbox requis"}

        ob = _overpass_bbox(bbox)

        height_filter = '["height"]' if with_height else ""

        if building_type == "all":
            q = (f'way["building"]{height_filter}{ob};\n'
                 f'relation["building"]{height_filter}{ob};')
        else:
            q = (f'way["building"="{building_type}"]{height_filter}{ob};\n'
                 f'relation["building"="{building_type}"]{height_filter}{ob};')

        query = f"""
            [out:json][timeout:{OVERPASS_TIMEOUT}];
            (
              {q}
            );
            out body geom;
        """

        data    = _run_overpass(query)
        if not data:
            return {"error": "Overpass indisponible"}

        geojson = _overpass_to_geojson(data, f"Bâtiments OSM {building_type}")
        n       = len(geojson["features"])

        return {
            "action":        "add_layer",
            "geojson":       geojson,
            "layer_name":    f"Bâtiments OSM {building_type} ({n})",
            "building_type": building_type,
            "with_height":   with_height,
            "feature_count": n,
            "provider":      "OSM/Overpass",
        }
