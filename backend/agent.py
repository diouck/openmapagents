"""
Overture Maps Explorer — Agent Backend
LiteLLM multi-provider (Claude/OpenAI/Ollama/OpenRouter/DeepSeek/Mistral)
Tool calling pour requêtes Overture Maps via DuckDB
"""
import os
import json
import hashlib
import logging
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import duckdb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from litellm import completion

# 1. Imports en haut
from resilience.llm_resilience import get_resilient_client, resilience_router
from memory.session_memory import get_session_memory, memory_router

from observability.metrics import metrics_router


load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("overture-agent")

# ─── Orchestrateur multi-agent (RAG → Router → Agent → Validation) ───────────
try:
    from orchestrator import get_orchestrator, orchestrate
    ORCHESTRATOR_ENABLED = True
    log.info("✓ Orchestrateur multi-agent activé")
except ImportError as e:
    ORCHESTRATOR_ENABLED = False
    log.warning(f"Orchestrateur non disponible, mode legacy : {e}")

# ═══════════════════════════════════════════════════════════════
# CONFIG — tout vient du .env
# ═══════════════════════════════════════════════════════════════
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "claude")
OVERTURE_RELEASE = os.getenv("OVERTURE_RELEASE", "2026-03-18.0")
S3_BASE = f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"
S3_REGION = os.getenv("OVERTURE_S3_REGION", "us-west-2")
DUCKDB_MEMORY = os.getenv("DUCKDB_MEMORY", "4GB")
DUCKDB_THREADS = int(os.getenv("DUCKDB_THREADS", "4"))
CACHE_DIR = Path("./data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ─── Résolution du modèle LiteLLM ────────────────────────────
MODEL_MAP = {
    "claude": os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
    "openai": os.getenv("OPENAI_MODEL", "gpt-4o"),
    "ollama": os.getenv("OLLAMA_MODEL", "ollama/llama3.1"),
    "openrouter": os.getenv("OPENROUTER_MODEL", "openrouter/anthropic/claude-sonnet-4"),
    "deepseek": os.getenv("DEEPSEEK_MODEL", "deepseek/deepseek-chat"),
    "mistral": os.getenv("MISTRAL_MODEL", "mistral/mistral-large-latest"),
}
LLM_MODEL = MODEL_MAP.get(LLM_PROVIDER, "claude-sonnet-4-20250514")

log.info(f"LLM Provider: {LLM_PROVIDER} → Model: {LLM_MODEL}")

THEMES = {
    "places": {"types": ["place"], "columns": "id, names.primary AS name, categories.primary AS category, confidence, addresses[1].freeform AS address, ST_AsGeoJSON(geometry) AS geom_json"},
    "buildings": {"types": ["building"], "columns": "id, names.primary AS name, height, num_floors, class, ST_AsGeoJSON(geometry) AS geom_json"},
    "transportation": {"types": ["segment"], "columns": "id, class, subtype, ST_AsGeoJSON(geometry) AS geom_json"},
    "divisions": {"types": ["division_area"], "columns": "id, names.primary AS name, subtype, country, ST_AsGeoJSON(geometry) AS geom_json"},
    "addresses": {"types": ["address"], "columns": "id, address_levels, ST_AsGeoJSON(geometry) AS geom_json"},
}

# ═══════════════════════════════════════════════════════════════
# DUCKDB ENGINE
# ═══════════════════════════════════════════════════════════════
class DuckDBEngine:
    def __init__(self):
        self.conn = None

    def connect(self):
        self.conn = duckdb.connect(":memory:")
        self.conn.execute("INSTALL spatial; LOAD spatial;")
        self.conn.execute("INSTALL httpfs; LOAD httpfs;")
        try:
            self.conn.execute("INSTALL h3 FROM community; LOAD h3;")
        except Exception:
            pass
        self.conn.execute(f"SET s3_region='{S3_REGION}';")
        self.conn.execute(f"SET memory_limit='{DUCKDB_MEMORY}';")
        self.conn.execute(f"SET threads={DUCKDB_THREADS};")
        log.info("DuckDB connected with spatial + httpfs + h3")
        return self

    def query(self, sql: str):
        return self.conn.execute(sql).fetchdf()

    def close(self):
        if self.conn:
            self.conn.close()

db = DuckDBEngine()

# ═══════════════════════════════════════════════════════════════
# TOOL DEFINITIONS (format OpenAI function calling)
# ═══════════════════════════════════════════════════════════════
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "geocode",
            "description": "Convert a place name, address, or landmark into geographic coordinates. ALWAYS use this FIRST when the user mentions a specific place, monument, address, or landmark (e.g. 'Chateau des Ducs de Bretagne', '15 rue de la Paix Paris'). Returns lat/lon that you can then use to build a tight bbox for query_overture.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Place name, address, or landmark to geocode"},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_overture",
            "description": "Query Overture Maps data by theme and bounding box. Returns GeoJSON features. You can provide EITHER a bbox OR a center+radius. Use clip_to_layer to automatically keep only features INSIDE an existing polygon layer (isochrone, buffer, etc).",
            "parameters": {
                "type": "object",
                "properties": {
                    "theme": {"type": "string", "enum": list(THEMES.keys()), "description": "Overture Maps theme"},
                    "xmin": {"type": "number", "description": "West longitude (optional if center_lon+radius_m provided)"},
                    "ymin": {"type": "number", "description": "South latitude (optional if center_lat+radius_m provided)"},
                    "xmax": {"type": "number", "description": "East longitude (optional if center_lon+radius_m provided)"},
                    "ymax": {"type": "number", "description": "North latitude (optional if center_lat+radius_m provided)"},
                    "center_lon": {"type": "number", "description": "Center longitude — use with radius_m"},
                    "center_lat": {"type": "number", "description": "Center latitude — use with radius_m"},
                    "radius_m": {"type": "number", "description": "Search radius in meters. Converted to bbox internally."},
                    "category": {"type": "string", "description": "POI category filter (restaurant, pharmacy, school...). Only for places."},
                    "name_filter": {"type": "string", "description": "Filter by name (ILIKE pattern)"},
                    "min_confidence": {"type": "number", "description": "Minimum confidence score 0-1. Only for places."},
                    "min_height": {"type": "number", "description": "Min building height in meters. Only for buildings."},
                    "max_height": {"type": "number", "description": "Max building height in meters. Only for buildings."},
                    "limit": {"type": "integer", "description": "Max features to return. Default 500."},
                    "clip_to_layer": {"type": "string", "description": "IMPORTANT: Name of an existing polygon layer to clip results to. Use this when user asks for features WITHIN/INSIDE an isochrone, buffer, or drawn polygon. The query will load features in the bbox of that layer, then automatically clip to keep only those inside the polygon."},
                },
                "required": ["theme"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "set_layer_style",
            "description": "Change the visual style of a layer on the map.",
            "parameters": {
                "type": "object",
                "properties": {
                    "layer_id": {"type": "string", "description": "ID of the layer to style"},
                    "color": {"type": "string", "description": "Fill color hex (#ff6600)"},
                    "opacity": {"type": "number", "description": "Opacity 0-1"},
                    "radius": {"type": "number", "description": "Point radius in pixels"},
                    "stroke_color": {"type": "string", "description": "Stroke color hex"},
                    "stroke_width": {"type": "number", "description": "Stroke width px"},
                },
                "required": ["layer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fly_to",
            "description": "Move the map camera. Use when user mentions a city or area.",
            "parameters": {
                "type": "object",
                "properties": {
                    "longitude": {"type": "number"},
                    "latitude": {"type": "number"},
                    "zoom": {"type": "number", "description": "Zoom 1-20. City=12, neighborhood=14, street=16, building=18"},
                    "pitch": {"type": "number", "description": "Camera pitch 0-60 degrees"},
                },
                "required": ["longitude", "latitude"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "remove_layer",
            "description": "Remove a layer from the map.",
            "parameters": {
                "type": "object",
                "properties": {
                    "layer_id": {"type": "string", "description": "Layer ID or 'all' to clear everything"},
                },
                "required": ["layer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_layer_stats",
            "description": "Get statistics about displayed features.",
            "parameters": {
                "type": "object",
                "properties": {
                    "layer_id": {"type": "string", "description": "Layer ID or 'all'"},
                },
                "required": ["layer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "spatial_analysis",
            "description": "Execute a spatial analysis operation on layers displayed on the map. The operation runs client-side with turf.js. Operations: intersection, union, difference, clip, spatial_join, points_in_polygon, buffer, nearest, distance_matrix, centroid, convex_hull, dissolve, simplify, voronoi, hex_grid, area_perimeter, clustering. Use when user asks to combine, intersect, buffer, clip, dissolve, or analyze spatial relationships between layers.",
            "parameters": {
                "type": "object",
                "properties": {
                    "operation": {"type": "string", "description": "Operation ID: intersection, union, difference, clip, spatial_join, points_in_polygon, buffer, nearest, distance_matrix, centroid, convex_hull, dissolve, simplify, voronoi, hex_grid, area_perimeter, clustering"},
                    "layer_a_name": {"type": "string", "description": "Name of source layer A (exact name from map context)"},
                    "layer_b_name": {"type": "string", "description": "Name of layer B for binary operations"},
                    "params": {"type": "object", "description": "Params: {radius: meters} for buffer, {attribute: string} for dissolve, {maxDistance: km, minPoints: int} for clustering, {cellSide: km} for grids"},
                    "result_name": {"type": "string", "description": "Name for result layer"},
                },
                "required": ["operation", "layer_a_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compute_route",
            "description": "Compute a route between two or more points using Mapbox Directions API. Returns route geometry, distance, duration, and turn-by-turn instructions. IMPORTANT: You must geocode place names first to get [lon,lat] coordinates before calling this tool. Use when user asks for directions, itinerary, route, or how to get from A to B.",
            "parameters": {
                "type": "object",
                "properties": {
                    "waypoints": {"type": "array", "items": {"type": "array", "items": {"type": "number"}}, "description": "Array of [lon, lat] coordinate pairs. At least 2 points."},
                    "profile": {"type": "string", "enum": ["foot", "bike", "car"], "description": "Transport mode. Default: foot"},
                },
                "required": ["waypoints"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compute_isochrone",
            "description": "Compute an isochrone (area reachable within X minutes) from a point using Mapbox Isochrone API. Returns polygons showing accessible areas. IMPORTANT: Geocode the place name first to get [lon,lat]. Use when user asks about reachability, walking distance, 'what can I reach in X minutes', service area, accessibility zone.",
            "parameters": {
                "type": "object",
                "properties": {
                    "center": {"type": "array", "items": {"type": "number"}, "description": "[longitude, latitude] of center point"},
                    "time_minutes": {"type": "integer", "description": "Max travel time in minutes. Default 10."},
                    "profile": {"type": "string", "enum": ["foot", "bike", "car"], "description": "Transport mode. Default: foot"},
                },
                "required": ["center"],
            },
        },
    },
        {
        "type": "function",
        "function": {
            "name": "thematic_analysis",
            "description": (
                "Apply a thematic cartographic analysis on an EXISTING layer already loaded on the map. "
                "Use for: proportional symbols (graduated circles), choropleth maps, classification, "
                "graduated colors, heatmaps, or any visual styling based on a numeric attribute of a layer. "
                "ALWAYS use this tool (never GEE) when the user mentions an existing layer by name "
                "(e.g. 'Africa_cities', 'couche X', 'la couche Y'). "
                "No bbox required — the layer is already on the map."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "layer_name": {
                        "type": "string",
                        "description": "Exact name of the existing layer on the map (from map context)"
                    },
                    "analysis_type": {
                        "type": "string",
                        "enum": [
                            "proportional_symbols",
                            "choropleth",
                            "classification",
                            "graduated_colors",
                            "heatmap"
                        ],
                        "description": (
                            "Type of thematic analysis: "
                            "'proportional_symbols' for graduated circles sized by a numeric field, "
                            "'choropleth' for polygon fill color by value, "
                            "'classification' for class-based coloring (jenks/quantile/equal), "
                            "'graduated_colors' for continuous color ramp on points/lines, "
                            "'heatmap' for density visualization."
                        )
                    },
                    "attribute": {
                        "type": "string",
                        "description": "Name of the numeric attribute/field to map (e.g. 'population', 'pop', 'value')"
                    },
                    "method": {
                        "type": "string",
                        "enum": ["jenks", "quantile", "equal", "manual"],
                        "description": "Classification method. Default: jenks"
                    },
                    "n_classes": {
                        "type": "integer",
                        "description": "Number of classes (3-7). Default: 5"
                    },
                    "color_ramp": {
                        "type": "string",
                        "enum": ["viridis", "spectral", "blues", "reds", "oranges", "greens", "purples", "diverging"],
                        "description": "Color ramp. Default: viridis"
                    },
                    "min_radius": {
                        "type": "number",
                        "description": "Min circle radius in pixels for proportional_symbols. Default: 4"
                    },
                    "max_radius": {
                        "type": "number",
                        "description": "Max circle radius in pixels for proportional_symbols. Default: 30"
                    },
                },
                "required": ["layer_name", "analysis_type", "attribute"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "world_bank_indicator",
            "description": "Affiche un indicateur World Bank sur une carte choroplèthe mondiale (population, PIB, espérance de vie, CO2, chômage, forêt, électricité, alphabétisation...). Utiliser pour toute demande de données mondiales par pays.",
            "parameters": {
                "type": "object",
                "properties": {
                    "indicator": {"type": "string", "description": "Code WB: SP.POP.TOTL, NY.GDP.PCAP.CD, SP.DYN.LE00.IN, EN.ATM.CO2E.PC, AG.LND.FRST.ZS, EG.ELC.ACCS.ZS, SE.ADT.LITR.ZS, SH.DYN.MORT, SI.POV.GINI, SL.UEM.TOTL.ZS, SP.URB.TOTL.IN.ZS, EN.POP.DNST, NY.GDP.MKTP.CD, NY.GDP.MKTP.KD.ZG, SP.POP.GROW, SP.POP.65UP.TO.ZS, SH.XPD.CHEX.GD.ZS, SE.XPD.TOTL.GD.ZS, SH.STA.WASH.P5, AG.LND.ARBL.ZS"},
                    "year": {"type": "integer", "description": "Année (ex: 2022). Omis = dernière année dispo."},
                    "keyword": {"type": "string", "description": "Mot-clé si code inconnu (ex: 'mortalité')"},
                },
                "required": [],
            },
        },
    },
    
]

SYSTEM_PROMPT = """Tu es un assistant cartographique expert en données Overture Maps.

WORKFLOW PRINCIPAL:
1. Lieu précis → `geocode` d'abord, puis `query_overture` avec center_lon/lat + radius_m.
2. Si 0 résultats → réessayer avec radius plus grand ou sans filtre category.
3. "commerces/magasins" → theme=places SANS filtre category.
4. "restaurants" → theme=places, category=restaurant.
5. Rayons: "à côté"=500m, "près"=800m, "autour"=1000m, "quartier"=1500m.

WORKFLOW CRITIQUE — "FEATURES DANS UNE ZONE" (isochrone, buffer, polygone):
Quand l'utilisateur demande des features DANS une zone déjà affichée:
  → Utilise query_overture avec clip_to_layer="<nom exact de la couche polygone>"
  → Ça charge les features dans la bbox de la zone PUIS clip automatiquement
  → Le résultat ne contient QUE les features DANS le polygone

Exemples:
- "commerces dans l'isochrone" → query_overture(theme="places", clip_to_layer="Isochrone 5-10min foot")
- "restaurants dans le buffer" → query_overture(theme="places", category="restaurant", clip_to_layer="buffer_500m")
- "bâtiments dans la zone" → query_overture(theme="buildings", clip_to_layer="<nom de la couche polygone>")
- Le nom de la couche doit correspondre EXACTEMENT au nom dans le contexte carte!

ROUTING / ITINÉRAIRE:
- "de la gare à l'aéroport" → geocode CHAQUE lieu, puis compute_route(waypoints=[[lon1,lat1],[lon2,lat2]])
- Profils: foot (défaut), bike, car

ISOCHRONE:
- "zone accessible en 10 min à vélo depuis X" → geocode(X), compute_isochrone(center=[lon,lat], time_minutes=10, profile="bike")

ANALYSE SPATIALE (spatial_analysis):
- operation=clip : garder features de A qui sont DANS B (le plus utilisé!)
- operation=intersection : zone commune entre 2 polygones
- operation=buffer : zone tampon, params={radius: meters}
- operation=points_in_polygon : compter points de A dans polygones de B
- operation=dissolve : fusionner par attribut, params={attribute: "class"}
- operation=clustering : DBSCAN, params={maxDistance: km, minPoints: int}
- operation=centroid, convex_hull, voronoi, etc.
- Noms de couches = EXACTEMENT ceux du contexte carte!

BBOX VILLES (sans lieu précis):
  Nantes: xmin=-1.72, ymin=47.15, xmax=-1.42, ymax=47.32
  Paris: xmin=2.22, ymin=48.81, xmax=2.47, ymax=48.90
  Dakar: xmin=-17.55, ymin=14.63, xmax=-17.33, ymax=14.82

CARTES THÉMATIQUES SUR COUCHES EXISTANTES — OBLIGATOIRE (thematic_analysis):
Tu DOIS appeler thematic_analysis (JAMAIS GEE, JAMAIS query_overture) quand l'utilisateur
mentionne UNE COUCHE DÉJÀ CHARGÉE par son nom ET demande une représentation visuelle.
Pas besoin de bbox — la couche est déjà sur la carte.

Exemples OBLIGATOIRES :
- "carte de symboles proportionnels de la population de Africa_cities"
  → thematic_analysis(layer_name="Africa_cities", analysis_type="proportional_symbols", attribute="population")
- "carte choroplèthe par PIB sur la couche pays"
  → thematic_analysis(layer_name="pays", analysis_type="choropleth", attribute="gdp")
- "classifie la couche batiments par hauteur"
  → thematic_analysis(layer_name="batiments", analysis_type="classification", attribute="height")
- "heatmap de densité sur les points"
  → thematic_analysis(layer_name="points", analysis_type="heatmap", attribute="value")
- "symboles gradués par population" (couche visible dans le contexte carte)
  → thematic_analysis(layer_name="<nom exact du contexte>", analysis_type="proportional_symbols", attribute="population")

RÈGLE ABSOLUE : si le nom d'une couche existante est mentionné → thematic_analysis.
JAMAIS de GEE/compute_* pour des données déjà chargées.

DONNÉES MONDIALES — OBLIGATOIRE (world_bank_indicator):
Tu DOIS appeler world_bank_indicator pour TOUTE question sur des données par pays.
Ne JAMAIS répondre en texte sur ces sujets — appeler le tool DIRECTEMENT.

Exemples OBLIGATOIRES :
- "PIB par pays" → world_bank_indicator(indicator="NY.GDP.PCAP.CD")
- "PIB mondial" → world_bank_indicator(indicator="NY.GDP.MKTP.CD")
- "population mondiale" → world_bank_indicator(indicator="SP.POP.TOTL")
- "espérance de vie" → world_bank_indicator(indicator="SP.DYN.LE00.IN")
- "émissions CO2 par pays" → world_bank_indicator(indicator="EN.ATM.CO2E.PC")
- "taux de chômage mondial" → world_bank_indicator(indicator="SL.UEM.TOTL.ZS")
- "couverture forestière" → world_bank_indicator(indicator="AG.LND.FRST.ZS")
- "accès électricité" → world_bank_indicator(indicator="EG.ELC.ACCS.ZS")
- "alphabétisation" → world_bank_indicator(indicator="SE.ADT.LITR.ZS")
- "mortalité infantile" → world_bank_indicator(indicator="SH.DYN.MORT")
- "densité population" → world_bank_indicator(indicator="EN.POP.DNST")
- "inégalités gini" → world_bank_indicator(indicator="SI.POV.GINI")
Le résultat est un GeoJSON affiché directement sur la carte. Pas besoin de fly_to.

RÈGLES: carte vide au départ, fly_to APRÈS query, français, concis."""


# ═══════════════════════════════════════════════════════════════
# TOOL EXECUTION
# ═══════════════════════════════════════════════════════════════
import math
import requests as http_requests

def meters_to_degrees(meters, latitude):
    """Convert meters to approximate degrees at a given latitude."""
    lat_deg = meters / 111320
    lon_deg = meters / (111320 * math.cos(math.radians(latitude)))
    return lon_deg, lat_deg


def execute_geocode(args: dict) -> dict:
    """Geocode a place name using Nominatim (free, no API key)."""
    query = args.get("query", "")
    log.info(f"Geocoding: {query}")
    try:
        resp = http_requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1, "addressdetails": 1},
            headers={"User-Agent": "OvertureExplorer/1.0"},
            timeout=10,
        )
        results = resp.json()
        if not results:
            return {"error": f"Lieu non trouvé: {query}"}

        r = results[0]
        return {
            "action": "geocode_result",
            "latitude": float(r["lat"]),
            "longitude": float(r["lon"]),
            "display_name": r.get("display_name", query),
            "bbox": [float(r["boundingbox"][2]), float(r["boundingbox"][0]),
                     float(r["boundingbox"][3]), float(r["boundingbox"][1])],
            "type": r.get("type", ""),
        }
    except Exception as e:
        log.error(f"Geocode error: {e}")
        return {"error": f"Erreur geocoding: {str(e)}"}


def execute_query_overture(args: dict, map_context: dict = None) -> dict:
    """Execute DuckDB query on Overture Maps S3 data."""
    theme = args.get("theme")
    if not theme or theme not in THEMES:
        return {"error": f"Thème inconnu: {theme}"}

    # If clip_to_layer specified but no bbox, try to use bbox from map context
    clip_layer = args.get("clip_to_layer")
    if clip_layer and not args.get("xmin") and not args.get("center_lon") and map_context:
        for l in map_context.get("layers", []):
            if l.get("name") == clip_layer or clip_layer in l.get("name", ""):
                bbox = l.get("bbox")
                if bbox and len(bbox) == 4:
                    # Expand bbox by 10% to ensure we get all edge features
                    dx = (bbox[2] - bbox[0]) * 0.1
                    dy = (bbox[3] - bbox[1]) * 0.1
                    args["xmin"] = bbox[0] - dx
                    args["ymin"] = bbox[1] - dy
                    args["xmax"] = bbox[2] + dx
                    args["ymax"] = bbox[3] + dy
                    log.info(f"Using bbox from clip layer '{clip_layer}': {args['xmin']:.5f},{args['ymin']:.5f},{args['xmax']:.5f},{args['ymax']:.5f}")
                    break

    # Handle center + radius → convert to bbox
    if args.get("center_lon") is not None and args.get("center_lat") is not None:
        radius = args.get("radius_m", 500)
        clon, clat = args["center_lon"], args["center_lat"]
        dlon, dlat = meters_to_degrees(radius, clat)
        args["xmin"] = clon - dlon
        args["xmax"] = clon + dlon
        args["ymin"] = clat - dlat
        args["ymax"] = clat + dlat
        log.info(f"Radius {radius}m around ({clat:.5f}, {clon:.5f}) → bbox [{args['xmin']:.5f}, {args['ymin']:.5f}, {args['xmax']:.5f}, {args['ymax']:.5f}]")

    # Validate bbox
    if not all(k in args and args[k] is not None for k in ["xmin", "ymin", "xmax", "ymax"]):
        return {"error": "Bbox manquant. Fournis xmin/ymin/xmax/ymax ou center_lon/center_lat/radius_m."}

    ptype = THEMES[theme]["types"][0]
    columns = THEMES[theme]["columns"]
    path = f"{S3_BASE}/theme={theme}/type={ptype}/*"
    limit = min(args.get("limit", 2000), 5000)

    where = [
        f"bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}",
        f"bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}",
    ]
    if args.get("category") and theme == "places":
        where.append(f"categories.primary = '{args['category']}'")
    if args.get("name_filter"):
        where.append(f"names.primary ILIKE '%{args['name_filter']}%'")
    if args.get("min_confidence") and theme == "places":
        where.append(f"confidence >= {args['min_confidence']}")
    if args.get("min_height") and theme == "buildings":
        where.append(f"height >= {args['min_height']}")
    if args.get("max_height") and theme == "buildings":
        where.append(f"height <= {args['max_height']}")

    sql = f"""SELECT {columns}
FROM read_parquet('{path}', filename=true, hive_partitioning=1)
WHERE {' AND '.join(where)}
LIMIT {limit}"""

    log.info(f"DuckDB query: {sql[:200]}...")

    # Check cache
    cache_key = hashlib.md5(sql.encode()).hexdigest()
    cache_path = CACHE_DIR / f"{cache_key}.json"
    if cache_path.exists():
        log.info("Cache hit!")
        return json.loads(cache_path.read_text())

    try:
        df = db.query(sql)
        # Convert to GeoJSON — geom_json is already valid GeoJSON from ST_AsGeoJSON
        features = []
        for _, row in df.iterrows():
            geom_str = row.get("geom_json", "")
            geom = None
            if geom_str and str(geom_str) not in ("", "None", "nan"):
                try:
                    geom = json.loads(str(geom_str))
                except (json.JSONDecodeError, TypeError):
                    continue

            props = {}
            for k, v in row.items():
                if k == "geom_json":
                    continue
                if hasattr(v, "item"):
                    props[k] = v.item()
                elif str(v) in ("nan", "None", "NaT") or v is None:
                    props[k] = None
                elif isinstance(v, dict):
                    # Handle nested structs like num_floors
                    props[k] = v if v else None
                else:
                    props[k] = v

            if geom and geom.get("coordinates"):
                features.append({"type": "Feature", "properties": props, "geometry": geom})

        result = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "theme": theme, "total": len(features),
                "bbox": [args["xmin"], args["ymin"], args["xmax"], args["ymax"]],
                "query_params": {k: v for k, v in args.items() if v is not None},
            }
        }

        # Cache
        cache_path.write_text(json.dumps(result, default=str))
        return result

    except Exception as e:
        log.error(f"DuckDB error: {e}")
        return {"error": str(e), "sql": sql}


def execute_tool(name: str, args: dict, map_context: dict = None) -> dict:
    """Route tool execution."""
    if name == "geocode":
        return execute_geocode(args)
    elif name == "query_overture":
        return execute_query_overture(args, map_context=map_context)
    elif name == "fly_to":
        return {"action": "fly_to", **args}
    elif name == "set_layer_style":
        return {"action": "set_layer_style", **args}
    elif name == "remove_layer":
        return {"action": "remove_layer", **args}
    elif name == "get_layer_stats":
        return {"action": "get_layer_stats", **args}
    elif name == "spatial_analysis":
        return {
            "action": "spatial_analysis",
            "operation": args.get("operation"),
            "layer_a_name": args.get("layer_a_name"),
            "layer_b_name": args.get("layer_b_name"),
            "params": args.get("params", {}),
            "result_name": args.get("result_name", f"{args.get('operation', 'result')}"),
        }
    elif name == "compute_route":
        return {
            "action": "compute_route",
            "waypoints": args.get("waypoints", []),
            "profile": args.get("profile", "foot"),
        }
    elif name == "compute_isochrone":
        return {
            "action": "compute_isochrone",
            "center": args.get("center", [0, 0]),
            "time_minutes": args.get("time_minutes", 10),
            "profile": args.get("profile", "foot"),
        }

    elif name == "thematic_analysis":
        return {
            "action": "thematic_analysis",
            "layer_name":     args.get("layer_name"),
            "analysis_type":  args.get("analysis_type", "proportional_symbols"),
            "attribute":      args.get("attribute"),
            "method":         args.get("method", "jenks"),
            "n_classes":      args.get("n_classes", 5),
            "color_ramp":     args.get("color_ramp", "viridis"),
            "min_radius":     args.get("min_radius", 4),
            "max_radius":     args.get("max_radius", 30),
        }

    elif name == "world_bank_indicator":
        from worldbank.indicators import INDICATORS, find_indicator_by_keyword
        from worldbank.fetcher import build_choropleth, fetch_latest_year

        code    = args.get("indicator", "")
        year    = args.get("year")
        keyword = args.get("keyword")

        if keyword and (not code or code not in INDICATORS):
            code = find_indicator_by_keyword(keyword)
        if not code or code not in INDICATORS:
            code = "SP.POP.TOTL"

        meta = INDICATORS[code]
        if not year:
            year, _ = fetch_latest_year(code)

        return build_choropleth(code, year, meta["label_fr"], meta["unit"])
  
    return {"error": f"Unknown tool: {name}"}


# ═══════════════════════════════════════════════════════════════
# LLM CALL (LiteLLM — provider-agnostic)
# ═══════════════════════════════════════════════════════════════
def call_llm(messages: list, map_context: dict = None) -> dict:
    """
    Call the configured LLM with tool calling support.
    Returns: { "text": str, "tool_calls": [...], "tool_results": [...] }
    """
    # Inject map context into system prompt
    system = SYSTEM_PROMPT
    if map_context:
        layers_list = map_context.get("layers", [])
        layers_desc = []
        for l in layers_list:
            desc = f"  - \"{l.get('name', '?')}\" ({l.get('featureCount', 0)} features, types: {l.get('geomTypes', [])}"
            bbox = l.get("bbox")
            if bbox:
                desc += f", bbox: [{bbox[0]:.4f},{bbox[1]:.4f},{bbox[2]:.4f},{bbox[3]:.4f}]"
            desc += ")"
            layers_desc.append(desc)
        layers_str = "\n".join(layers_desc) if layers_desc else "  (aucune couche)"
        system += f"\n\nCONTEXTE CARTE ACTUEL:\nCouches affichées:\n{layers_str}\nCentre: {map_context.get('center', 'inconnu')}\nZoom: {map_context.get('zoom', 'inconnu')}\n\nPour charger des features DANS une couche polygone existante: d'abord query_overture avec le bbox de cette couche, puis spatial_analysis(operation='clip', layer_a_name='<features>', layer_b_name='<polygone>')."

    full_messages = [{"role": "system", "content": system}] + messages

    # LiteLLM kwargs
    kwargs = {
        "model": LLM_MODEL,
        "messages": full_messages,
        "tools": TOOLS,
        "tool_choice": "auto",
        "max_tokens": 2000,
        "temperature": 0.3,
    }

    # Provider-specific overrides
    if LLM_PROVIDER == "ollama":
        kwargs["api_base"] = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")

    try:
        response = completion(**kwargs)
    except Exception as e:
        log.error(f"LLM error ({LLM_PROVIDER}): {e}")
        return {"text": f"Erreur LLM ({LLM_PROVIDER}): {str(e)}", "tool_calls": [], "tool_results": []}

    result = {"text": "", "tool_calls": [], "tool_results": []}
    current_messages = list(full_messages)
    max_rounds = 7  # geocode + query + clip + fly_to needs 4+ rounds

    for round_num in range(max_rounds):
        msg = response.choices[0].message
        tool_calls_raw = getattr(msg, "tool_calls", None) or []

        # No more tool calls → we have the final text response
        if not tool_calls_raw:
            result["text"] = msg.content or ""
            break

        # Add assistant message with tool calls to conversation
        current_messages.append(msg.model_dump())

        # Execute each tool call
        for tc in tool_calls_raw:
            fn_name = tc.function.name
            fn_args = json.loads(tc.function.arguments)
            log.info(f"[Round {round_num + 1}] Tool: {fn_name}({json.dumps(fn_args, default=str)[:120]})")

            tool_result = execute_tool(fn_name, fn_args, map_context=map_context)
            result["tool_calls"].append({"name": fn_name, "args": fn_args})
            result["tool_results"].append(tool_result)

            current_messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(tool_result, default=str)[:4000],
            })

        # Call LLM again with tool results — it may want to call more tools
        try:
            response = completion(
                model=LLM_MODEL,
                messages=current_messages,
                tools=TOOLS,
                tool_choice="auto",
                max_tokens=2000,
                temperature=0.3,
                **({"api_base": os.getenv("OLLAMA_API_BASE")} if LLM_PROVIDER == "ollama" else {}),
            )
        except Exception as e:
            log.error(f"LLM round {round_num + 1} error: {e}")
            result["text"] = f"Données récupérées ({len(result['tool_results'])} résultats)."
            break
    else:
        # Max rounds reached
        result["text"] = result.get("text") or "Traitement terminé."

    return result


# ═══════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    db.connect()
    # Warmup orchestrateur : charge modele embedding + router au demarrage
    if ORCHESTRATOR_ENABLED:
        try:
            get_orchestrator().warmup()
        except Exception as e:
            log.warning(f"Orchestrateur warmup ignore : {e}")
    yield
    db.close()

app = FastAPI(title="Overture Maps Agent", version="2.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ── Routers ────────────────────────────────────────────────────
try:
    from db_routes import router as db_router
    app.include_router(db_router)
    log.info("✓ DB router chargé (/api/db/*)")
except Exception as e:
    log.warning(f"⚠ DB router : {e}")

try:
    from gee_routes import router as gee_router
    app.include_router(gee_router)
    log.info("✓ GEE router chargé (/api/gee/*)")
except Exception as e:
    log.warning(f"⚠ GEE router : {e}")

try:
    from gee_timelapse import router as tl_router
    app.include_router(tl_router)
    log.info("✓ Timelapse router chargé (/api/gee/timelapse)")
except Exception as e:
    log.warning(f"⚠ Timelapse router : {e}")


try:
    from elevation_routes import router as elev_router
    app.include_router(elev_router)
    log.info("✓ Elevation router chargé (/api/elevation/profile)")
except Exception as e:
    log.warning(f"⚠ Elevation router : {e}")


try:
    from gee_change_detection import router as change_router
    app.include_router(change_router)
    log.info("✓ Elevation router chargé (/api/gee/change-detection)")
except Exception as e:
    log.warning(f"⚠ change-detection : {e}")

try:
    from gee_classification import router as classif_router
    app.include_router(classif_router)
    log.info("✓ Classification router chargé (/api/gee/classify)")
except Exception as e:
    log.warning(f"⚠ gee-classification : {e}")

try:
    from worldbank.wb_routes import router as wb_router
    app.include_router(wb_router)
    log.info("✓ WorldBank router chargé (/api/worldbank/*)")
except Exception as e:
    log.warning(f"⚠ WorldBank router : {e}")

try:
    from auth.routes import router as auth_router
    from maps.routes import router as maps_router
    from auth.core   import init_db
    app.include_router(auth_router)
    app.include_router(maps_router)
    init_db()
    log.info("✓ Auth + Maps router chargés (/api/auth/*, /api/maps/*)")
except Exception as e:
    log.warning(f"⚠ Auth/Maps router : {e}")

try:
    from sql_routes import router as sql_router
    app.include_router(sql_router)
    log.info("✓ SQL router chargé (/api/sql/run)")
except Exception as e:
    log.warning(f"⚠ SQL router : {e}")



from osm_routes import router as osm_router
app.include_router(osm_router)


# Conversion LiDAR LAS/LAZ (laspy) → nuages de points 3D
try:
    from lidar_routes import router as lidar_router
    app.include_router(lidar_router)
    print("✓ LiDAR routes chargées (/api/lidar)")
except Exception as e:
    print(f"✗ LiDAR routes non chargées: {e}")

# Foresterie LiDAR : MNT/MNS/MNH + détection d'arbres + houppiers
try:
    from canopy_routes import router as canopy_router
    app.include_router(canopy_router)
    print("✓ Canopy routes chargées (/api/lidar/canopy)")
except Exception as e:
    print(f"✗ Canopy routes non chargées: {e}")

# Import de rasters GeoTIFF (reprojection 4326 → overlay carte)
try:
    from raster_routes import router as raster_router
    app.include_router(raster_router)
    print("✓ Raster routes chargées (/api/raster/import)")
except Exception as e:
    print(f"✗ Raster routes non chargées: {e}")

# Navigateur STAC + COG (chercher/ajouter des scènes satellite)
try:
    from stac_routes import router as stac_router
    app.include_router(stac_router)
    print("✓ STAC routes chargées (/api/stac/search, /api/stac/scene)")
except Exception as e:
    print(f"✗ STAC routes non chargées: {e}")

# Statistiques spatiales (Moran + hotspots Getis-Ord) sur couche vecteur
try:
    from spatialstats_routes import router as spatialstats_router
    app.include_router(spatialstats_router)
    print("✓ Stats spatiales chargées (/api/spatialstats/run)")
except Exception as e:
    print(f"✗ Stats spatiales non chargées: {e}")

# Géoréférenceur (caler une image sur la carte par points d'appui)
try:
    from georef_routes import router as georef_router
    app.include_router(georef_router)
    print("✓ Géoréférenceur chargé (/api/georef/warp)")
except Exception as e:
    print(f"✗ Géoréférenceur non chargé: {e}")

# Textures planétaires (viewer Système solaire 3D)
try:
    from planet_routes import router as planet_router
    app.include_router(planet_router)
    print("✓ Textures planétaires chargées (/api/planet/texture)")
except Exception as e:
    print(f"✗ Textures planétaires non chargées: {e}")

# Viewshed (analyse de visibilité depuis un point, MNT Terrarium)
try:
    from viewshed_routes import router as viewshed_router
    app.include_router(viewshed_router)
    print("✓ Viewshed chargé (/api/viewshed/compute)")
except Exception as e:
    print(f"✗ Viewshed non chargé: {e}")

# Maxar Open Data (imagerie catastrophe avant/après, COG bucket public AWS)
try:
    from maxar_routes import router as maxar_router
    app.include_router(maxar_router)
    print("✓ Maxar Open Data chargé (/api/maxar/events)")
except Exception as e:
    print(f"✗ Maxar Open Data non chargé: {e}")

# Ombres portées — canopée Meta (GEE) vectorisée pour l'outil d'ombrage
try:
    from shadow_routes import router as shadow_router
    app.include_router(shadow_router)
    print("✓ Ombres/canopée chargé (/api/shadow/canopy)")
except Exception as e:
    print(f"✗ Ombres/canopée non chargé: {e}")

# Routage & isochrones via backend (jeton backend, pas de dépendance au build front)
try:
    from routing_routes import router as routing_router
    app.include_router(routing_router)
    print("✓ Routage/isochrone chargé (/api/route/directions)")
except Exception as e:
    print(f"✗ Routage/isochrone non chargé: {e}")


# 2. Routers
app.include_router(resilience_router)
app.include_router(memory_router)


app.include_router(metrics_router)
# Dans lifespan() : rien à faire, le logger s'initialise lazily

class ChatRequest(BaseModel):
    messages: list  # [{role, content}, ...]
    map_context: Optional[dict] = None  # {layers, center, zoom}


class ExportRequest(BaseModel):
    theme: str
    bbox: list  # [xmin, ymin, xmax, ymax]
    format: str = "GeoJSON"
    limit: int = 10000


@app.get("/")
def root():
    return {
        "service": "Overture Maps Agent",
        "llm_provider": LLM_PROVIDER,
        "llm_model": LLM_MODEL,
        "overture_release": OVERTURE_RELEASE,
        "tools": [t["function"]["name"] for t in TOOLS],
    }


@app.get("/api/config")
def get_config():
    """Frontend fetches this to know which LLM is active."""
    return {
        "llm_provider": LLM_PROVIDER,
        "llm_model": LLM_MODEL,
        "overture_release": OVERTURE_RELEASE,
        "themes": {k: v["types"] for k, v in THEMES.items()},
        "tools": [t["function"]["name"] for t in TOOLS],
    }


@app.post("/api/chat")
def chat(req: ChatRequest):
    """Main chat endpoint — orchestrateur multi-agent ou fallback legacy."""
    if ORCHESTRATOR_ENABLED:
        # Pipeline complet : RAG → Router → Sous-agent → Validation → GeoJSON
        return orchestrate(req.messages, req.map_context)
    # Fallback legacy (call_llm direct, si orchestrateur non disponible)
    return call_llm(req.messages, req.map_context)

  
from fastapi import Header

@app.post("/api/chat")
def chat(req: ChatRequest, x_session_id: str = Header(None)):
    sid  = x_session_id or (req.map_context or {}).get("session_id", "anon")

    mem  = get_session_memory()
    sess = mem.load(sid)
    mem.update_from_map_context(sess, req.map_context)

    if ORCHESTRATOR_ENABLED:
        result = orchestrate(req.messages, req.map_context)
    else:
        result = call_llm(req.messages, req.map_context)

    user_msg = next((m["content"] for m in reversed(req.messages)
                     if m.get("role") == "user"), "")

    mem.update_from_response(sess, result, user_msg)
    mem.save(sess)

    return result


@app.post("/api/debug/route")
def debug_route(req: ChatRequest):
    """Debug : inspecte le routing RAG + domaine sans executer les tools."""
    if not ORCHESTRATOR_ENABLED:
        return {"error": "Orchestrateur non disponible"}
    from agents.router import classify
    from rag.retriever import retrieve_tools
    query = next(
        (m["content"] for m in reversed(req.messages) if m.get("role") == "user"), ""
    )
    rag_tools    = retrieve_tools(query, top_k=5)
    route_result = classify(query)
    return {
        "query":      query,
        "rag_tools":  [{"id": t["id"], "score": round(t.get("score", 0), 3)} for t in rag_tools],
        "domain":     route_result["domain"],
        "confidence": route_result["confidence"],
        "method":     route_result["method"],
        "latency_ms": route_result.get("latency_ms", 0),
    }


@app.get("/api/debug/orchestrator")
def debug_orchestrator():
    """Statut de l orchestrateur et de ses composants."""
    status = {
        "orchestrator_enabled": ORCHESTRATOR_ENABLED,
        "llm_provider":  LLM_PROVIDER,
        "llm_model":     LLM_MODEL,
        "enable_rag":    os.getenv("ENABLE_RAG", "true"),
        "enable_multi":  os.getenv("ENABLE_MULTI_AGENT", "true"),
    }
    # pgvector
    try:
        from rag.retriever import _pool
        conn = _pool.get()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT tool_id) FROM rag_tools")
            total, tools = cur.fetchone()
        status["pgvector"] = "ok"
        status["rag_vectors"] = total
        status["rag_tools"]   = tools
    except Exception as e:
        status["pgvector"] = f"indisponible : {e}"
    # GEE
    try:
        import requests as _req
        resp = _req.get("http://localhost:8000/api/gee/health", timeout=3)
        status["gee"] = resp.json().get("status", "unknown")
    except Exception:
        status["gee"] = "indisponible"
    return status


@app.post("/api/query/{theme}")
def direct_query(theme: str, xmin: float = Query(...), ymin: float = Query(...),
                 xmax: float = Query(...), ymax: float = Query(...),
                 limit: int = Query(500), category: str = Query(None)):
    """Direct query bypass (no LLM, just DuckDB)."""
    args = {"theme": theme, "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
            "limit": limit, "category": category}
    return execute_query_overture(args)


@app.get("/api/query")
def direct_query_get(theme: str = Query(...), xmin: float = Query(...), ymin: float = Query(...),
                     xmax: float = Query(...), ymax: float = Query(...),
                     limit: int = Query(1000), category: str = Query(None)):
    """Direct query GET endpoint for frontend auto-clip re-queries."""
    args = {"theme": theme, "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
            "limit": limit}
    if category:
        args["category"] = category
    return execute_query_overture(args)


@app.post("/api/export")
def export_data(req: ExportRequest):
    """Export data in various formats."""
    args = {"theme": req.theme, "xmin": req.bbox[0], "ymin": req.bbox[1],
            "xmax": req.bbox[2], "ymax": req.bbox[3], "limit": req.limit}
    return execute_query_overture(args)



# ─── LLM Proxy pour AgriPanel et autres modules frontend ──────────────────────
# Expose /api/llm/chat/completions compatible OpenAI
# AgriPanel l'utilise directement (VITE_LITELLM_API_URL=/api/llm)
from fastapi import Request as FARequest
from fastapi.responses import JSONResponse, StreamingResponse
import json as _json

@app.post("/api/llm/chat/completions")
async def llm_proxy_completions(request: FARequest):
    """
    Proxy LLM pour les modules frontend (AgriPanel, etc.).
    Forward vers LiteLLM avec le modèle configuré.
    Supporte streaming et non-streaming.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    messages = body.get("messages", [])
    stream   = body.get("stream", False)
    model    = body.get("model", LLM_MODEL)
    # Forcer notre modèle configuré (ignorer celui du client)
    model = LLM_MODEL

    kwargs = {
        "model":       model,
        "messages":    messages,
        "temperature": body.get("temperature", 0.3),
        "max_tokens":  body.get("max_tokens", 1500),
        "stream":      stream,
    }
    if LLM_PROVIDER == "ollama":
        kwargs["api_base"] = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")

    try:
        if stream:
            # Streaming SSE
            def generate():
                for chunk in completion(**kwargs):
                    delta = chunk.choices[0].delta if chunk.choices else None
                    if delta and delta.content:
                        data = _json.dumps({
                            "choices": [{"delta": {"content": delta.content}, "finish_reason": None}]
                        })
                        yield f"data: {data}\n\n"
                yield "data: [DONE]\n\n"
            return StreamingResponse(generate(), media_type="text/event-stream")
        else:
            resp = completion(**kwargs)
            # Retourner format OpenAI standard
            return {
                "id":      resp.id,
                "object":  "chat.completion",
                "model":   model,
                "choices": [{
                    "index":         0,
                    "message":       {"role": "assistant", "content": resp.choices[0].message.content},
                    "finish_reason": resp.choices[0].finish_reason,
                }],
                "usage": dict(resp.usage) if resp.usage else {},
            }
    except Exception as e:
        log.error(f"LLM proxy error: {e}")
        return JSONResponse(
            {"error": {"message": str(e), "type": "llm_error"}},
            status_code=500
        )


@app.get("/api/llm/models")
def llm_models():
    """Liste les modèles disponibles (compatibilité OpenAI)."""
    return {"data": [{"id": LLM_MODEL, "object": "model"}], "object": "list"}

if __name__ == "__main__":
    import uvicorn
    import platform
    port = int(os.getenv("BACKEND_PORT", 8000))
    # reload=True crashes on Windows with multiprocessing.spawn + DuckDB
    use_reload = platform.system() != "Windows"
    uvicorn.run("agent:app", host="0.0.0.0", port=port, reload=use_reload)