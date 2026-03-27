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

from db_routes import router as db_router


load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("overture-agent")

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
    limit = args.get("limit", 500)

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
    yield
    db.close()

app = FastAPI(title="Overture Maps Agent", version="2.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)
# ── DB routes ──────────────────────────────────────────────────
try:
    from db_routes import router as db_router
    app.include_router(db_router)
except ImportError:
    pass

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
    """Main chat endpoint — sends messages to LLM with tool calling."""
    result = call_llm(req.messages, req.map_context)
    return result


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


if __name__ == "__main__":
    import uvicorn
    import platform
    port = int(os.getenv("BACKEND_PORT", 8000))
    # reload=True crashes on Windows with multiprocessing.spawn + DuckDB
    use_reload = platform.system() != "Windows"
    uvicorn.run("agent:app", host="0.0.0.0", port=port, reload=use_reload)
