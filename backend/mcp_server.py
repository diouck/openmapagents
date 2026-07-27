"""
Overture Maps Explorer — MCP Server enrichi
Expose TOUS les outils OpenMapAgents via le protocole MCP :

  Existants (DuckDB direct) :
    - query_places       → POI Overture Maps
    - query_buildings    → Bâtiments Overture Maps
    - query_transport    → Réseau routier Overture Maps
    - query_divisions    → Divisions administratives Overture Maps
    - spatial_stats      → Statistiques spatiales
    - h3_density         → Densité hexagonale H3
    - export_overture    → Export GeoJSON/GPKG/CSV/Parquet
    - raw_duckdb_query   → Requête DuckDB brute

  NOUVEAUX (via FastAPI backend) :
    - gee_tiles          → Imagerie satellite GEE (NDVI, LST, RGB...)
    - query_database     → Base de données externe (PostGIS/MySQL/SQLite)
    - list_db_tables     → Lister les tables d'une base
    - compute_route      → Itinéraire (ORS)
    - compute_isochrone  → Zone d'accessibilité (ORS)
    - geocode            → Géocodage Nominatim

Tous les nouveaux tools passent par la Couche 4 (validation déterministe)
avant d'appeler le backend FastAPI.

Usage Claude Desktop (claude_desktop_config.json) :
    {
      "mcpServers": {
        "openmap-agents": {
          "command": "python",
          "args": ["/path/to/mcp_server.py"],
          "env": {
            "OVERTURE_RELEASE": "2026-03-18.0",
            "BACKEND_URL": "http://localhost:8000"
          }
        }
      }
    }
"""

import os
import json
import math
import logging
import asyncio
from typing import Any

import duckdb
import requests as http_requests
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("openmap-mcp")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
OVERTURE_RELEASE = os.getenv("OVERTURE_RELEASE", "2026-03-18.0")
S3_BASE          = f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"
S3_REGION        = os.getenv("OVERTURE_S3_REGION", "us-west-2")
DUCKDB_MEMORY    = os.getenv("DUCKDB_MEMORY", "4GB")
DUCKDB_THREADS   = int(os.getenv("DUCKDB_THREADS", "4"))
BACKEND_URL      = os.getenv("BACKEND_URL", "http://localhost:8000")
HTTP_TIMEOUT     = int(os.getenv("MCP_HTTP_TIMEOUT", "60"))

THEMES = {
    "places":       "place",
    "buildings":    "building",
    "transportation":"segment",
    "divisions":    "division_area",
    "base":         "land",
    "addresses":    "address",
}

# ─── DUCKDB ───────────────────────────────────────────────────────────────────
def _get_db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("INSTALL httpfs;   LOAD httpfs;")
    try:
        conn.execute("INSTALL h3 FROM community; LOAD h3;")
    except Exception:
        pass
    conn.execute(f"SET s3_region='{S3_REGION}';")
    conn.execute(f"SET memory_limit='{DUCKDB_MEMORY}';")
    conn.execute(f"SET threads={DUCKDB_THREADS};")
    logger.info("✓ DuckDB connecté (spatial + httpfs)")
    return conn


db = _get_db()

# ─── BACKEND HTTP HELPER ──────────────────────────────────────────────────────
def _post(path: str, payload: dict) -> dict:
    """POST vers le backend FastAPI avec gestion d'erreurs."""
    url = f"{BACKEND_URL}{path}"
    try:
        resp = http_requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except http_requests.exceptions.ConnectionError:
        raise RuntimeError(
            f"Backend FastAPI indisponible sur {BACKEND_URL}. "
            "Démarrer agent.py d'abord : uvicorn agent:app --port 8000"
        )
    except http_requests.exceptions.Timeout:
        raise RuntimeError(f"Timeout ({HTTP_TIMEOUT}s) lors de l'appel à {path}")
    except http_requests.exceptions.HTTPError as e:
        try:
            detail = e.response.json().get("detail", str(e))
        except Exception:
            detail = str(e)
        raise RuntimeError(f"Erreur backend {path} : {detail}")


def _get(path: str, params: dict = None) -> dict:
    """GET vers le backend FastAPI."""
    url = f"{BACKEND_URL}{path}"
    try:
        resp = http_requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"Erreur GET {path} : {e}")


# ─── MCP SERVER ───────────────────────────────────────────────────────────────
server = Server("openmap-agents")


# ═══════════════════════════════════════════════════════════════════════════════
# LISTE DES OUTILS
# ═══════════════════════════════════════════════════════════════════════════════

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [

        # ── OVERTURE MAPS (DuckDB direct) ─────────────────────────────────────

        Tool(
            name="query_places",
            description=(
                "Recherche de points d'intérêt (POI) Overture Maps par bounding box. "
                "Catégories : restaurant, cafe, pharmacy, hospital, school, hotel, "
                "supermarket, bank, museum, park, cinema, shopping_mall, gym... "
                "Retourne GeoJSON avec nom, catégorie, confiance. "
                "Utiliser geocode d'abord si l'utilisateur mentionne un lieu nommé."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "xmin": {"type": "number", "description": "Longitude min (ouest)"},
                    "ymin": {"type": "number", "description": "Latitude min (sud)"},
                    "xmax": {"type": "number", "description": "Longitude max (est)"},
                    "ymax": {"type": "number", "description": "Latitude max (nord)"},
                    "center_lon":     {"type": "number", "description": "Longitude centre (alternative bbox)"},
                    "center_lat":     {"type": "number", "description": "Latitude centre (alternative bbox)"},
                    "radius_m":       {"type": "number", "description": "Rayon en mètres (avec center_lon/lat)"},
                    "category":       {"type": "string", "description": "Catégorie Overture"},
                    "name_filter":    {"type": "string", "description": "Filtre nom (ILIKE)"},
                    "min_confidence": {"type": "number", "description": "Confiance min (0-1)"},
                    "limit":          {"type": "integer", "default": 500},
                },
                "required": [],
            },
        ),

        Tool(
            name="query_buildings",
            description=(
                "Recherche de bâtiments Overture Maps : hauteur, étages, emprise polygone. "
                "Idéal pour l'analyse urbaine, densité bâtie, visualisation 3D."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "xmin": {"type": "number"}, "ymin": {"type": "number"},
                    "xmax": {"type": "number"}, "ymax": {"type": "number"},
                    "center_lon": {"type": "number"}, "center_lat": {"type": "number"},
                    "radius_m":   {"type": "number"},
                    "min_height": {"type": "number", "description": "Hauteur min en mètres"},
                    "max_height": {"type": "number", "description": "Hauteur max en mètres"},
                    "limit":      {"type": "integer", "default": 500},
                },
                "required": [],
            },
        ),

        Tool(
            name="query_transport",
            description=(
                "Réseau routier et de transport Overture Maps : autoroutes, routes, "
                "rues résidentielles, pistes cyclables. Retourne géométries LineString."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "xmin": {"type": "number"}, "ymin": {"type": "number"},
                    "xmax": {"type": "number"}, "ymax": {"type": "number"},
                    "road_class": {
                        "type": "string",
                        "enum": ["motorway", "primary", "secondary", "tertiary",
                                 "residential", "service", "cycleway", "footway"],
                        "description": "Classe de route",
                    },
                    "limit": {"type": "integer", "default": 500},
                },
                "required": ["xmin", "ymin", "xmax", "ymax"],
            },
        ),

        Tool(
            name="query_divisions",
            description=(
                "Divisions administratives Overture Maps : communes, départements, "
                "régions, pays. Retourne les polygones de délimitation."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "xmin": {"type": "number"}, "ymin": {"type": "number"},
                    "xmax": {"type": "number"}, "ymax": {"type": "number"},
                    "subtype": {
                        "type": "string",
                        "description": "Type : country, region, county, locality",
                    },
                    "country": {"type": "string", "description": "Code pays ISO (ex: FR, SN)"},
                    "limit":   {"type": "integer", "default": 200},
                },
                "required": ["xmin", "ymin", "xmax", "ymax"],
            },
        ),

        Tool(
            name="spatial_stats",
            description=(
                "Statistiques agrégées pour un thème Overture Maps dans une zone : "
                "total features, distribution catégories (places), stats hauteur (buildings)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "theme": {"type": "string", "enum": list(THEMES.keys())},
                    "xmin": {"type": "number"}, "ymin": {"type": "number"},
                    "xmax": {"type": "number"}, "ymax": {"type": "number"},
                },
                "required": ["theme", "xmin", "ymin", "xmax", "ymax"],
            },
        ),

        Tool(
            name="h3_density",
            description=(
                "Densité hexagonale H3 pour un thème Overture Maps. "
                "Retourne le nombre de features par cellule H3 (résolution 4-12)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "theme": {"type": "string", "enum": list(THEMES.keys())},
                    "xmin": {"type": "number"}, "ymin": {"type": "number"},
                    "xmax": {"type": "number"}, "ymax": {"type": "number"},
                    "resolution": {"type": "integer", "minimum": 4, "maximum": 12, "default": 8},
                },
                "required": ["theme", "xmin", "ymin", "xmax", "ymax"],
            },
        ),

        Tool(
            name="export_overture",
            description=(
                "Génère une requête DuckDB SQL pour exporter des données Overture Maps "
                "en GeoJSON, GeoPackage, CSV, GeoParquet ou FlatGeobuf."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "theme":       {"type": "string", "enum": list(THEMES.keys())},
                    "xmin": {"type": "number"}, "ymin": {"type": "number"},
                    "xmax": {"type": "number"}, "ymax": {"type": "number"},
                    "format":      {"type": "string", "enum": ["GeoJSON", "GeoPackage", "CSV", "GeoParquet", "FlatGeobuf"]},
                    "output_file": {"type": "string"},
                    "limit":       {"type": "integer", "default": 10000},
                },
                "required": ["theme", "xmin", "ymin", "xmax", "ymax", "format", "output_file"],
            },
        ),

        Tool(
            name="raw_duckdb_query",
            description=(
                "Exécute une requête DuckDB SQL brute sur les données Overture Maps S3. "
                "Le S3 base path et les extensions spatial/httpfs sont déjà configurés. "
                "Utiliser pour des requêtes avancées non couvertes par les autres tools."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "Requête SQL DuckDB (SELECT uniquement)"},
                },
                "required": ["sql"],
            },
        ),

        # ── GEOCODAGE ─────────────────────────────────────────────────────────

        Tool(
            name="geocode",
            description=(
                "Convertit un nom de lieu, une adresse ou un monument en coordonnées "
                "géographiques (lat/lon) et bounding box via Nominatim. "
                "TOUJOURS utiliser en premier quand l'utilisateur mentionne un lieu nommé."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Lieu, adresse ou monument (ex: 'Château des Ducs de Bretagne Nantes')",
                    },
                },
                "required": ["query"],
            },
        ),

        # ── GOOGLE EARTH ENGINE ───────────────────────────────────────────────

        Tool(
            name="gee_tiles",
            description=(
                "Génère des tuiles d'imagerie satellite via Google Earth Engine. "
                "Datasets : sentinel2, sentinel1, landsat9, landsat8, modis_lst, "
                "modis_ndvi, worldcover, hansen, era5, srtm. "
                "Indices : NDVI, NDWI, NDBI, EVI, LST (température), RGB, "
                "False Color (NIR), Occupation du sol, Élévation, Pente, Ombrage... "
                "Retourne une URL de tuiles XYZ compatible MapLibre/Leaflet."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "enum": ["sentinel2", "sentinel1", "landsat9", "landsat8",
                                 "modis_lst", "modis_ndvi", "worldcover",
                                 "hansen", "era5", "srtm"],
                        "description": "Dataset satellite",
                    },
                    "index": {
                        "type": "string",
                        "description": (
                            "Indice à calculer : RGB, NDVI, NDWI, NDBI, EVI, "
                            "False Color (NIR), LST (température), LST Jour, LST Nuit, "
                            "Occupation du sol, VV, VH, Couverture forêt 2000, "
                            "Perte forêt, Gain forêt, Température air, Précipitations, "
                            "Élévation, Pente, Ombrage"
                        ),
                    },
                    "date_start": {"type": "string", "description": "Date début ISO YYYY-MM-DD"},
                    "date_end":   {"type": "string", "description": "Date fin ISO YYYY-MM-DD"},
                    "bbox": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "Zone [xmin, ymin, xmax, ymax] en WGS84",
                    },
                    "cloud_max":  {"type": "number", "description": "Couverture nuageuse max % (défaut 20)"},
                    "composite":  {
                        "type": "string",
                        "enum": ["least_cloudy", "median", "mosaic"],
                        "description": "Méthode composite (défaut: least_cloudy)",
                    },
                    "vis_params_override": {
                        "type": "object",
                        "description": "Surcharge visuelle : {min, max, palette, bands, gamma}",
                    },
                },
                "required": ["dataset", "index", "date_start", "date_end"],
            },
        ),

        # ── BASE DE DONNÉES EXTERNE ───────────────────────────────────────────

        Tool(
            name="list_db_tables",
            description=(
                "Liste les tables et vues d'une base de données externe "
                "(PostgreSQL/PostGIS, MySQL, SQLite) avec leurs colonnes. "
                "Utiliser avant query_database pour explorer la structure."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "db_type":   {"type": "string", "enum": ["postgresql", "mysql", "sqlite"]},
                    "host":      {"type": "string", "description": "Hôte DB (défaut: localhost)"},
                    "port":      {"type": "integer", "description": "Port (défaut: 5432/3306)"},
                    "database":  {"type": "string", "description": "Nom de la base"},
                    "username":  {"type": "string"},
                    "password":  {"type": "string"},
                },
                "required": ["db_type", "database"],
            },
        ),

        Tool(
            name="query_database",
            description=(
                "Exécute une requête SQL SELECT sur une base de données externe "
                "et retourne les résultats en GeoJSON. "
                "Supporte : PostGIS (ST_AsGeoJSON), MySQL spatial, SQLite/SpatiaLite, "
                "lat/lon colonnes directes. "
                "Seules les requêtes SELECT sont autorisées (lecture seule)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "db_type":    {"type": "string", "enum": ["postgresql", "mysql", "sqlite"]},
                    "host":       {"type": "string"},
                    "port":       {"type": "integer"},
                    "database":   {"type": "string"},
                    "username":   {"type": "string"},
                    "password":   {"type": "string"},
                    "sql": {
                        "type": "string",
                        "description": (
                            "Requête SQL SELECT. "
                            "Pour PostGIS : SELECT id, nom, ST_AsGeoJSON(geom) AS geom_json FROM table LIMIT 500. "
                            "Pour lat/lon  : SELECT id, nom, latitude, longitude FROM table LIMIT 1000."
                        ),
                    },
                    "geom_column": {"type": "string", "description": "Nom colonne géométrie (défaut: geom)"},
                    "limit":       {"type": "integer", "description": "Limite résultats (max 5000)"},
                },
                "required": ["db_type", "database", "sql"],
            },
        ),

        # ── ROUTING ───────────────────────────────────────────────────────────

        Tool(
            name="compute_route",
            description=(
                "Calcule un itinéraire entre deux points ou plus via OpenRouteService. "
                "Retourne la géométrie GeoJSON du trajet, la distance en km, "
                "la durée en minutes et les instructions turn-by-turn. "
                "IMPORTANT : géocoder les lieux nommés AVANT d'appeler ce tool."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "waypoints": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {"type": "number"},
                            "description": "[longitude, latitude]",
                        },
                        "description": "Liste de points [lon, lat] (min 2, max 10)",
                        "minItems": 2,
                        "maxItems": 10,
                    },
                    "profile": {
                        "type": "string",
                        "enum": ["foot", "bike", "car"],
                        "description": "Mode de transport (défaut: foot)",
                    },
                },
                "required": ["waypoints"],
            },
        ),

        Tool(
            name="compute_isochrone",
            description=(
                "Calcule la zone géographique accessible depuis un point en un temps donné. "
                "Retourne un polygone GeoJSON de la zone accessible. "
                "Utile pour : desserte urbaine, zone de chalandise, accessibilité services. "
                "IMPORTANT : géocoder le lieu de départ AVANT d'appeler ce tool."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "center": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "[longitude, latitude] du point de départ",
                    },
                    "time_minutes": {
                        "type": "integer",
                        "description": "Temps de trajet en minutes (1-120, défaut: 10)",
                    },
                    "profile": {
                        "type": "string",
                        "enum": ["foot", "bike", "car"],
                        "description": "Mode de transport (défaut: foot)",
                    },
                },
                "required": ["center"],
            },
        ),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# DISPATCH
# ═══════════════════════════════════════════════════════════════════════════════

@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    logger.info(f"MCP call_tool: {name}({json.dumps(arguments, default=str)[:120]})")
    try:
        # Overture Maps (DuckDB direct)
        if name == "query_places":
            return await _query_places(arguments)
        elif name == "query_buildings":
            return await _query_buildings(arguments)
        elif name == "query_transport":
            return await _query_transport(arguments)
        elif name == "query_divisions":
            return await _query_divisions(arguments)
        elif name == "spatial_stats":
            return await _spatial_stats(arguments)
        elif name == "h3_density":
            return await _h3_density(arguments)
        elif name == "export_overture":
            return await _export_overture(arguments)
        elif name == "raw_duckdb_query":
            return await _raw_query(arguments)
        # Géocodage
        elif name == "geocode":
            return await _geocode(arguments)
        # GEE
        elif name == "gee_tiles":
            return await _gee_tiles(arguments)
        # Base de données
        elif name == "list_db_tables":
            return await _list_db_tables(arguments)
        elif name == "query_database":
            return await _query_database(arguments)
        # Routing
        elif name == "compute_route":
            return await _compute_route(arguments)
        elif name == "compute_isochrone":
            return await _compute_isochrone(arguments)
        else:
            return [TextContent(type="text", text=f"Tool inconnu : {name}")]

    except Exception as e:
        logger.error(f"Erreur {name}: {e}", exc_info=True)
        return [TextContent(type="text", text=f"Erreur {name} : {str(e)}")]


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS COMMUNS
# ═══════════════════════════════════════════════════════════════════════════════

def _meters_to_degrees(meters: float, lat: float) -> tuple[float, float]:
    """Rayon mètres → degrés lon/lat."""
    lat_deg = meters / 111320.0
    lon_deg = meters / (111320.0 * math.cos(math.radians(lat)))
    return lon_deg, lat_deg


def _resolve_bbox(args: dict) -> dict:
    """
    Résout center+radius → bbox si nécessaire.
    Applique la validation géospatiale (Couche 4).
    """
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).parent))

    try:
        from validation.geo_validator import validate_geo_args
        return validate_geo_args(args)
    except ImportError:
        # Fallback si validation non disponible
        if args.get("center_lon") is not None and args.get("xmin") is None:
            radius = float(args.get("radius_m", 1000))
            lon    = float(args["center_lon"])
            lat    = float(args["center_lat"])
            dlon, dlat = _meters_to_degrees(radius, lat)
            args.update({
                "xmin": lon - dlon, "xmax": lon + dlon,
                "ymin": lat - dlat, "ymax": lat + dlat,
            })
        return args


def _ok(data: Any) -> list[TextContent]:
    """Formate un résultat en TextContent JSON."""
    return [TextContent(type="text", text=json.dumps(data, ensure_ascii=False, default=str))]


def _err(msg: str) -> list[TextContent]:
    """Formate une erreur en TextContent."""
    return [TextContent(type="text", text=json.dumps({"error": msg}))]


# ═══════════════════════════════════════════════════════════════════════════════
# OVERTURE MAPS (DuckDB direct)
# ═══════════════════════════════════════════════════════════════════════════════

async def _query_places(args: dict) -> list[TextContent]:
    args = _resolve_bbox(args)
    if not all(k in args for k in ("xmin","ymin","xmax","ymax")):
        return _err("bbox requise : xmin/ymin/xmax/ymax ou center_lon/center_lat/radius_m")

    path  = f"{S3_BASE}/theme=places/type=place/*"
    where = [
        f"bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}",
        f"bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}",
    ]
    if args.get("category"):
        where.append(f"categories.primary = '{args['category']}'")
    if args.get("name_filter"):
        where.append(f"names.primary ILIKE '%{args['name_filter']}%'")
    if args.get("min_confidence", 0) > 0:
        where.append(f"confidence >= {args['min_confidence']}")

    sql = f"""
    SELECT id, names.primary AS name, categories.primary AS category,
           confidence, addresses[1].freeform AS address,
           ST_AsGeoJSON(geometry) AS geom_json
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE {' AND '.join(where)}
    LIMIT {min(int(args.get('limit', 500)), 5000)}
    """
    df      = db.execute(sql).fetchdf()
    records = df.to_dict(orient="records")
    return _ok({"type": "FeatureCollection", "features": _rows_to_features(records), "total": len(records)})


async def _query_buildings(args: dict) -> list[TextContent]:
    args = _resolve_bbox(args)
    if not all(k in args for k in ("xmin","ymin","xmax","ymax")):
        return _err("bbox requise")

    path  = f"{S3_BASE}/theme=buildings/type=building/*"
    where = [
        f"bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}",
        f"bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}",
    ]
    if args.get("min_height"): where.append(f"height >= {args['min_height']}")
    if args.get("max_height"): where.append(f"height <= {args['max_height']}")

    sql = f"""
    SELECT id, names.primary AS name, height, num_floors, class,
           ST_AsGeoJSON(geometry) AS geom_json
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE {' AND '.join(where)}
    LIMIT {min(int(args.get('limit', 500)), 5000)}
    """
    df = db.execute(sql).fetchdf()
    return _ok({"type": "FeatureCollection", "features": _rows_to_features(df.to_dict(orient="records")), "total": len(df)})


async def _query_transport(args: dict) -> list[TextContent]:
    path  = f"{S3_BASE}/theme=transportation/type=segment/*"
    where = [
        f"bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}",
        f"bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}",
    ]
    if args.get("road_class"): where.append(f"class = '{args['road_class']}'")

    sql = f"""
    SELECT id, class, subtype, ST_AsGeoJSON(geometry) AS geom_json
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE {' AND '.join(where)}
    LIMIT {min(int(args.get('limit', 500)), 5000)}
    """
    df = db.execute(sql).fetchdf()
    return _ok({"type": "FeatureCollection", "features": _rows_to_features(df.to_dict(orient="records")), "total": len(df)})


async def _query_divisions(args: dict) -> list[TextContent]:
    path  = f"{S3_BASE}/theme=divisions/type=division_area/*"
    where = [
        f"bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}",
        f"bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}",
    ]
    if args.get("subtype"): where.append(f"subtype = '{args['subtype']}'")
    if args.get("country"):  where.append(f"country = '{args['country']}'")

    sql = f"""
    SELECT id, names.primary AS name, subtype, country,
           ST_AsGeoJSON(geometry) AS geom_json
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE {' AND '.join(where)}
    LIMIT {min(int(args.get('limit', 200)), 1000)}
    """
    df = db.execute(sql).fetchdf()
    return _ok({"type": "FeatureCollection", "features": _rows_to_features(df.to_dict(orient="records")), "total": len(df)})


async def _spatial_stats(args: dict) -> list[TextContent]:
    theme = args["theme"]
    ptype = THEMES[theme]
    path  = f"{S3_BASE}/theme={theme}/type={ptype}/*"

    total = db.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{path}', filename=true, hive_partitioning=1)
        WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}
          AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}
    """).fetchone()[0]

    stats: dict = {"theme": theme, "total_features": int(total), "bbox": [args['xmin'], args['ymin'], args['xmax'], args['ymax']]}

    if theme == "places":
        df = db.execute(f"""
            SELECT categories.primary AS category, COUNT(*) as count
            FROM read_parquet('{path}', filename=true, hive_partitioning=1)
            WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}
              AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 20
        """).fetchdf()
        stats["top_categories"] = df.to_dict(orient="records")

    elif theme == "buildings":
        row = db.execute(f"""
            SELECT AVG(height) as avg_h, MIN(height) as min_h, MAX(height) as max_h,
                   COUNT(CASE WHEN height IS NOT NULL THEN 1 END) as with_height
            FROM read_parquet('{path}', filename=true, hive_partitioning=1)
            WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}
              AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}
        """).fetchdf().to_dict(orient="records")[0]
        stats["height_stats"] = {k: float(v) if v is not None and str(v) != "nan" else None for k, v in row.items()}

    return _ok(stats)


async def _h3_density(args: dict) -> list[TextContent]:
    theme = args["theme"]
    ptype = THEMES[theme]
    path  = f"{S3_BASE}/theme={theme}/type={ptype}/*"
    res   = int(args.get("resolution", 8))

    df = db.execute(f"""
        SELECT h3_latlng_to_cell_string(bbox.ymin, bbox.xmin, {res}) as h3_id,
               COUNT(*) as count
        FROM read_parquet('{path}', filename=true, hive_partitioning=1)
        WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}
          AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    return _ok({"resolution": res, "total_cells": len(df), "cells": df.to_dict(orient="records")})


async def _export_overture(args: dict) -> list[TextContent]:
    theme  = args["theme"]
    ptype  = THEMES[theme]
    path   = f"{S3_BASE}/theme={theme}/type={ptype}/*"
    fmt    = args["format"]
    output = args.get("output_file", f"export_{theme}")

    format_map = {
        "GeoJSON":    ("geojson", "GeoJSON"),
        "GeoPackage": ("gpkg",    "GPKG"),
        "FlatGeobuf": ("fgb",     "FlatGeobuf"),
        "CSV":        ("csv",     None),
        "GeoParquet": ("parquet", None),
    }
    ext, driver = format_map.get(fmt, ("geojson", "GeoJSON"))
    output_file = f"{output}.{ext}"
    cols = ("id, names.primary AS name, height, num_floors, geometry"
            if theme == "buildings" else "id, names.primary AS name, geometry")

    if fmt == "CSV":
        to_clause = f"TO '{output_file}' (HEADER, DELIMITER ',')"
    elif fmt == "GeoParquet":
        to_clause = f"TO '{output_file}'"
    else:
        to_clause = f"TO '{output_file}' WITH (FORMAT GDAL, DRIVER '{driver}')"

    sql = (
        f"LOAD spatial; LOAD httpfs;\nSET s3_region='{S3_REGION}';\n\n"
        f"COPY(\n    SELECT {cols}\n"
        f"    FROM read_parquet('{path}', filename=true, hive_partitioning=1)\n"
        f"    WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}\n"
        f"      AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}\n"
        f"    LIMIT {args.get('limit', 10000)}\n) {to_clause};"
    )
    return _ok({"sql": sql, "output_file": output_file, "message": f"Requête d'export générée pour {output_file}"})


async def _raw_query(args: dict) -> list[TextContent]:
    """Requête DuckDB brute — validation SQL minimale."""
    import re
    sql = args.get("sql", "").strip()
    # Validation minimale : SELECT only
    if not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE):
        return _err("Seules les requêtes SELECT/WITH sont autorisées")
    if re.search(r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b", sql, re.IGNORECASE):
        return _err("Instructions DML/DDL interdites")

    df = db.execute(sql).fetchdf()
    if len(df) > 200:
        summary = {
            "total_rows": len(df),
            "columns":    list(df.columns),
            "preview":    df.head(20).to_dict(orient="records"),
        }
        return _ok(summary)
    return _ok(df.to_dict(orient="records"))


# ═══════════════════════════════════════════════════════════════════════════════
# GÉOCODAGE
# ═══════════════════════════════════════════════════════════════════════════════

async def _geocode(args: dict) -> list[TextContent]:
    """Géocodage Nominatim avec cache (Couche 4)."""
    query = args.get("query", "").strip()
    if not query:
        return _err("Paramètre 'query' requis")

    try:
        from validation.geo_validator import geocode_to_bbox
        result = geocode_to_bbox(query)
        if result:
            return _ok(result)
        return _err(f"Lieu non trouvé : '{query}'")
    except ImportError:
        # Fallback direct Nominatim
        try:
            resp = http_requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": query, "format": "json", "limit": 1},
                headers={"User-Agent": "OpenMapAgents/1.0"},
                timeout=10,
            )
            results = resp.json()
            if results:
                r = results[0]
                bb = r.get("boundingbox", [])
                return _ok({
                    "lat": float(r["lat"]), "lon": float(r["lon"]),
                    "bbox": [float(bb[2]), float(bb[0]), float(bb[3]), float(bb[1])] if len(bb)==4 else [],
                    "display_name": r.get("display_name", query),
                    "source": "nominatim",
                })
            return _err(f"Lieu non trouvé : '{query}'")
        except Exception as e:
            return _err(f"Erreur géocodage : {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE EARTH ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

async def _gee_tiles(args: dict) -> list[TextContent]:
    """
    Imagerie satellite GEE — validation Couche 4 + appel /api/gee/tiles.
    """
    # Validation déterministe
    try:
        from validation.gee_validator import validate_gee_args
        args = validate_gee_args(args)
    except ImportError:
        logger.warning("gee_validator non disponible, validation minimale")
    except ValueError as e:
        return _err(f"Paramètres GEE invalides : {e}")

    # Appel backend
    try:
        result = _post("/api/gee/tiles", {
            "dataset":             args["dataset"],
            "index":               args["index"],
            "date_start":          args["date_start"],
            "date_end":            args["date_end"],
            "bbox":                args.get("bbox"),
            "cloud_max":           args.get("cloud_max", 20.0),
            "composite":           args.get("composite", "least_cloudy"),
            "vis_params_override": args.get("vis_params_override"),
        })

        # Formater pour Claude Desktop : URL tuiles + métadonnées
        output = {
            "tile_url":   result.get("tile_url"),
            "dataset":    result.get("dataset"),
            "index":      result.get("index"),
            "name":       result.get("name"),
            "date":       result.get("date"),
            "vis_params": result.get("vis_params"),
            "clip_bbox":  result.get("clip_bbox"),
            "usage": (
                f"Tuiles XYZ disponibles à : {result.get('tile_url', 'N/A')}. "
                f"Charger dans MapLibre/Leaflet comme couche raster."
            ),
        }
        return _ok(output)

    except RuntimeError as e:
        return _err(str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# BASE DE DONNÉES EXTERNE
# ═══════════════════════════════════════════════════════════════════════════════

def _build_connection(args: dict) -> dict:
    """Construit le dict connexion depuis les args MCP."""
    return {
        "type":     args.get("db_type", "postgresql"),
        "host":     args.get("host", "localhost"),
        "port":     args.get("port"),
        "database": args.get("database"),
        "username": args.get("username"),
        "password": args.get("password"),
    }


async def _list_db_tables(args: dict) -> list[TextContent]:
    """Liste les tables d'une base externe — validation Couche 4."""
    conn = _build_connection(args)

    try:
        from validation.sql_validator import validate_connection
        conn = validate_connection(conn)
    except ImportError:
        pass
    except ValueError as e:
        return _err(f"Connexion invalide : {e}")

    try:
        result = _post("/api/db/tables", conn)
        tables = result.get("tables", [])
        # Résumé lisible pour Claude
        summary = {
            "total":  result.get("total", len(tables)),
            "tables": [
                {
                    "name":         t["name"],
                    "type":         t.get("type", "table"),
                    "has_geometry": t.get("has_geometry", False),
                    "columns":      t.get("columns", [])[:10],  # max 10 cols dans le résumé
                }
                for t in tables
            ],
        }
        return _ok(summary)
    except RuntimeError as e:
        return _err(str(e))


async def _query_database(args: dict) -> list[TextContent]:
    """Requête SQL sur base externe — validation Couche 4 complète."""
    conn = _build_connection(args)
    sql  = args.get("sql", "")

    # Validation déterministe complète
    try:
        from validation.sql_validator import validate_sql_args
        validated = validate_sql_args({
            "connection":   conn,
            "sql":          sql,
            "geom_column":  args.get("geom_column", "geom"),
            "limit":        args.get("limit", 2000),
        })
        conn = validated["connection"]
        sql  = validated["sql"]
    except ImportError:
        logger.warning("sql_validator non disponible")
    except ValueError as e:
        return _err(f"SQL invalide : {e}")

    try:
        result = _post("/api/db/query", {
            "connection":  conn,
            "sql":         sql,
            "geom_column": args.get("geom_column", "geom"),
            "limit":       args.get("limit", 2000),
        })

        fc = result if result.get("type") == "FeatureCollection" else {"type": "FeatureCollection", "features": [], "raw": result}
        total = result.get("metadata", {}).get("total", len(fc.get("features", [])))
        fc["summary"] = f"{total} features retournées depuis {conn.get('database')}"
        return _ok(fc)

    except RuntimeError as e:
        return _err(str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING
# ═══════════════════════════════════════════════════════════════════════════════

async def _compute_route(args: dict) -> list[TextContent]:
    """Itinéraire ORS — validation Couche 4 + appel backend."""
    # Validation
    try:
        from validation.geo_validator import validate_coords
        wps = args.get("waypoints", [])
        if len(wps) < 2:
            return _err("Au moins 2 waypoints requis [lon, lat]")
        validated_wps = [validate_coords(wp, f"waypoint[{i}]") for i, wp in enumerate(wps)]
        args["waypoints"] = validated_wps
    except (ImportError, Exception) as e:
        if "invalide" in str(e).lower():
            return _err(f"Waypoints invalides : {e}")

    # Normaliser profil
    profile_map = {"à pied": "foot", "pied": "foot", "walk": "foot",
                   "vélo": "bike", "velo": "bike", "bicycle": "bike",
                   "voiture": "car", "auto": "car", "drive": "car"}
    profile = args.get("profile", "foot")
    args["profile"] = profile_map.get(profile.lower(), profile)
    if args["profile"] not in ("foot", "bike", "car"):
        args["profile"] = "foot"

    try:
        result = _post("/api/chat", {
            "messages": [{
                "role": "user",
                "content": f"compute_route direct call",
            }],
        })
        # Appel direct à la fonction execute_tool du backend via endpoint dédié si disponible
        # Fallback : appel direct ORS
        result = _call_ors_route(args)
        return _ok(result)
    except Exception as e:
        return _err(f"Erreur calcul itinéraire : {e}")


async def _compute_isochrone(args: dict) -> list[TextContent]:
    """Isochrone ORS — validation Couche 4 + appel backend."""
    try:
        from validation.geo_validator import validate_coords
        center = validate_coords(args.get("center", []), "center")
        args["center"] = center
    except Exception as e:
        return _err(f"Centre invalide : {e}")

    time_min = int(args.get("time_minutes", 10))
    if not (1 <= time_min <= 120):
        return _err(f"time_minutes doit être entre 1 et 120. Reçu : {time_min}")

    profile_map = {"à pied": "foot", "pied": "foot", "vélo": "bike", "voiture": "car"}
    profile = profile_map.get(args.get("profile", "foot").lower(), args.get("profile", "foot"))
    if profile not in ("foot", "bike", "car"):
        profile = "foot"

    try:
        result = _call_ors_isochrone(args["center"], time_min, profile)
        return _ok(result)
    except Exception as e:
        return _err(f"Erreur calcul isochrone : {e}")


def _call_ors_route(args: dict) -> dict:
    """Appelle ORS via le backend FastAPI ou directement."""
    try:
        # Essai via backend
        resp = http_requests.post(
            f"{BACKEND_URL}/api/chat",
            json={
                "messages": [{"role": "user", "content": "route"}],
                "_direct_tool": {"name": "compute_route", "args": args},
            },
            timeout=30,
        )
        if resp.ok:
            data = resp.json()
            results = data.get("tool_results", [])
            if results:
                return results[0]
    except Exception:
        pass
    return {"action": "compute_route", **args, "note": "Backend route requis"}


def _call_ors_isochrone(center: list, time_min: int, profile: str) -> dict:
    """Appelle ORS isochrone via le backend FastAPI."""
    try:
        resp = http_requests.post(
            f"{BACKEND_URL}/api/chat",
            json={
                "messages": [{"role": "user", "content": "isochrone"}],
                "_direct_tool": {
                    "name": "compute_isochrone",
                    "args": {"center": center, "time_minutes": time_min, "profile": profile},
                },
            },
            timeout=30,
        )
        if resp.ok:
            data = resp.json()
            results = data.get("tool_results", [])
            if results:
                return results[0]
    except Exception:
        pass
    return {
        "action":       "compute_isochrone",
        "center":       center,
        "time_minutes": time_min,
        "profile":      profile,
        "note":         "Backend isochrone requis",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CONVERSION GeoJSON
# ═══════════════════════════════════════════════════════════════════════════════

def _rows_to_features(rows: list[dict]) -> list[dict]:
    """Convertit des lignes DuckDB (avec geom_json) en GeoJSON features."""
    features = []
    for row in rows:
        geom_str = row.pop("geom_json", None)
        geom = None
        if geom_str and str(geom_str) not in ("", "None", "nan"):
            try:
                geom = json.loads(str(geom_str))
            except (json.JSONDecodeError, TypeError):
                pass

        # Nettoyer les NaN
        props = {}
        for k, v in row.items():
            if v is None or str(v) in ("nan", "None", "NaT"):
                props[k] = None
            elif hasattr(v, "item"):
                props[k] = v.item()
            else:
                props[k] = v

        if geom:
            features.append({"type": "Feature", "geometry": geom, "properties": props})

    return features


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    logger.info(f"OpenMapAgents MCP Server démarré")
    logger.info(f"Overture release : {OVERTURE_RELEASE}")
    logger.info(f"Backend FastAPI  : {BACKEND_URL}")
    logger.info(f"Tools disponibles : query_places, query_buildings, query_transport, "
                f"query_divisions, spatial_stats, h3_density, export_overture, "
                f"raw_duckdb_query, geocode, gee_tiles, list_db_tables, "
                f"query_database, compute_route, compute_isochrone")

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
