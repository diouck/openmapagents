"""
Overture Maps MCP Server
Expose les données Overture Maps via le protocole MCP
pour intégration avec Claude, Cursor, etc.
"""
import os
import json
import logging
from typing import Any

import duckdb
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("overture-mcp")

# ─── CONFIG ──────────────────────────────────────────────────────
OVERTURE_RELEASE = os.getenv("OVERTURE_RELEASE", "2026-03-18.0")
S3_BASE = f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"
DUCKDB_MEMORY = os.getenv("DUCKDB_MEMORY", "4GB")

THEMES = {
    "places": "place",
    "buildings": "building",
    "transportation": "segment",
    "divisions": "division",
    "base": "land",
    "addresses": "address",
}


# ─── DUCKDB CONNECTION ───────────────────────────────────────────
def get_db():
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    try:
        conn.execute("INSTALL h3 FROM community; LOAD h3;")
    except Exception:
        pass
    conn.execute(f"SET s3_region='us-west-2';")
    conn.execute(f"SET memory_limit='{DUCKDB_MEMORY}';")
    return conn


db = get_db()


# ─── MCP SERVER ──────────────────────────────────────────────────
server = Server("overture-maps")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="query_places",
            description="Recherche de points d'intérêt (POI) Overture Maps par bounding box, catégorie et nom. "
                       "Retourne nom, catégorie, confiance et coordonnées.",
            inputSchema={
                "type": "object",
                "properties": {
                    "xmin": {"type": "number", "description": "Longitude min (ouest)"},
                    "ymin": {"type": "number", "description": "Latitude min (sud)"},
                    "xmax": {"type": "number", "description": "Longitude max (est)"},
                    "ymax": {"type": "number", "description": "Latitude max (nord)"},
                    "category": {"type": "string", "description": "Catégorie Overture (ex: restaurant, pharmacy, school)"},
                    "name_filter": {"type": "string", "description": "Filtre sur le nom (ILIKE)"},
                    "min_confidence": {"type": "number", "description": "Score de confiance minimum (0-1)"},
                    "limit": {"type": "integer", "description": "Nombre max de résultats", "default": 100},
                },
                "required": ["xmin", "ymin", "xmax", "ymax"],
            },
        ),
        Tool(
            name="query_buildings",
            description="Recherche de bâtiments Overture Maps par bounding box et critères de hauteur. "
                       "Retourne nom, hauteur, nombre d'étages et emprise.",
            inputSchema={
                "type": "object",
                "properties": {
                    "xmin": {"type": "number"}, "ymin": {"type": "number"},
                    "xmax": {"type": "number"}, "ymax": {"type": "number"},
                    "min_height": {"type": "number", "description": "Hauteur minimum en mètres"},
                    "max_height": {"type": "number", "description": "Hauteur maximum en mètres"},
                    "limit": {"type": "integer", "default": 100},
                },
                "required": ["xmin", "ymin", "xmax", "ymax"],
            },
        ),
        Tool(
            name="query_transport",
            description="Recherche de segments du réseau routier Overture Maps par bbox et classe.",
            inputSchema={
                "type": "object",
                "properties": {
                    "xmin": {"type": "number"}, "ymin": {"type": "number"},
                    "xmax": {"type": "number"}, "ymax": {"type": "number"},
                    "road_class": {"type": "string", "description": "Classe de route (motorway, primary, secondary, tertiary, residential)"},
                    "limit": {"type": "integer", "default": 100},
                },
                "required": ["xmin", "ymin", "xmax", "ymax"],
            },
        ),
        Tool(
            name="spatial_stats",
            description="Statistiques spatiales agrégées pour un thème Overture Maps : "
                       "total features, distribution par catégorie/classe, stats de hauteur.",
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
            description="Calcul de densité hexagonale H3 pour un thème Overture Maps. "
                       "Retourne le nombre de features par cellule H3.",
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
            description="Génère une requête DuckDB SQL pour exporter des données Overture Maps "
                       "dans le format demandé (GeoJSON, GeoPackage, CSV, Parquet, etc.).",
            inputSchema={
                "type": "object",
                "properties": {
                    "theme": {"type": "string", "enum": list(THEMES.keys())},
                    "xmin": {"type": "number"}, "ymin": {"type": "number"},
                    "xmax": {"type": "number"}, "ymax": {"type": "number"},
                    "format": {"type": "string", "enum": ["GeoJSON", "GeoPackage", "CSV", "GeoParquet", "FlatGeobuf"]},
                    "output_file": {"type": "string", "description": "Nom du fichier de sortie"},
                    "limit": {"type": "integer", "default": 10000},
                },
                "required": ["theme", "xmin", "ymin", "xmax", "ymax", "format", "output_file"],
            },
        ),
        Tool(
            name="raw_duckdb_query",
            description="Exécute une requête DuckDB SQL brute sur les données Overture Maps. "
                       "Utiliser avec précaution. Le S3 base path est déjà configuré.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "Requête SQL DuckDB"},
                },
                "required": ["sql"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    try:
        if name == "query_places":
            return await _query_places(arguments)
        elif name == "query_buildings":
            return await _query_buildings(arguments)
        elif name == "query_transport":
            return await _query_transport(arguments)
        elif name == "spatial_stats":
            return await _spatial_stats(arguments)
        elif name == "h3_density":
            return await _h3_density(arguments)
        elif name == "export_overture":
            return await _export_overture(arguments)
        elif name == "raw_duckdb_query":
            return await _raw_query(arguments)
        else:
            return [TextContent(type="text", text=f"Tool inconnu: {name}")]
    except Exception as e:
        logger.error(f"Erreur {name}: {e}")
        return [TextContent(type="text", text=f"Erreur: {str(e)}")]


async def _query_places(args: dict) -> list[TextContent]:
    path = f"{S3_BASE}/theme=places/type=place/*"
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
           confidence, ST_AsText(geometry) AS geom_wkt
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE {' AND '.join(where)}
    LIMIT {args.get('limit', 100)}
    """
    df = db.execute(sql).fetchdf()
    result = df.to_dict(orient="records")
    return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, default=str))]


async def _query_buildings(args: dict) -> list[TextContent]:
    path = f"{S3_BASE}/theme=buildings/type=building/*"
    where = [
        f"bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}",
        f"bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}",
    ]
    if args.get("min_height"):
        where.append(f"height >= {args['min_height']}")
    if args.get("max_height"):
        where.append(f"height <= {args['max_height']}")

    sql = f"""
    SELECT id, names.primary AS name, height, num_floors, class,
           ST_AsText(geometry) AS geom_wkt
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE {' AND '.join(where)}
    LIMIT {args.get('limit', 100)}
    """
    df = db.execute(sql).fetchdf()
    return [TextContent(type="text", text=json.dumps(df.to_dict(orient="records"), default=str))]


async def _query_transport(args: dict) -> list[TextContent]:
    path = f"{S3_BASE}/theme=transportation/type=segment/*"
    where = [
        f"bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}",
        f"bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}",
    ]
    if args.get("road_class"):
        where.append(f"class = '{args['road_class']}'")

    sql = f"""
    SELECT id, class, subtype, ST_AsText(geometry) AS geom_wkt
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE {' AND '.join(where)}
    LIMIT {args.get('limit', 100)}
    """
    df = db.execute(sql).fetchdf()
    return [TextContent(type="text", text=json.dumps(df.to_dict(orient="records"), default=str))]


async def _spatial_stats(args: dict) -> list[TextContent]:
    theme = args["theme"]
    ptype = THEMES[theme]
    path = f"{S3_BASE}/theme={theme}/type={ptype}/*"

    sql_count = f"""
    SELECT COUNT(*) as total
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}
      AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}
    """
    total = db.execute(sql_count).fetchone()[0]

    stats = {"theme": theme, "total_features": total}

    if theme == "places":
        sql_cats = f"""
        SELECT categories.primary AS category, COUNT(*) as count
        FROM read_parquet('{path}', filename=true, hive_partitioning=1)
        WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}
          AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """
        cats_df = db.execute(sql_cats).fetchdf()
        stats["categories"] = cats_df.to_dict(orient="records")

    elif theme == "buildings":
        sql_h = f"""
        SELECT AVG(height) as avg_h, MIN(height) as min_h, MAX(height) as max_h
        FROM read_parquet('{path}', filename=true, hive_partitioning=1)
        WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}
          AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}
          AND height IS NOT NULL
        """
        h_df = db.execute(sql_h).fetchdf()
        stats["height_stats"] = h_df.to_dict(orient="records")[0]

    return [TextContent(type="text", text=json.dumps(stats, default=str))]


async def _h3_density(args: dict) -> list[TextContent]:
    theme = args["theme"]
    ptype = THEMES[theme]
    path = f"{S3_BASE}/theme={theme}/type={ptype}/*"
    res = args.get("resolution", 8)

    sql = f"""
    SELECT h3_latlng_to_cell_string(bbox.ymin, bbox.xmin, {res}) as h3_id,
           COUNT(*) as count
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}
      AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}
    GROUP BY 1 ORDER BY 2 DESC
    """
    df = db.execute(sql).fetchdf()
    return [TextContent(type="text", text=json.dumps({
        "resolution": res,
        "total_cells": len(df),
        "cells": df.to_dict(orient="records"),
    }, default=str))]


async def _export_overture(args: dict) -> list[TextContent]:
    theme = args["theme"]
    ptype = THEMES[theme]
    path = f"{S3_BASE}/theme={theme}/type={ptype}/*"

    format_map = {
        "GeoJSON": ("geojson", "GeoJSON"),
        "GeoPackage": ("gpkg", "GPKG"),
        "FlatGeobuf": ("fgb", "FlatGeobuf"),
        "CSV": ("csv", None),
        "GeoParquet": ("parquet", None),
    }
    ext, driver = format_map.get(args["format"], ("geojson", "GeoJSON"))
    output = args.get("output_file", f"export_{theme}") + f".{ext}"

    cols = "id, names.primary AS name, geometry" if theme != "buildings" \
        else "id, names.primary AS name, height, num_floors, geometry"

    if args["format"] == "CSV":
        to_clause = f"TO '{output}' (HEADER, DELIMITER ',')"
    elif args["format"] == "GeoParquet":
        to_clause = f"TO '{output}'"
    else:
        to_clause = f"TO '{output}' WITH (FORMAT GDAL, DRIVER '{driver}')"

    sql = f"""LOAD spatial; LOAD httpfs;
SET s3_region='us-west-2';

COPY(
    SELECT {cols}
    FROM read_parquet('{path}', filename=true, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {args['xmin']} AND {args['xmax']}
      AND bbox.ymin BETWEEN {args['ymin']} AND {args['ymax']}
    LIMIT {args.get('limit', 10000)}
) {to_clause};"""

    return [TextContent(type="text", text=f"Requête d'export générée :\n\n```sql\n{sql}\n```\n\nFichier de sortie: {output}")]


async def _raw_query(args: dict) -> list[TextContent]:
    sql = args["sql"]
    df = db.execute(sql).fetchdf()
    if len(df) > 100:
        summary = f"Résultat: {len(df)} lignes, {len(df.columns)} colonnes\n\n"
        summary += f"Colonnes: {', '.join(df.columns)}\n\n"
        summary += "Aperçu (10 premières lignes):\n"
        summary += df.head(10).to_string()
        return [TextContent(type="text", text=summary)]
    return [TextContent(type="text", text=df.to_string())]


# ─── MAIN ────────────────────────────────────────────────────────
async def main():
    logger.info(f"Démarrage Overture Maps MCP Server (release {OVERTURE_RELEASE})")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
