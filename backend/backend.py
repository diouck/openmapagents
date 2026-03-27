"""
Overture Maps Explorer — FastAPI Backend
Moteur DuckDB pour requêtes directes sur S3 GeoParquet
"""
import os
import json
import hashlib
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

# ─── CONFIG ──────────────────────────────────────────────────────
OVERTURE_RELEASE = os.getenv("OVERTURE_RELEASE", "2026-03-18.0")
S3_BASE = f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"
S3_REGION = "us-west-2"
DUCKDB_MEMORY = os.getenv("DUCKDB_MEMORY", "4GB")
DUCKDB_THREADS = int(os.getenv("DUCKDB_THREADS", "4"))
CACHE_DIR = Path(os.getenv("CACHE_DIR", "./data/cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

THEMES = {
    "places": ["place"],
    "buildings": ["building"],
    "transportation": ["segment", "connector"],
    "divisions": ["division", "division_area", "division_boundary"],
    "base": ["land", "land_cover", "land_use", "water"],
    "addresses": ["address"],
}


# ─── DUCKDB ENGINE ───────────────────────────────────────────────
class DuckDBEngine:
    """Gestionnaire de connexion DuckDB avec extensions spatiales."""

    def __init__(self):
        self.conn = None

    def connect(self):
        self.conn = duckdb.connect(database=":memory:")
        self.conn.execute("INSTALL spatial; LOAD spatial;")
        self.conn.execute("INSTALL httpfs; LOAD httpfs;")
        try:
            self.conn.execute("INSTALL h3 FROM community; LOAD h3;")
        except Exception:
            pass  # h3 optionnel
        self.conn.execute(f"SET s3_region='{S3_REGION}';")
        self.conn.execute(f"SET memory_limit='{DUCKDB_MEMORY}';")
        self.conn.execute(f"SET threads={DUCKDB_THREADS};")
        return self

    def query(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).fetchdf()

    def query_geojson(self, sql: str) -> dict:
        df = self.query(sql)
        features = []
        for _, row in df.iterrows():
            props = {k: v for k, v in row.items() if k != "geometry"}
            # Convertir les types numpy
            for k, v in props.items():
                if hasattr(v, "item"):
                    props[k] = v.item()
                elif pd.isna(v):
                    props[k] = None
            geom_wkt = row.get("geometry")
            geom = None
            if geom_wkt:
                # Conversion WKT simplifiée pour points
                if "POINT" in str(geom_wkt):
                    coords = str(geom_wkt).replace("POINT (", "").replace(")", "").split()
                    geom = {"type": "Point", "coordinates": [float(coords[0]), float(coords[1])]}
            features.append({"type": "Feature", "properties": props, "geometry": geom})
        return {"type": "FeatureCollection", "features": features}

    def close(self):
        if self.conn:
            self.conn.close()


db = DuckDBEngine()


# ─── CACHE ───────────────────────────────────────────────────────
def cache_key(sql: str) -> str:
    return hashlib.md5(sql.encode()).hexdigest()


def get_cached(sql: str) -> Optional[dict]:
    path = CACHE_DIR / f"{cache_key(sql)}.json"
    if path.exists():
        return json.loads(path.read_text())
    return None


def set_cache(sql: str, data: dict):
    path = CACHE_DIR / f"{cache_key(sql)}.json"
    path.write_text(json.dumps(data))


# ─── MODELS ──────────────────────────────────────────────────────
class BboxQuery(BaseModel):
    xmin: float
    ymin: float
    xmax: float
    ymax: float


class ImportRequest(BaseModel):
    theme: str
    zone_name: str
    bbox: BboxQuery
    max_features: int = 10000


class ExportRequest(BaseModel):
    theme: str
    bbox: BboxQuery
    format: str = "GeoJSON"
    columns: list[str] = ["id", "names.primary AS name", "geometry"]
    max_features: int = 10000


# ─── APP ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    db.connect()
    yield
    db.close()


app = FastAPI(
    title="Overture Maps Explorer API",
    version="1.0.0",
    description="API pour requêter les données Overture Maps via DuckDB",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── ROUTES ──────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "service": "Overture Maps Explorer API",
        "release": OVERTURE_RELEASE,
        "themes": list(THEMES.keys()),
    }


@app.get("/api/themes")
def get_themes():
    return {
        k: {"types": v, "s3_path": f"{S3_BASE}/theme={k}"}
        for k, v in THEMES.items()
    }


@app.get("/api/query/{theme}")
def query_theme(
    theme: str,
    xmin: float = Query(...),
    ymin: float = Query(...),
    xmax: float = Query(...),
    ymax: float = Query(...),
    limit: int = Query(1000, le=50000),
    category: Optional[str] = Query(None),
    min_confidence: float = Query(0.0),
):
    if theme not in THEMES:
        raise HTTPException(404, f"Thème '{theme}' inconnu")

    ptype = THEMES[theme][0]
    parquet_path = f"{S3_BASE}/theme={theme}/type={ptype}/*"

    # Construire les colonnes selon le thème
    cols_map = {
        "places": "id, names.primary AS name, categories.primary AS category, confidence, geometry",
        "buildings": "id, names.primary AS name, height, num_floors, class, geometry",
        "transportation": "id, class, subtype, geometry",
        "divisions": "id, names.primary AS name, subtype, country, geometry",
        "base": "id, class, subtype, geometry",
        "addresses": "id, address_levels, geometry",
    }
    columns = cols_map.get(theme, "id, geometry")

    where_clauses = [
        f"bbox.xmin BETWEEN {xmin} AND {xmax}",
        f"bbox.ymin BETWEEN {ymin} AND {ymax}",
    ]
    if category and theme == "places":
        where_clauses.append(f"categories.primary = '{category}'")
    if min_confidence > 0 and theme == "places":
        where_clauses.append(f"confidence >= {min_confidence}")

    sql = f"""
    SELECT {columns}
    FROM read_parquet('{parquet_path}', filename=true, hive_partitioning=1)
    WHERE {' AND '.join(where_clauses)}
    LIMIT {limit}
    """

    # Vérifier le cache
    cached = get_cached(sql)
    if cached:
        return cached

    try:
        result = db.query_geojson(sql)
        set_cache(sql, result)
        return result
    except Exception as e:
        raise HTTPException(500, f"Erreur DuckDB: {str(e)}")


@app.get("/api/stats/{theme}")
def theme_stats(
    theme: str,
    xmin: float = Query(...),
    ymin: float = Query(...),
    xmax: float = Query(...),
    ymax: float = Query(...),
):
    if theme not in THEMES:
        raise HTTPException(404, f"Thème '{theme}' inconnu")

    ptype = THEMES[theme][0]
    parquet_path = f"{S3_BASE}/theme={theme}/type={ptype}/*"

    # Stats de base : count
    sql_count = f"""
    SELECT COUNT(*) as total
    FROM read_parquet('{parquet_path}', filename=true, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {xmin} AND {xmax}
      AND bbox.ymin BETWEEN {ymin} AND {ymax}
    """

    try:
        count_df = db.query(sql_count)
        total = int(count_df["total"].iloc[0])

        stats = {"theme": theme, "total_features": total, "bbox": [xmin, ymin, xmax, ymax]}

        # Stats spécifiques par thème
        if theme == "places":
            sql_cats = f"""
            SELECT categories.primary AS category, COUNT(*) as count
            FROM read_parquet('{parquet_path}', filename=true, hive_partitioning=1)
            WHERE bbox.xmin BETWEEN {xmin} AND {xmax}
              AND bbox.ymin BETWEEN {ymin} AND {ymax}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 20
            """
            cats_df = db.query(sql_cats)
            stats["top_categories"] = cats_df.to_dict(orient="records")

        elif theme == "buildings":
            sql_heights = f"""
            SELECT
                AVG(height) as avg_height,
                MIN(height) as min_height,
                MAX(height) as max_height,
                COUNT(CASE WHEN height IS NOT NULL THEN 1 END) as with_height
            FROM read_parquet('{parquet_path}', filename=true, hive_partitioning=1)
            WHERE bbox.xmin BETWEEN {xmin} AND {xmax}
              AND bbox.ymin BETWEEN {ymin} AND {ymax}
            """
            h_df = db.query(sql_heights)
            stats["height_stats"] = h_df.to_dict(orient="records")[0]

        return stats
    except Exception as e:
        raise HTTPException(500, f"Erreur DuckDB: {str(e)}")


@app.get("/api/h3/{theme}")
def h3_density(
    theme: str,
    xmin: float = Query(...),
    ymin: float = Query(...),
    xmax: float = Query(...),
    ymax: float = Query(...),
    resolution: int = Query(8, ge=4, le=12),
):
    """Densité H3 pour un thème donné."""
    if theme not in THEMES:
        raise HTTPException(404, f"Thème '{theme}' inconnu")

    ptype = THEMES[theme][0]
    parquet_path = f"{S3_BASE}/theme={theme}/type={ptype}/*"

    sql = f"""
    SELECT
        h3_latlng_to_cell_string(bbox.ymin, bbox.xmin, {resolution}) as h3_id,
        COUNT(*) as count
    FROM read_parquet('{parquet_path}', filename=true, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {xmin} AND {xmax}
      AND bbox.ymin BETWEEN {ymin} AND {ymax}
    GROUP BY 1
    ORDER BY 2 DESC
    """

    try:
        df = db.query(sql)
        return {"resolution": resolution, "cells": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(500, f"Erreur DuckDB: {str(e)}")


@app.post("/api/export")
def export_data(req: ExportRequest):
    """Export de données dans différents formats."""
    if req.theme not in THEMES:
        raise HTTPException(404, f"Thème '{req.theme}' inconnu")

    ptype = THEMES[req.theme][0]
    parquet_path = f"{S3_BASE}/theme={req.theme}/type={ptype}/*"

    columns = ", ".join(req.columns)
    output_name = f"export_{req.theme}_{cache_key(str(req.bbox))}"

    format_map = {
        "GeoJSON": ("geojson", "GeoJSON"),
        "GeoPackage": ("gpkg", "GPKG"),
        "FlatGeobuf": ("fgb", "FlatGeobuf"),
        "Shapefile": ("shp", "ESRI Shapefile"),
        "CSV": ("csv", None),
        "GeoParquet": ("parquet", None),
    }

    ext, driver = format_map.get(req.format, ("geojson", "GeoJSON"))
    output_path = CACHE_DIR / f"{output_name}.{ext}"

    if req.format == "CSV":
        sql = f"""
        COPY(
            SELECT {columns}
            FROM read_parquet('{parquet_path}', filename=true, hive_partitioning=1)
            WHERE bbox.xmin BETWEEN {req.bbox.xmin} AND {req.bbox.xmax}
              AND bbox.ymin BETWEEN {req.bbox.ymin} AND {req.bbox.ymax}
            LIMIT {req.max_features}
        ) TO '{output_path}' (HEADER, DELIMITER ',');
        """
    elif req.format == "GeoParquet":
        sql = f"""
        COPY(
            SELECT {columns}
            FROM read_parquet('{parquet_path}', filename=true, hive_partitioning=1)
            WHERE bbox.xmin BETWEEN {req.bbox.xmin} AND {req.bbox.xmax}
              AND bbox.ymin BETWEEN {req.bbox.ymin} AND {req.bbox.ymax}
            LIMIT {req.max_features}
        ) TO '{output_path}';
        """
    else:
        sql = f"""
        COPY(
            SELECT {columns}
            FROM read_parquet('{parquet_path}', filename=true, hive_partitioning=1)
            WHERE bbox.xmin BETWEEN {req.bbox.xmin} AND {req.bbox.xmax}
              AND bbox.ymin BETWEEN {req.bbox.ymin} AND {req.bbox.ymax}
            LIMIT {req.max_features}
        ) TO '{output_path}' WITH (FORMAT GDAL, DRIVER '{driver}');
        """

    try:
        db.conn.execute(sql)
        return FileResponse(
            str(output_path),
            filename=f"{output_name}.{ext}",
            media_type="application/octet-stream",
        )
    except Exception as e:
        raise HTTPException(500, f"Erreur export: {str(e)}")


@app.get("/api/sql")
def raw_sql(sql: str = Query(..., description="Requête SQL DuckDB")):
    """Exécution de requêtes SQL brutes (attention: dangereux en prod)."""
    try:
        df = db.query(sql)
        return {
            "columns": list(df.columns),
            "rows": df.head(1000).to_dict(orient="records"),
            "total_rows": len(df),
        }
    except Exception as e:
        raise HTTPException(500, f"Erreur SQL: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend:app", host="0.0.0.0", port=8000, reload=True)
