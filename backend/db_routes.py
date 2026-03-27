"""
db_routes.py — Endpoints FastAPI pour connexion DB externe
À inclure dans agent.py ou backend.py :

    from db_routes import router as db_router
    app.include_router(db_router)

Dépendances :
    pip install psycopg2-binary pymysql sqlalchemy geoalchemy2
"""

import json
import re
from typing import Optional, Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/db", tags=["database"])

# ─── MODÈLES ─────────────────────────────────────────────────────

class DBConnection(BaseModel):
    type: str          # "postgresql" | "mysql" | "sqlite"
    host: Optional[str] = "localhost"
    port: Optional[int] = None
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    # Optionnel : URL complète (override les champs ci-dessus)
    url: Optional[str] = None

class DBQueryRequest(BaseModel):
    connection: DBConnection
    sql: str
    geom_column: Optional[str] = "geom"   # colonne géométrie
    srid: Optional[int] = 4326
    limit: Optional[int] = 2000

# ─── HELPERS ─────────────────────────────────────────────────────

DEFAULT_PORTS = {
    "postgresql": 5432,
    "mysql":      3306,
    "sqlite":     None,
}

def build_url(conn: DBConnection) -> str:
    """Construit la chaîne de connexion SQLAlchemy."""
    if conn.url:
        return conn.url
    port = conn.port or DEFAULT_PORTS.get(conn.type, 5432)
    if conn.type == "sqlite":
        return f"sqlite:///{conn.database}"
    if conn.type == "mysql":
        return f"mysql+pymysql://{conn.username}:{conn.password}@{conn.host}:{port}/{conn.database}"
    # postgresql par défaut
    return f"postgresql+psycopg2://{conn.username}:{conn.password}@{conn.host}:{port}/{conn.database}"


def rows_to_geojson(rows, columns: list[str], geom_col: str) -> dict:
    """
    Convertit des lignes SQL en GeoJSON FeatureCollection.
    Supporte :
      - Colonne geometry PostGIS retournée via ST_AsGeoJSON() → string JSON
      - Colonne WKT (ST_AsText)
      - Colonnes lat/lon séparées
    """
    features = []
    geom_col_lower = geom_col.lower()

    # Détecter les colonnes disponibles
    cols_lower = [c.lower() for c in columns]

    # Chercher colonne géométrie GeoJSON
    geojson_col = next(
        (columns[i] for i, c in enumerate(cols_lower) if c in (geom_col_lower, "geom_json", "geometry_json", "st_asgeojson")),
        None
    )
    # Chercher colonne WKT
    wkt_col = next(
        (columns[i] for i, c in enumerate(cols_lower) if c in (geom_col_lower, "geom_wkt", "wkt", "st_astext")),
        None
    ) if not geojson_col else None

    # Chercher colonnes lat/lon
    lat_col = next((columns[i] for i, c in enumerate(cols_lower) if c in ("lat", "latitude", "y")), None)
    lon_col = next((columns[i] for i, c in enumerate(cols_lower) if c in ("lon", "lng", "longitude", "x")), None)

    for row in rows:
        row_dict = dict(zip(columns, row))
        props = {}
        geometry = None

        for k, v in row_dict.items():
            kl = k.lower()
            # Exclure les colonnes géométrie des propriétés
            if kl in ("geom", "geometry", "geom_json", "geom_wkt", "wkt", "st_asgeojson", "st_astext", geom_col_lower):
                continue
            # Sérialiser les types non-JSON
            if hasattr(v, "isoformat"):  # datetime
                props[k] = v.isoformat()
            elif v is None:
                props[k] = None
            else:
                try:
                    props[k] = float(v) if isinstance(v, (int, float)) else str(v)
                except Exception:
                    props[k] = str(v)

        # Parser la géométrie
        if geojson_col and row_dict.get(geojson_col):
            raw = row_dict[geojson_col]
            try:
                geometry = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                pass
        elif wkt_col and row_dict.get(wkt_col):
            geometry = wkt_to_geojson(str(row_dict[wkt_col]))
        elif lat_col and lon_col:
            try:
                geometry = {
                    "type": "Point",
                    "coordinates": [float(row_dict[lon_col]), float(row_dict[lat_col])]
                }
            except Exception:
                pass

        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": props,
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {"total": len(features)},
    }


def wkt_to_geojson(wkt: str) -> Optional[dict]:
    """Conversion WKT basique → GeoJSON dict (Point, LineString, Polygon)."""
    wkt = wkt.strip()
    try:
        if wkt.startswith("POINT"):
            coords = re.findall(r"[-\d.]+", wkt)
            if len(coords) >= 2:
                return {"type": "Point", "coordinates": [float(coords[0]), float(coords[1])]}
        elif wkt.startswith("LINESTRING"):
            pairs = re.findall(r"([-\d.]+)\s+([-\d.]+)", wkt)
            return {"type": "LineString", "coordinates": [[float(x), float(y)] for x, y in pairs]}
        elif wkt.startswith("POLYGON"):
            rings = re.findall(r"\(([^()]+)\)", wkt)
            coordinates = []
            for ring in rings:
                pairs = re.findall(r"([-\d.]+)\s+([-\d.]+)", ring)
                coordinates.append([[float(x), float(y)] for x, y in pairs])
            if coordinates:
                return {"type": "Polygon", "coordinates": coordinates}
        elif wkt.startswith("MULTIPOLYGON") or wkt.startswith("MULTILINESTRING") or wkt.startswith("MULTIPOINT"):
            # Fallback simplifié : retourner None (shapely requis pour parsing complet)
            return None
    except Exception:
        pass
    return None


def sanitize_sql(sql: str) -> str:
    """
    Vérifie basique que la requête est en lecture seule.
    Refuse INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE.
    """
    forbidden = r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXECUTE|CALL)\b"
    if re.search(forbidden, sql.strip(), re.IGNORECASE):
        raise ValueError("Seules les requêtes SELECT sont autorisées.")
    return sql.strip()


# ─── ENDPOINTS ───────────────────────────────────────────────────

@router.post("/test")
def test_connection(conn: DBConnection):
    """Teste la connexion à la base de données."""
    try:
        from sqlalchemy import create_engine, text
        url = build_url(conn)
        engine = create_engine(url, connect_args={"connect_timeout": 5} if conn.type == "postgresql" else {})
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
        engine.dispose()
        return {"status": "ok", "message": f"Connexion {conn.type} réussie à {conn.host}/{conn.database}"}
    except ImportError as e:
        raise HTTPException(500, f"Driver manquant : {e}. Installez psycopg2-binary ou pymysql.")
    except Exception as e:
        raise HTTPException(400, f"Connexion échouée : {str(e)}")


@router.post("/tables")
def list_tables(conn: DBConnection):
    """Liste les tables/vues disponibles avec leurs colonnes."""
    try:
        from sqlalchemy import create_engine, inspect, text
        url = build_url(conn)
        engine = create_engine(url)
        inspector = inspect(engine)

        tables = []
        for table_name in inspector.get_table_names():
            cols = [c["name"] for c in inspector.get_columns(table_name)]
            has_geom = any(c.lower() in ("geom", "geometry", "the_geom", "shape", "wkb_geometry") for c in cols)
            tables.append({
                "name": table_name,
                "columns": cols,
                "has_geometry": has_geom,
                "type": "table",
            })

        # Vues aussi
        for view_name in inspector.get_view_names():
            cols = [c["name"] for c in inspector.get_columns(view_name)]
            has_geom = any(c.lower() in ("geom", "geometry", "the_geom", "shape", "wkb_geometry") for c in cols)
            tables.append({
                "name": view_name,
                "columns": cols,
                "has_geometry": has_geom,
                "type": "view",
            })

        engine.dispose()
        return {"tables": tables, "total": len(tables)}
    except Exception as e:
        raise HTTPException(400, f"Erreur liste tables : {str(e)}")


@router.post("/query")
def query_db(req: DBQueryRequest):
    """
    Exécute une requête SELECT et retourne du GeoJSON.

    Exemples de requêtes :
      PostGIS :
        SELECT id, name, ST_AsGeoJSON(geom) AS geom_json FROM parcelles LIMIT 500
        SELECT id, name, ST_AsText(geom) AS geom_wkt FROM batiments WHERE commune = 'Nantes'

      MySQL spatial :
        SELECT id, name, ST_AsGeoJSON(geom) AS geom_json FROM arbres LIMIT 500

      Lat/lon :
        SELECT id, nom, latitude, longitude FROM adresses LIMIT 1000
    """
    try:
        from sqlalchemy import create_engine, text
        sql = sanitize_sql(req.sql)

        # Ajouter LIMIT si absent
        if "limit" not in sql.lower():
            sql = f"{sql} LIMIT {req.limit}"

        url = build_url(req.connection)
        engine = create_engine(url)

        with engine.connect() as conn:
            result = conn.execute(text(sql))
            columns = list(result.keys())
            rows = result.fetchall()

        engine.dispose()

        geojson = rows_to_geojson(rows, columns, req.geom_column)
        geojson["metadata"]["sql"] = sql
        geojson["metadata"]["columns"] = columns

        return geojson

    except ValueError as e:
        raise HTTPException(400, str(e))
    except ImportError as e:
        raise HTTPException(500, f"Driver manquant : {e}")
    except Exception as e:
        raise HTTPException(500, f"Erreur requête : {str(e)}")


@router.post("/preview")
def preview_table(req: DBQueryRequest):
    """
    Aperçu rapide d'une table (50 premières lignes, sans géométrie).
    Utile pour explorer les colonnes avant de construire une requête.
    """
    try:
        from sqlalchemy import create_engine, text

        # Extraire le nom de table depuis une requête simple ou utiliser directement
        table_name = req.sql.strip()
        if " " not in table_name:
            # C'est un nom de table direct
            safe_table = re.sub(r"[^\w.]", "", table_name)
            sql = f"SELECT * FROM {safe_table} LIMIT 50"
        else:
            sql = sanitize_sql(req.sql)
            if "limit" not in sql.lower():
                sql = f"{sql} LIMIT 50"

        url = build_url(req.connection)
        engine = create_engine(url)
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            columns = list(result.keys())
            rows = [list(r) for r in result.fetchall()]
        engine.dispose()

        # Sérialiser les valeurs
        clean_rows = []
        for row in rows:
            clean_row = []
            for v in row:
                if hasattr(v, "isoformat"):
                    clean_row.append(v.isoformat())
                elif v is None:
                    clean_row.append(None)
                else:
                    try:
                        clean_row.append(float(v) if isinstance(v, (int, float)) else str(v)[:200])
                    except Exception:
                        clean_row.append(str(v)[:200])
            clean_rows.append(clean_row)

        return {"columns": columns, "rows": clean_rows, "total": len(rows)}

    except Exception as e:
        raise HTTPException(400, f"Erreur aperçu : {str(e)}")
