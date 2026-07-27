"""
mcp_servers/mcp_postgis.py — MCP Server PostGIS
================================================
Connexion : geoafrica.fr:5435/openmapagents (var env)
SELECT uniquement — aucune écriture en base.

Deux modes de fonctionnement :
  1. PostGIS server-side  → opérations spatiales en base (performant)
  2. Fallback turf.js     → délègue au frontend si PostGIS indispo
                            (identique à l'ancien spatial_analysis de agent.py)

Tools exposés :
    spatial_buffer          → zone tampon autour d'une géométrie
    spatial_join            → jointure spatiale entre deux couches
    spatial_intersect       → intersection de deux polygones
    spatial_union           → union de géométries
    spatial_difference      → différence A - B
    spatial_clip            → clip de A par B (points/lines dans polygone)
    points_in_polygon       → compte points de A dans polygones de B
    query_table             → requête SELECT sur une table PostGIS
    spatial_analysis        → alias rétrocompat agent.py (→ turf.js frontend)
    get_db_tables           → liste tables PostGIS disponibles
"""

import os
import json
import logging
import asyncio
from typing import Optional

log = logging.getLogger("mcp_postgis")

# ── Config PostGIS ────────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST",     "geoafrica.fr")
PG_PORT     = int(os.getenv("PG_PORT", "5435"))
PG_DB       = os.getenv("PG_DB",       "openmapagents")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_SCHEMA   = os.getenv("PG_SCHEMA",   "public")

# ── Whitelist tables autorisées en SELECT ─────────────────────
# Ajoutez ici les tables de votre base openmapagents
ALLOWED_TABLES = {
    # Exemples — à adapter à votre base
    "communes",
    "departements",
    "regions",
    "quartiers",
    "zones_inondables",
    "ilots_chaleur",
    "vegetation",
    "arbres",
    "batiments_auran",
    "parcelles",
    "equipements",
    "voiries",
    "canopy_nantes",
    "lidar_chm",
}

# ── Operations turf.js (rétrocompat agent.py) ─────────────────
TURF_OPERATIONS = {
    "intersection", "union", "difference", "clip", "spatial_join",
    "points_in_polygon", "buffer", "nearest", "distance_matrix",
    "centroid", "convex_hull", "dissolve", "simplify", "voronoi",
    "hex_grid", "area_perimeter", "clustering",
}

# ── Limites sécurité ──────────────────────────────────────────
MAX_FEATURES  = int(os.getenv("POSTGIS_MAX_FEATURES", "50000"))
MAX_RADIUS_M  = 200_000   # 200 km max pour buffer
QUERY_TIMEOUT = int(os.getenv("POSTGIS_QUERY_TIMEOUT", "30"))


def _get_conn():
    """Connexion psycopg2 avec timeout."""
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
        connect_timeout=5,
        options=f"-c statement_timeout={QUERY_TIMEOUT * 1000}",
    )


def _validate_table(table: str) -> str:
    """Vérifie que la table est dans la whitelist."""
    # Accepter schema.table ou juste table
    tname = table.split(".")[-1].lower().strip()
    if tname not in ALLOWED_TABLES:
        raise ValueError(
            f"Table '{tname}' non autorisée. "
            f"Tables disponibles: {sorted(ALLOWED_TABLES)}"
        )
    return tname


def _validate_sql(sql: str) -> str:
    """
    Valide que la requête est SELECT uniquement.
    Bloque tout mot-clé d'écriture ou d'injection.
    """
    sql_upper = sql.upper().strip()
    forbidden = [
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
        "TRUNCATE", "GRANT", "REVOKE", "EXECUTE", "CALL",
        "--", ";--", "/*", "*/", "UNION SELECT",
    ]
    for kw in forbidden:
        if kw in sql_upper:
            raise ValueError(
                f"Requête non autorisée: mot-clé '{kw}' interdit. "
                f"SELECT uniquement."
            )
    if not sql_upper.lstrip().startswith("SELECT"):
        raise ValueError("Seules les requêtes SELECT sont autorisées.")
    return sql


def _geojson_from_rows(rows, col_names: list) -> dict:
    """Convertit des lignes psycopg2 en GeoJSON FeatureCollection."""
    features = []
    geom_col = None
    # Détecter la colonne géométrie (geom, geometry, geojson, wkt...)
    for i, c in enumerate(col_names):
        if c.lower() in ("geom", "geometry", "geojson", "the_geom", "shape"):
            geom_col = i
            break

    for row in rows:
        props = {}
        geom  = None
        for i, (col, val) in enumerate(zip(col_names, row)):
            if i == geom_col:
                if val:
                    try:
                        geom = json.loads(val) if isinstance(val, str) else val
                    except Exception:
                        pass
            else:
                props[col] = val if val is not None else None

        if geom:
            features.append({
                "type":       "Feature",
                "geometry":   geom,
                "properties": props,
            })

    return {"type": "FeatureCollection", "features": features}


def _validate_bbox(bbox: list):
    if len(bbox) != 4:
        raise ValueError("bbox doit être [xmin, ymin, xmax, ymax]")
    xmin, ymin, xmax, ymax = bbox
    if not (-180 <= xmin < xmax <= 180) or not (-90 <= ymin < ymax <= 90):
        raise ValueError(f"bbox invalide: {bbox}")


# ═══════════════════════════════════════════════════════════════
# POSTGIS SERVER
# ═══════════════════════════════════════════════════════════════

class PostgisServer:
    """
    MCP Server PostGIS.
    Opérations spatiales server-side avec fallback turf.js frontend.
    """

    def __init__(self):
        self._pg_available = None   # None = pas encore testé

    def _check_pg(self) -> bool:
        """Test de connexion PostGIS (lazy, une seule fois)."""
        if self._pg_available is None:
            try:
                conn = _get_conn()
                conn.close()
                self._pg_available = True
                log.info(f"✓ PostGIS connecté: {PG_HOST}:{PG_PORT}/{PG_DB}")
            except Exception as e:
                self._pg_available = False
                log.warning(
                    f"PostGIS indisponible ({PG_HOST}:{PG_PORT}): {e}. "
                    f"Fallback turf.js actif."
                )
        return self._pg_available

    async def call(self, tool: str, args: dict) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._run, tool, args)

    def _run(self, tool: str, args: dict) -> dict:
        fn = {
            "spatial_buffer":       self.buffer,
            "spatial_join":         self.join,
            "spatial_intersect":    self.intersect,
            "spatial_union":        self.union,
            "spatial_difference":   self.difference,
            "spatial_clip":         self.clip,
            "points_in_polygon":    self.points_in_polygon,
            "query_table":          self.query_table,
            "spatial_analysis":     self.spatial_analysis,   # rétrocompat
            "get_db_tables":        self.get_db_tables,
        }.get(tool)
        if not fn:
            return {"error": f"PostGIS tool inconnu: '{tool}'"}
        try:
            return fn(args)
        except ValueError as e:
            return {"error": str(e), "tool": tool}
        except Exception as e:
            log.error(f"PostGIS {tool}: {e}")
            return {"error": f"Erreur PostGIS: {e}", "tool": tool}

    # ─── BUFFER ───────────────────────────────────────────────

    def buffer(self, a: dict) -> dict:
        """
        Zone tampon autour d'une géométrie GeoJSON.

        Args:
            geojson:   GeoJSON (Feature ou FeatureCollection)
            radius_m:  rayon en mètres (max 200km)
            dissolve:  bool — fusionner tous les buffers (défaut False)

        Returns:
            GeoJSON FeatureCollection des zones tampons
        """
        geojson  = a.get("geojson")
        radius_m = float(a.get("radius_m", a.get("params", {}).get("radius", 500)))
        dissolve = a.get("dissolve", False)

        if not geojson:
            return {"error": "geojson requis"}
        if radius_m <= 0 or radius_m > MAX_RADIUS_M:
            return {"error": f"radius_m invalide (1 - {MAX_RADIUS_M}m)"}

        if self._check_pg():
            return self._buffer_pg(geojson, radius_m, dissolve)
        else:
            return self._turf_fallback("buffer", a)

    def _buffer_pg(self, geojson: dict, radius_m: float, dissolve: bool) -> dict:
        """Buffer via ST_Buffer PostGIS."""
        geojson_str = json.dumps(geojson)
        if dissolve:
            sql = f"""
                SELECT ST_AsGeoJSON(
                    ST_Union(
                        ST_Buffer(
                            ST_Transform(ST_SetSRID(
                                ST_GeomFromGeoJSON(feat->>'geometry'), 4326
                            ), 3857),
                            {radius_m}
                        )
                    )::geography::geometry
                ) AS geom
                FROM jsonb_array_elements(
                    '{geojson_str}'::jsonb->'features'
                ) AS feat
            """
        else:
            sql = f"""
                SELECT
                    feat->'properties' AS props,
                    ST_AsGeoJSON(
                        ST_Transform(
                            ST_Buffer(
                                ST_Transform(ST_SetSRID(
                                    ST_GeomFromGeoJSON(feat->>'geometry'), 4326
                                ), 3857),
                                {radius_m}
                            ),
                            4326
                        )
                    ) AS geom
                FROM jsonb_array_elements(
                    '{geojson_str}'::jsonb->'features'
                ) AS feat
            """

        try:
            conn = _get_conn()
            cur  = conn.cursor()
            cur.execute(sql)
            rows   = cur.fetchall()
            cols   = [d[0] for d in cur.description]
            conn.close()

            features = []
            for row in rows:
                geom = json.loads(row[-1]) if row[-1] else None
                if geom:
                    props = json.loads(row[0]) if not dissolve and row[0] else {}
                    props["radius_m"]    = radius_m
                    props["buffer_type"] = "dissolved" if dissolve else "individual"
                    features.append({
                        "type":       "Feature",
                        "geometry":   geom,
                        "properties": props,
                    })

            return {
                "action":     "add_layer",
                "geojson":    {"type": "FeatureCollection", "features": features},
                "layer_name": f"Buffer {radius_m:.0f}m",
                "radius_m":   radius_m,
                "dissolved":  dissolve,
                "provider":   "PostGIS",
                "feature_count": len(features),
            }
        except Exception as e:
            log.warning(f"Buffer PostGIS failed: {e} → turf fallback")
            return self._turf_fallback("buffer", {"params": {"radius": radius_m}})

    # ─── JOIN SPATIAL ─────────────────────────────────────────

    def join(self, a: dict) -> dict:
        """
        Jointure spatiale — attribue les propriétés de B aux features de A
        qui intersectent B.

        Args:
            geojson_a:  features à enrichir
            geojson_b:  features source des propriétés
            predicate:  intersects | contains | within (défaut: intersects)
        """
        gjA       = a.get("geojson_a") or a.get("geojson")
        gjB       = a.get("geojson_b")
        predicate = a.get("predicate", "intersects")

        if not gjA or not gjB:
            return {"error": "geojson_a et geojson_b requis"}

        if self._check_pg():
            return self._join_pg(gjA, gjB, predicate)
        else:
            return self._turf_fallback("spatial_join", a)

    def _join_pg(self, gjA: dict, gjB: dict, predicate: str) -> dict:
        """Jointure spatiale via ST_Intersects PostGIS."""
        strA = json.dumps(gjA)
        strB = json.dumps(gjB)

        pred_fn = {
            "intersects": "ST_Intersects",
            "contains":   "ST_Contains",
            "within":     "ST_Within",
        }.get(predicate, "ST_Intersects")

        sql = f"""
            WITH
            layer_a AS (
                SELECT
                    feat->'properties' AS props_a,
                    ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326) AS geom_a
                FROM jsonb_array_elements('{strA}'::jsonb->'features') AS feat
            ),
            layer_b AS (
                SELECT
                    feat->'properties' AS props_b,
                    ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326) AS geom_b
                FROM jsonb_array_elements('{strB}'::jsonb->'features') AS feat
            )
            SELECT
                props_a || props_b AS merged_props,
                ST_AsGeoJSON(geom_a) AS geom
            FROM layer_a
            JOIN layer_b ON {pred_fn}(geom_a, geom_b)
            LIMIT {MAX_FEATURES}
        """

        try:
            conn = _get_conn()
            cur  = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            conn.close()

            features = []
            for row in rows:
                geom  = json.loads(row[1]) if row[1] else None
                props = json.loads(row[0]) if row[0] else {}
                if geom:
                    features.append({
                        "type":       "Feature",
                        "geometry":   geom,
                        "properties": props,
                    })

            return {
                "action":        "add_layer",
                "geojson":       {"type": "FeatureCollection", "features": features},
                "layer_name":    f"Jointure spatiale ({len(features)})",
                "predicate":     predicate,
                "provider":      "PostGIS",
                "feature_count": len(features),
            }
        except Exception as e:
            log.warning(f"Join PostGIS failed: {e} → turf fallback")
            return self._turf_fallback("spatial_join", a)

    # ─── INTERSECT ────────────────────────────────────────────

    def intersect(self, a: dict) -> dict:
        """
        Intersection géométrique de A et B.

        Args:
            geojson_a, geojson_b: GeoJSON polygones
        """
        gjA = a.get("geojson_a") or a.get("geojson")
        gjB = a.get("geojson_b")
        if not gjA or not gjB:
            return {"error": "geojson_a et geojson_b requis"}

        if self._check_pg():
            return self._set_operation_pg(gjA, gjB, "ST_Intersection",
                                          "Intersection")
        return self._turf_fallback("intersection", a)

    # ─── UNION ────────────────────────────────────────────────

    def union(self, a: dict) -> dict:
        """
        Union géométrique de A et B.

        Args:
            geojson_a, geojson_b: GeoJSON
        """
        gjA = a.get("geojson_a") or a.get("geojson")
        gjB = a.get("geojson_b")
        if not gjA or not gjB:
            return {"error": "geojson_a et geojson_b requis"}

        if self._check_pg():
            return self._set_operation_pg(gjA, gjB, "ST_Union", "Union")
        return self._turf_fallback("union", a)

    # ─── DIFFERENCE ───────────────────────────────────────────

    def difference(self, a: dict) -> dict:
        """
        Différence A - B (parties de A hors de B).

        Args:
            geojson_a, geojson_b: GeoJSON
        """
        gjA = a.get("geojson_a") or a.get("geojson")
        gjB = a.get("geojson_b")
        if not gjA or not gjB:
            return {"error": "geojson_a et geojson_b requis"}

        if self._check_pg():
            return self._set_operation_pg(gjA, gjB, "ST_Difference",
                                          "Différence")
        return self._turf_fallback("difference", a)

    def _set_operation_pg(self, gjA: dict, gjB: dict,
                          pg_fn: str, label: str) -> dict:
        """Opération ensembliste générique via PostGIS."""
        strA = json.dumps(gjA)
        strB = json.dumps(gjB)

        sql = f"""
            WITH
            geom_a AS (
                SELECT ST_Union(
                    ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326)
                ) AS g
                FROM jsonb_array_elements('{strA}'::jsonb->'features') AS feat
            ),
            geom_b AS (
                SELECT ST_Union(
                    ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326)
                ) AS g
                FROM jsonb_array_elements('{strB}'::jsonb->'features') AS feat
            )
            SELECT ST_AsGeoJSON({pg_fn}(a.g, b.g)) AS geom,
                   ST_Area({pg_fn}(a.g, b.g)::geography) / 1e6 AS area_km2
            FROM geom_a a, geom_b b
        """

        try:
            conn = _get_conn()
            cur  = conn.cursor()
            cur.execute(sql)
            row  = cur.fetchone()
            conn.close()

            if not row or not row[0]:
                return {"error": f"{label}: résultat vide (géométries sans overlap?)"}

            geom     = json.loads(row[0])
            area_km2 = round(float(row[1]), 4) if row[1] else None

            return {
                "action": "add_layer",
                "geojson": {
                    "type": "FeatureCollection",
                    "features": [{
                        "type":       "Feature",
                        "geometry":   geom,
                        "properties": {
                            "operation": label,
                            "area_km2":  area_km2,
                        },
                    }],
                },
                "layer_name":    f"{label} spatiale",
                "area_km2":      area_km2,
                "provider":      "PostGIS",
                "feature_count": 1,
            }
        except Exception as e:
            log.warning(f"{label} PostGIS failed: {e} → turf fallback")
            op_map = {
                "ST_Intersection": "intersection",
                "ST_Union":        "union",
                "ST_Difference":   "difference",
            }
            return self._turf_fallback(op_map.get(pg_fn, "intersection"), {})

    # ─── CLIP ─────────────────────────────────────────────────

    def clip(self, a: dict) -> dict:
        """
        Clip de A (points, lignes, polygones) par un masque B (polygone).
        Garde uniquement les features de A qui sont dans B.

        Args:
            geojson_a:   features à clipper
            geojson_b:   polygone masque
        """
        gjA = a.get("geojson_a") or a.get("geojson")
        gjB = a.get("geojson_b")
        if not gjA or not gjB:
            return {"error": "geojson_a et geojson_b requis"}

        if self._check_pg():
            return self._clip_pg(gjA, gjB)
        return self._turf_fallback("clip", a)

    def _clip_pg(self, gjA: dict, gjB: dict) -> dict:
        strA = json.dumps(gjA)
        strB = json.dumps(gjB)

        sql = f"""
            WITH
            mask AS (
                SELECT ST_Union(
                    ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326)
                ) AS g
                FROM jsonb_array_elements('{strB}'::jsonb->'features') AS feat
            )
            SELECT
                feat->'properties' AS props,
                ST_AsGeoJSON(
                    ST_Intersection(
                        ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326),
                        mask.g
                    )
                ) AS geom
            FROM jsonb_array_elements('{strA}'::jsonb->'features') AS feat, mask
            WHERE ST_Intersects(
                ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326),
                mask.g
            )
            LIMIT {MAX_FEATURES}
        """

        try:
            conn = _get_conn()
            cur  = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            conn.close()

            features = []
            for row in rows:
                geom  = json.loads(row[1]) if row[1] else None
                props = json.loads(row[0]) if row[0] else {}
                if geom and geom.get("coordinates"):
                    features.append({
                        "type": "Feature", "geometry": geom, "properties": props
                    })

            return {
                "action":        "add_layer",
                "geojson":       {"type": "FeatureCollection", "features": features},
                "layer_name":    f"Clip ({len(features)} features)",
                "provider":      "PostGIS",
                "feature_count": len(features),
            }
        except Exception as e:
            log.warning(f"Clip PostGIS failed: {e} → turf fallback")
            return self._turf_fallback("clip", {})

    # ─── POINTS IN POLYGON ────────────────────────────────────

    def points_in_polygon(self, a: dict) -> dict:
        """
        Compte et retourne les points de A qui sont dans les polygones de B.

        Args:
            geojson_a:  points
            geojson_b:  polygones
            count_only: bool — retourner seulement les comptes (défaut False)
        """
        gjA        = a.get("geojson_a") or a.get("geojson")
        gjB        = a.get("geojson_b")
        count_only = a.get("count_only", False)

        if not gjA or not gjB:
            return {"error": "geojson_a (points) et geojson_b (polygones) requis"}

        if self._check_pg():
            return self._pip_pg(gjA, gjB, count_only)
        return self._turf_fallback("points_in_polygon", a)

    def _pip_pg(self, gjA: dict, gjB: dict, count_only: bool) -> dict:
        strA = json.dumps(gjA)
        strB = json.dumps(gjB)

        if count_only:
            sql = f"""
                WITH
                polygons AS (
                    SELECT
                        feat->'properties' AS props_poly,
                        ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326) AS geom_poly
                    FROM jsonb_array_elements('{strB}'::jsonb->'features') AS feat
                ),
                pts AS (
                    SELECT ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326) AS geom_pt
                    FROM jsonb_array_elements('{strA}'::jsonb->'features') AS feat
                )
                SELECT
                    props_poly,
                    COUNT(pts.geom_pt) AS point_count,
                    ST_AsGeoJSON(geom_poly) AS geom
                FROM polygons
                LEFT JOIN pts ON ST_Contains(geom_poly, pts.geom_pt)
                GROUP BY props_poly, geom_poly
            """
        else:
            sql = f"""
                WITH
                polygons AS (
                    SELECT
                        feat->'properties' AS props_poly,
                        ST_SetSRID(ST_GeomFromGeoJSON(feat->>'geometry'), 4326) AS geom_poly
                    FROM jsonb_array_elements('{strB}'::jsonb->'features') AS feat
                )
                SELECT
                    pt.feat->'properties' AS props_pt,
                    ST_AsGeoJSON(
                        ST_SetSRID(ST_GeomFromGeoJSON(pt.feat->>'geometry'), 4326)
                    ) AS geom
                FROM jsonb_array_elements('{strA}'::jsonb->'features') AS pt
                JOIN polygons ON ST_Contains(
                    polygons.geom_poly,
                    ST_SetSRID(ST_GeomFromGeoJSON(pt.feat->>'geometry'), 4326)
                )
                LIMIT {MAX_FEATURES}
            """

        try:
            conn = _get_conn()
            cur  = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            conn.close()

            features = []
            for row in rows:
                geom  = json.loads(row[-1]) if row[-1] else None
                props = json.loads(row[0])  if row[0]  else {}
                if count_only and len(row) > 2:
                    props["point_count"] = row[1]
                if geom:
                    features.append({
                        "type": "Feature", "geometry": geom, "properties": props
                    })

            total_pts = sum(
                f["properties"].get("point_count", 1)
                for f in features
            )

            return {
                "action":        "add_layer",
                "geojson":       {"type": "FeatureCollection", "features": features},
                "layer_name":    f"Points dans polygone ({total_pts} pts)",
                "total_points":  total_pts,
                "provider":      "PostGIS",
                "feature_count": len(features),
            }
        except Exception as e:
            log.warning(f"PiP PostGIS failed: {e} → turf fallback")
            return self._turf_fallback("points_in_polygon", a)

    # ─── QUERY TABLE ──────────────────────────────────────────

    def query_table(self, a: dict) -> dict:
        """
        Requête SELECT sur une table PostGIS (whitelist).

        Args:
            table:    nom de la table (doit être dans ALLOWED_TABLES)
            bbox:     [xmin, ymin, xmax, ymax] — filtre spatial optionnel
            filters:  dict de filtres attributaires {colonne: valeur}
            columns:  liste de colonnes à retourner (défaut: *)
            limit:    max features (défaut 5000)
            geom_col: nom colonne géométrie (défaut: geom)
            sql:      requête SQL custom (SELECT uniquement, si fournie)
        """
        table   = a.get("table", "")
        bbox    = a.get("bbox")
        filters = a.get("filters", {})
        columns = a.get("columns", ["*"])
        limit   = min(int(a.get("limit", 5000)), MAX_FEATURES)
        geom_col = a.get("geom_col", "geom")
        custom_sql = a.get("sql")

        # SQL custom validé
        if custom_sql:
            try:
                _validate_sql(custom_sql)
            except ValueError as e:
                return {"error": str(e)}
            sql = custom_sql + f" LIMIT {limit}"
        else:
            # SQL généré
            try:
                tname = _validate_table(table)
            except ValueError as e:
                return {"error": str(e)}

            # Colonnes + géométrie en GeoJSON
            cols = list(columns) if columns != ["*"] else []
            if geom_col not in cols and "*" not in cols:
                cols.append(f"ST_AsGeoJSON({geom_col}) AS geojson")
            else:
                cols = [
                    f"ST_AsGeoJSON({geom_col}) AS geojson"
                    if c == geom_col or c == "*" else c
                    for c in (cols or ["*"])
                ]

            where_clauses = []

            # Filtre bbox
            if bbox:
                try:
                    _validate_bbox(bbox)
                    xmin, ymin, xmax, ymax = bbox
                    where_clauses.append(
                        f"{geom_col} && ST_MakeEnvelope"
                        f"({xmin},{ymin},{xmax},{ymax},4326)"
                    )
                except ValueError as e:
                    return {"error": str(e)}

            # Filtres attributaires
            for col, val in filters.items():
                # Sanitize basique
                col_clean = "".join(c for c in col if c.isalnum() or c == "_")
                if isinstance(val, str):
                    where_clauses.append(f"{col_clean} = '{val}'")
                elif isinstance(val, (int, float)):
                    where_clauses.append(f"{col_clean} = {val}")

            where = (
                "WHERE " + " AND ".join(where_clauses)
                if where_clauses else ""
            )
            cols_str = ", ".join(cols) if cols else "*"
            sql = (
                f"SELECT {cols_str} "
                f"FROM {PG_SCHEMA}.{tname} "
                f"{where} "
                f"LIMIT {limit}"
            )

        try:
            conn = _get_conn()
            cur  = conn.cursor()
            cur.execute(sql)
            rows     = cur.fetchall()
            col_names = [d[0] for d in cur.description]
            conn.close()

            geojson = _geojson_from_rows(rows, col_names)
            n       = len(geojson["features"])

            return {
                "action":        "add_layer",
                "geojson":       geojson,
                "layer_name":    f"{table} ({n} features)",
                "table":         table,
                "provider":      "PostGIS",
                "feature_count": n,
                "sql":           sql[:200],
            }
        except Exception as e:
            log.error(f"Query table {table}: {e}")
            return {"error": f"Erreur requête PostGIS: {e}"}

    # ─── SPATIAL_ANALYSIS (rétrocompat agent.py) ──────────────

    def spatial_analysis(self, a: dict) -> dict:
        """
        Alias rétrocompatibilité avec l'ancien spatial_analysis de agent.py.
        Tente d'abord une opération PostGIS, sinon délègue au frontend turf.js.

        Interface identique à agent.py :
            operation:    intersection | union | difference | clip |
                          spatial_join | points_in_polygon | buffer | ...
            layer_a_name: nom de la couche A (frontend)
            layer_b_name: nom de la couche B (frontend)
            params:       {radius: m} pour buffer, {attribute} pour dissolve...
            result_name:  nom du layer résultat
        """
        operation = a.get("operation", "")
        params    = a.get("params", {})

        # Si on a des GeoJSON directs → PostGIS
        if a.get("geojson_a") or a.get("geojson"):
            dispatch = {
                "buffer":           self.buffer,
                "intersection":     self.intersect,
                "union":            self.union,
                "difference":       self.difference,
                "clip":             self.clip,
                "spatial_join":     self.join,
                "points_in_polygon":self.points_in_polygon,
            }
            fn = dispatch.get(operation)
            if fn:
                return fn(a)

        # Sinon → déléguer au frontend turf.js
        # (identique à l'ancien execute_tool spatial_analysis de agent.py)
        return self._turf_fallback(operation, a)

    # ─── GET DB TABLES ────────────────────────────────────────

    def get_db_tables(self, a: dict) -> dict:
        """
        Liste les tables PostGIS disponibles (whitelist + infos).

        Args:
            with_stats: bool — inclure count et bbox de chaque table
        """
        with_stats = a.get("with_stats", False)

        if not self._check_pg():
            return {
                "error":   "PostGIS indisponible",
                "allowed": sorted(ALLOWED_TABLES),
            }

        tables_info = []
        for tname in sorted(ALLOWED_TABLES):
            info = {"table": tname, "schema": PG_SCHEMA}
            if with_stats:
                try:
                    conn = _get_conn()
                    cur  = conn.cursor()
                    # Vérifier que la table existe
                    cur.execute(
                        "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema=%s AND table_name=%s)",
                        (PG_SCHEMA, tname)
                    )
                    exists = cur.fetchone()[0]
                    if exists:
                        cur.execute(f"SELECT COUNT(*) FROM {PG_SCHEMA}.{tname}")
                        info["count"] = cur.fetchone()[0]
                    else:
                        info["exists"] = False
                    conn.close()
                except Exception as e:
                    info["error"] = str(e)
            tables_info.append(info)

        return {
            "action": "show_list",
            "tables": tables_info,
            "total":  len(tables_info),
            "connection": f"{PG_HOST}:{PG_PORT}/{PG_DB}",
        }

    # ─── TURF FALLBACK ────────────────────────────────────────

    def _turf_fallback(self, operation: str, a: dict) -> dict:
        """
        Délègue l'opération spatiale au frontend (turf.js).
        Retourne exactement le même format que l'ancien spatial_analysis
        de agent.py — le frontend sait déjà le traiter.
        """
        log.info(f"[PostGIS] turf.js fallback pour: {operation}")
        return {
            "action":       "spatial_analysis",
            "operation":    operation,
            "layer_a_name": a.get("layer_a_name"),
            "layer_b_name": a.get("layer_b_name"),
            "params":       a.get("params", {}),
            "result_name":  a.get("result_name", operation),
            "provider":     "turf.js",
        }
