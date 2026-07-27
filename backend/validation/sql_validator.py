"""
sql_validator.py — Validation SQL déterministe
Utilisé par : DatabaseAgent

Fonctions :
  sanitize_sql()       → SELECT only, détection injection
  check_geom_column()  → colonne géométrie présente ?
  inject_limit()       → LIMIT si absent
  validate_sql_args()  → pipeline complet
"""

import os
import re
import logging
from typing import Optional

log = logging.getLogger("sql_validator")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
MAX_SQL_LENGTH   = int(os.getenv("SQL_MAX_LENGTH", "4000")) if False else 4000
MAX_QUERY_LIMIT  = 5000
DEFAULT_LIMIT    = 2000

# ─── PATTERNS ─────────────────────────────────────────────────────────────────

# Instructions DML/DDL interdites
_FORBIDDEN_STMTS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE"
    r"|EXECUTE|CALL|MERGE|REPLACE|LOAD\s+DATA|INTO\s+OUTFILE"
    r"|COPY\s+TO|COPY\s+FROM)\b",
    re.IGNORECASE,
)

# Techniques d'injection classiques
_INJECTION_PATTERNS = [
    # Commentaires SQL pour terminer une requête
    re.compile(r"--\s*$",              re.MULTILINE),  # commentaire fin de ligne
    re.compile(r"/\*.*?\*/",           re.DOTALL),     # commentaire bloc
    # Stacked queries (plusieurs statements)
    re.compile(r";\s*(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE)", re.IGNORECASE),
    # UNION-based injection
    re.compile(r"\bUNION\s+(ALL\s+)?SELECT\b", re.IGNORECASE),
    # Fonctions dangereuses PostgreSQL
    re.compile(r"\b(pg_read_file|pg_ls_dir|pg_execute|lo_import|lo_export"
               r"|copy_from|dblink|pg_sleep|pg_cancel_backend)\b", re.IGNORECASE),
    # Fonctions système MySQL
    re.compile(r"\b(LOAD_FILE|INTO\s+DUMPFILE|BENCHMARK|SLEEP)\b", re.IGNORECASE),
    # Lecture fichiers système
    re.compile(r"\b(\/etc\/passwd|\/etc\/shadow|\.\.\/)\b", re.IGNORECASE),
]

# Colonnes géométriques standards
_GEOM_COLUMNS = {
    # PostGIS / GeoAlchemy
    "geom", "geometry", "the_geom", "wkb_geometry", "shape", "geom_pt",
    "geom_poly", "geom_line",
    # Fonctions PostGIS retournant GeoJSON/WKT
    "geom_json", "geom_wkt", "geometry_json",
    # Noms de fonctions d'export
    "st_asgeojson", "st_astext", "st_aswkt", "asgeojson",
    # Lat/Lon directs
    "latitude", "longitude", "lat", "lon", "lng", "x", "y",
    # SpatiaLite
    "aswkt", "asgeojson",
}

# Fonctions géométriques PostGIS (dans la requête SELECT)
_GEOM_FUNCTIONS = re.compile(
    r"\b(ST_AsGeoJSON|ST_AsText|ST_AsWKT|ST_X|ST_Y|ST_Centroid"
    r"|AsGeoJSON|AsText|ST_Transform|ST_Force2D)\s*\(",
    re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════════════════════
# SANITIZATION
# ═══════════════════════════════════════════════════════════════════════════════

class SQLValidationError(ValueError):
    """Erreur SQL bloquante — requête refusée."""
    pass


def sanitize_sql(sql: str) -> str:
    """
    Valide et nettoie une requête SQL.

    Vérifications :
      1. Longueur maximale
      2. Doit commencer par SELECT ou WITH
      3. Instructions DML/DDL interdites
      4. Patterns d'injection connus
      5. Normalisation des espaces

    Returns: SQL nettoyé
    Raises: SQLValidationError si refusé
    """
    if not sql or not sql.strip():
        raise SQLValidationError("Requête SQL vide")

    # Normaliser espaces/newlines
    sql = sql.strip()

    # Longueur
    if len(sql) > MAX_SQL_LENGTH:
        raise SQLValidationError(
            f"Requête SQL trop longue ({len(sql)} chars, max {MAX_SQL_LENGTH})"
        )

    # Doit commencer par SELECT ou WITH (CTE)
    if not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE):
        raise SQLValidationError(
            "Seules les requêtes SELECT ou WITH (CTE) sont autorisées. "
            f"Reçu : '{sql[:50]}...'"
        )

    # Instructions interdites
    forbidden_match = _FORBIDDEN_STMTS.search(sql)
    if forbidden_match:
        raise SQLValidationError(
            f"Instruction interdite détectée : '{forbidden_match.group()}'. "
            "Seules les requêtes SELECT en lecture seule sont autorisées."
        )

    # Patterns d'injection
    for pattern in _INJECTION_PATTERNS:
        m = pattern.search(sql)
        if m:
            raise SQLValidationError(
                f"Pattern d'injection SQL détecté : '{m.group()[:40]}'. "
                "Requête refusée."
            )

    return sql


# ═══════════════════════════════════════════════════════════════════════════════
# DÉTECTION COLONNE GÉOMÉTRIQUE
# ═══════════════════════════════════════════════════════════════════════════════

def check_geom_column(sql: str) -> tuple[bool, str]:
    """
    Vérifie si la requête retourne une colonne géométrique exploitable.

    Returns:
        (has_geom: bool, geom_type: str)
        geom_type: "geojson" | "wkt" | "latlon" | "function" | "none"
    """
    sql_lower = sql.lower()

    # Vérifier les fonctions d'export explicites
    if _GEOM_FUNCTIONS.search(sql):
        if re.search(r"\bst_asgeojson\b|\basgeojson\b", sql_lower):
            return True, "geojson"
        if re.search(r"\bst_astext\b|\baswkt\b|\bst_aswkt\b", sql_lower):
            return True, "wkt"
        return True, "function"

    # Extraire les colonnes/alias du SELECT
    # Regex simple pour colonnes et alias (AS xxx)
    col_pattern = re.compile(
        r"(?:^|\s|,)\s*(?:\w+\.)?"       # optionnel: table.
        r"(\w+)"                          # nom de colonne
        r"(?:\s+AS\s+(\w+))?",           # optionnel: AS alias
        re.IGNORECASE,
    )

    all_cols = set()
    # Extraire la partie SELECT (avant FROM)
    select_match = re.match(r"SELECT\s+(.+?)\s+FROM\b", sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_clause = select_match.group(1)
        for m in col_pattern.finditer(select_clause):
            col_name = m.group(1).lower()
            alias    = (m.group(2) or "").lower()
            all_cols.add(col_name)
            if alias:
                all_cols.add(alias)

    # Vérifier intersection avec colonnes géométriques connues
    geom_cols = all_cols & _GEOM_COLUMNS

    if geom_cols:
        if any(c in geom_cols for c in ("geom_json", "geometry_json", "st_asgeojson")):
            return True, "geojson"
        if any(c in geom_cols for c in ("geom_wkt", "st_astext", "wkt")):
            return True, "wkt"
        if any(c in geom_cols for c in ("latitude", "lat")) and \
           any(c in geom_cols for c in ("longitude", "lon", "lng")):
            return True, "latlon"
        if any(c in geom_cols for c in ("geom", "geometry", "the_geom", "wkb_geometry", "shape")):
            return True, "raw"

    return False, "none"


# ═══════════════════════════════════════════════════════════════════════════════
# INJECTION LIMIT
# ═══════════════════════════════════════════════════════════════════════════════

def inject_limit(sql: str, limit: int = DEFAULT_LIMIT) -> str:
    """
    Injecte LIMIT si absent.
    Cap à MAX_QUERY_LIMIT pour éviter les requêtes trop lourdes.
    """
    limit = min(int(limit), MAX_QUERY_LIMIT)

    if re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
        # Vérifier si le LIMIT existant est raisonnable
        existing = re.search(r"\bLIMIT\s+(\d+)", sql, re.IGNORECASE)
        if existing:
            existing_val = int(existing.group(1))
            if existing_val > MAX_QUERY_LIMIT:
                # Remplacer le LIMIT trop élevé
                sql = re.sub(
                    r"\bLIMIT\s+\d+",
                    f"LIMIT {MAX_QUERY_LIMIT}",
                    sql,
                    flags=re.IGNORECASE,
                )
                log.warning(f"LIMIT réduit de {existing_val} à {MAX_QUERY_LIMIT}")
        return sql

    # Ajouter LIMIT en fin de requête (avant éventuel ';')
    sql = sql.rstrip().rstrip(";")
    return f"{sql}\nLIMIT {limit}"


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION CONNEXION
# ═══════════════════════════════════════════════════════════════════════════════

_VALID_DB_TYPES = {"postgresql", "mysql", "sqlite"}
_DEFAULT_PORTS  = {"postgresql": 5432, "mysql": 3306}


def validate_connection(conn: dict) -> dict:
    """
    Valide les paramètres de connexion à une base de données.

    Vérifie :
      - type valide (postgresql | mysql | sqlite)
      - host présent (non-sqlite)
      - database présent
      - port dans plage valide
      - pas d'injection dans les paramètres de connexion

    Returns: conn dict nettoyé et complété
    """
    db_type = conn.get("type", "").lower().strip()

    if db_type not in _VALID_DB_TYPES:
        raise SQLValidationError(
            f"Type de base inconnu : '{db_type}'. "
            f"Valides : {sorted(_VALID_DB_TYPES)}"
        )
    conn["type"] = db_type

    # SQLite — chemin fichier uniquement
    if db_type == "sqlite":
        db = conn.get("database", "")
        if not db:
            raise SQLValidationError("Chemin fichier SQLite requis dans 'database'")
        # Refuser les chemins système sensibles
        if re.search(r"(\/etc\/|\/proc\/|C:\\Windows\\)", db, re.IGNORECASE):
            raise SQLValidationError(f"Chemin SQLite suspect refusé : '{db}'")
        return conn

    # PostgreSQL / MySQL
    if not conn.get("host"):
        conn["host"] = "localhost"

    if not conn.get("database"):
        raise SQLValidationError("Paramètre 'database' requis")

    # Port
    port = conn.get("port")
    if port is not None:
        port = int(port)
        if not (1 <= port <= 65535):
            raise SQLValidationError(f"Port invalide : {port}")
        conn["port"] = port
    else:
        conn["port"] = _DEFAULT_PORTS.get(db_type, 5432)

    # Vérifier pas d'injection dans host/database (caractères suspects)
    for field in ("host", "database", "username"):
        val = conn.get(field, "")
        if val and re.search(r"[;'\"\\\x00]", val):
            raise SQLValidationError(
                f"Caractères suspects dans '{field}' : '{val[:30]}'"
            )

    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE COMPLET
# ═══════════════════════════════════════════════════════════════════════════════

def validate_sql_args(args: dict) -> dict:
    """
    Validation complète pour un appel db_query.
    Pipeline : connexion → SQL → limit → geom check

    Returns: args validés et enrichis avec 'geom_detected' et 'geom_type'
    """
    # 1. Connexion
    conn = args.get("connection", {})
    args["connection"] = validate_connection(conn)

    # 2. SQL
    sql = args.get("sql", "")
    if not sql:
        raise SQLValidationError("Paramètre 'sql' requis")

    sql = sanitize_sql(sql)

    # 3. LIMIT
    limit = min(int(args.get("limit", DEFAULT_LIMIT)), MAX_QUERY_LIMIT)
    sql   = inject_limit(sql, limit)
    args["sql"]   = sql
    args["limit"] = limit

    # 4. Détection colonne géométrique (non-bloquant)
    has_geom, geom_type = check_geom_column(sql)
    args["geom_detected"] = has_geom
    args["geom_type"]     = geom_type

    if not has_geom:
        log.warning(
            "Aucune colonne géométrique détectée dans la requête SQL. "
            "Pour une sortie GeoJSON, utiliser ST_AsGeoJSON(geom) AS geom_json "
            "ou inclure des colonnes latitude/longitude."
        )

    return args


