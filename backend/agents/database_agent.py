"""
database_agent.py — Agent spécialisé bases de données externes
Gère : PostGIS, MySQL, SQLite — requêtes SQL → GeoJSON
"""

import re
import logging
from typing import Optional

import requests

from agents.base_agent import BaseAgent, AgentResult

log = logging.getLogger("database_agent")

DB_API_BASE = "http://localhost:8000"

# ─── VALIDATION SQL ───────────────────────────────────────────────────────────
_FORBIDDEN_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXECUTE|CALL)\b",
    re.IGNORECASE,
)
_GEOM_HINTS = ("st_asgeojson", "st_astext", "geom", "geometry", "the_geom",
               "wkb_geometry", "latitude", "longitude", "lat", "lon", "lng")

MAX_LIMIT = 5000


def _sanitize_sql(sql: str) -> str:
    """Refuse les requêtes non-SELECT. Retourne SQL nettoyé."""
    sql = sql.strip()
    if _FORBIDDEN_PATTERN.search(sql):
        raise ValueError(
            "Seules les requêtes SELECT sont autorisées. "
            "INSERT/UPDATE/DELETE/DROP refusés."
        )
    if not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE):
        raise ValueError("La requête doit commencer par SELECT ou WITH.")
    return sql


def _check_geom_hint(sql: str) -> bool:
    """Vérifie si la requête contient une colonne géométrique."""
    sql_lower = sql.lower()
    return any(h in sql_lower for h in _GEOM_HINTS)


def _inject_limit(sql: str, limit: int) -> str:
    """Injecte LIMIT si absent."""
    if re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
        return sql
    return f"{sql.rstrip(';')} LIMIT {limit}"


def _validate_connection(conn: dict) -> dict:
    """Valide les paramètres de connexion."""
    db_type = conn.get("type", "").lower()
    if db_type not in ("postgresql", "mysql", "sqlite"):
        raise ValueError(f"Type DB inconnu : '{db_type}'. Valides : postgresql, mysql, sqlite")

    if db_type != "sqlite":
        if not conn.get("host"):
            conn["host"] = "localhost"
        if not conn.get("database"):
            raise ValueError("Paramètre 'database' requis")

    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# AGENT
# ═══════════════════════════════════════════════════════════════════════════════

class DatabaseAgent(BaseAgent):
    """Agent DB externe — PostGIS, MySQL, SQLite → GeoJSON."""

    DOMAIN        = "database"
    ALLOWED_TOOLS = ["db_query", "db_tables"]

    SYSTEM_PROMPT = """Tu es un expert en bases de données spatiales (PostGIS, MySQL spatial, SQLite/SpatiaLite).
Tu exécutes des requêtes SQL pour récupérer des données géographiques et les afficher sur une carte MapLibre.

## WORKFLOW

### 1. Explorer d'abord (si base inconnue)
Utilise db_tables pour lister les tables/colonnes avant d'écrire une requête.

### 2. Construire la requête géospatiale
- PostGIS : utilise ST_AsGeoJSON() pour les géométries
- MySQL   : utilise ST_AsGeoJSON() (MySQL 5.7.6+)
- SQLite  : utilise AsGeoJSON() si SpatiaLite activé, sinon lat/lon

### 3. Formats géométrie supportés
```sql
-- PostGIS — recommandé
SELECT id, nom, ST_AsGeoJSON(geom) AS geom_json FROM ma_table LIMIT 500

-- PostGIS — WKT alternatif  
SELECT id, nom, ST_AsText(geom) AS geom_wkt FROM ma_table LIMIT 500

-- Lat/Lon directs (pas de géométrie spatiale)
SELECT id, nom, latitude, longitude FROM adresses LIMIT 1000

-- MySQL spatial
SELECT id, nom, ST_AsGeoJSON(shape) AS geom_json FROM arbres LIMIT 500
```

## RÈGLES SQL
- SELECT uniquement — INSERT/UPDATE/DELETE INTERDITS
- Toujours inclure LIMIT (max 5000)
- Préférer ST_AsGeoJSON() à ST_AsText() (plus précis)
- Alias obligatoire : geom_json, geom_wkt, latitude, longitude
- Filtres spatiaux possibles avec ST_Within, ST_Intersects, ST_DWithin

## RÈGLES CONNEXION
- Ne jamais inventer les credentials — les demander à l'utilisateur si manquants
- type : "postgresql" | "mysql" | "sqlite"
- host défaut : "localhost"
- port défaut : postgresql=5432, mysql=3306

## RÉPONSE
- Mentionner le nombre de features récupérées
- Signaler si aucune colonne géométrique détectée
- Proposer ST_AsGeoJSON() si la géométrie semble absente
- En cas d'erreur de connexion : vérifier host/port/credentials
- Langue française obligatoire"""

    # ── Validation ──────────────────────────────────────────────────────────
    def validate(self, tool_name: str, args: dict) -> dict:
        if tool_name == "db_query":
            # Valider connexion
            conn = args.get("connection", {})
            args["connection"] = _validate_connection(conn)

            # Valider SQL
            sql = args.get("sql", "")
            if not sql:
                raise ValueError("Paramètre 'sql' manquant")
            sql = _sanitize_sql(sql)

            # Avertissement si pas de colonne géométrique détectée
            if not _check_geom_hint(sql):
                log.warning(
                    "Aucune colonne géométrique détectée dans la requête. "
                    "Utiliser ST_AsGeoJSON(geom) AS geom_json pour une sortie GeoJSON."
                )

            # Injecter LIMIT
            limit = min(int(args.get("limit", 2000)), MAX_LIMIT)
            args["sql"]   = _inject_limit(sql, limit)
            args["limit"] = limit

        elif tool_name == "db_tables":
            conn = args.get("connection", {})
            args["connection"] = _validate_connection(conn)

        return args

    # ── Exécution ────────────────────────────────────────────────────────────
    def execute_tool(self, tool_name: str, args: dict, map_context=None) -> dict:
        if tool_name == "db_query":
            return self._call_db_query(args)
        elif tool_name == "db_tables":
            return self._call_db_tables(args)
        return {"error": f"Tool inconnu : {tool_name}"}

    def _call_db_query(self, args: dict) -> dict:
        try:
            resp = requests.post(
                f"{DB_API_BASE}/api/db/query",
                json=args,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            return {"error": "DB timeout (>30s) — vérifier connexion ou simplifier la requête"}
        except requests.exceptions.HTTPError as e:
            try:
                detail = e.response.json().get("detail", str(e))
            except Exception:
                detail = str(e)
            return {"error": f"Erreur DB : {detail}"}
        except Exception as e:
            return {"error": str(e)}

    def _call_db_tables(self, args: dict) -> dict:
        try:
            resp = requests.post(
                f"{DB_API_BASE}/api/db/tables",
                json=args["connection"],
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"error": str(e)}
