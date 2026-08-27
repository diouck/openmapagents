"""
sql_routes.py — SQL Workspace : éditeur SQL spatial (DuckDB) → carte.

POST /api/sql/run  {sql, layers:[{name, geojson}]}
  - Connexion DuckDB DÉDIÉE (spatial), isolée de la connexion Overture.
  - Enregistre les couches chargées comme tables (ST_Read d'un GeoJSON temporaire).
  - VERROUILLE ensuite l'accès externe (fichiers/URL) AVANT d'exécuter le SQL de
    l'utilisateur : l'app étant publique, on empêche toute lecture de fichiers du
    serveur ou SSRF. Fail-closed : si le verrou échoue, on refuse d'exécuter.
  - Détecte une colonne géométrie → renvoie un GeoJSON (carte) + un tableau ;
    sinon juste colonnes/lignes.

⚠️ GEE/DuckDB non testables ici : comportement du verrou à confirmer au 1er run.
Déps : duckdb (extension spatial).
"""
import os
import re
import json
import tempfile
import datetime
import decimal
from typing import Optional, List, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/sql", tags=["sql"])

_ROW_LIMIT = 5000          # borne le nb de lignes renvoyées (payload + carte)
_MAX_LAYER_FEATURES = 200000


class SqlLayer(BaseModel):
    name: str
    geojson: dict


class SqlRequest(BaseModel):
    sql: str
    layers: Optional[List[SqlLayer]] = None


def _safe_ident(name: str) -> str:
    """Nom de table sûr : alphanumérique + underscore, commence par une lettre."""
    s = re.sub(r"[^A-Za-z0-9_]", "_", (name or "layer").strip())
    if not s or not s[0].isalpha():
        s = "t_" + s
    return s[:60] or "layer"


def _jsonable(v: Any):
    """Rend une valeur DuckDB sérialisable en JSON."""
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (datetime.date, datetime.datetime, datetime.time)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    return str(v)


@router.post("/run")
def sql_run(req: SqlRequest):
    try:
        import duckdb
    except ImportError:
        raise HTTPException(503, "DuckDB indisponible côté serveur (pip install duckdb).")

    sql = (req.sql or "").strip().rstrip(";").strip()
    if not sql:
        raise HTTPException(422, "Requête SQL vide.")

    con = duckdb.connect(":memory:")
    tmpfiles: List[str] = []
    try:
        # ── 1. Charger spatial (accès externe encore autorisé) ──
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
        except Exception as e:
            raise HTTPException(503, f"Extension DuckDB « spatial » indisponible : {e}")

        # ── 2. Enregistrer les couches chargées comme tables (via ST_Read) ──
        registered: List[str] = []
        used = set()
        for lyr in (req.layers or []):
            feats = (lyr.geojson or {}).get("features") if isinstance(lyr.geojson, dict) else None
            if not feats:
                continue
            if len(feats) > _MAX_LAYER_FEATURES:
                continue
            tname = _safe_ident(lyr.name)
            base = tname
            i = 1
            while tname in used:      # noms uniques si doublon
                i += 1
                tname = f"{base}_{i}"
            used.add(tname)
            tf = tempfile.NamedTemporaryFile(suffix=".geojson", delete=False,
                                             mode="w", encoding="utf-8")
            json.dump(lyr.geojson, tf)
            tf.close()
            tmpfiles.append(tf.name)
            try:
                con.execute(f'CREATE TABLE "{tname}" AS SELECT * FROM ST_Read(?)', [tf.name])
                registered.append(tname)
            except Exception:
                pass   # couche illisible → on l'ignore, sans casser la requête

        # ── 3. VERROU : couper tout accès externe avant le SQL utilisateur ──
        # (app publique : empêche lecture de fichiers serveur + SSRF via httpfs)
        try:
            con.execute("SET enable_external_access=false;")
            con.execute("SET lock_configuration=true;")
        except Exception:
            raise HTTPException(
                503, "Impossible de sécuriser le moteur SQL sur ce serveur "
                     "(verrou d'accès externe indisponible). Exécution refusée.")

        # ── 4. Décrire le résultat (type des colonnes → détecter la géométrie) ──
        geom_col = None
        try:
            desc = con.execute(f"DESCRIBE {sql}").fetchall()   # (col, type, ...)
            for row in desc:
                cname, ctype = row[0], str(row[1]).upper()
                if "GEOMETRY" in ctype and geom_col is None:
                    geom_col = cname
        except Exception:
            desc = None   # non descriptible (DDL, PRAGMA, plusieurs statements…)

        if desc is None:
            # Requête non-SELECT : exécuter telle quelle, pas de tableau
            con.execute(sql)
            return {"columns": [], "rows": [], "rowCount": 0, "geojson": None,
                    "registered": registered,
                    "message": "Requête exécutée (aucun résultat tabulaire)."}

        # ── 5a. Résultat avec géométrie → GeoJSON + tableau ──
        if geom_col:
            others = [row[0] for row in desc if row[0] != geom_col]
            sel = ", ".join(f'"{c}"' for c in others)
            sel = (sel + ", ") if sel else ""
            q = (f'SELECT {sel}ST_AsGeoJSON("{geom_col}") AS __geo '
                 f'FROM ({sql}) AS _q LIMIT {_ROW_LIMIT}')
            res = con.execute(q).fetchall()
            features, table_rows = [], []
            for r in res:
                vals = [_jsonable(x) for x in r[:-1]]
                props = dict(zip(others, vals))
                try:
                    geom = json.loads(r[-1]) if r[-1] else None
                except Exception:
                    geom = None
                features.append({"type": "Feature", "geometry": geom, "properties": props})
                table_rows.append(vals)
            return {"columns": others, "rows": table_rows, "rowCount": len(res),
                    "geojson": {"type": "FeatureCollection", "features": features},
                    "truncated": len(res) >= _ROW_LIMIT, "registered": registered}

        # ── 5b. Résultat tabulaire simple ──
        res = con.execute(f'SELECT * FROM ({sql}) AS _q LIMIT {_ROW_LIMIT}')
        cols = [d[0] for d in res.description]
        rows = res.fetchall()
        table_rows = [[_jsonable(x) for x in r] for r in rows]
        return {"columns": cols, "rows": table_rows, "rowCount": len(rows),
                "geojson": None, "truncated": len(rows) >= _ROW_LIMIT,
                "registered": registered}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(422, f"Erreur SQL : {e}")
    finally:
        try:
            con.close()
        except Exception:
            pass
        for f in tmpfiles:
            try:
                os.remove(f)
            except Exception:
                pass
