"""
mcp_servers/mcp_overture.py — MCP Server Overture Maps
=======================================================
Réutilise DuckDBEngine et THEMES de l'agent.py existant.
Remplace query_overture + execute_query_overture de l'agent.py.

Release Overture : OVERTURE_RELEASE env var (défaut 2026-03-18.0)
S3 : s3://overturemaps-us-west-2/release/{release}/

Tools exposés :
    query_places        → POI, commerces, restaurants, équipements
    query_buildings     → Bâtiments avec hauteur, étages, classe
    query_roads         → Réseau routier (segments)
    query_divisions     → Divisions administratives (communes, régions, pays)
    query_addresses     → Adresses
    query_overture      → Requête générique (alias rétrocompat agent.py)
    get_theme_stats     → Stats rapides sur un thème dans une bbox
"""

import os
import json
import math
import hashlib
import logging
import asyncio
from pathlib import Path
from typing import Optional

log = logging.getLogger("mcp_overture")

# ── Config — reprend exactement les vars de agent.py ─────────
OVERTURE_RELEASE = os.getenv("OVERTURE_RELEASE", "2026-03-18.0")
S3_BASE          = f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"
S3_REGION        = os.getenv("OVERTURE_S3_REGION", "us-west-2")
DUCKDB_MEMORY    = os.getenv("DUCKDB_MEMORY", "4GB")
DUCKDB_THREADS   = int(os.getenv("DUCKDB_THREADS", "4"))
CACHE_DIR        = Path(os.getenv("CACHE_DIR", "./data/cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

MAX_FEATURES     = int(os.getenv("OVERTURE_MAX_FEATURES", "10000"))
DEFAULT_LIMIT    = 500

# ── Schémas des thèmes Overture ───────────────────────────────
# Reprend exactement THEMES de agent.py + colonnes enrichies
THEMES = {
    "places": {
        "types":   ["place"],
        "columns": (
            "id, "
            "names.primary AS name, "
            "categories.primary AS category, "
            "categories.alternate[1] AS category_alt, "
            "confidence, "
            "addresses[1].freeform AS address, "
            "addresses[1].locality AS city, "
            "addresses[1].country AS country, "
            "ST_AsGeoJSON(geometry) AS geom_json"
        ),
        "default_limit": 500,
    },
    "buildings": {
        "types":   ["building"],
        "columns": (
            "id, "
            "names.primary AS name, "
            "height, "
            "num_floors, "
            "class, "
            "subtype, "
            "ST_AsGeoJSON(geometry) AS geom_json"
        ),
        "default_limit": 2000,
    },
    "transportation": {
        "types":   ["segment"],
        "columns": (
            "id, "
            "class, "
            "subtype, "
            "names.primary AS name, "
            "ST_AsGeoJSON(geometry) AS geom_json"
        ),
        "default_limit": 1000,
    },
    "divisions": {
        "types":   ["division_area"],
        "columns": (
            "id, "
            "names.primary AS name, "
            "subtype, "
            "country, "
            "population, "
            "ST_AsGeoJSON(geometry) AS geom_json"
        ),
        "default_limit": 500,
    },
    "addresses": {
        "types":   ["address"],
        "columns": (
            "id, "
            "address_levels, "
            "ST_AsGeoJSON(geometry) AS geom_json"
        ),
        "default_limit": 1000,
    },
}

# ── Alias catégories Overture ─────────────────────────────────
CATEGORY_ALIASES = {
    # Restaurants / nourriture
    "restaurant":   "restaurant",
    "cafe":         "coffee_shop",
    "café":         "coffee_shop",
    "bar":          "bar",
    "boulangerie":  "bakery",
    "boucherie":    "butcher",
    "supermarché":  "grocery",
    "supermarche":  "grocery",
    "épicerie":     "grocery",
    # Santé
    "pharmacie":    "pharmacy",
    "hôpital":      "hospital",
    "hopital":      "hospital",
    "médecin":      "doctor",
    "clinique":     "clinic",
    # Éducation
    "école":        "school",
    "ecole":        "school",
    "université":   "university",
    "universite":   "university",
    "bibliothèque": "library",
    # Transport
    "station":      "train_station",
    "aéroport":     "airport",
    "aeroport":     "airport",
    "bus":          "bus_station",
    "parking":      "parking",
    # Hébergement
    "hôtel":        "hotel",
    "hotel":        "hotel",
    # Services
    "banque":       "bank",
    "poste":        "post_office",
    "mairie":       "government",
}


def _normalize_category(cat: str) -> str:
    """Normalise un nom de catégorie français/anglais → code Overture."""
    if not cat:
        return cat
    return CATEGORY_ALIASES.get(cat.lower(), cat.lower())


def _meters_to_degrees(meters: float, latitude: float):
    """Reprend exactement la fonction de agent.py."""
    lat_deg = meters / 111320
    lon_deg = meters / (111320 * math.cos(math.radians(latitude)))
    return lon_deg, lat_deg


def _validate_bbox(xmin, ymin, xmax, ymax):
    if not (-180 <= xmin < xmax <= 180):
        raise ValueError(f"bbox longitude invalide: {xmin},{xmax}")
    if not (-90 <= ymin < ymax <= 90):
        raise ValueError(f"bbox latitude invalide: {ymin},{ymax}")
    area = (xmax - xmin) * (ymax - ymin) * 111 * 111
    if area > 5000:
        raise ValueError(
            f"bbox trop grande ({area:.0f} km²). "
            f"Max ~5000 km² pour Overture. Réduisez la zone."
        )


def _build_bbox_from_args(args: dict) -> tuple:
    """
    Résout bbox depuis les args.
    Supporte : xmin/ymin/xmax/ymax OU center_lon/center_lat/radius_m
    Reprend la logique de execute_query_overture de agent.py.
    """
    # Bbox directe
    if all(k in args and args[k] is not None
           for k in ["xmin", "ymin", "xmax", "ymax"]):
        return (float(args["xmin"]), float(args["ymin"]),
                float(args["xmax"]), float(args["ymax"]))

    # Bbox depuis liste [xmin,ymin,xmax,ymax]
    if "bbox" in args and args["bbox"] and len(args["bbox"]) == 4:
        return tuple(float(v) for v in args["bbox"])

    # Center + radius
    if args.get("center_lon") is not None and args.get("center_lat") is not None:
        radius = args.get("radius_m", 500)
        clon, clat = float(args["center_lon"]), float(args["center_lat"])
        dlon, dlat = _meters_to_degrees(radius, clat)
        return (clon - dlon, clat - dlat, clon + dlon, clat + dlat)

    # Center depuis liste [lng, lat]
    if "center" in args and args["center"] and len(args["center"]) == 2:
        radius = args.get("radius_m", 500)
        clon, clat = float(args["center"][0]), float(args["center"][1])
        dlon, dlat = _meters_to_degrees(radius, clat)
        return (clon - dlon, clat - dlat, clon + dlon, clat + dlat)

    raise ValueError(
        "bbox manquant. Fournissez xmin/ymin/xmax/ymax, "
        "bbox=[xmin,ymin,xmax,ymax], ou center_lon/center_lat/radius_m"
    )


def _build_geojson(df, theme: str, args: dict) -> dict:
    """
    Convertit un DataFrame DuckDB en GeoJSON FeatureCollection.
    Reprend exactement la logique de execute_query_overture de agent.py.
    """
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
                props[k] = v if v else None
            else:
                props[k] = v

        if geom and geom.get("coordinates"):
            features.append({
                "type":       "Feature",
                "properties": props,
                "geometry":   geom,
            })

    return {
        "type":     "FeatureCollection",
        "features": features,
        "metadata": {
            "theme":        theme,
            "total":        len(features),
            "bbox":         list(args.get("_bbox", [])),
            "release":      OVERTURE_RELEASE,
            "query_params": {k: v for k, v in args.items()
                             if v is not None and not k.startswith("_")},
        },
    }


# ═══════════════════════════════════════════════════════════════
# DUCKDB ENGINE — réutilise la même classe que agent.py
# mais instanciée de façon indépendante pour le MCP server
# ═══════════════════════════════════════════════════════════════

class _DuckDBEngine:

    def __init__(self):
        self.conn = None

    def connect(self):
        import duckdb
        self.conn = duckdb.connect(":memory:")
        self.conn.execute("INSTALL spatial; LOAD spatial;")
        self.conn.execute("INSTALL httpfs;  LOAD httpfs;")
        try:
            self.conn.execute("INSTALL h3 FROM community; LOAD h3;")
        except Exception:
            pass
        self.conn.execute(f"SET s3_region='{S3_REGION}';")
        self.conn.execute(f"SET memory_limit='{DUCKDB_MEMORY}';")
        self.conn.execute(f"SET threads={DUCKDB_THREADS};")
        log.info(
            f"✓ Overture DuckDB connecté "
            f"(memory={DUCKDB_MEMORY}, threads={DUCKDB_THREADS})"
        )
        return self

    def query(self, sql: str):
        return self.conn.execute(sql).fetchdf()

    def close(self):
        if self.conn:
            self.conn.close()


# ═══════════════════════════════════════════════════════════════
# OVERTURE SERVER
# ═══════════════════════════════════════════════════════════════

class OvertureServer:
    """
    MCP Server Overture Maps via DuckDB + S3.
    Partage optionnellement le DuckDBEngine de agent.py
    ou crée sa propre instance.
    """

    def __init__(self):
        self._db    = None
        self._ready = False

    def _ensure_db(self):
        """
        Utilise le DuckDBEngine global de agent.py si disponible,
        sinon crée sa propre connexion.
        """
        if not self._ready:
            try:
                # Réutiliser l'engine global de agent.py
                from agent import db as global_db
                self._db = global_db
                log.info("✓ Overture MCP — réutilise DuckDB de agent.py")
            except ImportError:
                self._db = _DuckDBEngine().connect()
                log.info("✓ Overture MCP — DuckDB instance indépendante")
            self._ready = True

    async def call(self, tool: str, args: dict) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._run, tool, args)

    def _run(self, tool: str, args: dict) -> dict:
        fn = {
            "query_places":    self.query_places,
            "query_buildings": self.query_buildings,
            "query_roads":     self.query_roads,
            "query_divisions": self.query_divisions,
            "query_addresses": self.query_addresses,
            "query_overture":  self.query_overture,   # alias rétrocompat
            "get_theme_stats": self.get_theme_stats,
        }.get(tool)
        if not fn:
            return {"error": f"Overture tool inconnu: '{tool}'"}
        try:
            self._ensure_db()
            return fn(args)
        except ValueError as e:
            return {"error": str(e), "tool": tool}
        except Exception as e:
            log.error(f"Overture {tool}: {e}")
            return {"error": f"Erreur Overture: {e}", "tool": tool}

    # ─── QUERY INTERNE ────────────────────────────────────────

    def _query(self, theme: str, args: dict) -> dict:
        """
        Exécution DuckDB — reprend exactement execute_query_overture
        de agent.py avec les mêmes filtres et logique de cache.
        """
        if theme not in THEMES:
            return {"error": f"Thème inconnu: '{theme}'. "
                             f"Disponibles: {list(THEMES.keys())}"}

        # Résoudre bbox
        try:
            xmin, ymin, xmax, ymax = _build_bbox_from_args(args)
            _validate_bbox(xmin, ymin, xmax, ymax)
            args["_bbox"] = [xmin, ymin, xmax, ymax]
        except ValueError as e:
            return {"error": str(e)}

        schema = THEMES[theme]
        ptype  = schema["types"][0]
        cols   = schema["columns"]
        path   = f"{S3_BASE}/theme={theme}/type={ptype}/*"
        limit  = min(int(args.get("limit", schema["default_limit"])),
                     MAX_FEATURES)

        # Construire les filtres WHERE
        where = [
            f"bbox.xmin BETWEEN {xmin} AND {xmax}",
            f"bbox.ymin BETWEEN {ymin} AND {ymax}",
        ]

        # Filtres spécifiques par thème
        if theme == "places":
            cat = _normalize_category(args.get("category", ""))
            if cat:
                where.append(f"categories.primary = '{cat}'")
            if args.get("name_filter"):
                where.append(
                    f"names.primary ILIKE '%{args['name_filter']}%'"
                )
            if args.get("min_confidence") is not None:
                where.append(f"confidence >= {args['min_confidence']}")

        elif theme == "buildings":
            if args.get("min_height") is not None:
                where.append(f"height >= {args['min_height']}")
            if args.get("max_height") is not None:
                where.append(f"height <= {args['max_height']}")
            if args.get("building_class"):
                where.append(f"class = '{args['building_class']}'")
            if args.get("name_filter"):
                where.append(
                    f"names.primary ILIKE '%{args['name_filter']}%'"
                )

        elif theme == "transportation":
            if args.get("road_class"):
                where.append(f"class = '{args['road_class']}'")
            if args.get("road_type"):
                where.append(f"subtype = '{args['road_type']}'")

        elif theme == "divisions":
            if args.get("admin_level") or args.get("subtype"):
                st = args.get("admin_level") or args.get("subtype")
                where.append(f"subtype = '{st}'")
            if args.get("country"):
                where.append(f"country = '{args['country'].upper()}'")

        sql = (
            f"SELECT {cols}\n"
            f"FROM read_parquet('{path}', filename=true, hive_partitioning=1)\n"
            f"WHERE {' AND '.join(where)}\n"
            f"LIMIT {limit}"
        )

        log.info(f"[Overture] {theme} query — bbox [{xmin:.4f},{ymin:.4f},{xmax:.4f},{ymax:.4f}] limit={limit}")

        # Cache fichier (reprend logique de agent.py)
        cache_key  = hashlib.md5(sql.encode()).hexdigest()
        cache_path = CACHE_DIR / f"overture_{cache_key}.json"
        if cache_path.exists():
            log.info(f"[Overture] Cache hit: {cache_key[:8]}")
            cached = json.loads(cache_path.read_text())
            cached["metadata"]["cache_hit"] = True
            return cached

        try:
            df      = self._db.query(sql)
            geojson = _build_geojson(df, theme, args)

            # Mise en cache
            cache_path.write_text(json.dumps(geojson, default=str))
            log.info(
                f"[Overture] {theme} → {len(geojson['features'])} features"
            )
            return geojson

        except Exception as e:
            log.error(f"[Overture] DuckDB error: {e}\nSQL: {sql[:300]}")
            return {"error": str(e), "sql": sql[:300]}

    # ─── TOOLS SPÉCIALISÉS ────────────────────────────────────

    def query_places(self, a: dict) -> dict:
        """
        Points d'intérêt : restaurants, commerces, équipements, etc.

        Args:
            bbox / xmin,ymin,xmax,ymax / center+radius_m
            category:       restaurant | pharmacy | school | hotel | ...
            name_filter:    filtre ILIKE sur le nom
            min_confidence: seuil confiance 0-1
            limit:          max features (défaut 500, max 10000)
            clip_to_layer:  nom d'une couche polygone pour clip spatial
        """
        result = self._query("places", a)
        if "error" in result:
            return result

        n = len(result.get("features", []))
        cat = a.get("category", "")
        return {
            **result,
            "action":     "add_markers",
            "layer_name": f"{'Places' if not cat else cat.capitalize()} "
                          f"({n} résultats)",
            "theme":      "places",
        }

    def query_buildings(self, a: dict) -> dict:
        """
        Bâtiments Overture avec hauteur, étages, classe.

        Args:
            bbox / center+radius
            min_height:     hauteur min en mètres
            max_height:     hauteur max en mètres
            building_class: residential | commercial | industrial | ...
            name_filter:    filtre sur le nom du bâtiment
            limit:          défaut 2000
        """
        result = self._query("buildings", a)
        if "error" in result:
            return result

        n = len(result.get("features", []))
        return {
            **result,
            "action":     "add_layer",
            "layer_name": f"Bâtiments ({n})",
            "theme":      "buildings",
            # Infos pour extrusion 3D dans MapLibre
            "extrusion": {
                "enabled":        True,
                "height_field":   "height",
                "floors_field":   "num_floors",
                "base_height":    0,
                "default_height": 10,
            },
        }

    def query_roads(self, a: dict) -> dict:
        """
        Réseau routier Overture.

        Args:
            bbox / center+radius
            road_class: motorway | trunk | primary | secondary |
                        tertiary | residential | path | ...
            road_type:  road | rail | water | ...
            limit:      défaut 1000
        """
        result = self._query("transportation", a)
        if "error" in result:
            return result

        n = len(result.get("features", []))
        return {
            **result,
            "action":     "add_layer",
            "layer_name": f"Routes ({n})",
            "theme":      "transportation",
        }

    def query_divisions(self, a: dict) -> dict:
        """
        Divisions administratives : communes, départements, régions, pays.

        Args:
            bbox / center+radius
            admin_level / subtype:
                country | region | county | locality | localadmin |
                neighborhood | microhood
            country:    filtre ISO2 pays (ex: "FR", "SN")
            limit:      défaut 500
        """
        result = self._query("divisions", a)
        if "error" in result:
            return result

        n   = len(result.get("features", []))
        lvl = a.get("admin_level") or a.get("subtype", "")
        return {
            **result,
            "action":     "add_layer",
            "layer_name": f"Divisions {lvl} ({n})",
            "theme":      "divisions",
        }

    def query_addresses(self, a: dict) -> dict:
        """
        Adresses Overture.

        Args:
            bbox / center+radius
            limit: défaut 1000
        """
        result = self._query("addresses", a)
        if "error" in result:
            return result

        n = len(result.get("features", []))
        return {
            **result,
            "action":     "add_markers",
            "layer_name": f"Adresses ({n})",
            "theme":      "addresses",
        }

    def query_overture(self, a: dict) -> dict:
        """
        Alias rétrocompatibilité — reprend exactement l'interface
        de query_overture dans agent.py.

        Args:
            theme:       places | buildings | transportation |
                         divisions | addresses
            + tous les args de execute_query_overture
        """
        theme = a.get("theme")
        if not theme:
            return {"error": "theme requis"}

        dispatch = {
            "places":         self.query_places,
            "buildings":      self.query_buildings,
            "transportation": self.query_roads,
            "divisions":      self.query_divisions,
            "addresses":      self.query_addresses,
        }
        fn = dispatch.get(theme)
        if not fn:
            return {
                "error": f"Thème '{theme}' inconnu. "
                         f"Disponibles: {list(dispatch.keys())}"
            }
        return fn(a)

    def get_theme_stats(self, a: dict) -> dict:
        """
        Stats rapides sur un thème dans une bbox.
        Compte uniquement (pas de géométries) — très rapide.

        Args:
            theme: places | buildings | transportation | divisions
            bbox / xmin,ymin,xmax,ymax / center+radius
        """
        theme = a.get("theme", "places")
        if theme not in THEMES:
            return {"error": f"Thème '{theme}' inconnu."}

        try:
            xmin, ymin, xmax, ymax = _build_bbox_from_args(a)
            _validate_bbox(xmin, ymin, xmax, ymax)
        except ValueError as e:
            return {"error": str(e)}

        schema = THEMES[theme]
        ptype  = schema["types"][0]
        path   = f"{S3_BASE}/theme={theme}/type={ptype}/*"

        # Requête COUNT uniquement — sans ST_AsGeoJSON, très rapide
        where = [
            f"bbox.xmin BETWEEN {xmin} AND {xmax}",
            f"bbox.ymin BETWEEN {ymin} AND {ymax}",
        ]

        # Stats supplémentaires selon thème
        extra_cols = "COUNT(*) AS total"
        if theme == "buildings":
            extra_cols += (
                ", AVG(height) AS avg_height"
                ", MAX(height) AS max_height"
                ", COUNT(CASE WHEN height IS NOT NULL THEN 1 END) AS with_height"
            )
        elif theme == "places":
            extra_cols += (
                ", COUNT(DISTINCT categories.primary) AS category_count"
            )

        sql = (
            f"SELECT {extra_cols}\n"
            f"FROM read_parquet('{path}', filename=true, hive_partitioning=1)\n"
            f"WHERE {' AND '.join(where)}"
        )

        try:
            df     = self._db.query(sql)
            row    = df.iloc[0].to_dict() if len(df) > 0 else {}
            result = {k: (round(float(v), 1) if v is not None and
                          str(v) not in ("nan", "None") else None)
                      for k, v in row.items()}
            return {
                "action": "show_stats",
                "theme":  theme,
                "bbox":   [xmin, ymin, xmax, ymax],
                "stats":  result,
                "release": OVERTURE_RELEASE,
            }
        except Exception as e:
            return {"error": f"Stats error: {e}"}
