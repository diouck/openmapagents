"""
tool_registry.py — Registre des outils OpenMapAgents pour RAG pgvector
Chaque outil est un document enrichi :
  - description sémantique
  - triggers fr + en
  - examples few-shot
  - schema JSON (params)
  - endpoint FastAPI cible
  - domaine (pour le router)
"""

from typing import Any

# ═══════════════════════════════════════════════════════════════
# TYPE : ToolDoc
# ═══════════════════════════════════════════════════════════════
def tool_doc(
    id: str,
    domain: str,
    title: str,
    description: str,
    triggers: list[str],
    examples: list[dict],
    schema: dict,
    endpoint: str,
) -> dict[str, Any]:
    """Construit un document outil normalisé pour l'indexation."""
    return {
        "id": id,
        "domain": domain,
        "title": title,
        "description": description,
        "triggers": triggers,
        "examples": examples,
        "schema": schema,
        "endpoint": endpoint,
    }


# ═══════════════════════════════════════════════════════════════
# DOMAINE : geo_data  (Overture Maps / DuckDB)
# ═══════════════════════════════════════════════════════════════

GEOCODE = tool_doc(
    id="geocode",
    domain="geo_data",
    title="Géocodage d'un lieu",
    description=(
        "Convertit un nom de lieu, une adresse ou un monument en coordonnées géographiques "
        "latitude/longitude via Nominatim (OpenStreetMap). "
        "À utiliser en PREMIER quand l'utilisateur mentionne un lieu nommé, "
        "une ville, une adresse ou un monument spécifique. "
        "Retourne lat, lon, bbox et display_name."
    ),
    triggers=[
        "géocoder", "geocode", "coordonnées", "localiser", "où est",
        "adresse", "position", "lieu", "monument", "place", "ville",
        "locate", "coordinates", "address", "where is", "find location",
        "château", "gare", "aéroport", "quartier",
    ],
    examples=[
        {
            "query": "coordonnées latitude longitude d un lieu adresse",
            "tool": "geocode",
            "params": {"query": "Château des Ducs de Bretagne Nantes"},
        },
        {
            "query": "géocodage position géographique d une adresse postale",
            "tool": "geocode",
            "params": {"query": "15 rue de la Paix Paris"},
        },
        {
            "query": "où se trouve cette adresse localisation GPS point",
            "tool": "geocode",
            "params": {"query": "Dakar Senegal"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Nom du lieu, adresse ou monument à géocoder"},
        },
        "required": ["query"],
    },
    endpoint="internal/geocode",
)

QUERY_OVERTURE_PLACES = tool_doc(
    id="query_overture_places",
    domain="geo_data",
    title="Requête POI / lieux Overture Maps",
    description=(
        "Interroge la base Overture Maps (DuckDB sur S3) pour récupérer des points d'intérêt : "
        "restaurants, cafés, commerces, pharmacies, hôpitaux, écoles, hôtels, etc. "
        "Retourne un GeoJSON FeatureCollection avec nom, catégorie, confiance et coordonnées. "
        "Accepte une bbox ou un centre + rayon en mètres. "
        "Utiliser APRÈS geocode si l'utilisateur mentionne un lieu nommé."
    ),
    triggers=[
        "restaurant", "café", "commerce", "magasin", "pharmacie", "hôpital",
        "école", "hôtel", "POI", "point d'intérêt", "lieu", "place",
        "overture", "places", "shop", "store", "supermarché", "banque",
        "restaurant", "coffee", "pharmacy", "hospital", "school", "hotel",
        "autour de", "près de", "à côté de", "dans le quartier",
        "trouver", "afficher", "montrer", "chercher",
    ],
    examples=[
        {
            "query": "Restaurants à Nantes",
            "tool": "query_overture",
            "params": {"theme": "places", "category": "restaurant",
                       "xmin": -1.72, "ymin": 47.15, "xmax": -1.42, "ymax": 47.32},
        },
        {
            "query": "Pharmacies autour du Château des Ducs",
            "tool": "query_overture",
            "params": {"theme": "places", "category": "pharmacy",
                       "center_lon": -1.5534, "center_lat": 47.2184, "radius_m": 800},
        },
        {
            "query": "Commerces dans le centre de Dakar",
            "tool": "query_overture",
            "params": {"theme": "places",
                       "xmin": -17.45, "ymin": 14.68, "xmax": -17.42, "ymax": 14.72},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "theme": {"type": "string", "enum": ["places"]},
            "xmin": {"type": "number"}, "ymin": {"type": "number"},
            "xmax": {"type": "number"}, "ymax": {"type": "number"},
            "center_lon": {"type": "number"}, "center_lat": {"type": "number"},
            "radius_m": {"type": "number", "description": "Rayon en mètres"},
            "category": {"type": "string", "description": "Catégorie Overture"},
            "limit": {"type": "integer", "default": 500},
        },
        "required": ["theme"],
    },
    endpoint="internal/query_overture",
)

QUERY_OVERTURE_BUILDINGS = tool_doc(
    id="query_overture_buildings",
    domain="geo_data",
    title="Requête bâtiments Overture Maps",
    description=(
        "Interroge Overture Maps pour récupérer des bâtiments avec hauteur, "
        "nombre d'étages et emprise géométrique (polygone). "
        "Idéal pour l'analyse urbaine, la 3D, la densité bâtie. "
        "Filtrable par hauteur min/max."
    ),
    triggers=[
        "bâtiment", "immeuble", "construction", "édifice", "building",
        "hauteur", "étages", "emprise", "footprint", "urbain", "îlot",
        "height", "floors", "urban", "architecture", "3D",
        "bâti", "tissu urbain",
    ],
    examples=[
        {
            "query": "Bâtiments autour du château des Ducs",
            "tool": "query_overture",
            "params": {"theme": "buildings",
                       "center_lon": -1.5534, "center_lat": 47.2184, "radius_m": 500},
        },
        {
            "query": "Immeubles de plus de 50m à Paris",
            "tool": "query_overture",
            "params": {"theme": "buildings", "min_height": 50,
                       "xmin": 2.22, "ymin": 48.81, "xmax": 2.47, "ymax": 48.90},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "theme": {"type": "string", "enum": ["buildings"]},
            "xmin": {"type": "number"}, "ymin": {"type": "number"},
            "xmax": {"type": "number"}, "ymax": {"type": "number"},
            "center_lon": {"type": "number"}, "center_lat": {"type": "number"},
            "radius_m": {"type": "number"},
            "min_height": {"type": "number"}, "max_height": {"type": "number"},
            "limit": {"type": "integer", "default": 500},
        },
        "required": ["theme"],
    },
    endpoint="internal/query_overture",
)

QUERY_OVERTURE_TRANSPORT = tool_doc(
    id="query_overture_transport",
    domain="geo_data",
    title="Requête réseau routier / transport Overture Maps",
    description=(
        "Récupère les segments du réseau routier et de transport depuis Overture Maps : "
        "autoroutes, routes principales, secondaires, résidentielles, pistes cyclables. "
        "Retourne les géométries LineString avec classe et sous-type."
    ),
    triggers=[
        "route", "rue", "voirie", "réseau routier", "transport", "road",
        "autoroute", "avenue", "boulevard", "chemin", "piste cyclable",
        "highway", "street", "network", "motorway", "primary", "secondary",
        "réseau", "infrastructure", "mobilité",
    ],
    examples=[
        {
            "query": "Réseau routier de Dakar",
            "tool": "query_overture",
            "params": {"theme": "transportation",
                       "xmin": -17.55, "ymin": 14.63, "xmax": -17.33, "ymax": 14.82},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "theme": {"type": "string", "enum": ["transportation"]},
            "xmin": {"type": "number"}, "ymin": {"type": "number"},
            "xmax": {"type": "number"}, "ymax": {"type": "number"},
            "limit": {"type": "integer", "default": 500},
        },
        "required": ["theme"],
    },
    endpoint="internal/query_overture",
)

QUERY_OVERTURE_DIVISIONS = tool_doc(
    id="query_overture_divisions",
    domain="geo_data",
    title="Requête divisions administratives Overture Maps",
    description=(
        "Récupère les divisions administratives depuis Overture Maps : "
        "pays, régions, départements, communes, quartiers. "
        "Utile pour les analyses territoriales et la cartographie thématique."
    ),
    triggers=[
        "commune", "département", "région", "pays", "quartier", "arrondissement",
        "division", "administratif", "territoire", "frontière", "limite",
        "country", "region", "municipality", "boundary", "administrative",
        "zonage", "périmètre",
    ],
    examples=[
        {
            "query": "communes Loire-Atlantique département divisions administratives",
            "tool": "query_overture",
            "params": {"theme": "divisions",
                       "xmin": -2.5, "ymin": 46.8, "xmax": -1.0, "ymax": 47.8},
        },
        {
            "query": "communes régions départements pays territoires limites administratives",
            "tool": "query_overture",
            "params": {"theme": "divisions",
                       "xmin": -2.5, "ymin": 46.8, "xmax": -1.0, "ymax": 47.8},
        },
        {
            "query": "frontières zonage périmètre commune arrondissement canton",
            "tool": "query_overture",
            "params": {"theme": "divisions",
                       "xmin": 2.22, "ymin": 48.81, "xmax": 2.47, "ymax": 48.90},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "theme": {"type": "string", "enum": ["divisions"]},
            "xmin": {"type": "number"}, "ymin": {"type": "number"},
            "xmax": {"type": "number"}, "ymax": {"type": "number"},
            "limit": {"type": "integer", "default": 200},
        },
        "required": ["theme"],
    },
    endpoint="internal/query_overture",
)

FLY_TO = tool_doc(
    id="fly_to",
    domain="geo_data",
    title="Déplacer la caméra carte",
    description=(
        "Déplace la vue de la carte vers une position géographique donnée "
        "avec un niveau de zoom et une inclinaison optionnelle. "
        "Utiliser APRÈS une requête de données pour centrer la carte sur les résultats, "
        "ou quand l'utilisateur demande à voir une ville ou une zone."
    ),
    triggers=[
        "aller à", "voir", "afficher", "centrer", "zoomer", "vue",
        "go to", "fly to", "show", "center", "zoom", "navigate",
        "déplacer", "position", "carte sur",
    ],
    examples=[
        {
            "query": "Centre la carte sur Nantes",
            "tool": "fly_to",
            "params": {"longitude": -1.5534, "latitude": 47.2184, "zoom": 12},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "longitude": {"type": "number"},
            "latitude": {"type": "number"},
            "zoom": {"type": "number", "description": "1-20"},
            "pitch": {"type": "number", "description": "Inclinaison 0-60°"},
        },
        "required": ["longitude", "latitude"],
    },
    endpoint="internal/fly_to",
)

SET_LAYER_STYLE = tool_doc(
    id="set_layer_style",
    domain="geo_data",
    title="Changer le style d'une couche",
    description=(
        "Modifie le style visuel d'une couche affichée sur la carte : "
        "couleur, opacité, taille des points, couleur de contour, épaisseur de trait."
    ),
    triggers=[
        "couleur", "style", "changer", "modifier", "colorer", "opacité",
        "color", "style", "change", "opacity", "stroke", "fill",
        "rouge", "bleu", "vert", "jaune", "violet", "orange",
        "transparent", "visible", "épaisseur",
    ],
    examples=[
        {
            "query": "Colore les restaurants en rouge",
            "tool": "set_layer_style",
            "params": {"layer_id": "places_restaurant", "color": "#ff0000"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "layer_id": {"type": "string"},
            "color": {"type": "string"},
            "opacity": {"type": "number"},
            "radius": {"type": "number"},
            "stroke_color": {"type": "string"},
            "stroke_width": {"type": "number"},
        },
        "required": ["layer_id"],
    },
    endpoint="internal/set_layer_style",
)

REMOVE_LAYER = tool_doc(
    id="remove_layer",
    domain="geo_data",
    title="Supprimer une couche",
    description=(
        "Supprime une couche de la carte par son identifiant, "
        "ou supprime toutes les couches avec 'all'."
    ),
    triggers=[
        "supprimer", "effacer", "enlever", "retirer", "vider",
        "remove", "delete", "clear", "hide", "dismiss",
        "nettoyer la carte", "tout effacer",
    ],
    examples=[
        {
            "query": "Efface toutes les couches",
            "tool": "remove_layer",
            "params": {"layer_id": "all"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "layer_id": {"type": "string", "description": "ID de couche ou 'all'"},
        },
        "required": ["layer_id"],
    },
    endpoint="internal/remove_layer",
)


# ═══════════════════════════════════════════════════════════════
# DOMAINE : satellite (Google Earth Engine)
# ═══════════════════════════════════════════════════════════════

GEE_IMAGERY = tool_doc(
    id="gee_imagery",
    domain="satellite",
    title="Imagerie satellite Google Earth Engine",
    description=(
        "Génère des tuiles d'imagerie satellite via Google Earth Engine. "
        "Datasets disponibles : Sentinel-2, Landsat 8/9, MODIS LST/NDVI, "
        "ESA WorldCover, Sentinel-1 SAR, Hansen Forest, ERA5, SRTM. "
        "Indices calculables : NDVI (végétation), NDWI (eau), NDBI (bâti), "
        "EVI, LST (température), RGB, False Color NIR, VV/VH (radar), "
        "occupation du sol, élévation, pente, ombrage. "
        "Retourne une URL de tuiles XYZ à charger dans MapLibre."
    ),
    triggers=[
        "satellite", "imagerie", "image satellite", "Sentinel", "Landsat",
        "MODIS", "NDVI", "végétation", "NDWI", "eau", "NDBI", "bâti",
        "LST", "température", "WorldCover", "occupation du sol",
        "GEE", "Earth Engine", "radar", "SAR", "relief", "élévation",
        "forêt", "déforestation", "Hansen", "ERA5", "précipitations",
        "EVI", "False Color", "NIR", "infrarouge", "pente", "ombrage",
        "SRTM", "MNT", "nuages", "composite", "median",
        "remote sensing", "télédétection", "raster",
    ],
    examples=[
        {
            "query": "NDVI Dakar Sentinel-2 végétation",
            "tool": "gee_imagery",
            "params": {"dataset": "sentinel2", "index": "NDVI",
                       "date_start": "2024-01-01", "date_end": "2024-06-30",
                       "bbox": [-17.55, 14.63, -17.33, 14.82]},
        },
        {
            "query": "NDVI satellite imagerie télédétection indice végétation",
            "tool": "gee_imagery",
            "params": {"dataset": "sentinel2", "index": "NDVI",
                       "date_start": "2024-01-01", "date_end": "2024-06-30"},
        },
        {
            "query": "occupation du sol worldcover ESA land cover",
            "tool": "gee_imagery",
            "params": {"dataset": "worldcover", "index": "Occupation du sol",
                       "date_start": "2021-01-01", "date_end": "2021-12-31"},
        },
        {
            "query": "worldcover occupation sol classification raster",
            "tool": "gee_imagery",
            "params": {"dataset": "worldcover", "index": "Occupation du sol",
                       "date_start": "2021-01-01", "date_end": "2021-12-31"},
        },
        {
            "query": "Température de surface LST Landsat chaleur urbaine",
            "tool": "gee_imagery",
            "params": {"dataset": "landsat9", "index": "LST (température)",
                       "date_start": "2023-06-01", "date_end": "2023-09-30",
                       "bbox": [-1.72, 47.15, -1.42, 47.32]},
        },
        {
            "query": "satellite imagery temperature NDWI radar relief pente srtm",
            "tool": "gee_imagery",
            "params": {"dataset": "srtm", "index": "Élévation"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "dataset": {
                "type": "string",
                "enum": ["sentinel2", "landsat9", "landsat8", "modis_lst",
                         "modis_ndvi", "worldcover", "sentinel1", "hansen", "era5", "srtm"],
            },
            "index": {"type": "string", "description": "Indice à calculer (NDVI, LST, RGB, ...)"},
            "date_start": {"type": "string", "description": "Date début ISO YYYY-MM-DD"},
            "date_end": {"type": "string", "description": "Date fin ISO YYYY-MM-DD"},
            "bbox": {
                "type": "array", "items": {"type": "number"},
                "description": "[xmin, ymin, xmax, ymax]",
            },
            "cloud_max": {"type": "number", "description": "Couverture nuageuse max (%)"},
            "composite": {
                "type": "string", "enum": ["least_cloudy", "median", "mosaic"],
                "description": "Méthode de composite",
            },
        },
        "required": ["dataset", "index", "date_start", "date_end"],
    },
    endpoint="/api/gee/tiles",
)

GEE_TIMELAPSE = tool_doc(
    id="gee_timelapse",
    domain="satellite",
    title="Timelapse satellite GEE",
    description=(
        "Génère un timelapse GIF animé depuis Google Earth Engine. "
        "Utiliser quand l'utilisateur demande une animation, une évolution temporelle, "
        "un timelapse, un GIF ou des changements sur plusieurs années. "
        "Différent de gee_imagery qui retourne une image statique unique."
    ),
    triggers=[
        "timelapse", "time-lapse", "animation", "GIF", "gif animé",
        "évolution", "evolution", "changement sur", "années", "annees",
        "temporel", "temporelle", "au fil du temps", "progression",
        "dynamique", "entre 2019 et", "depuis 2015", "sur 5 ans",
    ],
    examples=[
        {
            "query": "timelapse NDVI Nantes 2019-2024 animation gif",
            "tool":  "gee_timelapse",
            "params": {"dataset": "sentinel2", "index": "NDVI",
                       "year_start": 2019, "year_end": 2024},
        },
        {
            "query": "animation évolution végétation gif 5 dernières années",
            "tool":  "gee_timelapse",
            "params": {"dataset": "sentinel2", "index": "NDVI",
                       "year_start": 2021, "year_end": 2026},
        },
        {
            "query": "timelapse RGB Sentinel évolution temporelle gif animé",
            "tool":  "gee_timelapse",
            "params": {"dataset": "sentinel2", "index": "RGB",
                       "year_start": 2018, "year_end": 2026},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "dataset":    {"type": "string", "enum": ["sentinel2", "landsat"],
                           "description": "sentinel2 (depuis 2015, 10m) ou landsat (depuis 1984, 30m)"},
            "index":      {"type": "string", "enum": ["RGB", "NDVI", "NDWI"],
                           "description": "RGB = couleurs naturelles, NDVI = végétation, NDWI = eau"},
            "year_start": {"type": "integer", "description": "Année de début (ex: 2019)"},
            "year_end":   {"type": "integer", "description": "Année de fin (ex: 2024)"},
            "frequency":  {"type": "string", "enum": ["annual", "seasonal", "monthly"],
                           "description": "annual=une image/an, seasonal=par saison, monthly=mensuel"},
            "bbox":       {"type": "array", "items": {"type": "number"},
                           "description": "[xmin,ymin,xmax,ymax] — utiliser emprise carte si non précisé"},
            "cloud_max":  {"type": "number", "description": "% nuages maximum (défaut 30)"},
            "fps":        {"type": "integer", "description": "Images par seconde dans le GIF (défaut 3)"},
        },
        "required": ["dataset", "index", "year_start", "year_end", "bbox"],
    },
    endpoint="/api/gee/timelapse",
)



# ═══════════════════════════════════════════════════════════════
# DOMAINE : database (PostGIS / MySQL / SQLite)
# ═══════════════════════════════════════════════════════════════

DB_QUERY = tool_doc(
    id="db_query",
    domain="database",
    title="Requête base de données externe (PostGIS / MySQL / SQLite)",
    description=(
        "Exécute une requête SQL SELECT sur une base de données externe "
        "PostgreSQL/PostGIS, MySQL ou SQLite et retourne les résultats en GeoJSON. "
        "Supporte les géométries PostGIS (ST_AsGeoJSON), WKT, ou colonnes lat/lon. "
        "Sécurisé : seules les requêtes SELECT sont autorisées. "
        "Idéal pour interroger des bases métier existantes, des données SIG stockées en PostGIS."
    ),
    triggers=[
        "base de données", "database", "PostGIS", "PostgreSQL", "MySQL", "SQLite",
        "SQL", "requête SQL", "table", "vue", "connecter", "connexion",
        "ma base", "mon serveur", "mes données", "données métier",
        "query", "connect", "my database", "my server",
        "parcelles", "cadastre", "données locales", "données internes",
    ],
    examples=[
        {
            "query": "Affiche les parcelles de ma base PostGIS",
            "tool": "db_query",
            "params": {
                "connection": {
                    "type": "postgresql", "host": "localhost",
                    "database": "sig_nantes", "username": "postgres",
                },
                "sql": "SELECT id, code, ST_AsGeoJSON(geom) AS geom_json FROM parcelles LIMIT 500",
            },
        },
        {
            "query": "Connecte-toi à ma base et liste les tables",
            "tool": "db_tables",
            "params": {
                "connection": {"type": "postgresql", "host": "localhost", "database": "mydb"},
            },
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "connection": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["postgresql", "mysql", "sqlite"]},
                    "host": {"type": "string"}, "port": {"type": "integer"},
                    "database": {"type": "string"},
                    "username": {"type": "string"}, "password": {"type": "string"},
                },
                "required": ["type", "database"],
            },
            "sql": {"type": "string"},
            "geom_column": {"type": "string", "default": "geom"},
            "limit": {"type": "integer", "default": 2000},
        },
        "required": ["connection", "sql"],
    },
    endpoint="/api/db/query",
)

DB_TABLES = tool_doc(
    id="db_tables",
    domain="database",
    title="Lister les tables d'une base de données",
    description=(
        "Liste toutes les tables et vues d'une base de données externe "
        "avec leurs colonnes et indique si une colonne géométrique est présente. "
        "Utiliser en premier pour explorer une base inconnue."
    ),
    triggers=[
        "lister les tables", "liste tables", "quelles tables", "explorer la base",
        "schéma", "structure", "colonnes", "list tables", "show tables",
        "what tables", "schema", "explore database",
    ],
    examples=[
        {
            "query": "Quelles tables sont dans ma base ?",
            "tool": "db_tables",
            "params": {
                "connection": {"type": "postgresql", "host": "localhost", "database": "sig"},
            },
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "connection": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["postgresql", "mysql", "sqlite"]},
                    "host": {"type": "string"}, "database": {"type": "string"},
                    "username": {"type": "string"}, "password": {"type": "string"},
                },
                "required": ["type", "database"],
            },
        },
        "required": ["connection"],
    },
    endpoint="/api/db/tables",
)


# ═══════════════════════════════════════════════════════════════
# DOMAINE : routing (OpenRouteService)
# ═══════════════════════════════════════════════════════════════

COMPUTE_ROUTE = tool_doc(
    id="compute_route",
    domain="routing",
    title="Calcul d'itinéraire",
    description=(
        "Calcule un itinéraire entre deux points ou plus via OpenRouteService. "
        "Profils : à pied (foot), vélo (bike), voiture (car). "
        "Retourne la géométrie du trajet, la distance en km, la durée en minutes "
        "et les instructions turn-by-turn. "
        "IMPORTANT : géocoder les lieux d'abord si nommés."
    ),
    triggers=[
        "itinéraire", "itineraire", "trajet", "chemin", "aller de", "route entre",
        "directions", "route", "path", "navigation", "go from",
        "comment aller", "relier", "rejoindre", "de A à B",
        "à pied", "en vélo", "en voiture", "pédestre", "cyclable",
        "vélo", "velo", "bicycle", "foot", "bike", "car", "walk", "drive", "cycle",
        "distance", "durée", "temps de trajet", "GPS navigation",
    ],
    examples=[
        {
            "query": "itinéraire vélo gare château trajet route",
            "tool": "compute_route",
            "params": {"waypoints": [[-1.5419, 47.2173], [-1.5534, 47.2184]],
                       "profile": "bike"},
        },
        {
            "query": "itinéraire à vélo de la gare au château navigation",
            "tool": "compute_route",
            "params": {"waypoints": [[-1.5419, 47.2173], [-1.5534, 47.2184]],
                       "profile": "bike"},
        },
        {
            "query": "aller de A à B trajet directions route pied voiture vélo",
            "tool": "compute_route",
            "params": {"waypoints": [[-1.5419, 47.2173], [-1.5534, 47.2184]],
                       "profile": "foot"},
        },
        {
            "query": "Comment aller à pied de Dakar à l'île de Gorée ?",
            "tool": "compute_route",
            "params": {"waypoints": [[-17.4467, 14.6937], [-17.3987, 14.6693]],
                       "profile": "foot"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "waypoints": {
                "type": "array",
                "items": {"type": "array", "items": {"type": "number"}},
                "description": "Liste de [lon, lat]",
            },
            "profile": {"type": "string", "enum": ["foot", "bike", "car"], "default": "foot"},
        },
        "required": ["waypoints"],
    },
    endpoint="internal/compute_route",
)

COMPUTE_ISOCHRONE = tool_doc(
    id="compute_isochrone",
    domain="routing",
    title="Calcul d'isochrone (zone d'accessibilité)",
    description=(
        "Calcule la zone accessible depuis un point en un temps donné (en minutes). "
        "Profils : à pied, vélo, voiture. "
        "Retourne un polygone GeoJSON représentant la zone d'accessibilité. "
        "Utile pour l'analyse de desserte, la localisation de services, "
        "les études d'accessibilité urbaine."
    ),
    triggers=[
        "isochrone", "zone accessible", "accessibilité", "desserte",
        "en X minutes", "dans un rayon de temps", "reachable", "isochrone",
        "zone de chalandise", "aire d'influence", "bassin de vie",
        "accessible en", "atteignable en", "service area",
        "10 minutes à pied", "15 min vélo", "30 min voiture",
    ],
    examples=[
        {
            "query": "Zone accessible à pied en 10 min depuis la gare de Nantes",
            "tool": "compute_isochrone",
            "params": {
                "center": [-1.5419, 47.2173],
                "time_minutes": 10, "profile": "foot",
            },
        },
        {
            "query": "Isochrone 20 min vélo depuis le château de Dakar",
            "tool": "compute_isochrone",
            "params": {
                "center": [-17.4467, 14.6937],
                "time_minutes": 20, "profile": "bike",
            },
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "center": {
                "type": "array", "items": {"type": "number"},
                "description": "[longitude, latitude]",
            },
            "time_minutes": {"type": "integer", "default": 10},
            "profile": {"type": "string", "enum": ["foot", "bike", "car"], "default": "foot"},
        },
        "required": ["center"],
    },
    endpoint="internal/compute_isochrone",
)


# ═══════════════════════════════════════════════════════════════
# DOMAINE : spatial (analyse spatiale turf.js)
# ═══════════════════════════════════════════════════════════════

WORLD_BANK_INDICATOR = tool_doc(
    id="world_bank_indicator",
    domain="world_data",
    title="Indicateurs World Bank (choroplèthe mondiale)",
    description=(
        "Affiche un indicateur World Bank sur une carte choroplèthe mondiale. "
        "20 indicateurs : population, PIB, santé, éducation, environnement. "
        "Retourne un GeoJSON FeatureCollection avec polygones pays + valeur indicateur. "
        "Utiliser pour toute demande de données mondiales par pays."
    ),
    triggers=[
        "world bank", "indicateur", "indicateur mondial", "données mondiales",
        "population mondiale", "PIB par pays", "PIB par habitant",
        "espérance de vie", "mortalité infantile", "alphabétisation",
        "émissions CO2", "déforestation", "accès eau", "électricité",
        "chômage", "inégalités", "gini", "croissance PIB", "croissance démographique",
        "densité population", "urbanisation", "dépenses santé", "dépenses éducation",
        "terres arables", "couverture forestière", "par pays", "mondial",
        "carte mondiale", "comparaison internationale",
    ],
    examples=[
        {
            "query": "population mondiale choroplèthe par pays",
            "tool":  "world_bank_indicator",
            "params": {"indicator": "SP.POP.TOTL"},
        },
        {
            "query": "PIB par habitant carte mondiale comparaison pays",
            "tool":  "world_bank_indicator",
            "params": {"indicator": "NY.GDP.PCAP.CD"},
        },
        {
            "query": "espérance de vie par pays monde santé longévité",
            "tool":  "world_bank_indicator",
            "params": {"indicator": "SP.DYN.LE00.IN"},
        },
        {
            "query": "émissions CO2 par habitant environnement carbone",
            "tool":  "world_bank_indicator",
            "params": {"indicator": "EN.ATM.CO2E.PC"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "indicator": {
                "type": "string",
                "description": (
                    "Code World Bank. Exemples : SP.POP.TOTL (population), "
                    "NY.GDP.PCAP.CD (PIB/hab), SP.DYN.LE00.IN (espérance vie), "
                    "EN.ATM.CO2E.PC (CO2), SI.POV.GINI (inégalités), "
                    "SL.UEM.TOTL.ZS (chômage), SE.ADT.LITR.ZS (alphabétisation)"
                ),
            },
            "year": {
                "type": "integer",
                "description": "Année des données (défaut: dernière disponible)",
            },
            "keyword": {
                "type": "string",
                "description": "Mot-clé fr/en si code inconnu — résolution automatique",
            },
        },
        "required": ["indicator"],
    },
    endpoint="/api/worldbank/choropleth",
)

SET_3D_EXTRUSION = tool_doc(
    id="set_3d_extrusion",
    domain="spatial",
    title="Carte 3D par extrusion de polygones",
    description=(
        "Active l'extrusion 3D sur une couche polygone existante. "
        "Extrud les polygones selon un attribut numérique (hauteur, population, etc.). "
        "Nécessite une couche Polygon/MultiPolygon déjà chargée sur la carte. "
        "Bascule la vue en perspective 3D automatiquement."
    ),
    triggers=[
        "3d", "3D", "extrusion", "extruder", "carte 3d", "vue 3d",
        "bâtiments 3d", "hauteur", "volume", "relief", "perspective",
        "représentation 3d", "visualisation 3d", "buildings 3d",
    ],
    examples=[
        {
            "query": "carte 3D bâtiments Paris couche BatiParis hauteur",
            "tool":  "set_3d_extrusion",
            "params": {"layer_name": "BatiParis", "attribute": "HAUTEUR", "scale": 1},
        },
        {
            "query": "extrusion 3D population par pays symboles volumétriques",
            "tool":  "set_3d_extrusion",
            "params": {"layer_name": "Africa", "attribute": "pop_est", "scale": 0.0001},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "layer_name": {"type": "string",
                           "description": "Nom exact de la couche polygone sur la carte"},
            "attribute":  {"type": "string",
                           "description": "Attribut numérique pour la hauteur d'extrusion"},
            "scale":      {"type": "number",
                           "description": "Facteur d'échelle (1 pour hauteur en m, 0.001 pour grandes valeurs)"},
            "color":      {"type": "string",
                           "description": "Couleur hex optionnelle (défaut: couleur de la couche)"},
            "pitch":      {"type": "number",
                           "description": "Inclinaison de la vue en degrés (défaut: 45)"},
        },
        "required": ["layer_name", "attribute"],
    },
    endpoint="client-side",
)

THEMATIC_ANALYSIS = tool_doc(
    id="thematic_analysis",
    domain="spatial",
    title="Analyse thématique (choroplèthe, symboles proportionnels)",
    description=(
        "Visualise des attributs numériques ou catégoriels sur une couche existante. "
        "Opérations : choropleth (carte choroplèthe par valeur numérique ou catégorielle), "
        "proportional_symbols (cercles proportionnels — fonctionne aussi sur polygones via centroïdes turf.js), "
        "categorized (couleurs distinctes par catégorie). "
        "Fonctionne sur toute couche : points, lignes, polygones."
    ),
    triggers=[
        "choroplèthe", "choropleth", "symboles proportionnels", "proportionnel",
        "thématique", "thematique", "carte thématique",
        "colorie par", "colorier", "visualiser", "afficher la population",
        "carte population", "PIB par pays", "revenus", "densité", "distribution",
        "catégoriel", "classifie", "représentation", "légende",
        "evolution de la population", "par pays", "par région",
        "représenter", "visualisation attribut",
    ],
    examples=[
        {
            "query": "carte choroplèthe population Afrique",
            "tool": "thematic_analysis",
            "params": {"operation": "choropleth", "layer_name": "Africa",
                       "attribute": "population", "palette": "viridis", "method": "quantile"},
        },
        {
            "query": "symboles proportionnels PIB par pays couche polygone",
            "tool": "thematic_analysis",
            "params": {"operation": "proportional_symbols", "layer_name": "Africa",
                       "attribute": "gdp_md", "min_size": 3, "max_size": 40},
        },
        {
            "query": "colorie les pays par continent catégorie légende",
            "tool": "thematic_analysis",
            "params": {"operation": "categorized", "layer_name": "Africa",
                       "attribute": "continent", "palette": "categorial"},
        },
        {
            "query": "affiche évolution population carte thématique légende choroplèthe",
            "tool": "thematic_analysis",
            "params": {"operation": "choropleth", "layer_name": "Africa",
                       "attribute": "pop_est", "palette": "blues"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "operation":   {"type": "string",
                            "enum": ["choropleth", "proportional_symbols", "categorized"],
                            "description": "Type de représentation cartographique"},
            "layer_name":  {"type": "string",
                            "description": "Nom exact de la couche dans le contexte carte"},
            "attribute":   {"type": "string",
                            "description": "Nom de l'attribut à visualiser"},
            "method":      {"type": "string",
                            "enum": ["quantile", "jenks", "equal"],
                            "description": "Méthode de classification (défaut: quantile)"},
            "n_classes":   {"type": "integer",
                            "description": "Nombre de classes (défaut: 5)"},
            "palette":     {"type": "string",
                            "enum": ["viridis", "spectral", "blues", "reds", "greens",
                                     "oranges", "categorial"],
                            "description": "Palette de couleurs"},
            "min_size":    {"type": "number",
                            "description": "Taille min symboles proportionnels px (défaut: 3)"},
            "max_size":    {"type": "number",
                            "description": "Taille max symboles proportionnels px (défaut: 40)"},
            "result_name": {"type": "string",
                            "description": "Nom de la couche résultante"},
            "label_attr":  {"type": "string",
                            "description": "Attribut à afficher en étiquette"},
        },
        "required": ["operation", "layer_name", "attribute"],
    },
    endpoint="client-side",
)

SPATIAL_ANALYSIS = tool_doc(
    id="spatial_analysis",
    domain="spatial",
    title="Analyse spatiale (turf.js)",
    description=(
        "Exécute des opérations d'analyse spatiale côté client avec turf.js "
        "sur des couches déjà chargées sur la carte. "
        "Opérations disponibles : "
        "intersection (zone commune), union (fusionner), difference (soustraire), "
        "clip (découper A par B), buffer (zone tampon), "
        "points_in_polygon (compter points dans polygones), "
        "spatial_join (attributs B vers A), nearest (plus proche voisin), "
        "centroid (centroïde), convex_hull (enveloppe convexe), "
        "dissolve (fusionner par attribut), simplify (simplifier géométrie), "
        "voronoi (diagramme de Voronoi), hex_grid (grille hexagonale), "
        "area_perimeter (surfaces et périmètres), clustering (DBSCAN), "
        "distance_matrix (matrice de distances)."
    ),
    triggers=[
        "intersection", "union", "différence", "découper", "clip", "buffer",
        "zone tampon", "points dans", "joindre", "plus proche",
        "centroïde", "enveloppe", "fusionner", "dissoudre", "simplifier",
        "voronoi", "hexagone", "grille", "surface", "périmètre",
        "clustering", "DBSCAN", "regrouper", "matrice de distance",
        "analyse spatiale", "spatial analysis",
        "entre les couches", "combine", "overlay", "merge",
        "dans la zone", "qui intersecte", "à l'intérieur",
        "autour des", "autour de", "rayon mètres", "périmètre tampon",
        "500m", "1km", "distance mètres",
    ],
    examples=[
        {
            "query": "buffer 500m autour des commerces zone tampon",
            "tool": "spatial_analysis",
            "params": {"operation": "buffer", "layer_a_name": "places_commerce",
                       "params": {"radius": 500}, "result_name": "buffer_500m"},
        },
        {
            "query": "buffer zone tampon périmètre distance mètres autour",
            "tool": "spatial_analysis",
            "params": {"operation": "buffer", "layer_a_name": "places_restaurant",
                       "params": {"radius": 500}},
        },
        {
            "query": "Restaurants dans l'isochrone clip découper",
            "tool": "spatial_analysis",
            "params": {"operation": "clip", "layer_a_name": "places_restaurant",
                       "layer_b_name": "Isochrone 10min foot"},
        },
        {
            "query": "intersection union dissolve clustering voronoi analyse spatiale",
            "tool": "spatial_analysis",
            "params": {"operation": "intersection", "layer_a_name": "zone_a",
                       "layer_b_name": "zone_b"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "operation": {
                "type": "string",
                "enum": ["intersection", "union", "difference", "clip", "spatial_join",
                         "points_in_polygon", "buffer", "nearest", "distance_matrix",
                         "centroid", "convex_hull", "dissolve", "simplify",
                         "voronoi", "hex_grid", "area_perimeter", "clustering"],
            },
            "layer_a_name": {"type": "string", "description": "Nom exact de la couche A"},
            "layer_b_name": {"type": "string", "description": "Nom exact de la couche B"},
            "params": {
                "type": "object",
                "description": "{radius: m, attribute: str, maxDistance: km, minPoints: int, cellSide: km}",
            },
            "result_name": {"type": "string"},
        },
        "required": ["operation", "layer_a_name"],
    },
    endpoint="internal/spatial_analysis",
)

GET_LAYER_STATS = tool_doc(
    id="get_layer_stats",
    domain="spatial",
    title="Statistiques d'une couche",
    description=(
        "Retourne des statistiques sur les features d'une couche affichée sur la carte : "
        "nombre de features, types géométriques, attributs numériques (min/max/moyenne), "
        "attributs catégoriels (distribution)."
    ),
    triggers=[
        "statistiques", "stats", "combien", "compter", "résumé",
        "statistics", "count", "summary", "how many", "distribution",
        "analyse", "données", "infos sur la couche",
    ],
    examples=[
        {
            "query": "Combien de restaurants sont affichés ?",
            "tool": "get_layer_stats",
            "params": {"layer_id": "places_restaurant"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "layer_id": {"type": "string", "description": "ID de couche ou 'all'"},
        },
        "required": ["layer_id"],
    },
    endpoint="internal/get_layer_stats",
)



# ── 1. DÉFINITION DU TOOL  WORLD_BANK_INDICATOR ───────────────────
 
WORLD_BANK_INDICATOR = tool_doc(
    id="world_bank_indicator",
    domain="world_data",
    title="Indicateur mondial World Bank",
    description=(
        "Affiche un indicateur World Bank sur une carte choroplèthe mondiale. "
        "Retourne un GeoJSON avec les polygones de tous les pays colorés selon la valeur. "
        "Utiliser pour : population mondiale, PIB par pays, espérance de vie, "
        "émissions CO2, taux de chômage, couverture forestière, mortalité infantile, "
        "taux d'alphabétisation, accès électricité/eau, indice de Gini. "
        "Supporte les requêtes en français et en anglais."
    ),
    triggers=[
        # Population
        "population mondiale", "population par pays", "démographie",
        "densité population", "croissance démographique", "urban population",
        "habitants", "world population",
        # Économie
        "pib", "gdp", "richesse", "économie mondiale", "revenu par habitant",
        "pib par habitant", "croissance économique", "chômage", "inégalités",
        "gini", "unemployment", "pauvreté",
        # Santé
        "espérance de vie", "mortalité infantile", "santé mondiale",
        "life expectancy", "child mortality", "eau potable",
        # Éducation
        "alphabétisation", "éducation mondiale", "literacy",
        # Environnement
        "co2", "émissions carbone", "forêt", "déforestation",
        "électricité", "accès électricité", "terres arables",
        # Génériques
        "indicateur mondial", "world bank", "banque mondiale",
        "carte mondiale", "par pays", "choroplèthe", "monde entier",
        "comparaison pays", "classement mondial", "données mondiales",
    ],
    examples=[
        {
            "query": "population mondiale en 2024",
            "tool": "world_bank_indicator",
            "params": {"indicator": "SP.POP.TOTL", "year": 2023},
        },
        {
            "query": "PIB par habitant dans le monde",
            "tool": "world_bank_indicator",
            "params": {"indicator": "NY.GDP.PCAP.CD"},
        },
        {
            "query": "espérance de vie par pays",
            "tool": "world_bank_indicator",
            "params": {"indicator": "SP.DYN.LE00.IN"},
        },
        {
            "query": "émissions CO2 par habitant dans le monde",
            "tool": "world_bank_indicator",
            "params": {"indicator": "EN.ATM.CO2E.PC"},
        },
        {
            "query": "taux d'alphabétisation mondial",
            "tool": "world_bank_indicator",
            "params": {"indicator": "SE.ADT.LITR.ZS"},
        },
        {
            "query": "accès à l'électricité par pays",
            "tool": "world_bank_indicator",
            "params": {"indicator": "EG.ELC.ACCS.ZS"},
        },
        {
            "query": "couverture forestière mondiale",
            "tool": "world_bank_indicator",
            "params": {"indicator": "AG.LND.FRST.ZS"},
        },
    ],
    schema={
        "type": "object",
        "properties": {
            "indicator": {
                "type": "string",
                "description": (
                    "Code indicateur World Bank. Exemples : "
                    "SP.POP.TOTL (population), NY.GDP.PCAP.CD (PIB/hab), "
                    "SP.DYN.LE00.IN (espérance vie), EN.ATM.CO2E.PC (CO2), "
                    "AG.LND.FRST.ZS (forêt), EG.ELC.ACCS.ZS (électricité), "
                    "SE.ADT.LITR.ZS (alphabétisation), SH.DYN.MORT (mortalité), "
                    "SI.POV.GINI (gini), SL.UEM.TOTL.ZS (chômage), "
                    "SP.URB.TOTL.IN.ZS (urbain%), EN.POP.DNST (densité), "
                    "SH.XPD.CHEX.GD.ZS (santé%PIB), SE.XPD.TOTL.GD.ZS (éduc%PIB), "
                    "NY.GDP.MKTP.CD (PIB total), NY.GDP.MKTP.KD.ZG (croissance PIB), "
                    "SP.POP.GROW (croissance pop), SP.POP.65UP.TO.ZS (65+%), "
                    "SH.STA.WASH.P5 (eau potable), AG.LND.ARBL.ZS (terres arables)"
                ),
            },
            "year": {
                "type": "integer",
                "description": "Année des données (ex: 2022). Omis = dernière année disponible.",
            },
            "keyword": {
                "type": "string",
                "description": "Mot-clé alternatif si le code exact est inconnu (ex: 'mortalité').",
            },
        },
        "required": [],
    },
    endpoint="/api/worldbank/choropleth",
)
 
# ═══════════════════════════════════════════════════════════════
# REGISTRE GLOBAL
# ═══════════════════════════════════════════════════════════════

TOOL_REGISTRY: list[dict] = [
    # geo_data
    GEOCODE,
    QUERY_OVERTURE_PLACES,
    QUERY_OVERTURE_BUILDINGS,
    QUERY_OVERTURE_TRANSPORT,
    QUERY_OVERTURE_DIVISIONS,
    FLY_TO,
    SET_LAYER_STYLE,
    REMOVE_LAYER,
    # satellite
    GEE_IMAGERY,
    GEE_TIMELAPSE,
    # database
    DB_QUERY,
    DB_TABLES,
    # routing
    COMPUTE_ROUTE,
    COMPUTE_ISOCHRONE,
    # world_data
    WORLD_BANK_INDICATOR,
    # spatial
    SPATIAL_ANALYSIS,
    SET_3D_EXTRUSION,
    THEMATIC_ANALYSIS,
    GET_LAYER_STATS,
]

# Index par id pour accès rapide
TOOL_BY_ID: dict[str, dict] = {t["id"]: t for t in TOOL_REGISTRY}

# Index par domaine
TOOLS_BY_DOMAIN: dict[str, list[dict]] = {}
for t in TOOL_REGISTRY:
    TOOLS_BY_DOMAIN.setdefault(t["domain"], []).append(t)

DOMAINS = list(TOOLS_BY_DOMAIN.keys())  # ["geo_data", "satellite", "database", "routing", "spatial"]


def get_tool(tool_id: str) -> dict | None:
    """Retourne un outil par son id."""
    return TOOL_BY_ID.get(tool_id)


def get_tools_by_domain(domain: str) -> list[dict]:
    """Retourne tous les outils d'un domaine."""
    return TOOLS_BY_DOMAIN.get(domain, [])


def get_all_tools() -> list[dict]:
    """Retourne tous les outils."""
    return TOOL_REGISTRY


def get_tool_schema_for_llm(tool_doc: dict) -> dict:
    """
    Convertit un ToolDoc en format JSON Schema pour LiteLLM tool calling.
    Compatible OpenAI function calling format.
    """
    return {
        "type": "function",
        "function": {
            "name": tool_doc["id"],
            "description": tool_doc["description"],
            "parameters": tool_doc["schema"],
        },
    }


def get_schemas_for_llm(tool_ids: list[str]) -> list[dict]:
    """
    Retourne les schemas LiteLLM pour une liste d'ids d'outils.
    Utilisé par les sous-agents après filtrage RAG.
    """
    schemas = []
    for tid in tool_ids:
        t = TOOL_BY_ID.get(tid)
        if t:
            schemas.append(get_tool_schema_for_llm(t))
    return schemas


def search_by_triggers(query: str, top_k: int = 5) -> list[dict]:
    """
    Recherche lexicale simple par triggers (fallback si pgvector indisponible).
    Retourne les top_k outils avec le plus de triggers matchés.
    """
    query_lower = query.lower()
    scores = []
    for t in TOOL_REGISTRY:
        score = sum(1 for trigger in t["triggers"] if trigger.lower() in query_lower)
        if score > 0:
            scores.append((score, t))
    scores.sort(key=lambda x: -x[0])
    return [t for _, t in scores[:top_k]]


if __name__ == "__main__":
    print(f"✓ {len(TOOL_REGISTRY)} outils chargés")
    print(f"✓ Domaines : {DOMAINS}")
    for domain, tools in TOOLS_BY_DOMAIN.items():
        print(f"  {domain}: {[t['id'] for t in tools]}")

    # Test fallback lexical
    print("\n--- Test recherche lexicale ---")
    results = search_by_triggers("NDVI Dakar satellite")
    print(f"Query: 'NDVI Dakar satellite' → {[r['id'] for r in results]}")

    results = search_by_triggers("restaurants Nantes")
    print(f"Query: 'restaurants Nantes' → {[r['id'] for r in results]}")

    results = search_by_triggers("itinéraire vélo gare")
    print(f"Query: 'itinéraire vélo gare' → {[r['id'] for r in results]}")
