"""
osm/routes.py — Import de données OpenStreetMap
Permet d'importer des POI, lignes et polygones OSM
via l'API Overpass, à partir d'une emprise bbox ou d'une couche existante.
"""
import json
import logging
import httpx
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from auth.routes import current_user

log = logging.getLogger("osm")
router = APIRouter(prefix="/api/osm", tags=["osm"])

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# ── Catégories OSM prédéfinies ────────────────────────────────
OSM_PRESETS = {
    # POI
    "amenity_restaurant":    {"type": "poi",     "label": "Restaurants",       "query": 'node["amenity"="restaurant"]'},
    "amenity_cafe":          {"type": "poi",     "label": "Cafés",             "query": 'node["amenity"="cafe"]'},
    "amenity_school":        {"type": "poi",     "label": "Écoles",            "query": 'node["amenity"="school"]'},
    "amenity_hospital":      {"type": "poi",     "label": "Hôpitaux",          "query": 'node["amenity"="hospital"]'},
    "amenity_pharmacy":      {"type": "poi",     "label": "Pharmacies",        "query": 'node["amenity"="pharmacy"]'},
    "amenity_parking":       {"type": "poi",     "label": "Parkings",          "query": 'node["amenity"="parking"]'},
    "amenity_bank":          {"type": "poi",     "label": "Banques",           "query": 'node["amenity"="bank"]'},
    "amenity_fuel":          {"type": "poi",     "label": "Stations essence",  "query": 'node["amenity"="fuel"]'},
    "shop_supermarket":      {"type": "poi",     "label": "Supermarchés",      "query": 'node["shop"="supermarket"]'},
    "tourism_hotel":         {"type": "poi",     "label": "Hôtels",            "query": 'node["tourism"="hotel"]'},
    "tourism_attraction":    {"type": "poi",     "label": "Attractions",       "query": 'node["tourism"="attraction"]'},
    "natural_tree":          {"type": "poi",     "label": "Arbres",            "query": 'node["natural"="tree"]'},
    # Lignes
    "highway_primary":       {"type": "line",    "label": "Routes principales","query": 'way["highway"~"primary|secondary"]'},
    "highway_cycleway":      {"type": "line",    "label": "Pistes cyclables",  "query": 'way["highway"="cycleway"]'},
    "railway_rail":          {"type": "line",    "label": "Voies ferrées",     "query": 'way["railway"="rail"]'},
    "waterway_river":        {"type": "line",    "label": "Rivières",          "query": 'way["waterway"~"river|stream"]'},
    # Polygones
    "landuse_forest":        {"type": "polygon", "label": "Forêts",            "query": 'way["landuse"="forest"]'},
    "landuse_residential":   {"type": "polygon", "label": "Zones résidentielles","query": 'way["landuse"="residential"]'},
    "natural_water":         {"type": "polygon", "label": "Plans d\'eau",      "query": 'way["natural"="water"]'},
    "building":              {"type": "polygon", "label": "Bâtiments",         "query": 'way["building"]'},
    "leisure_park":          {"type": "polygon", "label": "Parcs",             "query": 'way["leisure"="park"]'},
}

# ── Schémas ───────────────────────────────────────────────────
class BboxModel(BaseModel):
    south: float = Field(..., ge=-90,  le=90)
    west:  float = Field(..., ge=-180, le=180)
    north: float = Field(..., ge=-90,  le=90)
    east:  float = Field(..., ge=-180, le=180)

class OsmImportRequest(BaseModel):
    bbox:       BboxModel
    presets:    List[str]          = []   # clés dans OSM_PRESETS
    custom_tag: Optional[str]      = None # ex: "amenity=bar" libre
    geom_types: List[str]          = ["poi", "line", "polygon"]
    layer_name: Optional[str]      = None # nom de la couche résultante
    max_results: int               = Field(500, ge=1, le=5000)

# ── Helpers ───────────────────────────────────────────────────
def _build_overpass_query(bbox: BboxModel, queries: List[str], max_results: int) -> str:
    b = f"{bbox.south},{bbox.west},{bbox.north},{bbox.east}"
    parts = []
    for q in queries:
        # nœuds, ways et relations pour chaque filtre
        parts.append(f"{q}({b});")
        parts.append(f"relation{q[q.index('['):]}({b});")
    union = "\n  ".join(parts)
    return f"""[out:json][timeout:60][maxsize:{max_results * 2000}];
(
  {union}
);
out body {max_results};
>;
out skel qt;"""

def _elements_to_geojson(elements: list) -> dict:
    """Convertit les éléments Overpass en GeoJSON FeatureCollection."""
    nodes = {e["id"]: e for e in elements if e["type"] == "node"}
    features = []

    for el in elements:
        tags = el.get("tags", {})
        if not tags:
            continue

        props = {**tags, "_osm_id": el["id"], "_osm_type": el["type"]}

        if el["type"] == "node" and "lat" in el:
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [el["lon"], el["lat"]]},
                "properties": props,
            })

        elif el["type"] == "way" and "nodes" in el:
            coords = [
                [nodes[nid]["lon"], nodes[nid]["lat"]]
                for nid in el["nodes"]
                if nid in nodes
            ]
            if len(coords) < 2:
                continue
            closed = coords[0] == coords[-1] and len(coords) >= 4
            if closed:
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Polygon", "coordinates": [coords]},
                    "properties": props,
                })
            else:
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": coords},
                    "properties": props,
                })

    return {"type": "FeatureCollection", "features": features}

# ── Routes ────────────────────────────────────────────────────

@router.get("/presets")
def list_presets():
    """Retourne les catégories OSM disponibles groupées par type."""
    grouped = {"poi": [], "line": [], "polygon": []}
    for key, meta in OSM_PRESETS.items():
        grouped[meta["type"]].append({"key": key, "label": meta["label"]})
    return grouped


@router.post("/import")
async def import_osm(body: OsmImportRequest, user=Depends(current_user)):
    """
    Interroge l'API Overpass et retourne un GeoJSON.
    - body.presets : liste de clés prédéfinies
    - body.custom_tag : filtre libre "key=value"
    - body.bbox : emprise géographique
    """
    if not body.presets and not body.custom_tag:
        raise HTTPException(400, "Spécifiez au moins un preset ou un tag personnalisé.")

    # Construire la liste de filtres Overpass
    overpass_queries: List[str] = []

    for key in body.presets:
        if key not in OSM_PRESETS:
            raise HTTPException(400, f"Preset inconnu : {key}")
        preset = OSM_PRESETS[key]
        if preset["type"] in body.geom_types or (
            preset["type"] == "poi"     and "poi"     in body.geom_types or
            preset["type"] == "line"    and "line"    in body.geom_types or
            preset["type"] == "polygon" and "polygon" in body.geom_types
        ):
            overpass_queries.append(preset["query"])

    if body.custom_tag:
        # "amenity=bar" → node["amenity"="bar"]
        parts = body.custom_tag.split("=", 1)
        if len(parts) == 2:
            k, v = parts
            filter_str = f'["{ k.strip() }"="{ v.strip() }"]'
        else:
            filter_str = f'["{body.custom_tag.strip()}"]'
        overpass_queries.append(f"node{filter_str}")
        overpass_queries.append(f"way{filter_str}")

    if not overpass_queries:
        raise HTTPException(400, "Aucun filtre valide après sélection des types de géométrie.")

    oql = _build_overpass_query(body.bbox, overpass_queries, body.max_results)
    log.info(f"Requête Overpass pour {user.email} | bbox={body.bbox} | presets={body.presets}")

    try:
        async with httpx.AsyncClient(timeout=65) as client:
            resp = await client.post(OVERPASS_URL, data={"data": oql})
        if resp.status_code != 200:
            raise HTTPException(502, f"Overpass API erreur {resp.status_code}")
        data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(504, "Timeout : l'emprise est trop grande ou le serveur Overpass est surchargé.")
    except Exception as e:
        log.error(f"Erreur Overpass : {e}")
        raise HTTPException(502, "Impossible de joindre l'API Overpass.")

    elements = data.get("elements", [])
    geojson  = _elements_to_geojson(elements)

    counts = {"Point": 0, "LineString": 0, "Polygon": 0}
    for f in geojson["features"]:
        t = f["geometry"]["type"]
        counts[t] = counts.get(t, 0) + 1

    return {
        "geojson":    geojson,
        "layer_name": body.layer_name or "Données OSM",
        "stats": {
            "total":    len(geojson["features"]),
            "poi":      counts.get("Point",      0),
            "lines":    counts.get("LineString",  0),
            "polygons": counts.get("Polygon",     0),
        },
    }


@router.post("/bbox-from-layer")
def bbox_from_layer(geojson_str: str, user=Depends(current_user)):
    """
    Calcule la bbox d'une couche GeoJSON existante.
    Utile pour pré-remplir l'emprise depuis une couche de la carte.
    """
    try:
        fc = json.loads(geojson_str)
    except Exception:
        raise HTTPException(400, "GeoJSON invalide")

    coords_flat = []
    for feat in fc.get("features", []):
        geom = feat.get("geometry", {})
        gtype = geom.get("type")
        c = geom.get("coordinates", [])
        if gtype == "Point":
            coords_flat.append(c)
        elif gtype in ("LineString", "MultiPoint"):
            coords_flat.extend(c)
        elif gtype in ("Polygon", "MultiLineString"):
            for ring in c:
                coords_flat.extend(ring)
        elif gtype == "MultiPolygon":
            for poly in c:
                for ring in poly:
                    coords_flat.extend(ring)

    if not coords_flat:
        raise HTTPException(400, "Aucune coordonnée trouvée dans la couche.")

    lons = [p[0] for p in coords_flat]
    lats = [p[1] for p in coords_flat]
    return {
        "south": min(lats), "west": min(lons),
        "north": max(lats), "east": max(lons),
    }
