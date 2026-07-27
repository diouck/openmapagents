"""
elevation_routes.py — Profil altimétrique FastAPI
Sources : IGN RGE Alti (France, 1m) ou SRTM via GEE (monde, 30m)
Détecte automatiquement si la ligne est en France → IGN, sinon → SRTM.

Intégration dans agent.py :
    from elevation_routes import router as elev_router
    app.include_router(elev_router)
"""

import math
import logging
from typing import List, Optional
log = logging.getLogger("elevation")
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/elevation", tags=["elevation"])

# ── Modèles ───────────────────────────────────────────────────────────────────
class CrossFeature(BaseModel):
    id:       str
    name:     str
    color:    str
    features: list

class ProfileRequest(BaseModel):
    coordinates:  List[List[float]]   # [[lng, lat], ...]
    n_samples:    int                 = 300   # points d'échantillonnage
    cross_layers: List[CrossFeature]  = []
    with_canopy:  bool                = False  # extraire canopée GEE
    with_buildings: bool              = False  # extraire bâti OSM

# ── Bbox France métropolitaine + DOM ─────────────────────────────────────────
FRANCE_BBOX = (-5.5, 41.0, 9.7, 51.5)

def is_in_france(coords: list) -> bool:
    """Vérifie si la majorité des points est dans la bbox France."""
    in_fr = sum(
        1 for lng, lat in coords
        if FRANCE_BBOX[0] <= lng <= FRANCE_BBOX[2]
        and FRANCE_BBOX[1] <= lat <= FRANCE_BBOX[3]
    )
    return in_fr > len(coords) * 0.5


# ── Interpolation de points le long d'une ligne ───────────────────────────────
def haversine_km(p1, p2) -> float:
    lon1, lat1 = math.radians(p1[0]), math.radians(p1[1])
    lon2, lat2 = math.radians(p2[0]), math.radians(p2[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return 6371 * 2 * math.asin(math.sqrt(a))


def sample_line(coords: list, n: int) -> list:
    """Rééchantillonne une polyligne en n points équidistants."""
    if len(coords) < 2:
        return coords

    # Calculer la longueur totale et les distances cumulées
    cum = [0.0]
    for i in range(1, len(coords)):
        cum.append(cum[-1] + haversine_km(coords[i-1], coords[i]))
    total = cum[-1]
    if total == 0:
        return coords

    sampled = []
    for j in range(n):
        target = (j / (n - 1)) * total
        # Trouver le segment contenant ce point
        for k in range(len(cum) - 1):
            if cum[k] <= target <= cum[k+1]:
                seg_len = cum[k+1] - cum[k]
                t = (target - cum[k]) / seg_len if seg_len > 0 else 0
                lng = coords[k][0] + t * (coords[k+1][0] - coords[k][0])
                lat = coords[k][1] + t * (coords[k+1][1] - coords[k][1])
                sampled.append([round(lng, 7), round(lat, 7), round(target, 4)])
                break
    return sampled


# ── Récupération élévation IGN RGE Alti ──────────────────────────────────────
def get_elevation_ign(sampled: list) -> list:
    """
    Interroge l'API IGN Géoportail altimétrie par batch de 200 points.
    Retourne une liste d'altitudes dans le même ordre.
    """
    import requests

    alts = []
    batch_size = 200

    for i in range(0, len(sampled), batch_size):
        batch = sampled[i:i + batch_size]
        # Format IGN : lon|lat séparés par |, plusieurs points séparés par |
        # endpoint : https://data.geopf.fr/altimetrie/1.0/calcul/alti/rest/elevationLine.json
        lons = [str(p[0]) for p in batch]
        lats = [str(p[1]) for p in batch]

        params = {
            "lon":    "|".join(lons),
            "lat":    "|".join(lats),
            "resource": "ign_rge_alti_wld",
            "delimiter": "|",
            "indent": "false",
            "measures": "false",
            "zonly": "true",
        }

        try:
            r = requests.get(
                "https://data.geopf.fr/altimetrie/1.0/calcul/alti/rest/elevationLine.json",
                params=params,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            elevs = data.get("elevations", [])
            for e in elevs:
                z = e.get("z", -99999)
                alts.append(None if z <= -99000 else round(float(z), 1))
        except Exception as ex:
            print(f"IGN elevation error: {ex}")
            alts.extend([None] * len(batch))

    return alts


# ── Récupération élévation SRTM via GEE ──────────────────────────────────────
def get_elevation_srtm(sampled: list) -> list:
    """Interroge le MNT SRTM via GEE pour une liste de points."""
    try:
        import ee
        try:
            ee.data.getAsset("projects/earthengine-public/assets/s2cloudless/2020")
        except Exception:
            credentials = ee.ServiceAccountCredentials(
                email='mcpopenmapagents@laravelauth-477918.iam.gserviceaccount.com',
                key_file='/var/www/google/laravelauth-477918-9f353bf03d0b.json',
            )
            ee.Initialize(credentials)

        srtm = ee.Image("USGS/SRTMGL1_003")
        
        # Construire une FeatureCollection de points
        features = [
            ee.Feature(ee.Geometry.Point([p[0], p[1]]), {"idx": i})
            for i, p in enumerate(sampled)
        ]
        fc = ee.FeatureCollection(features)

        # Extraire les altitudes
        sampled_fc = srtm.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.first(),
            scale=30,
        )

        result = sampled_fc.getInfo()
        alts_map = {}
        for feat in result["features"]:
            idx = feat["properties"].get("idx")
            z   = feat["properties"].get("first", None)
            if idx is not None:
                alts_map[idx] = round(float(z), 1) if z is not None else None

        return [alts_map.get(i) for i in range(len(sampled))]

    except Exception as ex:
        raise HTTPException(500, f"Erreur SRTM GEE : {ex}")


# ── Lissage des altitudes (moyenne mobile) ────────────────────────────────────
def smooth(alts: list, w: int = 3) -> list:
    result = []
    for i in range(len(alts)):
        vals = [alts[j] for j in range(max(0, i-w), min(len(alts), i+w+1)) if alts[j] is not None]
        result.append(round(sum(vals) / len(vals), 1) if vals else alts[i])
    return result


# ── Calcul des pentes ─────────────────────────────────────────────────────────
def compute_slopes(sampled_with_alt: list) -> list:
    """
    sampled_with_alt : liste de [lng, lat, dist_km, alt]
    Retourne liste de pentes en % (0 pour le premier point).
    """
    slopes = [0.0]
    for i in range(1, len(sampled_with_alt)):
        da = sampled_with_alt[i][3] - sampled_with_alt[i-1][3]   # delta altitude m
        dd = (sampled_with_alt[i][2] - sampled_with_alt[i-1][2]) * 1000  # delta dist m
        slope = round((da / dd * 100) if dd > 0 else 0, 2)
        slopes.append(slope)
    return slopes


# ── Détection pics / creux ────────────────────────────────────────────────────
def detect_peaks(alts: list, min_prominence: float = 5.0) -> list:
    """Retourne une liste de booléens (isPeak, isValley)."""
    n = len(alts)
    is_peak   = [False] * n
    is_valley = [False] * n
    w = max(3, n // 20)  # fenêtre adaptative

    for i in range(w, n - w):
        window = alts[i-w:i+w+1]
        if alts[i] == max(window) and alts[i] - min(window) >= min_prominence:
            is_peak[i] = True
        elif alts[i] == min(window) and max(window) - alts[i] >= min_prominence:
            is_valley[i] = True

    return is_peak, is_valley


# ── Croisement couches polygones ──────────────────────────────────────────────
def compute_cross_segments(sampled: list, cross_layers: list) -> list:
    """
    Pour chaque couche polygone, trouve les intervalles [dist_start, dist_end]
    où la ligne traverse les polygones.
    """
    try:
        from shapely.geometry import Point, shape
    except ImportError:
        return []

    result = []
    for cl in cross_layers:
        polygons = []
        for feat in cl.features:
            geom = feat.get("geometry")
            if geom and geom.get("type") in ("Polygon", "MultiPolygon"):
                try:
                    polygons.append(shape(geom))
                except Exception:
                    pass
        if not polygons:
            continue

        # Pour chaque point échantillonné, tester l'appartenance
        in_poly = []
        for pt in sampled:
            p = Point(pt[0], pt[1])
            in_poly.append(any(poly.contains(p) for poly in polygons))

        # Extraire les segments continus dans un polygone
        segments = []
        seg_start = None
        for i, inside in enumerate(in_poly):
            if inside and seg_start is None:
                seg_start = sampled[i][2]  # dist_km
            elif not inside and seg_start is not None:
                segments.append({"start": seg_start, "end": sampled[i-1][2]})
                seg_start = None
        if seg_start is not None:
            segments.append({"start": seg_start, "end": sampled[-1][2]})

        if segments:
            result.append({
                "label":    cl.name,
                "color":    cl.color,
                "segments": segments,
            })

    return result


# ── Construction GeoJSON couche colorée par pente ─────────────────────────────
def build_slope_geojson(sampled_with_alt: list, slopes: list) -> dict:
    """
    Construit un GeoJSON LineString multi-features, une feature par segment,
    avec la couleur de pente en propriété.
    """
    PENTE_COLORS = [
        (3,   "#1D9E75"),
        (5,   "#EF9F27"),
        (8,   "#D85A30"),
        (999, "#E24B4A"),
    ]

    def slope_color(s):
        for thresh, col in PENTE_COLORS:
            if abs(s) <= thresh:
                return col
        return "#E24B4A"

    features = []
    for i in range(1, len(sampled_with_alt)):
        p1 = sampled_with_alt[i-1]
        p2 = sampled_with_alt[i]
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[p1[0], p1[1]], [p2[0], p2[1]]],
            },
            "properties": {
                "slope_pct":   slopes[i],
                "color":       slope_color(slopes[i]),
                "alt_start":   p1[3],
                "alt_end":     p2[3],
                "dist_km":     p2[2],
            },
        })

    return {"type": "FeatureCollection", "features": features}


# ── Canopée GEE (WRI/Meta High Resolution Canopy Height) ────────────────────
def get_canopy_gee(sampled: list) -> list:
    """
    Extrait la hauteur de canopée depuis WRI/Meta High Resolution Canopy Height 2020.
    Dataset GEE : projects/meta-forest-monitoring-okw37/assets/CanopyHeight
    Résolution : ~1m. Retourne liste de hauteurs en mètres.
    """
    try:
        from gee_auth import get_ee
        ee = get_ee()

        # WRI/Meta High Resolution Canopy Height 2020 (~1m)
        # Bande "cover_code" = hauteur canopée en mètres
        # Essayer plusieurs datasets canopée
        try:
            # ETH Global Canopy Height 2020 (Lang et al.) — bande "b1" en mètres
            canopy = ee.Image("users/nlang/ETH_GlobalCanopyHeight_2020_10m_v1")
        except Exception:
            # Fallback: WRI/Meta
            canopy = ee.Image("projects/meta-forest-monitoring-okw37/assets/CanopyHeight")

        features = [
            ee.Feature(ee.Geometry.Point([p[0], p[1]]), {"idx": i})
            for i, p in enumerate(sampled)
        ]
        fc = ee.FeatureCollection(features)

        sampled_fc = canopy.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.first(),
            scale=10,   # 10m pour vitesse (1m trop lent sur profil)
        )

        result  = sampled_fc.getInfo()
        alt_map = {}
        for feat in result.get("features", []):
            idx = feat["properties"].get("idx")
            # La bande peut s'appeler "first" ou "cover_code" selon la version
            z = (feat["properties"].get("first") or
                 feat["properties"].get("cover_code") or
                 feat["properties"].get("b1"))
            if idx is not None:
                alt_map[int(idx)] = round(float(z), 1) if z is not None else 0.0

        vals = [alt_map.get(i, 0.0) for i in range(len(sampled))]
        log.info(f"Canopee GEE: {sum(1 for v in vals if v>0)} points avec valeur")
        return vals

    except Exception as ex:
        import logging
        logging.getLogger("elevation").warning(f"Canopy GEE error: {ex}")
        return [0.0] * len(sampled)


# ── Bâti OSM (hauteur via Overture Maps DuckDB) ───────────────────────────────
def get_buildings_profile(sampled: list, buffer_m: float = 30.0) -> list:
    """
    Hauteur des bâtiments le long du profil via Overpass API (OSM).
    Plus fiable que DuckDB S3 pour les petites zones.
    """
    import requests

    lngs = [p[0] for p in sampled]
    lats = [p[1] for p in sampled]
    buf  = buffer_m / 111000
    s, w = min(lats)-buf, min(lngs)-buf
    n, e = max(lats)+buf, max(lngs)+buf

    # Requête Overpass — bâtiments avec height ou building:levels
    overpass_query = f"""
[out:json][timeout:25];
(
  way["building"]["height"]({s},{w},{n},{e});
  way["building"]["building:levels"]({s},{w},{n},{e});
  relation["building"]["height"]({s},{w},{n},{e});
);
out center tags;
"""
    try:
        r = requests.post(
            "https://overpass-api.de/api/interpreter",
            data={"data": overpass_query},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        elements = data.get("elements", [])

        if not elements:
            log.info("Buildings OSM: aucun bâtiment avec hauteur dans la zone")
            return [0.0] * len(sampled)

        log.info(f"Buildings OSM: {len(elements)} bâtiments trouvés")

        # Extraire centroïde + hauteur de chaque bâtiment
        buildings = []
        for el in elements:
            center = el.get("center") or {}
            blng = center.get("lon") or el.get("lon")
            blat = center.get("lat") or el.get("lat")
            if not blng or not blat:
                continue
            tags = el.get("tags", {})
            # Hauteur en mètres
            h = 0.0
            if "height" in tags:
                try: h = float(str(tags["height"]).replace("m","").strip())
                except: pass
            elif "building:levels" in tags:
                try: h = float(tags["building:levels"]) * 3.0
                except: pass
            if h > 0:
                buildings.append((float(blng), float(blat), h))

        if not buildings:
            return [0.0] * len(sampled)

        # Pour chaque point du profil → hauteur max dans le buffer
        buf_deg = buffer_m / 111000
        result  = []
        for p in sampled:
            max_h = 0.0
            for blng, blat, bh in buildings:
                if abs(blng - p[0]) < buf_deg and abs(blat - p[1]) < buf_deg:
                    max_h = max(max_h, bh)
            result.append(round(max_h, 1))
        return result

    except Exception as ex:
        log.warning(f"Buildings OSM Overpass error: {ex}")
        return [0.0] * len(sampled)


# ── Route principale ──────────────────────────────────────────────────────────
@router.post("/profile")
async def elevation_profile(req: ProfileRequest):
    """
    Calcule le profil altimétrique d'une polyligne.
    Retourne les points avec altitude, pente, pics/creux, et segments de croisement.
    """
    if len(req.coordinates) < 2:
        raise HTTPException(400, "La ligne doit avoir au moins 2 points")

    # ── Échantillonnage ────────────────────────────────────────
    n = min(max(50, req.n_samples), 500)
    sampled = sample_line(req.coordinates, n)  # [[lng, lat, dist_km], ...]

    # ── Source d'élévation ─────────────────────────────────────
    use_ign = is_in_france([[p[0], p[1]] for p in sampled])
    source  = "IGN" if use_ign else "SRTM"

    if use_ign:
        alts_raw = get_elevation_ign(sampled)
    else:
        alts_raw = get_elevation_srtm(sampled)

    # Remplacer les None par interpolation linéaire
    for i in range(len(alts_raw)):
        if alts_raw[i] is None:
            # Chercher voisins valides
            left  = next((alts_raw[j] for j in range(i-1, -1, -1) if alts_raw[j] is not None), None)
            right = next((alts_raw[j] for j in range(i+1, len(alts_raw)) if alts_raw[j] is not None), None)
            if left is not None and right is not None:
                alts_raw[i] = round((left + right) / 2, 1)
            elif left is not None:
                alts_raw[i] = left
            elif right is not None:
                alts_raw[i] = right
            else:
                alts_raw[i] = 0.0

    # Lissage
    alts = smooth(alts_raw, w=2)

    # Ajouter altitude à sampled
    sampled_with_alt = [[s[0], s[1], s[2], alts[i]] for i, s in enumerate(sampled)]

    # ── Pentes ────────────────────────────────────────────────
    slopes = compute_slopes(sampled_with_alt)

    # ── Pics / creux ──────────────────────────────────────────
    is_peak, is_valley = detect_peaks(alts)

    # ── Croisement couches ────────────────────────────────────
    cross_segs = compute_cross_segments(sampled, req.cross_layers)

    # ── GeoJSON pente ─────────────────────────────────────────
    slope_gj = build_slope_geojson(sampled_with_alt, slopes)

    # ── Réponse ───────────────────────────────────────────────
    points_out = [
        {
            "dist":      s[2],
            "alt":       s[3],
            "slope":     slopes[i],
            "lng":       s[0],
            "lat":       s[1],
            "isPeak":    is_peak[i],
            "isValley":  is_valley[i],
        }
        for i, s in enumerate(sampled_with_alt)
    ]

    # ── Canopée GEE (optionnel) ──────────────────────────────
    canopy_alts = None
    if req.with_canopy:
        try:
            raw_canopy  = get_canopy_gee([[p[0], p[1]] for p in sampled])
            canopy_alts = smooth(raw_canopy, w=3)
        except Exception as ex:
            import logging
            logging.getLogger("elevation").warning(f"Canopy skip: {ex}")

    # ── Bâti OSM (optionnel) ──────────────────────────────
    building_heights = None
    if req.with_buildings:
        try:
            building_heights = get_buildings_profile([[p[0], p[1]] for p in sampled])
        except Exception as ex:
            import logging
            logging.getLogger("elevation").warning(f"Buildings skip: {ex}")

    # Enrichir points_out avec canopée et bâti
    for i, pt in enumerate(points_out):
        pt["canopy"]    = canopy_alts[i]    if canopy_alts    else None
        pt["building"]  = building_heights[i] if building_heights else None
        # Altitude totale = terrain + canopée (sommet des arbres)
        if canopy_alts and canopy_alts[i] is not None:
            pt["alt_top"] = round(pt["alt"] + canopy_alts[i], 1)
        else:
            pt["alt_top"] = None

    return {
        "points":           points_out,
        "source":           source,
        "cross_segments":   cross_segs,
        "slope_geojson":    slope_gj,
        "n_points":         len(points_out),
        "total_km":         sampled_with_alt[-1][2],
        "has_canopy":       canopy_alts is not None and any(v > 0 for v in canopy_alts),
        "has_buildings":    building_heights is not None and any(v > 0 for v in building_heights),
    }
