"""
worldbank/fetcher.py — Appel API World Bank + cache RAM + jointure GeoJSON pays
"""

import json
import logging
import time
from typing import Optional
from urllib.request import urlopen
from urllib.error import URLError

log = logging.getLogger("worldbank.fetcher")

# ── URLs ──────────────────────────────────────────────────────────────────────
WB_API = "https://api.worldbank.org/v2/country/all/indicator/{code}?format=json&date={year}&per_page=300&mrv=1"
GEOJSON_URL = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"

# ── Cache RAM simple (pas de Redis, pas de fichier) ───────────────────────────
_wb_cache:      dict[str, tuple[float, list]] = {}   # key → (timestamp, data)
_geojson_cache: Optional[dict] = None                # chargé une seule fois
CACHE_TTL = 3600 * 24  # 24h pour World Bank (données annuelles)


# ═══════════════════════════════════════════════════════════════════════════════
# WORLD BANK DATA
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_indicator(code: str, year: int) -> dict[str, float]:
    """
    Récupère les valeurs d'un indicateur pour tous les pays.
    Retourne {iso3: value} — ex: {"FRA": 67970000, "USA": 331000000, ...}
    Résultat mis en cache RAM 24h.
    """
    cache_key = f"{code}_{year}"
    now = time.time()

    # Vérifier cache
    if cache_key in _wb_cache:
        ts, data = _wb_cache[cache_key]
        if now - ts < CACHE_TTL:
            log.debug(f"Cache hit: {cache_key}")
            return data

    url = WB_API.format(code=code, year=year)
    log.info(f"World Bank API: {url}")

    try:
        with urlopen(url, timeout=10) as r:
            raw = json.loads(r.read())
    except (URLError, Exception) as e:
        log.error(f"World Bank API error: {e}")
        return {}

    # Format World Bank: [metadata, [records]]
    if not isinstance(raw, list) or len(raw) < 2:
        return {}

    records = raw[1] or []
    result: dict[str, float] = {}

    # Codes WB non-standard → ISO3 correct
    WB_NORMALIZE = {
        "ROM": "ROU", "ZAR": "COD", "TMP": "TLS", "WBG": "PSE",
        "KSV": "XKX", "ADO": "AND", "YUG": "SRB", "SCG": "SRB",
    }
    # Agrégats régionaux WB à exclure (pas des pays)
    WB_AGGREGATES = {
        "AFE","AFW","ARB","CEB","CSS","EAP","EAR","EAS","ECA","EMU",
        "EUU","FCS","HIC","HPC","IBD","IBT","IDA","IDB","IDX","LAC",
        "LCN","LDC","LIC","LMC","LMY","LTE","MEA","MIC","MNA","NAC",
        "OED","OSS","PRE","PSS","PST","SAS","SSA","SSF","SST","TEA",
        "TEC","TLA","TMN","TSA","TSS","UMC","WLD","XZN",
    }

    for rec in records:
        iso3 = (rec.get("countryiso3code") or "").upper().strip()
        value = rec.get("value")
        # Skipper les enregistrements sans valeur numérique
        if not iso3 or value is None or value == "":
            continue
        if iso3 in WB_AGGREGATES:
            continue
        iso3 = WB_NORMALIZE.get(iso3, iso3)
        try:
            fval = float(value)
            # Valeur doit être un nombre réel (pas NaN/Inf)
            if fval == fval and abs(fval) < 1e15:
                result[iso3] = fval
        except (TypeError, ValueError):
            pass

    _wb_cache[cache_key] = (now, result)
    log.info(f"World Bank: {len(result)} pays (hors agrégats) pour {code} ({year})")
    return result


def fetch_latest_year(code: str, max_year: int = 2023) -> tuple[int, dict[str, float]]:
    """
    Cherche la dernière année disponible (max_year → max_year-3).
    Retourne (year, {iso3: value}).
    """
    for year in range(max_year, max_year - 4, -1):
        data = fetch_indicator(code, year)
        if len(data) > 50:  # Au moins 50 pays → données suffisantes
            return year, data
    return max_year, {}


# ═══════════════════════════════════════════════════════════════════════════════
# GEOJSON PAYS (cache RAM permanent)
# ═══════════════════════════════════════════════════════════════════════════════

def load_countries_geojson() -> dict:
    """
    Charge le GeoJSON des pays depuis CDN (une seule fois en RAM).
    ~500kb, polygones simplifiés, ISO_A3 disponible.
    """
    global _geojson_cache
    if _geojson_cache is not None:
        return _geojson_cache

    log.info(f"Chargement GeoJSON pays: {GEOJSON_URL}")
    try:
        with urlopen(GEOJSON_URL, timeout=15) as r:
            _geojson_cache = json.loads(r.read())
        log.info(f"GeoJSON pays chargé: {len(_geojson_cache.get('features', []))} pays")
    except Exception as e:
        log.error(f"Impossible de charger le GeoJSON pays: {e}")
        _geojson_cache = {"type": "FeatureCollection", "features": []}

    return _geojson_cache


# ═══════════════════════════════════════════════════════════════════════════════
# JOINTURE : données WB + géométries pays
# ═══════════════════════════════════════════════════════════════════════════════

def build_choropleth(
    code: str,
    year: int,
    label: str,
    unit: str,
) -> dict:
    """
    Joint les données World Bank avec les géométries pays.
    Retourne un GeoJSON FeatureCollection enrichi, prêt pour la carte.

    Chaque feature a :
      - geometry : polygone du pays
      - properties.value : valeur de l'indicateur
      - properties.label : label fr de l'indicateur
      - properties.country : nom du pays
      - properties.iso3 : code ISO3
      - properties.year : année des données
    """
    wb_data = fetch_indicator(code, year)
    countries = load_countries_geojson()

    if not wb_data:
        return {"type": "FeatureCollection", "features": [], "metadata": {"error": "No data from World Bank"}}

    features = []
    matched = 0

    for feat in countries.get("features", []):
        props = feat.get("properties", {})

        # Chercher l'ISO3 — ce GeoJSON utilise "ISO3166-1-Alpha-3"
        iso3 = (
            props.get("ISO3166-1-Alpha-3") or   # champ exact du CDN geo-countries
            props.get("ISO_A3") or
            props.get("ADM0_A3") or
            props.get("iso_a3") or
            props.get("ISO3") or
            ""
        ).upper().strip()

        # Ignorer codes invalides
        if iso3 in ("-99", "N/A", "NA", ""):
            iso3 = ""

        value = wb_data.get(iso3) if iso3 else None

        new_props = {
            "country":  props.get("ADMIN") or props.get("name") or props.get("NAME") or iso3,
            "iso3":     iso3,
            "year":     year,
            "indicator": code,
            "label":    label,
            "unit":     unit,
            "value":    value,  # None si pas de données
        }

        if value is not None:
            matched += 1
            # N'ajouter que les pays avec données
            features.append({
                "type":       "Feature",
                "geometry":   feat.get("geometry"),
                "properties": new_props,
            })

    log.info(f"Matching: {matched} pays avec données sur {len(wb_data)} reçus de l'API")

    if matched == 0:
        return {
            "type": "FeatureCollection", "features": [],
            "metadata": {
                "error": "no_match",
                "message": f"Aucune correspondance géographique pour {code} ({year}). Vérifiez les codes ISO3.",
                "indicator": code, "matched": 0, "theme": "world_data",
            }
        }

    # Stats pour la légende
    values = [f["properties"]["value"] for f in features if f["properties"]["value"] is not None]
    stats = {}
    if values:
        stats = {
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / len(values),
            "count": len(values),
        }

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "indicator": code,
            "label": label,
            "unit": unit,
            "year": year,
            "matched": matched,
            "total": len(features),
            "stats": stats,
            "theme": "world_data",
        },
    }
