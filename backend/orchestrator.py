"""
orchestrator.py — Orchestrateur principal OpenMapAgents v2
==========================================================
Interface compatible avec agent.py existant :
    from orchestrator import get_orchestrator, orchestrate
    return orchestrate(req.messages, req.map_context)

Pipeline complet en 10 étapes :
  1.  Mémoire session  → contexte bbox/layers/dates
  2.  Router          → domaine (gee|routing|worldbank|overture|osm|spatial)
  3.  RAG retriever   → MCP tools candidats
  4.  Couche débat    → 5 agents en parallèle
  5.  Modérateur      → plan final validé
  6.  Exécution MCP   → appel tools step by step
  7.  Validation      → contrats Pydantic + validators geo/gee/sql
  8.  Agrégation      → AgentResponse final
  9.  Mémoire update  → sauvegarder contexte
  10. Réponse         → {text, tool_calls, tool_results}

Adapté au .env :
  LLM_PROVIDER=openrouter
  OPENROUTER_MODEL=openrouter/openrouter/free
  ENABLE_RAG=true / ENABLE_MULTI_AGENT=true
  ROUTER_CONFIDENCE_HIGH=0.85 / ROUTER_CONFIDENCE_LOW=0.55
"""

import os
import json
import time
import logging
import asyncio
from typing import Optional

log = logging.getLogger("orchestrator")

# ── Feature flags depuis .env ─────────────────────────────────
ENABLE_RAG         = os.getenv("ENABLE_RAG",          "true").lower() == "true"
ENABLE_MULTI_AGENT = os.getenv("ENABLE_MULTI_AGENT",   "true").lower() == "true"
ENABLE_DEBATE      = os.getenv("ENABLE_DEBATE",        "true").lower() == "true"
ENABLE_VALIDATION  = os.getenv("ENABLE_VALIDATION",    "true").lower() == "true"

# Timeouts
ORCHESTRATOR_TIMEOUT = int(os.getenv("ORCHESTRATOR_TIMEOUT_S", "120"))
MCP_STEP_TIMEOUT     = int(os.getenv("MCP_STEP_TIMEOUT_S",      "300"))  # 5 minutes

# ── Domaines cartographiques ──────────────────────────────────
KNOWN_DOMAINS = {
    "gee", "satellite", "worldbank", "routing",
    "overture", "osm", "spatial", "postgis",
    "nominatim", "elevation", "default",
}

# ── Mapping domaine → server MCP primaire ─────────────────────
DOMAIN_TO_SERVER = {
    "gee":        "gee",
    "satellite":  "gee",
    "worldbank":  "worldbank",
    "routing":    "ors",
    "overture":   "overture",
    "osm":        "osm",
    "spatial":    "postgis",
    "postgis":    "postgis",
    "nominatim":  "nominatim",
    "elevation":  "maptiler",
    "default":    "overture",
}


# ═══════════════════════════════════════════════════════════════
# ROUTER INTERNE
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
# HELPERS — extraction lieu / dates depuis la query
# ═══════════════════════════════════════════════════════════════

_GEO_STOPWORDS = {
    # Transport — jamais des lieux géographiques
    "pied","vélo","velo","voiture","bike","foot","walking","cycling","driving",
    "ici","here","là","la","cela","ca","moi","nous",
    # Indices/termes techniques
    "ndvi","evi","savi","ndwi","nbr","lst","mndwi","sar",
    "temperature","température","surface","indice","couverture",
    "carte","cartes","image","données","donnees","analyse","analyser",
    "affiche","montre","monter","calcule","calculer","générer","generer",
    "en","de","du","le","la","les","des","sur","pour","avec","dans",
    "2020","2021","2022","2023","2024","2025",
    "janvier","février","mars","avril","mai","juin",
    "juillet","août","septembre","octobre","novembre","décembre",
    "satellite","sentinel","landsat","modis","gee","sar","raster",
    # Termes GEE / indices — jamais des lieux
    "hauteur","canopee","canopée","canopy","canopie",
    "occupation","worldcover","world","cover","esa",
    "arbres","arbre","foret","forêt","vegetation","végétation",
    "taille","largeur","profondeur",
    # Fragments de mots techniques (coupures regex)
    "mperature","erature","ature","errain","errestre",
    # Pays génériques — dé-priorisés si un lieu plus précis existe
    "france","espagne","italie","allemagne","belgique","suisse",
    "portugal","maroc","algérie","tunisie","sénégal","mali","niger",
    "cameroun","ghana","nigeria","kenya","egypt","brasil",
    "europe","afrique","asie","amérique","africa",
}

# Pays à dé-prioriser par rapport aux villes
_COUNTRY_NAMES = {
    "france","espagne","italie","allemagne","belgique","suisse",
    "portugal","maroc","algérie","tunisie","sénégal","mali","niger",
    "cameroun","ghana","nigeria","kenya","egypt","brasil",
    "europe","afrique","asie","amérique","africa",
}

# Sous-chaînes indiquant un terme technique (pas un lieu)
_SKIP_PLACE_SUBSTRINGS = [
    "satellite","indice","mperature","ature","errain",
    "canop","hauteur des","hauteur de can",
    "occupation du","world cover","esa world",
    "végétat","vegetat",
]

def _extract_place_from_query(query: str) -> str:
    """
    Extrait le lieu géographique.
    Priorité 1 : nom propre composé après préposition spatiale (autour du/de/d')
    Priorité 2 : lieu simple après préposition
    Priorité 3 : dernier token significatif
    """
    import re

    # Priorité 1 — nom propre composé après préposition spatiale
    # Ex: "autour du Château des Ducs de Bretagne" → "Château des Ducs de Bretagne"
    spatial = re.search(
        r"""(?:autour\s+du|autour\s+de\s+la|autour\s+de\s+l.|autour\s+d.|"""
        r"""autour\s+de|pres\s+du|pres\s+de\s+la|pres\s+de|"""
        r"""pr.s\s+du|pr.s\s+de\s+la|pr.s\s+de|"""
        r"""around\s+the|around\s+a|next\s+to)\s+(.+?)(?:\s*$)""",
        query, re.IGNORECASE | re.UNICODE
    )
    if spatial:
        candidate = spatial.group(1).strip()
        # Garder seulement si contient une majuscule (nom propre)
        if re.search(r"[A-ZÀ-Ý]", candidate):
            candidate = re.sub(
                r"\s+(en|de|du|pour|sur|avec|dans|et|ou)\s*$",
                "", candidate, flags=re.IGNORECASE
            ).strip()
            if candidate:
                return candidate

    # Priorité 2 — lieu après préposition simple
    q = re.sub(r"[,;.()]", " ", query.strip())
    q = re.sub(r"\s+", " ", q).strip()

    patterns = [
        r"(?:à|a|sur|dans|pour|en|over)\s+([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-]{1,30}?)(?=\s+(?:en|de|du|pour|sur|avec|dans|\d{4})|$)",
        r"([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-]{1,25}?)\s+\d{4}\s*$",
        r"([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\-]{2,25})\s*$",
    ]
    candidates = []
    for pattern in patterns:
        for m in re.finditer(pattern, q, re.IGNORECASE):
            place  = m.group(1).strip()
            tokens = place.split()
            filtered = [t for t in tokens
                        if t.lower() not in _GEO_STOPWORDS
                        and len(t) >= 3 and t[0].isalpha()]
            if not filtered:
                continue
            candidate = " ".join(filtered)
            if any(sw in candidate.lower() for sw in _SKIP_PLACE_SUBSTRINGS):
                continue
            candidates.append(candidate)

    if not candidates:
        return ""
    multi   = [c for c in candidates if len(c.split()) > 1]
    precise = [c for c in candidates if c.lower() not in _COUNTRY_NAMES]
    return (multi[0] if multi else precise[0] if precise else candidates[0])


def _extract_dates_from_query(query: str) -> dict:
    """
    Extrait les dates d'une query.
    Ex: "en 2024" -> {"date_start":"2024-01-01","date_end":"2024-12-31"}
        "juillet 2024" -> {"date_start":"2024-07-01","date_end":"2024-07-31"}
    """
    import re
    from datetime import datetime

    q = query.lower()
    today = datetime.now().strftime("%Y-%m-%d")
    current_year = datetime.now().year

    MONTHS = {
        "janvier":1,"février":2,"fevrier":2,"mars":3,"avril":4,
        "mai":5,"juin":6,"juillet":7,"août":8,"aout":8,
        "septembre":9,"octobre":10,"novembre":11,"décembre":12,"decembre":12,
    }
    MONTH_DAYS = {1:31,2:28,3:31,4:30,5:31,6:30,7:31,8:31,9:30,10:31,11:30,12:31}

    for month_name, month_num in MONTHS.items():
        m = re.search(rf"{month_name}\s+(\d{{4}})", q)
        if m:
            year = int(m.group(1))
            if 2000 <= year <= current_year:
                return {
                    "date_start": f"{year}-{month_num:02d}-01",
                    "date_end":   f"{year}-{month_num:02d}-{MONTH_DAYS[month_num]}",
                }

    m = re.search(r"(\d{4})\s*[-–]\s*(\d{4})", q)
    if m:
        y1, y2 = int(m.group(1)), int(m.group(2))
        if 2000 <= y1 <= current_year and y1 <= y2 <= current_year:
            return {"date_start": f"{y1}-01-01", "date_end": f"{y2}-12-31"}

    m = re.search(r"(?:en\s+)?(\d{4})", q)
    if m:
        year = int(m.group(1))
        if 2000 <= year <= current_year:
            end = f"{year}-12-31" if f"{year}-12-31" <= today else today
            return {"date_start": f"{year}-01-01", "date_end": end}

    return {}


def _classify_domain(query: str, rag_tools: list) -> dict:
    """
    Détermine le domaine de la requête.
    90% règles triggers, 10% RAG score.
    """
    import re as _re_cls
    q = query.lower()

    # ── PRIORITÉ 0 : "X dans l'isochrone/zone" → overture (clip spatial) ──
    # Ce pattern doit passer AVANT routing pour éviter que "isochrone" déclenche
    # un recalcul au lieu d'une requête Overture filtrée
    if _re_cls.search(
        r"dans\s+l.{0,2}isochrone|dans\s+lisochrone|"
        r"dans\s+la\s+zone|dans\s+le\s+buffer|"
        r"dans\s+l.{0,2}aire|within.*isochrone|inside.*zone|"
        r"dans\s+cette\s+zone|dans\s+cet\s+isochrone|"
        r"dans\s+la\s+couche|dans\s+ce\s+polygone",
        q
    ):
        return {"domain": "overture", "confidence": 0.95, "method": "clip_pattern"}
    # ── PRIORITÉ 0ter : route implicite "ville1 ville2 en voiture/vélo/pied" ──
    import re as _re_route
    if _re_route.search(
        r"[a-zA-ZÀ-ÿ]{3,}\s+[a-zA-ZÀ-ÿ]{3,}\s+(?:en\s+voiture|en\s+v[eé]lo|à\s+pied|a\s+pied)",
        q
    ):
        return {"domain": "routing", "confidence": 0.92, "method": "route_implicit"}
            
    # Règles déterministes (triggers durs)
    rules = [
        ("gee", ["ndvi","evi","savi","ndwi","timelapse","sentinel",
                 "landsat","gee","sar","radar","lst","era5","modis",
                 "worldcover","world cover","esa world","canopée","canopee","canopy","canopie","déforestation",
                 "forest watch","hansen","global forest watch",
                 # LST / température de surface
                 "temperature de surface","température de surface",
                 "température surface","temperature surface",
                 "chaleur urbaine","îlot de chaleur","icu",
                 "surface temperature","land surface temperature",
                 # Végétation / forêt (Hansen)
                 "indice végétation","indice de végétation",
                 "végétation satellite","image satellite",
                 # Hansen / Forest Watch — toutes variantes françaises
                 "couverture foret","couverture forêt",
                 "couverture de la foret","couverture de la forêt",
                 "couverture de foret","couverture de forêt",
                 "perte foret","perte forêt","perte de foret","perte de forêt",
                 "perte de la foret","perte de la forêt",
                 "gain foret","gain forêt","gain de foret","gain de forêt",
                 "gain de la foret","gain de la forêt",
                 "deforestation","déforestation","forest loss","forest cover","tree cover",
                 "foret tropicale","forêt tropicale","canopée forêt","couvert forestier",
                 "couvert arboré","couvrerture foret","couvrerture forêt",
                 "couvrerture de la foret","couvrerture de la forêt",
                 # Occupation du sol
                 "land cover","esa worldcover","occupation du sol","occupation sol",
                 "couverture sol","couverture du sol","land use satellite",
                 # Canopée
                 "hauteur de canopée","hauteur de canopee","hauteur canopée","hauteur canopee",
                 "canopy height","hauteur des arbres","hauteur arbres","taille des arbres",
                 # SRTM / relief GEE statique
                 "srtm","mnt","modèle numérique de terrain","modele numerique de terrain",
                 "relief satellite","elevation satellite","pente satellite","ombrage relief",
                 # Feu / eau
                 "nbr","mndwi","indice eau","indice feu","burn"]),
        ("worldbank", ["pib","gdp","population mondiale","world bank",
                       "banque mondiale","indicateur mondial","espérance",
                       "co2 pays","alphabétisation","chômage mondial",
                       "mortalité","inégalités","gini","développement humain",
                       "choroplèthe mondial"]),
        ("routing", ["isochrone","itinéraire","route","trajet","directions",
                     "à pied","à vélo","en voiture","minutes","accessible",
                     "ors","accessibilité","zone accessible","distance"]),
        ("elevation", ["altitude","élévation","pente","relief","courbes de niveau",
                       "hillshade","ombrage","profil altimétrique","dénivelé",
                       "topographie"]),
        ("osm", ["openstreetmap","osm","overpass","parcs","espaces verts",
                 "transport en commun","bus","tram","métro","cours d'eau",
                 "landuse","occupation sol osm"]),
        ("spatial", ["buffer","zone tampon","intersection","union","différence",
                     "jointure spatiale","clip","postgis","base de données"]),
    ]

    for domain, triggers in rules:
        if any(t in q for t in triggers):
            return {
                "domain":     domain,
                "confidence": 0.90,
                "method":     "rules",
            }

    # RAG — validé seulement si la query contient un signal cohérent
    _DOMAIN_SIGNALS = {
        "gee":       {"ndvi","evi","savi","ndwi","lst","nbr","sar","sentinel","landsat",
                      "modis","era5","gee","satellite","timelapse","worldcover",
                      "canopée","canopy","srtm","déforestation","temperature de surface"},
        "worldbank": {"pib","gdp","world bank","banque mondiale","indicateur","mondial",
                      "espérance","mortalité","chômage","co2","alphabétisation"},
        "routing":   {"isochrone","itinéraire","trajet","ors","à pied","à vélo","accessibilité"},
        "osm":       {"osm","openstreetmap","transport en commun","bus","tram","métro"},
        "spatial":   {"buffer","zone tampon","intersection","union","clip","postgis"},
        "elevation": {"altitude","élévation","pente","profil altimétrique","dénivelé"},
    }
    if rag_tools:
        top    = rag_tools[0]
        score  = top.get("score", 0.0)
        srv    = top.get("server","overture")
        srv_to_domain = {
            "gee":"gee","ors":"routing","worldbank":"worldbank",
            "overture":"overture","osm":"osm","postgis":"spatial",
            "nominatim":"nominatim","maptiler":"elevation",
        }
        rag_domain = srv_to_domain.get(srv, "overture")
        signals    = _DOMAIN_SIGNALS.get(rag_domain, set())
        has_signal = any(kw in q for kw in signals)
        if rag_domain == "overture" or has_signal:
            return {"domain": rag_domain, "confidence": round(score*0.8,3), "method": "rag"}
        log.debug(f"[classify] RAG ignoré: {srv}/{rag_domain} sans signal dans '{q[:40]}'")

    # ── Requêtes conversationnelles → pas d'API ──────────────
    import re as _re_chat
    _chat_pats = [
        "^(salut|bonjour|bonsoir|coucou|hello|hi|hey|yo)\\b",
        "^(merci|thanks|ok|okay|super|parfait|bien|cool|nickel)\\s*[!.]?\\s*$",
        "^(oui|non|si|yes|no|nope|yep|ouais|nan)\\s*[!.]?\\s*$",
        "^(au revoir|bye|ciao|bonne journee|bonne soiree|a bientot)\\b",
        "^[!?.]{1,3}$",
    ]
    for _p in _chat_pats:
        if _re_chat.match(_p, q.strip(), _re_chat.IGNORECASE):
            return {"domain": "chat", "confidence": 0.97, "method": "chat_pattern"}

    # ── Hors-sujet cartographique → refus poli, zéro appel API ─
    import re as _re_chat
    _OFFTOPIC_KWS = [
        # Salutations
        "salut","bonjour","bonsoir","coucou","hello","hi\\b","hey\\b","merci",
        "au revoir","bye\\b","ciao",
        # Recettes / cuisine
        "recette","cuisin","plat ","manger","nourriture"," food","ingredient",
        "preparer","cuire","cuisson","pizza","pates","dessert","gateau",
        # Actualité / news générale
        "actualite","actu\\b","news\\b","journal du jour",
        # Définitions générales
        "comment faire un","comment faire une","comment preparer",
        "c est quoi","qu est ce que","definition de","signifie quoi",
    ]
    _GEO_ANCHORS = [
        "carte","map","couche","layer","zone","ville","pays","region",
        "latitude","longitude","bbox","geo","spatial","isochrone",
        "restaurant","cafe","batiment","route","ndvi","satellite",
        "nantes","paris","dakar","lyon","bordeaux","marseille",
        "france","afrique","europe","coordinates",
    ]
    _has_offtopic = any(_re_chat.search(kw, q, _re_chat.IGNORECASE) for kw in _OFFTOPIC_KWS)
    _has_geo      = any(kw in q for kw in _GEO_ANCHORS)
    if _has_offtopic and not _has_geo:
        return {"domain": "chat", "confidence": 0.95, "method": "offtopic"}

    # Défaut : Overture (tout ce qui n'est pas satellite/routing/worldbank)
    return {"domain": "overture", "confidence": 0.6, "method": "default_overture"}


# ═══════════════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════════════

def _validate_result(result: dict, server: str, tool: str) -> dict:
    """
    Valide le résultat d'un MCP tool.
    Retourne le résultat (éventuellement corrigé) ou une erreur.
    """
    if not ENABLE_VALIDATION:
        return result

    if "error" in result:
        return result

    action = result.get("action","")

    # Validation tile_url pour les layers raster
    if action == "add_layer":
        tile_url = result.get("tile_url","")
        if tile_url and "{z}" not in tile_url:
            log.warning(f"tile_url invalide (pas de {{z}}): {tile_url[:80]}")
            return {
                **result,
                "_validation_warning": "tile_url manque {z}/{x}/{y}",
            }

    # Validation GeoJSON
    if action in ("add_isochrone","add_choropleth","add_markers","add_route"):
        # mcp_overture retourne le FeatureCollection directement (pas dans "geojson")
        # mcp_ors retourne {"geojson":{...}, "action":"add_isochrone"}
        if result.get("type") == "FeatureCollection":
            geojson = result  # Structure directe
        else:
            geojson = result.get("geojson", {})

        if not geojson or geojson.get("type") != "FeatureCollection":
            log.warning(f"GeoJSON invalide pour {action}")
            return {
                **result,
                "_validation_warning": "GeoJSON invalide ou vide",
            }
        features = geojson.get("features", [])
        if len(features) == 0:
            return {
                **result,
                "_validation_warning": "GeoJSON vide (0 features)",
            }
        # Normaliser : s'assurer que "geojson" est toujours présent
        if result.get("type") == "FeatureCollection" and "geojson" not in result:
            result = {
                **result,
                "geojson": {
                    "type":     "FeatureCollection",
                    "features": features,
                    "metadata": result.get("metadata", {}),
                },
                "feature_count": len(features),
            }

    # Validation timelapse
    if action == "add_timelapse":
        frames = result.get("frames",[])
        dates  = result.get("dates",[])
        if len(frames) < 2:
            return {
                "error": f"Timelapse insuffisant ({len(frames)} frames). "
                         "Élargissez la période ou augmentez cloud_cover.",
            }
        if len(frames) != len(dates):
            result["dates"] = dates[:len(frames)]

    # Validation + enrichissement GEE
    if server == "gee" and "tile_url" in result:
        # Construire vis_params structuré pour le frontend
        if not result.get("vis_params"):
            palette = result.get("palette", [])
            if palette:
                result["vis_params"] = {
                    "min":     result.get("min", -0.2),
                    "max":     result.get("max",  0.9),
                    "palette": palette,
                }
        # layer_name lisible
        if not result.get("layer_name"):
            index   = result.get("index",   result.get("band", ""))
            date    = result.get("date",    result.get("date_start", ""))
            year    = str(date)[:4] if date else ""
            result["layer_name"] = f"{index}{' ' + year if year else ''}".strip() or "GEE layer"
        # bbox unifiée
        if not result.get("bbox") and result.get("clip_bbox"):
            result["bbox"] = result["clip_bbox"]

        try:
            from validation.gee_validator import validate_vis_params
            palette = result.get("palette", [])
            if palette:
                validate_vis_params({
                    "min": result.get("min", -1),
                    "max": result.get("max",  1),
                    "palette": palette,
                })
        except Exception as e:
            log.debug(f"gee_validator: {e}")

    return result


# ═══════════════════════════════════════════════════════════════
# EXÉCUTION DU PLAN MCP
# ═══════════════════════════════════════════════════════════════

async def _execute_plan(plan: dict, context: dict) -> dict:
    """
    Exécute le plan MCP step by step.
    Gère les secondary_steps (multi-agents).
    """
    from mcp_client import get_mcp_client
    client = get_mcp_client()

    server = plan.get("server","overture")
    tool   = plan.get("tool","query_places")
    args   = dict(plan.get("args",{}))
    secondary = plan.get("secondary_steps",[])

    # Injecter bbox depuis contexte si absente
    if not args.get("bbox") and context.get("current_bbox"):
        args["bbox"] = context["current_bbox"]
        log.debug(f"bbox injectée depuis contexte: {args['bbox']}")

    # Injecter xmin/ymin/xmax/ymax depuis bbox si le tool les attend
    if args.get("bbox") and not args.get("xmin"):
        b = args["bbox"]
        if len(b) == 4:
            args.update({
                "xmin": b[0], "ymin": b[1],
                "xmax": b[2], "ymax": b[3],
            })

    results = {}
    tool_calls  = []
    tool_results = []

    # ── Step principal ────────────────────────────────────────
    log.info(f"[Orchestrateur] Exec: {server}.{tool}")
    t0 = time.time()
    try:
        primary_result = await asyncio.wait_for(
            client.call_tool(tool, args, server_name=server),
            timeout=MCP_STEP_TIMEOUT,
        )
        latency = int((time.time()-t0)*1000)
        log.info(
            f"[Orchestrateur] {server}.{tool} ✓ {latency}ms "
            f"| action={primary_result.get('action','?')}"
        )
        # Injecter "action" depuis le plan si le MCP server ne le retourne pas
        if "action" not in primary_result and plan.get("output_action"):
            primary_result["action"] = plan["output_action"]

        # Normaliser le résultat Overture (FeatureCollection directe)
        if primary_result.get("type") == "FeatureCollection":
            feats = primary_result.get("features", [])
            if "feature_count" not in primary_result:
                primary_result["feature_count"] = len(feats)
            # NE PAS faire geojson = result (référence circulaire → RecursionError)
            # Créer une copie légère sans les features pour les metadata
            if "geojson" not in primary_result:
                primary_result["geojson"] = {
                    "type":     "FeatureCollection",
                    "features": feats,
                    "metadata": primary_result.get("metadata", {}),
                }

        # Injecter layer_name lisible depuis les args du plan
        if not primary_result.get("layer_name") and plan.get("output_action") == "add_markers":
            _args      = plan.get("args", {})
            _category  = _args.get("category", "")
            _geo_place = plan.get("_geocoded_place", "")
            _cat_label = _category.replace("_"," ").title() if _category else "Données"
            _loc_label = f" — {_geo_place.title()}" if _geo_place else ""
            primary_result["layer_name"] = f"{_cat_label}{_loc_label}"

        # ── Enrichir le résultat GEE avec les args du plan ────
        # Le cache retourne le tile_url original SANS bbox ni layer_name.
        # On ré-injecte toujours bbox/dates/index depuis les args du plan.
        if server == "gee" and "tile_url" in primary_result:
            # bbox : toujours depuis les args (priorité sur ce que GEE retourne)
            bbox_from_args = args.get("clip_bbox") or args.get("bbox")
            if bbox_from_args:
                primary_result["bbox"]     = bbox_from_args
                primary_result["clip_bbox"] = bbox_from_args

            # Dates depuis les args
            if not primary_result.get("date_start") and args.get("date_start"):
                primary_result["date_start"] = args["date_start"]
            if not primary_result.get("date_end") and args.get("date_end"):
                primary_result["date_end"] = args["date_end"]

            # Index (NDVI / LST / NDWI...) depuis le tool name si absent
            if not primary_result.get("index"):
                _tool_to_index = {
                    "compute_ndvi":          "NDVI",
                    "compute_lst":           "LST",
                    "compute_ndwi":          "NDWI",
                    "compute_evi":           "EVI",
                    "compute_worldcover":    "WorldCover",
                    "compute_canopy_height": "Hauteur canopée",
                    "compute_forest_watch":  "Couverture forêt 2000",
                    "compute_srtm":          "Élévation",
                    "compute_nbr":           "NBR",
                    "compute_sar":           "SAR",
                    "compute_rgb":           "RGB",
                }
                primary_result["index"] = _tool_to_index.get(tool, tool.replace("compute_","").upper())

            # Forcer layer_name avec ville geocodée + année
            geocoded = plan.get("_geocoded_place", "")
            index     = primary_result.get("index", "NDVI")
            year      = str(primary_result.get("date_start", ""))[:4]
            name_parts = [index]
            if year:      name_parts.append(year)
            if geocoded:  name_parts.append(geocoded.title())
            primary_result["layer_name"] = "_".join(p.replace(" ","_") for p in name_parts)

        # Propager _buffer_m depuis le plan vers le tool_result (pour App.jsx doClipAndAdd)
        if plan.get("_buffer_m"):
            primary_result["_buffer_m"] = plan["_buffer_m"]
            # Propager layer_name dans metadata pour App.jsx layerDisplayName
            _plan_layer_name = plan.get("_layer_name") or plan.get("args",{}).get("_layer_name")
            if _plan_layer_name:
                primary_result["layer_name"] = _plan_layer_name
                if isinstance(primary_result.get("metadata"), dict):
                    primary_result["metadata"]["layer_name"] = _plan_layer_name
                elif primary_result.get("type") == "FeatureCollection":
                    primary_result.setdefault("metadata", {})["layer_name"] = _plan_layer_name

        # ── Détecter échecs silencieux ────────────────────────
        # Couche vide
        if primary_result.get("type") == "FeatureCollection":
            _n_feats = len(primary_result.get("features", []))
            if _n_feats == 0:
                log.warning(f"[Orchestrateur] {server}.{tool} → 0 features (couche vide)")
                primary_result["_validation_warning"] = "GeoJSON vide (0 features)"
        # Résultat sans action ni type reconnu (ex: spatial_buffer vide)
        _has_action = primary_result.get("action") or primary_result.get("type")
        _has_data   = (primary_result.get("features") or primary_result.get("tile_url")
                       or primary_result.get("geojson") or primary_result.get("frames"))
        if not _has_action and not _has_data and "error" not in primary_result:
            log.warning(
                f"[Orchestrateur] {server}.{tool} → résultat inattendu: "
                f"{list(primary_result.keys())[:6]}"
            )
            primary_result["_warning"] = (
                f"Le serveur {server} n'a pas retourné de données exploitables. "
                f"Vérifiez la connexion ou la disponibilité du service."
            )

        primary_result = _validate_result(primary_result, server, tool)
        results["primary"] = primary_result
        tool_calls.append({"name": tool, "args": args})
        tool_results.append(primary_result)

    except asyncio.TimeoutError:
        err = f"Timeout ({MCP_STEP_TIMEOUT}s) sur {server}.{tool}"
        log.error(f"[Orchestrateur] {err}")
        return {"error": err, "tool_calls": tool_calls, "tool_results": tool_results}
    except Exception as e:
        log.error(f"[Orchestrateur] {server}.{tool} error: {e}")
        return {
            "error": str(e),
            "tool_calls": tool_calls, "tool_results": tool_results,
        }

    # ── Secondary steps (ex: worldbank → geo_data pour choroplèthe) ──
    for step in secondary[:3]:   # max 3 steps secondaires
        s_server = step.get("server","")
        s_tool   = step.get("tool","")
        s_args   = dict(step.get("args",{}))

        if not s_server or not s_tool:
            continue

        # Injecter les résultats du step précédent si besoin
        if step.get("inject_primary_bbox") and primary_result.get("bbox"):
            s_args["bbox"] = primary_result["bbox"]

        log.info(f"[Orchestrateur] Secondary: {s_server}.{s_tool}")
        try:
            step_result = await asyncio.wait_for(
                client.call_tool(s_tool, s_args, server_name=s_server),
                timeout=MCP_STEP_TIMEOUT,
            )
            step_result = _validate_result(step_result, s_server, s_tool)
            results[f"step_{s_server}_{s_tool}"] = step_result
            tool_calls.append({"name": s_tool, "args": s_args})
            tool_results.append(step_result)
        except Exception as e:
            log.warning(f"[Orchestrateur] Secondary {s_server}.{s_tool}: {e}")

    return {
        "primary":    results.get("primary",{}),
        "results":    results,
        "tool_calls": tool_calls,
        "tool_results": tool_results,
    }


# ═══════════════════════════════════════════════════════════════
# AGRÉGATION FINALE
# ═══════════════════════════════════════════════════════════════

def _build_vector_response(primary: dict, query: str, moderation: dict) -> str:
    """Réponse concise pour les résultats vecteur Overture/OSM."""
    nl = "\n"
    if primary.get("type") == "FeatureCollection":
        features = primary.get("features") or []
    else:
        features = (primary.get("features") or
                    primary.get("geojson", {}).get("features") or [])
    n         = primary.get("feature_count") or primary.get("total") or len(features)
    category  = moderation.get("plan",{}).get("args",{}).get("category","")
    geo_place = moderation.get("corrections_applied",[""])
    geo_place = geo_place[0].replace("geocoded:","") if geo_place else ""
    cat_label = category.replace("_"," ") if category else "éléments"
    loc_label = f" autour de **{geo_place}**" if geo_place else ""

    if n == 0:
        return (
            f"Aucun **{cat_label}**{loc_label} trouvé. "
            f"Essayez d'élargir la zone ou une autre catégorie."
        )

    # Résumé + 3 noms d'exemple
    top_names = []
    for f in features[:3]:
        if not isinstance(f, dict): continue
        props = f.get("properties") or {}
        name  = props.get("name") or props.get("primary") or props.get("label") or ""
        if isinstance(name, dict):
            name = name.get("primary","") or next(iter(name.values()),"")
        if name:
            top_names.append(f"**{name}**")

    text = f"**{n} {cat_label}**{loc_label} affichés sur la carte."
    if top_names:
        text += f" Ex : {', '.join(top_names)}."

    # Une seule suggestion contextuelle
    if n > 20:
        if category in ("restaurant","cafe","bar"):
            text += nl + f"_Suggestion : 'commerces dans l\\'isochrone' ou 'heatmap des {cat_label}'_"
        elif category:
            text += nl + f"_Suggestion : 'heatmap des {cat_label}' pour voir la densité_"

    return text


def _aggregate(
    execution:  dict,
    moderation: dict,
    query:      str,
) -> dict:
    """
    Construit la réponse finale compatible avec call_llm() de agent.py :
    {"text": str, "tool_calls": [...], "tool_results": [...]}
    """
    primary      = execution.get("primary",{})
    tool_calls   = execution.get("tool_calls",[])
    tool_results = execution.get("tool_results",[])
    action       = primary.get("action","")
    nl           = "\n"

    # ── Clarification demandée ────────────────────────────────
    if moderation.get("needs_clarification"):
        q = moderation.get("clarification_question","")
        return {
            "text":         q or "Pouvez-vous préciser votre demande ?",
            "tool_calls":   [],
            "tool_results": [],
            "_needs_clarification": True,
        }

    # ── Construire le texte selon l'action ─────────────────────
    if action == "add_markers":
        # Propager layer_name dans tool_result pour nommage de couche correct
        for tr in tool_results:
            if isinstance(tr, dict) and tr.get("action") == "add_markers":
                if not tr.get("layer_name") and primary.get("layer_name"):
                    tr["layer_name"] = primary["layer_name"]
                # Aussi dans le geojson metadata
                if tr.get("geojson") and primary.get("layer_name"):
                    tr["geojson"].setdefault("metadata", {})["layer_name"] = primary["layer_name"]
        text = _build_vector_response(primary, query, moderation)

    elif action == "add_layer":
        layer    = primary.get("layer_name","")
        tool_name = moderation.get("plan",{}).get("tool","")
        gee_def  = _get_gee_definition(tool_name) if "_get_gee_definition" in dir() else None
        if gee_def:
            idx_name, idx_desc, idx_use = gee_def
            text = idx_name + " ajouté sur la carte." + nl + nl + idx_desc + nl + nl + "_" + idx_use + "_"
        elif layer:
            text = f"Couche **{layer}** affichée sur la carte."
        else:
            text = "Couche affichée sur la carte."

    elif action == "add_isochrone":
        _iso_args  = moderation.get("plan",{}).get("args",{})
        dur        = (primary.get("time_minutes") or primary.get("duration")
                      or _iso_args.get("time_minutes") or "10")
        prof_raw   = (primary.get("profile") or _iso_args.get("profile") or "foot")
        prof_label = {"foot":"à pied","bike":"à vélo","car":"en voiture"}.get(prof_raw, prof_raw)
        place_iso  = primary.get("layer_name","").split("Isochrone")[-1].strip() or ""
        loc_iso    = f" — **{place_iso}**" if place_iso and "min" not in place_iso else ""
        feats      = len((primary.get("geojson") or {}).get("features") or [])
        text = (
            f"Isochrone **{dur} min** {prof_label}{loc_iso} affichée."
            + (f" ({feats} polygones)" if feats > 1 else "")
            + nl +
            f"_Zones accessibles en {dur} min {prof_label}. "
            f"Suggestions : 'restaurants dans l\'isochrone', 'pharmacies dans la zone'_"
        )

    elif action == "add_choropleth":
        ind  = primary.get("indicator_label","")
        yr   = primary.get("year","")
        text = f"Carte mondiale : **{ind}** ({yr})." + nl + "_Cliquez sur un pays pour voir sa valeur._"

    elif action == "add_timelapse":
        n_f  = primary.get("frame_count",0)
        text = f"Timelapse prêt ({n_f} frames). Cliquez ▶ pour lancer l'animation."

    elif "error" in primary:
        text = f"⚠️ Erreur : {primary['error']}"

    elif primary.get("_validation_warning"):
        _warn = primary["_validation_warning"]
        if "0 features" in _warn or "vide" in _warn.lower():
            text = (
                "⚠️ Aucun résultat retourné pour cette requête.\n"
                "_Essayez d'élargir la zone, de changer la catégorie, "
                "ou de vérifier que des données existent dans cette région._"
            )
        else:
            text = f"⚠️ {_warn}"

    elif primary.get("_warning"):
        text = (
            f"⚠️ {primary['_warning']}\n"
            f"_Vérifiez que la zone contient des données ou élargissez la recherche._"
        )

    else:
        text = moderation.get("message_utilisateur","") or "Données ajoutées sur la carte."

    # ── Warnings ───────────────────────────────────────────────
    warnings     = moderation.get("warnings",[])
    domain_final = moderation.get("plan",{}).get("server","")
    if domain_final not in ("gee","stac"):
        warnings = [w for w in warnings
                    if "gee" not in w.lower() and "bbox trop grande" not in w.lower()]
    if warnings:
        text += f" ⚠️ " + " | ".join(warnings[:2])

    return {
        "text":         text,
        "tool_calls":   tool_calls,
        "tool_results": tool_results,
        "_confidence":  moderation.get("confidence", 0.0),
        "_domain":      moderation.get("plan",{}).get("server",""),
    }


# ═══════════════════════════════════════════════════════════════
# ORCHESTRATEUR PRINCIPAL
# ═══════════════════════════════════════════════════════════════

class Orchestrator:
    """
    Orchestrateur OpenMapAgents v2.
    Interface compatible avec get_orchestrator() / orchestrate() de agent.py.
    """

    def __init__(self):
        self._ready = False
        log.info(
            f"Orchestrateur init | "
            f"RAG={ENABLE_RAG} | "
            f"debate={ENABLE_DEBATE} | "
            f"validation={ENABLE_VALIDATION}"
        )

    def warmup(self):
        """Appelé dans lifespan() de agent.py."""
        try:
            if ENABLE_RAG:
                from rag.embedder import get_embedder
                emb = get_embedder()
                emb.setup()
                stats = emb.warmup()
                log.info(
                    f"RAG warmup: {stats['embedded']} embedés, "
                    f"{stats['skipped']} inchangés"
                )
        except Exception as e:
            log.warning(f"Orchestrateur warmup: {e}")
        self._ready = True

    # ─── ENTRY POINT COMPATIBLE agent.py ─────────────────────

    def run_sync(self, messages: list, map_context: Optional[dict]) -> dict:
        """
        Version synchrone de run() — utilisée par agent.py.

        Toujours déléguer asyncio.run() dans un ThreadPoolExecutor dédié.
        asyncio.run() crée son propre event loop isolé — compatible avec :
          - threads AnyIO (FastAPI sync endpoints)  ← plantait avec get_event_loop()
          - threads asyncio classiques
          - threads sans event loop
        """
        import concurrent.futures
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, self.run(messages, map_context))
                return future.result(timeout=ORCHESTRATOR_TIMEOUT)
        except concurrent.futures.TimeoutError:
            log.error(f"Orchestrateur timeout ({ORCHESTRATOR_TIMEOUT}s)")
            return {
                "text": f"Timeout ({ORCHESTRATOR_TIMEOUT}s) — réessayez ou réduisez la zone.",
                "tool_calls": [], "tool_results": [],
            }
        except Exception as e:
            log.error(f"Orchestrateur sync error: {e}")
            return {
                "text": f"Erreur orchestrateur: {str(e)}",
                "tool_calls": [], "tool_results": [],
            }

    async def run(
        self,
        messages:    list,
        map_context: Optional[dict] = None,
        session_id:  str            = "anon",
        user_id:     str            = "",
    ) -> dict:
        """
        Pipeline complet en 10 étapes.
        """
        t_total = time.time()

        # Extraire la dernière requête utilisateur
        query = next(
            (m["content"] for m in reversed(messages)
             if m.get("role") == "user"),
            ""
        )
        if not query:
            return {"text":"Requête vide.", "tool_calls":[], "tool_results":[]}

        log.info(f"[Orchestrateur] '{query[:60]}' | session={session_id[:12]}")

        # ──────────────────────────────────────────────────────
        # ÉTAPE 1 — Mémoire session
        # ──────────────────────────────────────────────────────
        from memory.session_memory import get_session_memory
        mem     = get_session_memory()
        session = mem.load(session_id, user_id)
        mem.update_from_map_context(session, map_context)
        context = mem.get_context(session)
        # Enrichir le contexte avec les extras de session (last_center, last_geo_place)
        _extras = getattr(session, "extra", {}) or {}
        if _extras.get("last_center"):
            context["last_center"]    = _extras["last_center"]
        if _extras.get("last_geo_place"):
            context["last_geo_place"] = _extras["last_geo_place"]
        # Injecter last_center dans map_context pour _resolve_geo_llm
        _last_center = context.get("last_center")
        if _last_center:
            _mc = dict(map_context) if map_context else {}
            _mc["_last_center"] = _last_center
            map_context = _mc
            log.debug(f"[1] last_center injecté: {_last_center}")
        log.debug(f"[1] Mémoire: bbox={bool(context.get('current_bbox'))}, "
                  f"layers={len(session.active_layers)}")
        # Log session.extra pour débugger les pending clarifs
        _ses_extra = getattr(session, "extra", {}) or {}
        log.info(f"[1] session.extra keys: {list(_ses_extra.keys())}")
        # ──────────────────────────────────────────────────────
        # ÉTAPE 1quinquies — Court-circuit BUFFER ROUTE
        # Détection directe : query = une distance (ex: "200m", "500m", "1km")
        # ET une couche route est visible sur la carte
        # Pas besoin de session — on détecte depuis map_context
        # ──────────────────────────────────────────────────────
        import re as _re_buf_q
        _clean_q_buf = _re_buf_q.sub(r'[*_`]', '', query).strip()
        _is_distance_reply = bool(_re_buf_q.fullmatch(
            r"\s*(\d+(?:[.,]\d+)?)\s*(m|km|mètre|metre|kilometre|kilomètre)?\s*",
            _clean_q_buf, _re_buf_q.IGNORECASE
        ))
        if _is_distance_reply:
            # Chercher une couche route dans map_context
            _mc_layers = (map_context or {}).get("layers", [])
            _route_layer_found = next(
                (l for l in _mc_layers
                 if isinstance(l, dict)
                 and (l.get("theme") == "route" or "itin" in l.get("name","").lower())
                 and " — " not in l.get("name","")
                 and l.get("bbox")),
                None
            )
            if _route_layer_found:
                # Extraire la distance
                _m_dist = _re_buf_q.search(
                    r"(\d+(?:[.,]\d+)?)\s*(m\b|km\b|mètre|metre|kilometre|kilomètre)?",
                    _clean_q_buf, _re_buf_q.IGNORECASE
                )
                _val   = float(_m_dist.group(1).replace(",", ".")) if _m_dist else 300
                _unit  = (_m_dist.group(2) or "m").lower() if _m_dist else "m"
                _buf_m = int(_val * 1000 if "km" in _unit or "kilo" in _unit else _val)
                log.info(f"[1quinquies] Distance reply détectée: {_buf_m}m → couche route: {_route_layer_found.get('name')}")

                # Récupérer la query originale depuis la session si disponible
                _ses_extra = getattr(session, "extra", {}) or {}
                _orig_q    = (_ses_extra.get("pending_buffer_clarif") or {}).get("orig_query", "")
                # Nettoyer la session
                try:
                    if "pending_buffer_clarif" in _ses_extra:
                        session.extra.pop("pending_buffer_clarif", None)
                        mem.save(session)
                except Exception: pass

                # Injecter le buffer dans la couche et lancer le clip
                _route_with_buf = {**_route_layer_found, "_buffer_m": _buf_m}
                _mc_buf = dict(map_context) if map_context else {}
                _mc_buf["layers"] = [
                    _route_with_buf if l.get("name") == _route_layer_found.get("name") else l
                    for l in _mc_layers
                ]
                clip_result = self._resolve_clip_layer(
                    _orig_q or query, _mc_buf, session, mem
                )
                if clip_result["status"] == "ok":
                    plan     = clip_result["plan"]
                    exec_out = await _execute_plan(plan, context)
                    primary  = exec_out.get("primary", {})
                    n        = primary.get("feature_count") or len(
                        (primary.get("geojson") or {}).get("features") or []
                    )
                    cat      = clip_result["category"]
                    clip_nm  = clip_result["clip_name"]
                    layer_nm = clip_result["layer_name"]
                    # Reformater avec _ si contient des espaces
                    layer_nm = layer_nm.replace(" — ","_dans_").replace(" ","_")
                    nl       = "\n"
                    text = (
                        f"**{cat}** dans **{_buf_m}m** autour de **{clip_nm}**." + nl +
                        f"_Couche : {layer_nm}_"
                    ) if n else (
                        f"⚠️ Aucun **{cat.lower()}** trouvé dans {_buf_m}m autour de **{clip_nm}**.\n"
                        f"_Essayez un rayon plus grand (ex: 500m) ou une autre catégorie._"
                    )
                    for tr in exec_out.get("tool_results", []):
                        if isinstance(tr, dict):
                            tr["layer_name"] = layer_nm
                            tr["_buffer_m"]  = _buf_m
                    try:
                        mem.update_from_response(session, {"text": text}, query)
                        mem.save(session)
                    except Exception: pass
                    total_ms = int((time.time() - t_total) * 1000)
                    log.info(f"[Orchestrateur] ✓ {total_ms}ms | buffer_clip | {n} feat | {_buf_m}m")
                    return {
                        "text":         text,
                        "tool_calls":   exec_out.get("tool_calls", []),
                        "tool_results": exec_out.get("tool_results", []),
                    }
        # ──────────────────────────────────────────────────────
        # ÉTAPE 1ter — Court-circuit CLARIFICATION CLIP
        # Si pending_clip_clarif existe → la query est une réponse
        # à "Quelle isochrone ?" → traiter directement sans routing
        # ──────────────────────────────────────────────────────
        _pending_clip = (getattr(session, "extra", {}) or {}).get("pending_clip_clarif")
        # ──────────────────────────────────────────────────────
        # ÉTAPE 1quater — Court-circuit CLARIFICATION ROUTING
        # Si pending_routing_clarif existe → la query est une réponse
        # à "Précisez l'adresse" → geocoder directement + isochrone
        # ──────────────────────────────────────────────────────
        _pending_routing = (getattr(session, "extra", {}) or {}).get("pending_routing_clarif")
        if _pending_routing:
            log.info(f"[1quater] Clarification routing en attente → geocoder '{query}'")

            _resolved_center = None
            try:
                import requests as _req_clarif
                _gr = _req_clarif.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": query, "format": "json", "limit": 1},
                    headers={"User-Agent": "OpenMapAgents/1.0"},
                    timeout=6,
                )
                _gresults = _gr.json()
                if _gresults:
                    _resolved_center = [float(_gresults[0]["lon"]), float(_gresults[0]["lat"])]
                    log.info(f"[1quater] Geocodé '{query}' → {_resolved_center}")
            except Exception as _eg:
                log.warning(f"[1quater] geocode '{query}' échoué: {_eg}")

            if _resolved_center:
                # Effacer l'état de clarification
                try:
                    session.extra.pop("pending_routing_clarif", None)
                    mem.save(session)
                except Exception: pass

                # Récupérer les params de la requête originale
                _time_min  = _pending_routing.get("time_minutes", 10)
                _profile   = _pending_routing.get("profile", "foot")
                _intervals = list(range(5, _time_min + 1, 5))
                if not _intervals or _intervals[-1] != _time_min:
                    _intervals.append(_time_min)
                _intervals = sorted(set(_intervals))

                _prof_label      = {"foot":"à pied","bike":"à vélo","car":"en voiture"}.get(_profile, _profile)
                _prof_label_slug = _prof_label.replace(" ", "_")
                _place_slug      = query.strip().replace(" ", "_")
                _layer_name      = f"Isochrone_{_intervals[0]}–{_intervals[-1]}min_{_prof_label_slug}_{_place_slug}"

                # Sauvegarder le center en session
                try:
                    session.extra = getattr(session, "extra", {}) or {}
                    session.extra["last_center"] = _resolved_center
                    mem.save(session)
                except Exception: pass

                total_ms = int((time.time() - t_total) * 1000)
                log.info(f"[Orchestrateur] ✓ {total_ms}ms | domain=routing_clarif")

                _tr = {
                    "action":       "compute_isochrone",
                    "center":       _resolved_center,
                    "time_minutes": _time_min,
                    "intervals":    _intervals,
                    "profile":      _profile,
                    "layer_name":   _layer_name,
                }
                return {
                    "text": (
                        f"**{_layer_name}** affichée.\n"
                        f"_Zones accessibles en {_time_min} min {_prof_label} depuis {query.strip()}._"
                    ),
                    "tool_calls":   [{"name": "compute_isochrone", "args": {
                        "center": _resolved_center,
                        "time_minutes": _time_min,
                        "intervals": _intervals,
                        "profile": _profile,
                    }}],
                    "tool_results": [_tr],
                }
            else:
                # Toujours pas trouvé → re-demander
                return {
                    "text": (
                        f"Je n'ai toujours pas trouvé **\"{query}\"**.\n"
                        f"Essayez avec le code postal ou un nom de lieu plus précis."
                    ),
                    "tool_calls":   [],
                    "tool_results": [],
                    "_needs_clarification": True,
                }

        if _pending_clip:
            log.info(f"[1ter] Clarification clip en attente → résoudre directement")
            clip_result = self._resolve_clip_layer(query, map_context, session, mem)
            if clip_result["status"] == "ok":
                plan     = clip_result["plan"]
                exec_out = await _execute_plan(plan, context)
                primary  = exec_out.get("primary", {})
                n        = primary.get("feature_count") or len(
                    (primary.get("geojson") or {}).get("features") or
                    (primary if primary.get("type")=="FeatureCollection" else {}).get("features") or []
                )
                cat      = clip_result["category"]
                clip_nm  = clip_result["clip_name"]
                layer_nm = clip_result["layer_name"].replace(" — ","_dans_").replace(" ","_")
                nl       = "\n"
                text = (
                    f"**{n} {cat.lower()}** trouvés dans **{clip_nm}**." + nl +
                    f"_Couche : {layer_nm}_"
                ) if n else (
                    f"Aucun **{cat.lower()}** trouvé dans **{clip_nm}**. "
                    f"Essayez une zone plus grande ou une autre catégorie."
                )
                for tr in exec_out.get("tool_results", []):
                    if isinstance(tr, dict):
                        tr["layer_name"]    = layer_nm
                        tr["_layer_name"]   = layer_nm
                        tr["_clip_to_layer"]= clip_nm
                try:
                    mem.update_from_response(session, {"text":text}, query)
                    mem.save(session)
                except Exception: pass
                total_ms = int((time.time()-t_total)*1000)
                log.info(f"[Orchestrateur] ✓ {total_ms}ms | domain=clip_clarif | {n} features")
                return {
                    "text":         text,
                    "tool_calls":   exec_out.get("tool_calls", []),
                    "tool_results": exec_out.get("tool_results", []),
                }
            elif clip_result["status"] == "clarify":
                # Re-clarification
                return {
                    "text":         clip_result["question"],
                    "tool_calls":   [],
                    "tool_results": [],
                    "_needs_clarification": True,
                }
            # status == no_layer → continuer normalement

        # ──────────────────────────────────────────────────────
        # ÉTAPE 2 + 3 — Router + RAG (en parallèle)
        # ──────────────────────────────────────────────────────
        rag_tools = []
        if ENABLE_RAG:
            try:
                from rag.retriever import retrieve_tools
                rag_tools = retrieve_tools(query, top_k=5)
                log.debug(
                    f"[2/3] RAG: {len(rag_tools)} tools "
                    f"(top={rag_tools[0]['tool'] if rag_tools else 'none'} "
                    f"score={rag_tools[0]['score'] if rag_tools else 0})"
                )
            except Exception as e:
                log.warning(f"RAG error: {e}")

        # ── ÉTAPE 2 — IntentParser : LLM interprète la requête ──
        from intent_parser import get_intent_parser
        _ctx_for_parser = {
            "last_geo_place": context.get("last_geo_place",""),
            "active_layers":  (map_context or {}).get("layers", []),
        }
        intent_result = get_intent_parser().parse(query, _ctx_for_parser)

        # Mapper intent → domaine orchestrateur
        _INTENT_TO_DOMAIN = {
            "geo_search": "overture",
            "isochrone":  "routing",
            "route":      "routing",
            "gee":        "gee",
            "worldbank":  "worldbank",
            "thematic":   "local",
            "clip":       "overture",
            "spatial":    "spatial",
            "chat":       "chat",
            "unknown":    "overture",
        }
        _intent  = intent_result.get("intent", "unknown")
        _domain  = _INTENT_TO_DOMAIN.get(_intent, "overture")
        _method  = "clip_pattern" if _intent == "clip" else intent_result.get("_method","llm")

        route = {
            "domain":        _domain,
            "confidence":    intent_result.get("confidence", 0.7),
            "method":        _method,
            "_intent":       _intent,
            "_intent_result": intent_result,
        }

        log.info(
            f"[2] intent={_intent} → domain={_domain} "
            f"conf={route['confidence']:.2f} [{intent_result.get('_method','?')}] "
            f"{intent_result.get('_latency_ms',0)}ms"
            + (f" place='{intent_result['geo'].get('place','')}'" if intent_result['geo'].get('place') else "")
        )

        # ──────────────────────────────────────────────────────
        # ÉTAPE 2bis — Court-circuit CLIP : "X dans l'isochrone/zone"
        # ──────────────────────────────────────────────────────
        if route.get("method") == "clip_pattern":
            clip_result = self._resolve_clip_layer(query, map_context, session, mem)

            if clip_result["status"] == "no_layer":
                return {
                    "text":         clip_result["message"],
                    "tool_calls":   [],
                    "tool_results": [],
                }

            if clip_result["status"] == "clarify":
                return {
                    "text":         clip_result["question"],
                    "tool_calls":   [],
                    "tool_results": [],
                    "_needs_clarification": True,
                }

            # status == "ok" → exécuter le plan
            plan     = clip_result["plan"]
            exec_out = await _execute_plan(plan, context)
            # Enrichir la réponse avec le nombre de features trouvées
            primary  = exec_out.get("primary", {})
            n        = primary.get("feature_count") or len(
                (primary.get("geojson") or {}).get("features") or []
            )
            cat      = clip_result["category"]
            clip_nm  = clip_result["clip_name"]
            layer_nm = clip_result["layer_name"].replace(" — ","_dans_").replace(" ","_")
            nl       = "\n"
            if n == 0:
                text = (
                    f"Aucun **{cat.lower()}** trouvé dans **{clip_nm}**. "
                    f"Essayez une zone plus grande ou une autre catégorie."
                )
            else:
                text = (
                    f"**{n} {cat.lower()}** trouvés dans **{clip_nm}**." + nl +
                    f"_Couche : {layer_nm}_"
                )
            # Propager layer_name
            for tr in exec_out.get("tool_results", []):
                if isinstance(tr, dict):
                    tr["layer_name"] = layer_nm
            try:
                mem.update_from_response(session, {"text":text}, query)
                mem.save(session)
            except Exception: pass
            total_ms = int((time.time()-t_total)*1000)
            log.info(f"[Orchestrateur] ✓ {total_ms}ms | domain=clip | {n} features")
            return {
                "text":         text,
                "tool_calls":   exec_out.get("tool_calls", []),
                "tool_results": exec_out.get("tool_results", []),
            }

        # ──────────────────────────────────────────────────────
        # ÉTAPE 2quater — Court-circuit CHAT
        # ──────────────────────────────────────────────────────
        if route.get("domain") == "chat":
            # Le LLM a déjà rédigé la réponse dans response_text
            _chat_text = (
                intent_result.get("response_text")
                or "Je suis OpenMapAgents, votre assistant cartographique. "
                   "Demandez-moi des restaurants, une isochrone, du NDVI ou une analyse spatiale."
            )
            return {"text": _chat_text, "tool_calls": [], "tool_results": []}

        # ──────────────────────────────────────────────────────
        # ÉTAPE 3 — Court-circuit ROUTING : utiliser RoutingAgent dédié
        # RoutingAgent orchestre geocode + mcp_ors directement
        # sans passer par le débat LLM ni le fallback
        # ──────────────────────────────────────────────────────
        if route.get("domain") == "routing":
            try:
                result = await self._run_routing(
                    query, context, map_context, rag_tools, session, mem, t_total,
                    intent_result=route.get("_intent_result")
                )
                if result:
                    return result
                else:
                    log.warning("[3] _run_routing retourné None → fallback")
            except Exception as e:
                import traceback
                log.warning(f"[3] Routing error: {e}")
                log.warning(f"[3] Traceback: {traceback.format_exc()}")

        # ──────────────────────────────────────────────────────
        # ÉTAPE 4 + 5 — Débat + Modération
        # ──────────────────────────────────────────────────────
        moderation = {}

        # Débat LLM uniquement pour GEE/WorldBank (params complexes).
        # Overture/OSM/routing/spatial → fallback direct, zéro crédit LLM.
        _DEBATE_DOMAINS = {"gee","satellite","worldbank"}
        domain_for_debate = route.get("domain","overture")
        debate_needed = (
            ENABLE_DEBATE
            and ENABLE_MULTI_AGENT
            and route.get("confidence", 0.0) < 0.85
            and domain_for_debate in _DEBATE_DOMAINS
        )

        if debate_needed:
            try:
                from debate.debate_layer import run_debate
                from debate.moderator    import moderate

                debate_out = await run_debate(query, context, rag_tools)
                moderation = moderate(debate_out)
                log.info(
                    f"[4/5] Débat terminé | "
                    f"conf={moderation.get('confidence',0):.2f} | "
                    f"plan={moderation.get('plan',{}).get('tool','?')}"
                )
            except Exception as e:
                log.warning(f"Débat/modération error: {e} → plan fallback")
        else:
            log.info(
                f"[4/5] Débat skippé (conf={route.get('confidence',0):.2f} ≥ 0.85,"
                f" méthode={route.get('method','?')}) → plan fallback direct"
            )

        # ── Domaines qui n'ont JAMAIS besoin de dates ni bbox stricte ──
        # Overture, OSM → bbox optionnelle (geocodable depuis le lieu)
        # WorldBank → pas de bbox du tout
        # Spatial, Nominatim → pas de dates
        _DOMAINS_NO_DATES = {"overture","osm","worldbank","nominatim","spatial","routing"}
        _DOMAINS_NO_BBOX  = {"worldbank"}
        domain = route.get("domain","overture")

        # Plan fallback si débat désactivé ou échoué
        if not moderation or not moderation.get("plan"):
            moderation = self._build_fallback_plan(
                query, route, rag_tools, context, map_context
            )

        # ──────────────────────────────────────────────────────
        # ÉTAPE 5bis — Clarification : uniquement si vraiment bloqué
        # ──────────────────────────────────────────────────────
        if moderation.get("needs_clarification"):
            # Construire d'abord un fallback — il peut résoudre le blocage
            fallback = self._build_fallback_plan(query, route, rag_tools, context)
            fallback_has_bbox = bool(fallback.get("plan",{}).get("args",{}).get("bbox"))

            # Raisons légitimes de bloquer (uniquement pour GEE temporel)
            gee_temporal = (domain == "gee" and moderation.get("plan",{}).get("tool","")
                            not in ("compute_worldcover","compute_canopy_height",
                                    "compute_forest_watch","compute_srtm"))

            should_clarify = (
                # Blocage légitime : GEE temporel sans bbox ET fallback ne peut pas geocoder
                (gee_temporal and not fallback_has_bbox)
                # Pas de clarification pour les domaines sans dates/bbox obligatoires
                and domain not in _DOMAINS_NO_DATES
            )

            if should_clarify:
                q = moderation.get("clarification_question","")
                log.info(f"[5] Clarification demandée: {q[:60]}")
                return {
                    "text":         q or "Pouvez-vous préciser la zone géographique ?",
                    "tool_calls":   [],
                    "tool_results": [],
                    "_clarification": True,
                }
            else:
                # Ignorer la clarification.
                # Préférer le plan du débat (correct sur server/tool) enrichi
                # avec la bbox du fallback, plutôt qu'un fallback GEE erroné.
                debate_plan   = moderation.get("plan", {})
                debate_server = debate_plan.get("server", "")
                debate_tool   = debate_plan.get("tool", "")

                # Plan débat valide = server cohérent avec le domaine détecté
                debate_ok = bool(debate_server and debate_tool) and (
                    (domain != "gee" and debate_server != "gee")
                    or (domain == "gee" and debate_server == "gee")
                )

                if debate_ok and fallback_has_bbox:
                    # Injecter bbox du fallback dans les args du plan débat
                    fb_args = fallback.get("plan", {}).get("args", {})
                    debate_plan["args"] = {
                        **debate_plan.get("args", {}),
                        **{k: v for k, v in fb_args.items()
                           if k in ("bbox","xmin","ymin","xmax","ymax","clip_bbox")}
                    }
                    moderation["needs_clarification"] = False
                    log.info(f"[5] Clarif ignorée → débat enrichi: {debate_server}.{debate_tool}")
                else:
                    moderation = fallback
                    log.info(f"[5] Clarif ignorée → fallback: {fallback.get('plan',{}).get('tool','?')}")

        # ──────────────────────────────────────────────────────
        # ÉTAPE 6 — Exécution MCP
        # ──────────────────────────────────────────────────────
        plan = moderation.get("plan",{})
        log.info(
            f"[6] Exécution: {plan.get('server','?')}.{plan.get('tool','?')} "
            f"| action={plan.get('output_action','?')}"
        )

        execution = await _execute_plan(plan, context)

        if "error" in execution and not execution.get("tool_results"):
            return {
                "text":         f"Erreur: {execution['error']}",
                "tool_calls":   [],
                "tool_results": [],
            }

        # ──────────────────────────────────────────────────────
        # ÉTAPE 7 — Validation (incluse dans _execute_plan)
        # ──────────────────────────────────────────────────────
        # (La validation est appelée dans _execute_plan → _validate_result)

        # ──────────────────────────────────────────────────────
        # ÉTAPE 8 — Agrégation
        # ──────────────────────────────────────────────────────
        response = _aggregate(execution, moderation, query)

        # ──────────────────────────────────────────────────────
        # ÉTAPE 9 — Mise à jour mémoire
        # ──────────────────────────────────────────────────────
        try:
            mem.update_from_response(session, response, query)
            # Sauvegarder le dernier lieu geocodé pour les requêtes suivantes
            _geo_place_saved = moderation.get("corrections_applied",[""])
            _geo_place_saved = (_geo_place_saved[0].replace("geocoded:","")
                                if _geo_place_saved else "")
            _plan_args = moderation.get("plan",{}).get("args",{})
            if _plan_args.get("center"):
                session.extra = getattr(session, "extra", {}) or {}
                session.extra["last_center"]    = _plan_args["center"]
                session.extra["last_geo_place"] = _geo_place_saved
            elif _plan_args.get("center_lon") and _plan_args.get("center_lat"):
                session.extra = getattr(session, "extra", {}) or {}
                session.extra["last_center"]    = [_plan_args["center_lon"], _plan_args["center_lat"]]
                session.extra["last_geo_place"] = _geo_place_saved
            mem.save(session)
        except Exception as e:
            log.warning(f"Session update error: {e}")

        # ──────────────────────────────────────────────────────
        # ÉTAPE 10 — Logging final + retour
        # ──────────────────────────────────────────────────────
        total_ms = int((time.time()-t_total)*1000)
        log.info(
            f"[Orchestrateur] ✓ {total_ms}ms | "
            f"domain={route['domain']} | "
            f"tools={len(response.get('tool_calls',[]))} | "
            f"conf={moderation.get('confidence',0):.2f}"
        )

        return response

    # ─── PLAN FALLBACK ────────────────────────────────────────

    # ─── ROUTING ─────────────────────────────────────────────

    async def _run_routing(
        self,
        query:        str,
        context:      dict,
        map_context:  Optional[dict],
        rag_tools:    list,
        session,
        mem,
        t_total:      float,
        intent_result: Optional[dict] = None,
    ) -> Optional[dict]:
        """
        Délègue à RoutingAgent.
        Utilise intent_result (IntentParser) si disponible pour éviter
        une double extraction LLM des paramètres.

        Flow :
          1. Import RoutingAgent
          2. Construire routing_ctx depuis context + IntentParser
          3. Détecter route implicite "ville1 ville2 en voiture"
             → injecter _intent=route, _from_place, _to_place dans routing_ctx
             → RoutingAgent gérera le geocodage des 2 villes
          4. Geocoder le lieu isochrone (si pas route implicite)
             → jamais de fallback silencieux si lieu fourni mais introuvable
          5. Déléguer à RoutingAgent.run()
        """
        import re as _re_routing
        import requests as _req_geocode
        import importlib.util, pathlib

        # ── Import RoutingAgent ───────────────────────────────
        _ra_path = pathlib.Path(__file__).parent / "agents" / "routing_agent.py"
        _spec = importlib.util.spec_from_file_location("routing_agent", _ra_path)
        _mod  = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        RoutingAgent = _mod.RoutingAgent

        # ── Mots-clés routing — jamais des noms de villes ────
        _ROUTING_WORDS = {
            "itineraire","itinéraire","trajet","route","direction","aller",
            "directions","routing","navigation","chemin",
        }
        _TRANSPORT_STOP = {
            "pied","vélo","velo","voiture","car","bike","foot",
            "walking","ici","là","la","moi","nous","carte","map",
            "minutes","minute","min",
        }

        # Recharger MAPBOX_ACCESS_TOKEN dans mcp_ors si absent
        for _mod_name in ["mcp_servers.mcp_ors", "mcp_ors"]:
            try:
                import importlib as _il
                _mors = _il.import_module(_mod_name)
                if not _mors.MAPBOX_TOKEN:
                    _token = os.getenv("MAPBOX_ACCESS_TOKEN", "")
                    if _token:
                        _mors.MAPBOX_TOKEN = _token
                        log.info(f"[Routing] MAPBOX_TOKEN injecté dans {_mod_name}")
                break
            except ImportError:
                continue

        # ── Construire routing_ctx ────────────────────────────
        routing_ctx = dict(context)
        if context.get("last_center"):
            routing_ctx["map_center"] = context["last_center"]
        elif (map_context or {}).get("center"):
            mc = map_context["center"]
            routing_ctx["map_center"] = [float(mc[0]), float(mc[1])]

        # Injecter les paramètres pré-extraits par IntentParser
        _ir_place = ""
        if intent_result:
            _ir_params = intent_result.get("params", {})
            _ir_geo    = intent_result.get("geo", {})
            if _ir_params.get("time_minutes"):
                routing_ctx["_time_minutes"] = _ir_params["time_minutes"]
            if _ir_params.get("profile"):
                routing_ctx["_profile"]      = _ir_params["profile"]
            if _ir_geo.get("place"):
                _ir_place = _ir_geo["place"]
                routing_ctx["_place"] = _ir_place
                log.info(f"[Routing] lieu depuis IntentParser: '{_ir_place}'")

        # ── Fallback regex si IntentParser n'a pas extrait de lieu ──
        if not _ir_place:
            _lieu_patterns_fb = [
                r"(?:depuis|from)\s+([a-zA-ZÀ-ÿ0-9][a-zA-ZÀ-ÿ0-9\s\,\-]{3,60}?)(?=\s*$)",
                r"[àa]\s+pied\s+(?:de\s+la\s+rue|de\s+la|du|de\s+l\.|d\.|de|depuis)\s+([a-zA-ZÀ-ÿ0-9][a-zA-ZÀ-ÿ0-9\s\,\-]{3,60}?)(?=\s*$)",
                r"(?:de\s+la\s+rue|de\s+la|du|de\s+l\.|d\.|de)\s+([a-zA-ZÀ-ÿ0-9][a-zA-ZÀ-ÿ0-9\s\,\-]{3,60}?)(?=\s*$)",
            ]
            for _pat in _lieu_patterns_fb:
                _m = _re_routing.search(_pat, query, _re_routing.IGNORECASE | _re_routing.UNICODE)
                if _m:
                    _cand = _m.group(1).strip().rstrip(",").strip()
                    _cand = _re_routing.sub(
                        r"^(?:la\s+rue\s+|rue\s+)?", "", _cand,
                        flags=_re_routing.IGNORECASE
                    ).strip()
                    _words = [w for w in _cand.split() if w.lower() not in _TRANSPORT_STOP]
                    if _words and len(_cand) > 3:
                        _ir_place = _cand
                        log.info(f"[Routing] lieu depuis fallback regex: '{_ir_place}'")
                        break

        # ══════════════════════════════════════════════════════
        # ÉTAPE A — Détecter route implicite AVANT geocodage isochrone
        # "rennes dinard en voiture" / "paris lyon à vélo"
        # Chaque ville = 1 mot strict, rejet des mots-clés routing
        # ══════════════════════════════════════════════════════
        _route_implicit = _re_routing.search(
            r"^(?:itinéraire\s+|itineraire\s+)?"   # préfixe optionnel
            r"([a-zA-ZÀ-ÿ]{3,25})\s+"              # ville1 = 1 mot
            r"([a-zA-ZÀ-ÿ]{3,25})\s+"              # ville2 = 1 mot
            r"(?:en\s+(?:voiture|v[eé]lo)|[àa]\s+(?:pied|v[eé]lo))",
            query, _re_routing.IGNORECASE
        )
        if _route_implicit:
            _from = _route_implicit.group(1).strip()
            _to   = _route_implicit.group(2).strip()
            # Rejeter si mot-clé routing capturé par erreur
            if (_from.lower() not in _ROUTING_WORDS
                    and _to.lower() not in _ROUTING_WORDS
                    and _from.lower() not in _TRANSPORT_STOP
                    and _to.lower() not in _TRANSPORT_STOP):
                log.info(f"[Routing] route implicite détectée: '{_from}' → '{_to}'")
                routing_ctx["_intent"]     = "route"
                routing_ctx["_from_place"] = _from
                routing_ctx["_to_place"]   = _to
                # Pour route implicite : RoutingAgent gérera le geocodage des 2 villes
                # On passe directement sans geocoder ici
                agent  = RoutingAgent()
                result = await agent.run(query, routing_ctx, rag_tools)
                # Sauvegarder le center en session
                try:
                    for tc in result.get("tool_calls", []):
                        if isinstance(tc, dict) and tc.get("args", {}).get("center"):
                            session.extra = getattr(session, "extra", {}) or {}
                            session.extra["last_center"] = tc["args"]["center"]
                            break
                    mem.save(session)
                except Exception: pass
                total_ms = int((time.time() - t_total) * 1000)
                log.info(f"[Orchestrateur] ✓ {total_ms}ms | domain=routing | route_implicit")
                return result
            else:
                log.debug(f"[Routing] route implicite ignorée (mot-clé): '{_from}' / '{_to}'")

        # ══════════════════════════════════════════════════════
        # ÉTAPE B — Geocoder le lieu pour isochrone
        # Jamais de fallback silencieux si lieu fourni mais introuvable
        # ══════════════════════════════════════════════════════
        if _ir_place:
            # Détecter si c'est une adresse précise (numéro + rue)
            _is_address = bool(_re_routing.search(
                r"\d+\s+(?:rue|avenue|boulevard|allée|impasse|chemin|place|route|voie)",
                _ir_place, _re_routing.IGNORECASE
            ))

            # Enrichir avec le pays si ville courte
            _place_query = _ir_place
            if len(_ir_place.split()) <= 2 and "," not in _ir_place:
                _place_query = f"{_ir_place}, France"

            _resolved_center = None
            try:
                _gr = _req_geocode.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={
                        "q":            _place_query,
                        "format":       "json",
                        "limit":        1,
                        "countrycodes": "fr",
                    },
                    headers={"User-Agent": "OpenMapAgents/1.0"},
                    timeout=6,
                )
                _gresults = _gr.json()
                if _gresults:
                    _resolved_center = [float(_gresults[0]["lon"]), float(_gresults[0]["lat"])]
                    log.info(f"[Routing] geocodé '{_place_query}' → {_resolved_center}")
                else:
                    log.warning(f"[Routing] Nominatim: aucun résultat pour '{_place_query}'")
            except Exception as _eg:
                log.warning(f"[Routing] geocode '{_place_query}' échoué: {_eg}")

            if _resolved_center:
                routing_ctx["_resolved_center"] = _resolved_center
                routing_ctx["map_center"]       = _resolved_center
            else:
                # Lieu introuvable → demander précision, jamais de fallback silencieux
                log.warning(f"[Routing] Lieu introuvable: '{_ir_place}'")
                # Sauvegarder l'état de clarification si adresse précise
                if _is_address:
                    try:
                        session.extra = getattr(session, "extra", {}) or {}
                        session.extra["pending_routing_clarif"] = {
                            "time_minutes":   routing_ctx.get("_time_minutes", 10),
                            "profile":        routing_ctx.get("_profile", "foot"),
                            "original_query": query,
                        }
                        mem.save(session)
                    except Exception: pass
                    total_ms = int((time.time() - t_total) * 1000)
                    return {
                        "text": (
                            f"Je n'ai pas trouvé l'adresse **\"{_ir_place}\"**.\n\n"
                            f"Pouvez-vous préciser :\n"
                            f"- L'orthographe exacte (ex: *8 rue de la Paix*)\n"
                            f"- Le code postal (ex: *8 rue de la Paix, 35000 Rennes*)\n"
                            f"- Ou un point de repère proche (ex: *gare de Rennes*)"
                        ),
                        "tool_calls":   [],
                        "tool_results": [],
                        "_needs_clarification": True,
                    }
                else:
                    total_ms = int((time.time() - t_total) * 1000)
                    return {
                        "text": (
                            f"Je n'ai pas trouvé **\"{_ir_place}\"**.\n\n"
                            f"Pouvez-vous préciser le lieu ?\n"
                            f"Exemple : *gare de Rennes*, *place de la République Rennes*, *35000 Rennes*"
                        ),
                        "tool_calls":   [],
                        "tool_results": [],
                        "_needs_clarification": True,
                    }

        # ── Déléguer à RoutingAgent ───────────────────────────
        agent  = RoutingAgent()
        result = await agent.run(query, routing_ctx, rag_tools)

        # Sauvegarder le center et les params isochrone dans la session
        try:
            for tc in result.get("tool_calls", []):
                if isinstance(tc, dict) and tc.get("args", {}).get("center"):
                    session.extra = getattr(session, "extra", {}) or {}
                    session.extra["last_center"]          = tc["args"]["center"]
                    session.extra["last_iso_time"]        = tc["args"].get("time_minutes", 10)
                    session.extra["last_iso_profile"]     = tc["args"].get("profile", "foot")
                    session.extra["last_iso_intervals"]   = tc["args"].get("intervals", [])
                    break
            mem.save(session)
        except Exception as e:
            log.debug(f"[Routing] session save: {e}")

        total_ms = int((time.time() - t_total) * 1000)
        log.info(f"[Orchestrateur] ✓ {total_ms}ms | domain=routing | RoutingAgent")
        return result


    # ─── CLIP SPATIAL — "X dans l'isochrone/zone" ───────────

    def _resolve_clip_layer(
        self,
        query:       str,
        map_context: Optional[dict],
        session,
        mem,
    ) -> dict:
        """
        Workflow en 3 étapes pour "X dans l'isochrone/zone/buffer".

        Étape 1 — Identifier la couche polygone cible sur la carte
            → 0 couche  : message d'erreur
            → 1 couche  : utiliser directement
            → N couches : demander clarification (mémoriser dans session)

        Étape 2 — Construire le plan query_overture avec :
            - bbox de la couche
            - category extraite de la query
            - clip_to_layer = nom de la couche

        Étape 3 — Retourner le plan avec layer_name approprié

        Retourne :
            {"status": "ok",    "plan": {...}, "message": "..."}
            {"status": "clarify", "question": "...", "layers": [...]}
            {"status": "no_layer", "message": "..."}
        """
        import re as _re

        layers = (map_context or {}).get("layers", [])

        # ══════════════════════════════════════════════════════
        # ÉTAPE 1 — Trouver la/les couche(s) polygone candidates
        # ══════════════════════════════════════════════════════

        # Types de couches qui peuvent être utilisées comme zone de clip
        _POLYGON_TYPES = {"isochrone","buffer","zone","polygon","drawing","analyse"}

        # Chercher d'abord les couches isochrones
        # App.jsx envoie "theme" (pas "type") dans mapCtx.layers
        def _is_iso(l):
            n = l.get("name","").lower()
            t = l.get("theme", l.get("type","")).lower()
            # Exclure les couches résultats clip ("places — Isochrone", "restaurant — Isochrone")
            # Ces couches ont theme="data" ou contiennent " — " avant "isochrone"
            if " — " in n or " - " in n:
                return False   # "places — Isochrone 10min" → résultat clip, pas une zone
            return "sochrone" in n or "sochrone" in t or t == "isochrone"
        
        def _is_route(l):
            n = l.get("name","").lower()
            t = l.get("theme", l.get("type","")).lower()
            # Exclure couches résultats clip
            if " — " in l.get("name","") or " - " in l.get("name",""):
                return False
            return t == "route" or "itin" in n or "itin" in t
                
        def _is_poly(l):
            n = l.get("name","").lower()
            t = l.get("theme", l.get("type","")).lower()
            if " — " in l.get("name","") or " - " in l.get("name",""):
                return False
            if t in ("data","places","markers","world_data","analysis","route"):
                return False
            return (
                any(kw in t for kw in _POLYGON_TYPES) or
                any(kw in n for kw in ["zone","buffer","tampon","aire","polygon","drawing"])
            )

        iso_layers   = [l for l in layers if isinstance(l,dict) and _is_iso(l)   and l.get("bbox")]
        route_layers = [l for l in layers if isinstance(l,dict) and _is_route(l) and l.get("bbox")]
        poly_layers  = iso_layers or [l for l in layers if isinstance(l,dict) and _is_poly(l) and l.get("bbox")]
        log.info(f"[ClipLayer] {len(layers)} couches totales, {len(iso_layers)} isochrones, {len(route_layers)} routes, {len(poly_layers)} polygones candidats")

        # ── Couche route → demander distance buffer (défaut 300m) ──
        if not poly_layers and route_layers:
            _pending_buf = (getattr(session, "extra", {}) or {}).get("pending_buffer_clarif")
            if _pending_buf:
                import re as _re_buf
                _m_dist = _re_buf.search(
                    r"(\d+(?:[.,]\d+)?)\s*(m\b|km\b|mètre|metre|kilometre|kilomètre)?",
                    query, _re_buf.IGNORECASE
                )
                if _m_dist:
                    _val  = float(_m_dist.group(1).replace(",","."))
                    _unit = (_m_dist.group(2) or "m").lower()
                    _buf_m = int(_val * 1000 if "km" in _unit or "kilo" in _unit else _val)
                else:
                    _buf_m = 300  # défaut 300m si réponse incompréhensible
                try:
                    session.extra.pop("pending_buffer_clarif", None)
                    mem.save(session)
                except Exception: pass
                poly_layers = [{**_pending_buf["layer"], "_buffer_m": _buf_m}]
                log.info(f"[ClipLayer] buffer route réponse: {_buf_m}m")
            else:
                # Première fois → demander la distance (défaut 300m)
                _route_layer = route_layers[0]
                _route_name  = _route_layer.get("name", "l'itinéraire")
                try:
                    session.extra = getattr(session, "extra", {}) or {}
                    session.extra["pending_buffer_clarif"] = {
                        "layer":      _route_layer,
                        "orig_query": query,
                    }
                    mem.save(session)
                except Exception: pass
                return {
                    "status":   "clarify",
                    "question": (
                        f"À quelle distance autour de **{_route_name}** voulez-vous chercher ?\n"
                        f"_(défaut : 300m si vous ne précisez pas)_\n"
                        f"Ex : *200m*, *500m*, *1km*"
                    ),
                    "layers": route_layers,
                }

        if not poly_layers:
            return {
                "status":  "no_layer",
                "message": (
                    "Aucune couche polygone (isochrone, buffer, zone) sur la carte. "
                    "Créez d'abord une isochrone : _'isochrone 10 min à pied'_"
                ),
            }

        # Vérifier si une clarification est en attente
        _pending = (getattr(session, "extra", {}) or {}).get("pending_clip_clarif")
        if _pending:
            import re as _re_cl
            q_low = query.lower()
            chosen = None
            pending_layers = _pending.get("layers", [])

            # Extraire nom explicite après "couche", "layer", "utilise"
            _m_explicit = _re_cl.search(
                r"(?:couche|layer|utilise|prends)\s+(.{3,50}?)(?:\s*$|[,;])",
                query, _re_cl.IGNORECASE
            )
            if _m_explicit:
                _expl = _m_explicit.group(1).strip().lower()
                for l in pending_layers:
                    lname = l.get("name","").lower()
                    if lname == _expl or _expl in lname or lname in _expl:
                        chosen = l
                        break

            # Correspondance nom exact ou partiel
            if not chosen:
                for l in pending_layers:
                    name = l.get("name","").lower()
                    if name == q_low.strip() or name in q_low:
                        chosen = l
                        break
            if not chosen:
                for l in pending_layers:
                    name = l.get("name","").lower()
                    words = [w for w in name.split() if len(w) > 3]
                    if any(w in q_low for w in words):
                        chosen = l
                        break
            # Défaut : première couche isochrone pure
            if not chosen:
                chosen = next(
                    (l for l in pending_layers if _is_iso(l)),
                    pending_layers[0] if pending_layers else None
                )
            try:
                session.extra.pop("pending_clip_clarif", None)
                mem.save(session)
            except Exception: pass
            if chosen:
                poly_layers = [chosen]

        if len(poly_layers) > 1:
            # Plusieurs couches → demander clarification
            names = [l.get("name","?") for l in poly_layers[:4]]
            question = (
                f"Plusieurs zones disponibles sur la carte : "
                + ", ".join(f"**{n}**" for n in names)
                + ". Laquelle utiliser ?"
            )
            try:
                session.extra = getattr(session, "extra", {}) or {}
                session.extra["pending_clip_clarif"] = {"layers": poly_layers[:4]}
                mem.save(session)
            except Exception: pass
            return {"status": "clarify", "question": question, "layers": poly_layers}

        # ══════════════════════════════════════════════════════
        # ÉTAPE 2 — Extraire la catégorie et construire le plan
        # ══════════════════════════════════════════════════════
        clip_layer = poly_layers[0]
        clip_name  = clip_layer.get("name", "Zone")
        clip_bbox  = clip_layer.get("bbox")   # [xmin, ymin, xmax, ymax]
        _buffer_m  = clip_layer.get("_buffer_m", 0)  # 0 = pas de buffer (polygone normal)

        q = query.lower()

        # ── Détecter le theme et tool Overture selon la requête ──
        _THEME_MAP = [
            ("buildings", "query_buildings", "Bâtiments", [
                "bâtiment","batiment","building","immeuble","construction",
                "édifice","edifice","logement","maison","habitation","structure"
            ]),
            ("transportation", "query_roads", "Routes", [
                "route","voie","chemin","boulevard","avenue",
                "road","street","highway","réseau routier"
            ]),
            ("divisions", "query_divisions", "Divisions", [
                "commune","quartier","arrondissement","division",
                "département","region","territoire"
            ]),
        ]

        detected_theme = "places"
        detected_tool  = "query_places"
        detected_label = "POI"

        for theme_name, tool_name, label, keywords in _THEME_MAP:
            if any(kw in q for kw in keywords):
                detected_theme = theme_name
                detected_tool  = tool_name
                detected_label = label
                break

        # Catégorie POI (uniquement pour places)
        category  = None
        cat_label = detected_label

        if detected_theme == "places":
            _CATS = [
                ("restaurant", ["restaurant","resto","brasserie","bistrot"]),
                ("cafe",        ["café","cafe","coffee"]),
                ("bar",         ["bar","pub","bière"]),
                ("hotel",       ["hôtel","hotel","hébergement"]),
                ("pharmacy",    ["pharmacie","pharmacy"]),
                ("hospital",    ["hôpital","hopital","clinique"]),
                ("school",      ["école","ecole","collège","lycée","université"]),
                ("bank",        ["banque","bank"]),
                ("supermarket", ["supermarché","supermarche","épicerie"]),
                ("museum",      ["musée","musee","museum"]),
                ("park",        ["parc","park","jardin"]),
                ("parking",     ["parking","stationnement"]),
                ("place_of_worship", ["mosquée","mosquee","église","eglise","cathédrale"]),
            ]
            for cat, kws in _CATS:
                if any(kw in q for kw in kws):
                    category  = cat
                    cat_label = cat.replace("_"," ").title()
                    break
            if not category and any(w in q for w in ["commerce","magasin","boutique","shop","poi"]):
                cat_label = "Commerces"


        # Nom de couche résultat
        layer_name = f"{cat_label.replace(' ','_')}_dans_{clip_name.replace(' ','_')}"

        # ══════════════════════════════════════════════════════
        # ÉTAPE 3 — Plan
        # ══════════════════════════════════════════════════════
        args = {
            "theme":         detected_theme,
            "clip_to_layer": clip_name,
            "limit":         2000,
        }
        if category:
            args["category"] = category
        # Propager _buffer_m pour App.jsx (turf.buffer sur LineString)
        if _buffer_m:
            args["_buffer_m"]      = _buffer_m
            args["_clip_is_route"] = True
        if clip_bbox and len(clip_bbox) == 4:
            # Élargir la bbox selon le buffer pour récupérer assez de features
            _pad_deg = (_buffer_m / 111000) if _buffer_m else 0
            dx = (clip_bbox[2] - clip_bbox[0]) * 0.1 + _pad_deg
            dy = (clip_bbox[3] - clip_bbox[1]) * 0.1 + _pad_deg
            args["bbox"]  = clip_bbox
            args["xmin"]  = clip_bbox[0] - dx
            args["ymin"]  = clip_bbox[1] - dy
            args["xmax"]  = clip_bbox[2] + dx
            args["ymax"]  = clip_bbox[3] + dy

        args["_layer_name"]    = layer_name
        args["_clip_to_layer"] = clip_name

        return {
            "status":     "ok",
            "layer_name": layer_name,
            "clip_name":  clip_name,
            "category":   cat_label,
            "plan": {
                "server":          "overture",
                "tool":            detected_tool,
                "args":            args,
                "output_action":   "add_markers" if detected_theme == "places" else "add_layer",
                "secondary_steps": [],
                "_geocoded_place": clip_name,
                "_layer_name":     layer_name,
                "_buffer_m":       _buffer_m,   # propagé → App.jsx doClipAndAdd
            },
            "confidence":          0.92,
            "needs_clarification": False,
            "corrections_applied": [f"clip:{clip_name}"],
            "warnings":            [],
            "message_utilisateur": query,
        }


    # ─── RÉSOLUTION GÉOGRAPHIQUE VIA LLM ─────────────────────

    def _resolve_geo_llm(self, query: str, map_context: Optional[dict]) -> dict:
        """
        Étape 1 — Comprendre la géographie de la requête via LLM.
        Étape 2 — Geocoder si nécessaire via Nominatim.
        Étape 3 — Retourner bbox + params prêts à l'emploi.

        Le LLM répond un JSON minimal (< 60 tokens).
        Fallback déterministe si LLM indisponible.

        Retourne :
        {
            "geo_type":  "monument|address|city|region|country|global|none",
            "place":     "requête geocodage ou vide",
            "relation":  "around|in|at|none",
            "radius_m":  500,
            "bbox":      [xmin,ymin,xmax,ymax] ou null,
            "center":    [lon,lat] ou null,
            "used_map":  bool  — True si bbox vient de la carte
        }
        """
        import json, requests as _req
        from litellm import completion

        # ── 1. LLM extrait la géographie ──────────────────────
        LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openrouter")
        _MODEL_MAP = {
            "claude":     os.getenv("CLAUDE_MODEL",    "claude-sonnet-4-20250514"),
            "openai":     os.getenv("OPENAI_MODEL",    "gpt-4o"),
            "openrouter": os.getenv("OPENROUTER_MODEL","openrouter/google/gemma-4-26b-a4b-it:free"),
            "deepseek":   os.getenv("DEEPSEEK_MODEL",  "deepseek/deepseek-chat"),
            "mistral":    os.getenv("MISTRAL_MODEL",   "mistral/mistral-large-latest"),
            "ollama":     os.getenv("OLLAMA_MODEL",    "ollama/llama3.1"),
        }
        model = _MODEL_MAP.get(LLM_PROVIDER, "claude-sonnet-4-20250514")

        system = """Tu es un extracteur géographique. Analyse la requête et retourne UNIQUEMENT ce JSON (rien d'autre) :
{
  "geo_type": "monument|address|city|region|country|global|none",
  "place": "texte exact à geocoder (vide si global/none)",
  "relation": "around|in|at|none",
  "radius_m": 500
}

Règles :
- monument : lieu nommé précis avec nom propre (Château des Ducs, Tour Eiffel, Sacré-Cœur)
- address : adresse avec numéro ou rue (15 rue de la Paix)
- city : ville ou commune (Nantes, Dakar, Paris)
- region : région/département/état (Bretagne, Loire-Atlantique, Île-de-France)
- country : pays (France, Sénégal, Maroc)
- global : données mondiales, par pays, monde entier
- none : aucune référence géographique explicite
- relation around → radius_m 500 par défaut, ajuster si précisé ("dans 1km" → 1000)
- place = texte EXACT pour Nominatim (inclure la ville si monument ambigu)"""

        geo = None
        try:
            resp = completion(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user",   "content": f"Requête: {query}"},
                ],
                max_tokens=80,
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or "{}"
            raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
            parsed = json.loads(raw)
            # Certains modèles retournent une liste au lieu d'un dict
            if isinstance(parsed, list):
                parsed = parsed[0] if parsed else {}
            geo = parsed
            log.info(f"[GeoLLM] {geo}")
        except Exception as e:
            log.warning(f"[GeoLLM] LLM indisponible: {e} → fallback regex")

        # ── Fallback déterministe si LLM échoue ───────────────
        if not geo:
            place = _extract_place_from_query(query)
            geo = {
                "geo_type":  "city" if place else "none",
                "place":     place,
                "relation":  "around" if any(w in query.lower() for w in ["autour","around","près","near"]) else "in",
                "radius_m":  500,
            }

        geo_type = geo.get("geo_type", "none")
        place    = (geo.get("place") or "").strip()
        relation = geo.get("relation", "none")
        radius_m = int(geo.get("radius_m") or 500)

        result = {
            "geo_type": geo_type,
            "place":    place,
            "relation": relation,
            "radius_m": radius_m,
            "bbox":     None,
            "center":   None,
            "used_map": False,
        }

        # ── 2. Geocoder si nécessaire ─────────────────────────
        if geo_type in ("monument","address","city","region","country") and place:
            try:
                resp = _req.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": place, "format": "json", "limit": 1, "addressdetails": 1},
                    headers={"User-Agent": "OpenMapAgents/1.0"},
                    timeout=8,
                )
                results = resp.json()
                if results:
                    r    = results[0]
                    lat  = float(r["lat"])
                    lon  = float(r["lon"])
                    bb   = r.get("boundingbox", [])
                    result["center"] = [lon, lat]

                    if geo_type in ("monument", "address"):
                        # Point précis → bbox centrée + radius
                        d = radius_m / 111_000
                        result["bbox"] = [lon-d, lat-d, lon+d, lat+d]
                    elif relation == "around" and len(bb) == 4:
                        # "autour de" une ville → recentrer sur le point
                        d = radius_m / 111_000
                        result["bbox"] = [lon-d, lat-d, lon+d, lat+d]
                    elif len(bb) == 4:
                        # Ville/région/pays → bbox Nominatim directe
                        result["bbox"] = [float(bb[2]), float(bb[0]),
                                          float(bb[3]), float(bb[1])]
                    else:
                        d = radius_m / 111_000
                        result["bbox"] = [lon-d, lat-d, lon+d, lat+d]

                    log.info(
                        f"[GeoLLM] Geocodé '{place}' ({geo_type}) → "
                        f"[{lat:.4f},{lon:.4f}] bbox={[round(x,4) for x in result['bbox']]}"
                    )
                else:
                    log.warning(f"[GeoLLM] Nominatim: aucun résultat pour '{place}'")
            except Exception as e:
                log.warning(f"[GeoLLM] Geocoding échoué: {e}")

        # ── 2bis. Fallback center depuis la session (dernier lieu) ──
        # "isochrone 10 min" sans lieu → utiliser le dernier lieu geocodé
        if result["center"] is None and result["bbox"] is None:
            # Chercher dans les messages précédents un lieu connu
            _last_center = (map_context or {}).get("_last_center")
            if _last_center:
                result["center"] = _last_center
                d = 0.01
                lon2, lat2 = _last_center
                result["bbox"] = [lon2-d, lat2-d, lon2+d, lat2+d]
                log.info(f"[GeoLLM] center depuis session: {_last_center}")

        # ── 3. Fallback : bbox de la carte courante ───────────
        if result["bbox"] is None and geo_type not in ("global",):
            map_bbox = (map_context or {}).get("bbox")
            if not map_bbox:
                # Reconstituer depuis le centre + zoom si disponible
                center = (map_context or {}).get("center")
                zoom   = (map_context or {}).get("zoom", 10)
                if center and len(center) == 2:
                    lon, lat = center
                    d = max(0.01, 0.5 / (2 ** max(0, zoom - 8)))
                    map_bbox = [lon-d, lat-d, lon+d, lat+d]
            if map_bbox and len(map_bbox) == 4:
                result["bbox"]     = list(map_bbox)
                result["used_map"] = True
                log.info(f"[GeoLLM] Aucun lieu → bbox carte courante: {[round(x,3) for x in map_bbox]}")

        return result


    def _build_fallback_plan(
        self,
        query:      str,
        route:      dict,
        rag_tools:  list,
        context:    dict,
        map_context: Optional[dict] = None,
    ) -> dict:
        """
        Plan déterministe. Utilise _resolve_geo_llm pour comprendre
        la géographie de la requête en 3 étapes :
          1. LLM classifie (monument/ville/région/global/none)
          2. Nominatim geocode si nécessaire
          3. Fallback bbox carte si aucun lieu trouvé
        """
        domain = route.get("domain", "overture")
        server = DOMAIN_TO_SERVER.get(domain, "overture")
        q      = query.lower()

        # ══════════════════════════════════════════════════════
        # ÉTAPE A — Choisir le tool selon le domaine
        # ══════════════════════════════════════════════════════
        _STATIC_GEE_TOOLS = {
            "compute_worldcover","compute_esa_worldcover",
            "compute_canopy_height","compute_canopy",
            "compute_forest_watch","compute_forest_cover",
            "compute_srtm","compute_dem","compute_elevation",
            "compute_hansen","compute_global_surface_water",
        }

        if domain == "gee":
            server = "gee"
            if any(t in q for t in ["lst","chaleur urbaine","îlot de chaleur","icu",
                                    "temperature de surface","température de surface",
                                    "surface temperature","land surface temperature"]):
                tool, action = "compute_lst",          "add_layer"
            elif any(t in q for t in ["ndwi","mndwi","inondation","eau","water index"]):
                tool, action = "compute_ndwi",         "add_layer"
            elif "timelapse" in q:
                tool, action = "compute_timelapse",    "add_timelapse"
            elif any(t in q for t in ["worldcover","occupation du sol","land cover","esa"]):
                tool, action = "compute_worldcover",   "add_layer"
            elif any(t in q for t in ["canopée","canopy","hauteur arbres","hauteur des arbres"]):
                tool, action = "compute_canopy_height","add_layer"
            elif any(t in q for t in ["foret","forêt","forest","hansen","déforestation","tree cover"]):
                tool, action = "compute_forest_watch", "add_layer"
            elif any(t in q for t in ["srtm","mnt","relief satellite","elevation satellite"]):
                tool, action = "compute_srtm",         "add_layer"
            elif any(t in q for t in ["nbr","burn","feu","incendie"]):
                tool, action = "compute_nbr",          "add_layer"
            elif any(t in q for t in ["sar","radar","sentinel-1"]):
                tool, action = "compute_sar",          "add_layer"
            elif any(t in q for t in ["evi"]):
                tool, action = "compute_evi",          "add_layer"
            else:
                tool, action = "compute_ndvi",         "add_layer"

        elif domain == "local":
            # Géré en amont par _resolve_local_layer — ne devrait pas arriver ici
            tool, action = "thematic_analysis", "thematic_analysis"
            server = "local"

        else:
            # Valider le RAG : n'utiliser que si cohérent avec le domaine
            _domain_servers = {
                "gee":       {"gee","stac"},
                "worldbank": {"worldbank"},
                "routing":   {"ors","routing"},
                "osm":       {"osm"},
                "spatial":   {"postgis","spatial"},
                "elevation": {"maptiler","elevation"},
                "overture":  {"overture","nominatim"},
            }
            allowed = _domain_servers.get(domain, {"overture"})
            rag_ok = rag_tools and rag_tools[0].get("server","") in allowed

            if rag_ok:
                top    = rag_tools[0]
                server = top.get("server", server)
                tool   = top.get("tool",   "query_places")
                action = top.get("output_action", "add_markers")
            else:
                if rag_tools:
                    log.debug(f"[fallback] RAG ignoré ({rag_tools[0].get('server')} ≠ {domain})")
                _defaults = {
                    "worldbank": ("get_indicator",         "add_choropleth"),
                    "routing":   ("compute_isochrone",     "add_isochrone"),
                    "overture":  ("query_places",          "add_markers"),
                    "osm":       ("get_amenities",         "add_markers"),
                    "spatial":   ("spatial_buffer",        "add_layer"),
                    "elevation": ("get_elevation_profile", "show_elevation_profile"),
                }
                tool, action = _defaults.get(domain, ("query_places","add_markers"))

        # ══════════════════════════════════════════════════════
        # ÉTAPE B — Résolution géographique via LLM
        # ══════════════════════════════════════════════════════
        geo = self._resolve_geo_llm(query, map_context)
        bbox         = geo.get("bbox")
        center       = geo.get("center")
        geo_type     = geo.get("geo_type", "none")
        geo_place    = geo.get("place", "")
        used_map     = geo.get("used_map", False)

        # ══════════════════════════════════════════════════════
        # ÉTAPE C — Construire les args de l'outil
        # ══════════════════════════════════════════════════════
        args = {}

        if bbox:
            args["bbox"] = bbox
            args.update({"xmin":bbox[0],"ymin":bbox[1],"xmax":bbox[2],"ymax":bbox[3]})
        # ORS (routing) ne prend pas de bbox — seulement center
        if server == "ors":
            for _k in ["bbox","xmin","ymin","xmax","ymax"]:
                args.pop(_k, None)

        # ── Overture : catégorie + center+radius si monument ──
        if domain == "overture":
            _OVERTURE_CATS = [
                ("restaurant", ["restaurant","resto","brasserie","bistrot"]),
                ("cafe",        ["café","cafe","coffee"]),
                ("bar",         ["bar","pub","bière","beer"]),
                ("hotel",       ["hôtel","hotel","hébergement","auberge"]),
                ("supermarket", ["supermarché","supermarche","épicerie","marché"]),
                ("pharmacy",    ["pharmacie","pharmacy"]),
                ("hospital",    ["hôpital","hopital","clinique","urgences"]),
                ("school",      ["école","ecole","collège","lycée","université"]),
                ("bank",        ["banque","bank","distributeur"]),
                ("museum",      ["musée","musee","museum","galerie"]),
                ("park",        ["parc","park","jardin","square"]),
                ("parking",     ["parking","stationnement"]),
                ("place_of_worship", ["mosquée","mosquee","église","eglise","cathédrale","temple"]),
                ("sports_center",    ["stade","piscine","salle de sport","gymnase"]),
            ]
            for cat, kws in _OVERTURE_CATS:
                if any(kw in q for kw in kws):
                    args["category"] = cat
                    break

            # Pour monument/adresse : center+radius plutôt que bbox large
            if geo_type in ("monument","address") and center:
                args["center_lon"] = center[0]
                args["center_lat"] = center[1]
                args["radius_m"]   = geo.get("radius_m", 500)
                # Garder aussi la bbox pour compatibilité
            args.setdefault("limit", 500)

            # Détection "dans l'isochrone/zone" → clip sur la couche isochrone
            import re as _re_clip
            if _re_clip.search(
                r"dans\s+l['']isochrone|dans\s+la\s+zone|dans\s+le\s+buffer|"
                r"dans\s+l['']aire|within.*isochrone",
                q, _re_clip.IGNORECASE
            ):
                # Chercher une couche isochrone active dans le contexte
                active = context.get("active_layers", [])
                iso_layer = next(
                    (l.get("name","") if isinstance(l,dict) else str(l)
                     for l in reversed(active)
                     if "sochrone" in (l.get("name","") if isinstance(l,dict) else str(l))),
                    "Isochrone"
                )
                args["clip_to_layer"] = iso_layer
                log.info(f"[fallback] clip_to_layer='{iso_layer}' détecté")

        # ── Routing : time_minutes, profile, center ──────────
        # ── Routing : time_minutes, profile, center ──────────
        if domain == "routing":
            import re as _re_rt
            import requests as _req_rt

            # Durée
            m_dur = _re_rt.search(r"(\d+)\s*(?:min|minute|mn)", q)
            time_min = int(m_dur.group(1)) if m_dur else 10

            # Profil
            if any(w in q for w in ["voiture","car","auto"]):
                prof = "car"
            elif any(w in q for w in ["vélo","velo","bike","bicyclette"]):
                prof = "bike"
            else:
                prof = "foot"

            # ── Extraction du lieu dans la query ─────────────────
            _TRANSPORT_STOP = {
                "pied","vélo","velo","voiture","car","bike","foot",
                "walking","ici","là","la","moi","nous","carte","map",
            }
            _lieu_patterns_rt = [
                # "depuis Rennes", "from Lyon"
                r"(?:depuis|from)\s+([a-zA-ZÀ-ÿ][a-zA-ZÀ-ÿ\s\-]{1,40}?)(?=\s+(?:en\s+\w{3,}|[àa]\s+\w{3,})|\s*$)",
                # "autour de Rennes", "autour du château"
                r"autour\s+(?:du|de\s+la|de\s+l.|d.|de)\s+([a-zA-ZÀ-ÿ][a-zA-ZÀ-ÿ\s\-]{1,40}?)(?=\s*$)",
                # "à pied de Rennes", "à pied de la gare"
                r"[àa]\s+pied\s+(?:de\s+la|du|de\s+l.|d.|de|depuis)\s+([a-zA-ZÀ-ÿ][a-zA-ZÀ-ÿ\s\-]{1,40}?)(?=\s*$)",
                # "isochrone 10 min à pied de Rennes"
                r"(?:de\s+la|du|de\s+l.|d.|de)\s+([a-zA-ZÀ-ÿ][a-zA-ZÀ-ÿ\s\-]{1,40}?)(?=\s+(?:en\s+\w{3,}|[àa]\s+\w{3,})|\s*$)",
                # dernier recours : dernier mot/groupe significatif
                r"(?:de|depuis|d.)\s+([a-zA-ZÀ-ÿ][a-zA-ZÀ-ÿ\s\-]{1,30}?)\s*$",
            ]
            _routing_place = ""
            for _pat in _lieu_patterns_rt:
                _m = _re_rt.search(_pat, query, _re_rt.IGNORECASE | _re_rt.UNICODE)
                if _m:
                    _cand = _m.group(1).strip()
                    # Nettoyer les mots de transport en fin de capture
                    _cand = _re_rt.sub(
                        r"\s+(à\s+pied|en\s+voiture|à\s+vélo|pied|vélo|velo|voiture|foot|bike|car)\s*$",
                        "", _cand, flags=_re_rt.IGNORECASE
                    ).strip()
                    if _cand and _cand.lower() not in _TRANSPORT_STOP and len(_cand) > 2:
                        _routing_place = _cand
                        break

            routing_center = None
            geocoded_place = ""

            # ── PRIORITÉ 1 : lieu explicite dans la query → geocoder ─
            if _routing_place:
                try:
                    _resp = _req_rt.get(
                        "https://nominatim.openstreetmap.org/search",
                        params={"q": _routing_place, "format": "json", "limit": 1},
                        headers={"User-Agent": "OpenMapAgents/1.0"},
                        timeout=6,
                    )
                    _results = _resp.json()
                    if _results:
                        routing_center = [float(_results[0]["lon"]), float(_results[0]["lat"])]
                        geocoded_place = _routing_place
                        log.info(f"[fallback] routing geocodé '{_routing_place}' → {routing_center}")
                    else:
                        log.warning(f"[fallback] Nominatim: aucun résultat pour '{_routing_place}'")
                except Exception as _e:
                    log.warning(f"[fallback] routing geocode '{_routing_place}' échoué: {_e}")

            # ── PRIORITÉ 2 : centre de la carte (aucun lieu fourni ou geocode échoué) ─
            if not routing_center:
                if (map_context or {}).get("center"):
                    mc = map_context["center"]
                    routing_center = [float(mc[0]), float(mc[1])]
                    log.info(f"[fallback] routing center map_context: {routing_center}")
                elif (map_context or {}).get("bbox"):
                    bb = map_context["bbox"]
                    routing_center = [(bb[0]+bb[2])/2, (bb[1]+bb[3])/2]
                    log.info(f"[fallback] routing center bbox: {routing_center}")
                elif context.get("last_center"):
                    routing_center = context["last_center"]
                    log.info(f"[fallback] routing center session: {routing_center}")

            # Réinitialiser args — ORS n'accepte que center, jamais bbox
            # Construire intervals automatiquement par tranches de 5 min
            _intervals = list(range(5, time_min + 1, 5))
            if not _intervals or _intervals[-1] != time_min:
                _intervals.append(time_min)
            _intervals = sorted(set(_intervals)) 

            args = {"time_minutes": time_min,  "intervals": _intervals, "profile": prof}
            if routing_center:
                args["center"] = routing_center
                for _k in ["bbox","xmin","ymin","xmax","ymax"]:
                    args.pop(_k, None)
            else:
                log.warning("[fallback] routing: aucun center disponible")
                return {
                    "plan": {
                        "server": "ors", "tool": "compute_isochrone",
                        "args": args, "output_action": "add_isochrone",
                        "secondary_steps": [],
                    },
                    "confidence": 0.3,
                    "needs_clarification": True,
                    "clarification_question": (
                        "Pour créer une isochrone, précisez un point de départ. "
                        "Exemple : 'isochrone 10 min à pied depuis la Tour Eiffel' "
                        "ou centrez la carte sur votre zone."
                    ),
                    "warnings": [],
                    "message_utilisateur": "",
                    "corrections_applied": [],
                }

        # ── Dates : uniquement GEE temporel ───────────────────
        _DOMAINS_NO_DATES = {"overture","osm","worldbank","nominatim","spatial","routing","elevation"}
        if domain not in _DOMAINS_NO_DATES and tool not in _STATIC_GEE_TOOLS:
            dates = _extract_dates_from_query(query)
            if dates:
                args.update(dates)
            elif context.get("current_dates") and domain == "gee":
                args.update(context["current_dates"])

        # ── Args GEE spécifiques ───────────────────────────────
        if domain == "gee":
            if tool not in _STATIC_GEE_TOOLS:
                args.setdefault("collection",  "COPERNICUS/S2_SR_HARMONIZED")
                args.setdefault("cloud_cover", 20)
                args.setdefault("composite",   "median")
            else:
                for k in ("collection","cloud_cover","composite","date_start","date_end"):
                    args.pop(k, None)
            if bbox:
                args["clip_bbox"] = bbox

        # ── Clarification : uniquement GEE temporel sans bbox ─
        _gee_temporal = (domain == "gee" and tool not in _STATIC_GEE_TOOLS)
        _needs_clarif = _gee_temporal and not bool(args.get("bbox"))

        log.info(
            f"[fallback] domain={domain} tool={tool} geo_type={geo_type} "
            f"place='{geo_place}' bbox={'oui' if bbox else 'non'} "
            f"used_map={used_map} category={args.get('category','')}"
        )

        return {
            "plan": {
                "server":          server,
                "tool":            tool,
                "args":            args,
                "output_action":   action,
                "secondary_steps": [],
                "_geocoded_place": geo_place,
            },
            "confidence":          0.80 if (bbox and not used_map) else (0.65 if bbox else 0.4),
            "needs_clarification": _needs_clarif,
            "clarification_question": (
                "Précisez la zone géographique (ville, région) pour l'analyse satellite."
                if _needs_clarif else ""
            ),
            "warnings":            [],
            "message_utilisateur": query,
            "corrections_applied": ([f"geocoded:{geo_place}"] if geo_place else []),
        }


# ═══════════════════════════════════════════════════════════════
# INTERFACE PUBLIQUE — compatible agent.py existant
# ═══════════════════════════════════════════════════════════════

_orchestrator: Optional[Orchestrator] = None


def get_orchestrator() -> Orchestrator:
    """Retourne l'instance singleton (compatible agent.py)."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = Orchestrator()
    return _orchestrator


def orchestrate(
    messages:    list,
    map_context: Optional[dict] = None,
    session_id:  str            = "anon",
) -> dict:
    """
    Point d'entrée synchrone — appelé par agent.py :
        return orchestrate(req.messages, req.map_context)

    Retourne le même format que call_llm() :
        {"text": str, "tool_calls": [...], "tool_results": [...]}
    """
    return get_orchestrator().run_sync(
        messages, map_context
    )