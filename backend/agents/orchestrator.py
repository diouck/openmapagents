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
MCP_STEP_TIMEOUT     = int(os.getenv("MCP_STEP_TIMEOUT_S",      "30"))

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
    q = query.lower()

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
        geojson = result.get("geojson",{})
        if not geojson or geojson.get("type") != "FeatureCollection":
            log.warning(f"GeoJSON invalide pour {action}")
            return {
                **result,
                "_validation_warning": "GeoJSON invalide ou vide",
            }
        features = geojson.get("features",[])
        if len(features) == 0:
            return {
                **result,
                "_validation_warning": "GeoJSON vide (0 features)",
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
            primary_result["layer_name"] = " — ".join(name_parts)

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
    nl = "\n"
    """
    Construit une réponse enrichie pour les résultats vecteur Overture/OSM.
    Inclut : résumé, top 5, suggestions d'analyse.
    """
    features  = (primary.get("features") or
                 primary.get("geojson", {}).get("features") or [])
    n         = primary.get("feature_count") or primary.get("total") or len(features)
    category  = moderation.get("plan",{}).get("args",{}).get("category","")
    geo_place = moderation.get("corrections_applied",[""])
    geo_place = geo_place[0].replace("geocoded:","") if geo_place else ""

    if n == 0:
        loc = f" autour de **{geo_place}**" if geo_place else ""
        return (
            f"Aucun résultat trouvé{loc}. "
            f"Essayez d'élargir la zone ou de reformuler (ex : 'restaurants à Nantes centre')."
        )

    # ── Résumé ────────────────────────────────────────────────
    cat_label = category.replace("_"," ") if category else "éléments"
    loc_label = f" autour de **{geo_place}**" if geo_place else ""
    lines = [f"**{n} {cat_label}** trouvés{loc_label}."]

    # ── Top 5 ─────────────────────────────────────────────────
    top = [f for f in features[:5] if isinstance(f, dict)]
    if top:
        lines.append(nl + "**Les 5 premiers :**")
        for i, f in enumerate(top, 1):
            props = f.get("properties") or {}
            name  = (props.get("name") or props.get("names") or
                     props.get("primary") or props.get("label") or "")
            if isinstance(name, dict):
                name = name.get("primary","") or next(iter(name.values()),"")
            addr  = props.get("address") or props.get("addresses","")
            if isinstance(addr, dict):
                addr = addr.get("freeform","") or addr.get("street","")
            cat   = props.get("category","") or props.get("categories","")
            if isinstance(cat, dict):
                cat = cat.get("primary","")
            parts = [f"**{name}**" if name else f"*(sans nom)*"]
            if cat and cat != category:
                parts.append(f"_{cat}_")
            if addr:
                parts.append(addr[:40])
            lines.append(f"{i}. {' — '.join(parts)}")

    # ── Suggestions d'analyse ─────────────────────────────────
    lines.append(nl + "**Suggestions :**")
    if category in ("restaurant","cafe","bar"):
        lines.append(f"- Isochrone 10 min à pied depuis ici → zone de chalandise")
        lines.append(f"- Densité des {cat_label} par quartier → analyse spatiale")
    elif category in ("hospital","pharmacy","school"):
        lines.append(f"- Accessibilité : isochrone 15 min en voiture")
        lines.append(f"- Couverture spatiale → buffer 500m autour de chaque point")
    elif category == "parking":
        lines.append(f"- Isochrone 5 min à pied → zone desservie")
    else:
        lines.append(f"- Clustering des points → `analyse spatiale, clustering`")
        lines.append(f"- Zone tampon → `buffer 300m autour des {cat_label}`")

    if n > 50:
        lines.append(f"- Heatmap de densité → demandez `heatmap des {cat_label}`")

    return nl.join(lines)


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
        text = f"Erreur : {primary['error']}"

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

        route = _classify_domain(query, rag_tools)
        log.info(
            f"[2] Domain: {route['domain']} "
            f"({route['method']}, conf={route['confidence']})"
        )

        # ──────────────────────────────────────────────────────
        # ÉTAPE 3 — Court-circuit ROUTING : utiliser RoutingAgent dédié
        # RoutingAgent orchestre geocode + mcp_ors directement
        # sans passer par le débat LLM ni le fallback
        # ──────────────────────────────────────────────────────
        if route.get("domain") == "routing":
            try:
                result = await self._run_routing(
                    query, context, map_context, rag_tools, session, mem, t_total
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
        query:       str,
        context:     dict,
        map_context: Optional[dict],
        rag_tools:   list,
        session,
        mem,
        t_total:     float,
    ) -> Optional[dict]:
        """
        Délègue à RoutingAgent (mcp_nominatim + mcp_ors).
        Injecte map_center depuis session ou map_context avant d'appeler l'agent.
        """
        from agents.routing_agent import RoutingAgent

        # Enrichir le contexte avec map_center
        routing_ctx = dict(context)

        # Priorité 1 : dernier lieu geocodé en session
        if context.get("last_center"):
            routing_ctx["map_center"] = context["last_center"]

        # Priorité 2 : centre de la carte courante
        elif (map_context or {}).get("center"):
            mc = map_context["center"]
            routing_ctx["map_center"] = [float(mc[0]), float(mc[1])]

        log.info(
            f"[Routing] RoutingAgent | "
            f"map_center={routing_ctx.get('map_center')} | "
            f"query='{query[:50]}'"
        )

        agent  = RoutingAgent()
        result = await agent.run(query, routing_ctx, rag_tools)

        # Sauvegarder last_center si l'agent a géocodé un lieu
        try:
            for tr in result.get("tool_results", []):
                if isinstance(tr, dict):
                    c = tr.get("center") or tr.get("args", {}).get("center")
                    if c and len(c) == 2:
                        session.extra = getattr(session, "extra", {}) or {}
                        session.extra["last_center"] = c
                        break
            mem.update_from_response(session, result, query)
            mem.save(session)
        except Exception as e:
            log.warning(f"Session save routing: {e}")

        total_ms = int((time.time() - t_total) * 1000)
        log.info(f"[Orchestrateur] ✓ {total_ms}ms | domain=routing | RoutingAgent")
        return result


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

        # ── Routing : time_minutes, profile, center ──────────
        if domain == "routing":
            import re as _re_rt
            # Durée
            m_dur = _re_rt.search(r"(\d+)\s*(?:min|minute)", q)
            time_min = int(m_dur.group(1)) if m_dur else 10
            # Profil
            if any(w in q for w in ["voiture","car","auto"]):
                prof = "car"
            elif any(w in q for w in ["vélo","velo","bike","bicyclette"]):
                prof = "bike"
            else:
                prof = "foot"
            # Center : session → map_context → geo LLM
            # JAMAIS de bbox pour routing — ORS n'accepte que center=[lon,lat]
            routing_center = None
            if context.get("last_center"):
                routing_center = context["last_center"]
                log.info(f"[fallback] routing center session: {routing_center}")
            elif center:  # geo LLM
                routing_center = center
            elif (map_context or {}).get("center"):
                mc = map_context["center"]
                routing_center = [float(mc[0]), float(mc[1])]
                log.info(f"[fallback] routing center map: {routing_center}")
            # Réinitialiser args — pas de bbox pour ORS
            args = {
                "time_minutes": time_min,
                "profile":      prof,
            }
            if routing_center:
                args["center"] = routing_center
            else:
                log.warning("[fallback] routing: aucun center disponible")
                # Pas de center = pas d'isochrone possible
                return {
                    "plan": {"server":"ors","tool":"compute_isochrone",
                             "args":args,"output_action":"add_isochrone","secondary_steps":[]},
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