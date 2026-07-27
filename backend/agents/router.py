"""
router.py — Router LLM léger pour classification de domaine
Modèle : claude-haiku-4-5 (rapide + pas cher)
Output  : {"domain": "satellite", "confidence": 0.92}

Stratégie en 3 niveaux :
  1. Règles déterministes (regex/keywords) — 0ms, ~60% des cas
  2. LLM Haiku si règles insuffisantes — ~200ms
  3. Fallback RAG triggers si LLM indisponible

Domaines :
  geo_data   → Overture Maps (POI, bâtiments, routes, divisions)
  satellite  → Google Earth Engine (NDVI, LST, WorldCover, Sentinel...)
  database   → Base externe (PostGIS, MySQL, SQLite, SQL)
  routing    → Itinéraire / isochrone (ORS)
  spatial    → Analyse spatiale (buffer, intersection, clip...)
  chat       → Question générale, pas d'outil nécessaire
"""

import os
import re
import json
import logging
import time
from typing import Optional
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("router")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# Le router utilise le même LLM_PROVIDER que l'agent principal (.env)
# Pour chaque provider, on privilégie le modèle léger/rapide par défaut
# ROUTER_MODEL dans .env surcharge tout
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "claude")

# Même MODEL_MAP que agent.py — lit les mêmes variables .env
_MODEL_MAP = {
    "claude":     os.getenv("CLAUDE_MODEL",     "claude-haiku-4-5-20251001"),
    "openai":     os.getenv("OPENAI_MODEL",     "gpt-4o-mini"),
    "ollama":     os.getenv("OLLAMA_MODEL",     "ollama/llama3.1"),
    "openrouter": os.getenv("OPENROUTER_MODEL", "openrouter/google/gemma-4-26b-a4b-it:free"),
    "deepseek":   os.getenv("DEEPSEEK_MODEL",   "deepseek/deepseek-chat"),
    "mistral":    os.getenv("MISTRAL_MODEL",    "mistral/mistral-small-latest"),
}

# ROUTER_MODEL : modèle dédié classification (léger + rapide)
# Par défaut = même modèle que l'agent principal
# Peut être surchargé via ROUTER_MODEL=... dans .env
ROUTER_MODEL = os.getenv("ROUTER_MODEL", _MODEL_MAP.get(LLM_PROVIDER, "claude-haiku-4-5-20251001"))
ROUTER_TEMP     = float(os.getenv("ROUTER_TEMP",            "0.0"))
ROUTER_MAX_TOK  = int(os.getenv("ROUTER_MAX_TOKENS",        "60"))
CONFIDENCE_HIGH = float(os.getenv("ROUTER_CONFIDENCE_HIGH", "0.85"))
CONFIDENCE_LOW  = float(os.getenv("ROUTER_CONFIDENCE_LOW",  "0.55"))

DOMAINS = ["geo_data", "satellite", "database", "routing", "spatial", "world_data", "clip", "chat"]


# ═══════════════════════════════════════════════════════════════════════════════
# COUCHE 1 — RÈGLES DÉTERMINISTES (zéro latence)
# ═══════════════════════════════════════════════════════════════════════════════

# Patterns exclusifs par domaine — si match → confiance 0.97, pas d'appel LLM
_RULES: list[tuple[str, list[str]]] = [

    ("satellite", [
        r"\bndvi\b", r"\bndwi\b", r"\bndbi\b", r"\bevi\b",
        r"\bsentinel[\s-]?[12]\b", r"\blandsat[\s-]?[89]\b",
        r"\bmodis\b", r"\bgee\b", r"\bearth\s*engine\b",
        r"\bworldcover\b", r"\boccupation\s*du\s*sol\b",
        r"\blast\b", r"\btempérature\s*surface\b", r"\bsurface\s*temperature\b",
        r"\bimagerie\s*satellite\b", r"\bimage\s*satellite\b",
        r"\btelédétection\b", r"\bremote\s*sensing\b",
        r"\bsrtm\b", r"\bmnt\b", r"\brelief\b", r"\bpente\s*terrain\b",
        r"\bhansen\b", r"\bdéforestation\b",
        r"\bera5\b", r"\bprecipitation\b", r"\bpluie\b",
        r"\bsar\b", r"\bradar\b", r"\bsentinel.1\b",
        r"\bfausse\s*couleur\b", r"\bfalse\s*color\b", r"\bnir\b",
        r"\bcomposite\s*median\b", r"\bnuage\b",
    ]),

    ("routing", [
        r"\bisochron\b",
        r"\bitinéraire\b", r"\btrajet\b", r"\broute\s*entre\b",
        r"\baller\s*de\b", r"\bcomment\s*aller\b", r"\brelier\b",
        r"\bdirections?\b", r"\bnavigation\b",
        r"\bzone\s*accessible\b", r"\baccessible\s*en\b",
        r"\bminutes?\s*à\s*pied\b", r"\bminutes?\s*vélo\b", r"\bminutes?\s*voiture\b",
        r"\bà\s*pied\s*de\b", r"\ben\s*vélo\s*de\b", r"\ben\s*voiture\s*de\b",
        r"\bwaypoint\b", r"\bturn.by.turn\b",
        r"\bdesserte\b", r"\bchalandise\b", r"\bbassin\s*de\s*vie\b",
    ]),

    ("database", [
        r"\bpostgis\b", r"\bpostgresql\b", r"\bpsycopg\b",
        r"\bma\s*base\b", r"\bmon\s*serveur\b", r"\bbase\s*de\s*données\b",
        r"\bsql\s*select\b", r"\brequête\s*sql\b", r"\bquery\s*sql\b",
        r"\bsqlite\b", r"\bmysql\b",
        r"\btables?\s*de\s*ma\b", r"\bmes\s*données\b",
        r"\bconnecte.toi\b", r"\bconnexion\s*db\b",
        r"\bst_asgeojson\b", r"\bst_astext\b", r"\bgeom\s*column\b",
    ]),

    ("spatial", [
        r"\bintersection\b", r"\bunion\s*(de|spatiale|entre)\b",
        r"\bbuffer\b", r"\bzone\s*tampon\b",
        r"\bclip\s+\b", r"\bdécouper\b",
        r"\bpoints?\s*dans\s*(le|les|la|un|une)\s*polyg\b",
        r"\bvoronoi\b", r"\bgrille\s*hex\b",
        r"\bdbscan\b", r"\bclustering\s*spatial\b",
        r"\bdissoudre\b", r"\bdissolve\b",
        r"\bconvex[\s_]hull\b", r"\benveloppe\s*convexe\b",
        r"\bmatrice\s*de\s*distance\b", r"\bdistance\s*matrix\b",
        r"\bcentroïde\b", r"\bcentroid\b",
        r"\bsimplifi(er|cation)\b",
        r"\banalyse\s*spatiale\b", r"\bopération\s*spatiale\b",
        # Analyse thématique
        r"\bchoroplèthe\b", r"\bchoroplet\b",
        r"\bsymboles?\s*proportionnel\b",
        r"\bcercles?\s*proportionnel\b",
        r"\banalyse\s*thématique\b", r"\bcarte\s*thématique\b",
        r"\brépartition\b", r"\brepartition\b",
        r"\bdistribution\s*(de|des|par)\b",
        r"\bcolori(er|e|s)\s*(par|selon|avec)\b",
        r"\bvisualise(r|z)?\s*(la|le|les|l[''\'])?\s*(variable|attribut|valeur)\b",
        r"\breprésentati(on|f)\s*(de|des|par)\b",
        r"\bclassifi(er|cation)\s*(par|selon|la|le)\b",
        r"\bavec\s*(la|le)?\s*variable\b",
        r"\bpour\s*(la|le)?\s*couche\b",
        r"\bcolori(er|e|s)\b",
        r"\b3[dD]\b", r"\bextrusion\b", r"\bextruder\b",
        r"\bcarte\s*3d\b", r"\bvue\s*3d\b", r"\brelief\b",
        r"\bbâtiments?\s*3d\b", r"\bvolum(e|étrique)\b",
    ]),

    ("geo_data", [
        r"\brestaurant\b", r"\bcafé\b", r"\bpharmaci\b",
        r"\bhôpital\b", r"\bhôtel\b", r"\bécole\b",
        r"\bcommerce\b", r"\bmagasin\b", r"\bsupermarché\b",
        r"\bpoi\b", r"\bpoint\s*d.intérêt\b",
        r"\bbâtiment\b", r"\bimmeuble\b",
        r"\boverture\b",
        r"\bvoirie\b", r"\bréseau\s*routier\b",
        r"\bcommune\b", r"\bdépartement\b", r"\brégion\s*admin\b",
        r"\bdivision\s*admin\b",
        r"\baffiche\s+(les|des|la|le)\b",
        r"\bmontre\s+(les|des|la|le)\b",
        r"\bautoroute\b", r"\brue\b", r"\bavenue\b",
    ]),

    ("clip", [
        r"dans\s+l.{0,2}isochrone\b",
        r"dans\s+l.{0,2}aire\b",
        r"dans\s+la\s+zone\b",
        r"dans\s+le\s+buffer\b",
        r"dans\s+ce\s+polygone\b",
        r"dans\s+la\s+couche\b",
        r"within\s+the\s+isochrone\b",
        r"inside\s+the\s+zone\b",
        r"à\s+l.intérieur\s+de\b",
    ]),

    ("chat", [
        r"^(salut|bonjour|bonsoir|coucou|hello|hi|hey|yo)\b",
        r"^(merci|thanks|ok|super|parfait|au\s+revoir|bye)\b",
        r"\brecette\b", r"\bcuisine\b", r"\bcuire\b",
        r"\bactualit", r"\bnews\b", r"\bjournal\s+du\s+jour\b",
        r"\bqu.est.ce que\b", r"\bexplique\b", r"\bc.est quoi\b",
        r"\bcomment\s+fonctionne\b",
        r"\bhelp\b", r"\bwhat\s+is\b", r"\bexplain\b",
    ]),

    ("world_data", [
        # Exclure si une couche locale est mentionnée
        # (ces patterns sont vérifiés APRES — l'override dans orchestrator prend la main)
        r"\bpopulation\s*mondiale\b",
        r"\bpib\s*(par\s*pays|mondial|par\s*habitant)\b",
        r"\bgdp\s*(per\s*capita|by\s*country|world)\b",
        r"\bbanque\s*mondiale\b", r"\bworld\s*bank\b",
        r"\bdonnées\s*mondiales\b", r"\bcarte\s*mondiale\b",
        r"\bchoroplèthe\b", r"\bindice.*pays\b",
        r"\bespérance\s*de\s*vie\b",
        r"\b(co2|carbone|émissions)\b",
        r"\balphabétisation\b", r"\bmortalité\s*infantile\b",
        r"\bindice\s*de\s*gini\b", r"\bchômage\s*mondial\b",
        r"\bdémographie\b", r"\bworld.?bank\b",
        r"SP\.[A-Z]{3}\.[A-Z]", r"NY\.GDP\.",
    ]),



]

# Compiler les patterns pour performance
_COMPILED_RULES: list[tuple[str, list[re.Pattern]]] = [
    (domain, [re.compile(pat, re.IGNORECASE) for pat in patterns])
    for domain, patterns in _RULES
]


def _rule_based_classify(query: str) -> Optional[dict]:
    """
    Classification par règles regex.
    Retourne {domain, confidence, method} si match exclusif,
    None si ambiguïté ou aucun match → délègue au LLM.
    """
    query_lower = query.lower()
    hits: dict[str, int] = {}

    for domain, patterns in _COMPILED_RULES:
        count = sum(1 for pat in patterns if pat.search(query_lower))
        if count > 0:
            hits[domain] = count

    if not hits:
        return None  # → LLM

    # Domaine dominant
    top_domain = max(hits, key=hits.get)
    top_count  = hits[top_domain]
    total      = sum(hits.values())

    # Confiance basée sur l'exclusivité du match
    if len(hits) == 1:
        # Un seul domaine matché → haute confiance
        confidence = min(0.97, 0.80 + top_count * 0.03)
    else:
        # Plusieurs domaines matchés → confiance proportionnelle
        confidence = top_count / total
        if confidence < 0.60:
            return None  # trop ambigu → LLM

    log.info(f"Router règles: '{query[:60]}' → {top_domain} ({confidence:.2f}) hits={hits}")
    return {
        "domain":     top_domain,
        "confidence": round(confidence, 2),
        "candidates": sorted(hits.items(), key=lambda x: -x[1]),
        "method":     "rules",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# COUCHE 2 — LLM HAIKU
# ═══════════════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT = """Tu es un classificateur d'intentions pour une application cartographique.
Classifie la requête utilisateur dans UN SEUL domaine parmi :

- geo_data   : données géographiques Overture Maps (POI, restaurants, bâtiments, routes, divisions administratives, commerces, lieux)
- satellite  : imagerie satellite et télédétection (NDVI, LST, Sentinel, Landsat, MODIS, WorldCover, GEE, relief SRTM)
- database   : base de données externe (PostGIS, MySQL, SQLite, requête SQL, connexion serveur)
- routing    : itinéraire et accessibilité (trajet, route, isochrone, zone accessible en X minutes)
- spatial    : analyse spatiale (buffer, intersection, union, clip, clustering, voronoi, dissolve)
- clip       : trouver des POI/données DANS une zone existante (isochrone, buffer, polygone). Ex: "restaurants dans l'isochrone", "bâtiments dans la zone"
- chat       : tout ce qui n'est PAS cartographique : salutations, recettes, actualités, questions générales. Si aucun outil cartographique n'est utile → chat
- world_data : données mondiales World Bank (population, PIB, CO2, santé, éducation). UNIQUEMENT si pas de couche locale mentionnée.

Réponds UNIQUEMENT avec un objet JSON valide, RIEN d'autre :
{"domain": "<domaine>", "confidence": <0.0-1.0>, "reason": "<1 phrase courte>"}

Règles :
- confidence > 0.85 = certitude élevée
- confidence 0.60-0.85 = incertitude modérée (plusieurs domaines possibles)
- confidence < 0.60 = très ambigu
- Si routing ET geo_data → routing si isochrone/trajet, sinon geo_data
- Si spatial ET geo_data → spatial si opération sur couches existantes, sinon geo_data
- "dans l'isochrone/zone/buffer" → TOUJOURS clip, jamais routing
- Salutation, recette, actualité, blague, météo du jour → TOUJOURS chat"""

_FEW_SHOT_EXAMPLES = [
    {"role": "user",      "content": "Restaurants à Nantes"},
    {"role": "assistant", "content": '{"domain": "geo_data", "confidence": 0.97, "reason": "POI restaurants Overture Maps"}'},
    {"role": "user",      "content": "NDVI de Dakar avec Sentinel-2 en 2024"},
    {"role": "assistant", "content": '{"domain": "satellite", "confidence": 0.99, "reason": "Indice NDVI télédétection GEE"}'},
    {"role": "user",      "content": "Connecte-toi à ma base PostGIS et affiche les parcelles"},
    {"role": "assistant", "content": '{"domain": "database", "confidence": 0.98, "reason": "Connexion base de données PostGIS SQL"}'},
    {"role": "user",      "content": "Zone accessible à pied en 10 min depuis la gare"},
    {"role": "assistant", "content": '{"domain": "routing", "confidence": 0.96, "reason": "Isochrone accessibilité piétonne"}'},
    {"role": "user",      "content": "Intersection entre l\'isochrone et les restaurants"},
    {"role": "assistant", "content": '{"domain": "spatial", "confidence": 0.94, "reason": "Opération spatiale clip/intersection entre couches"}'},
    {"role": "user",      "content": "Buffer 500m autour des pharmacies"},
    {"role": "assistant", "content": '{"domain": "spatial", "confidence": 0.91, "reason": "Zone tampon spatiale sur couche existante"}'},
    {"role": "user",      "content": "Comment fonctionne la carte ?"},
    {"role": "assistant", "content": '{"domain": "chat", "confidence": 0.95, "reason": "Question générale aide interface"}'},
    {"role": "user",      "content": "Température de surface à Paris en été"},
    {"role": "assistant", "content": '{"domain": "satellite", "confidence": 0.93, "reason": "LST température surface Landsat/MODIS"}'},
    {"role": "user",      "content": "Itinéraire vélo de la gare au château"},
    {"role": "assistant", "content": '{"domain": "routing", "confidence": 0.97, "reason": "Calcul itinéraire vélo ORS"}'},
    {"role": "user",      "content": "Affiche les communes de Loire-Atlantique"},
    {"role": "assistant", "content": '{"domain": "geo_data", "confidence": 0.95, "reason": "Divisions administratives Overture Maps"}'},
    {"role": "user",      "content": "Affiche les communes de Loire-Atlantique"},
    {"role": "assistant", "content": '{"domain": "world_data", "confidence": 0.95, "reason": "PIB par pays"}'},
    {"role": "user",      "content": "Restaurants dans l'isochrone"},
    {"role": "assistant", "content": '{"domain": "clip", "confidence": 0.97, "reason": "POI dans une zone existante"}'},
    {"role": "user",      "content": "Bâtiments dans la zone tampon"},
    {"role": "assistant", "content": '{"domain": "clip", "confidence": 0.96, "reason": "Features dans polygone existant"}'},
    {"role": "user",      "content": "salut"},
    {"role": "assistant", "content": '{"domain": "chat", "confidence": 0.99, "reason": "Salutation, pas cartographique"}'},
    {"role": "user",      "content": "recette de pizza"},
    {"role": "assistant", "content": '{"domain": "chat", "confidence": 0.99, "reason": "Cuisine, hors sujet cartographique"}'},
    {"role": "user",      "content": "actualités du jour"},
    {"role": "assistant", "content": '{"domain": "chat", "confidence": 0.99, "reason": "News, hors sujet cartographique"}'},
]


def _llm_classify(query: str) -> dict:
    """
    Classification via Claude Haiku.
    Retourne {domain, confidence, reason, method, latency_ms}
    """
    try:
        from litellm import completion
    except ImportError:
        log.warning("litellm non disponible, fallback RAG")
        return _rag_fallback_classify(query)

    t0 = time.time()

    messages = _FEW_SHOT_EXAMPLES + [{"role": "user", "content": query}]

    try:
        response = completion(
            model=ROUTER_MODEL,
            messages=[{"role": "system", "content": _SYSTEM_PROMPT}] + messages,
            max_tokens=ROUTER_MAX_TOK,
            temperature=ROUTER_TEMP,
        )
        raw = response.choices[0].message.content.strip()
        latency = round((time.time() - t0) * 1000)

        # Parser le JSON — robuste aux backticks ou espaces parasites
        raw_clean = raw.strip().lstrip("```json").rstrip("```").strip()
        data = json.loads(raw_clean)

        domain = data.get("domain", "chat")
        if domain not in DOMAINS:
            domain = "chat"

        result = {
            "domain":     domain,
            "confidence": round(float(data.get("confidence", 0.7)), 2),
            "reason":     data.get("reason", ""),
            "method":     "llm",
            "latency_ms": latency,
        }
        log.info(
            f"Router LLM [{LLM_PROVIDER}/{ROUTER_MODEL}]: '{query[:60]}' → {result['domain']} "
            f"({result['confidence']:.2f}) [{latency}ms]"
        )
        return result

    except json.JSONDecodeError as e:
        log.warning(f"Router LLM JSON parse error: {e} — raw: {raw!r}")
        return _rag_fallback_classify(query)
    except Exception as e:
        log.error(f"Router LLM error: {e}")
        return _rag_fallback_classify(query)


# ═══════════════════════════════════════════════════════════════════════════════
# COUCHE 3 — FALLBACK RAG (triggers lexicaux)
# ═══════════════════════════════════════════════════════════════════════════════

def _rag_fallback_classify(query: str) -> dict:
    """
    Fallback : classification par distribution des domaines dans les top-k tools RAG.
    Utilisé si LLM indisponible.
    """
    try:
        from rag.tool_registry import search_by_triggers
        tools = search_by_triggers(query, top_k=5)
        if not tools:
            return _default_result("chat", 0.50, "fallback_no_match")

        # Compter les domaines dans les résultats
        domain_counts: dict[str, int] = {}
        for t in tools:
            domain_counts[t["domain"]] = domain_counts.get(t["domain"], 0) + 1

        top_domain = max(domain_counts, key=domain_counts.get)
        top_count  = domain_counts[top_domain]
        confidence = top_count / len(tools)

        log.info(f"Router fallback RAG: '{query[:60]}' → {top_domain} ({confidence:.2f})")
        return _default_result(top_domain, round(confidence, 2), "rag_fallback")

    except Exception as e:
        log.error(f"Router fallback error: {e}")
        return _default_result("geo_data", 0.40, "error_fallback")


def _default_result(domain: str, confidence: float, method: str) -> dict:
    return {
        "domain":     domain,
        "confidence": confidence,
        "reason":     "",
        "method":     method,
        "latency_ms": 0,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTER PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

class Router:
    """
    Router d'intention pour OpenMapAgents.

    Stratégie en cascade :
      1. Règles déterministes (regex) → 0ms
      2. LLM Haiku si règles insuffisantes → ~200ms
      3. Fallback RAG triggers si LLM down → ~5ms
    """

    def classify(self, query: str) -> dict:
        """
        Classifie une query dans un domaine.

        Returns:
            {
                "domain":      str,   # domaine principal
                "confidence":  float, # 0.0 - 1.0
                "reason":      str,   # explication courte
                "method":      str,   # "rules" | "llm" | "rag_fallback" | ...
                "latency_ms":  int,
                "candidates":  list,  # domaines secondaires si ambiguïté
                "use_fallback_rag": bool,  # True si confidence < CONFIDENCE_HIGH
            }
        """
        t0 = time.time()

        # Niveau 1 — règles déterministes
        rule_result = _rule_based_classify(query)
        if rule_result and rule_result["confidence"] >= CONFIDENCE_HIGH:
            result = rule_result
            result["latency_ms"] = round((time.time() - t0) * 1000)
        else:
            # Niveau 2 — LLM Haiku
            llm_result = _llm_classify(query)

            # Si les règles et le LLM convergent → boost confiance
            if rule_result and rule_result["domain"] == llm_result["domain"]:
                llm_result["confidence"] = min(0.99, llm_result["confidence"] + 0.05)
                llm_result["method"] = "rules+llm"

            result = llm_result
            result["latency_ms"] = round((time.time() - t0) * 1000)

        # Ajouter candidates si absent
        if "candidates" not in result:
            result["candidates"] = [(result["domain"], result["confidence"])]

        # Signal pour l'orchestrateur : faut-il interroger plusieurs domaines ?
        result["use_fallback_rag"] = result["confidence"] < CONFIDENCE_HIGH

        log.info(
            f"Router final: '{query[:60]}' → {result['domain']} "
            f"({result['confidence']:.2f}) [{result['method']}] {result['latency_ms']}ms"
        )
        return result

    def classify_multi(self, query: str) -> list[dict]:
        """
        Retourne les domaines candidats triés par pertinence.
        Utilisé par l'orchestrateur quand confidence < seuil.

        Returns:
            [{"domain": str, "confidence": float}, ...]
        """
        result = self.classify(query)

        if result["confidence"] >= CONFIDENCE_HIGH:
            # Confiance élevée → un seul domaine
            return [{"domain": result["domain"], "confidence": result["confidence"]}]

        # Confiance modérée → interroger aussi le RAG sur les candidats
        candidates = result.get("candidates", [])
        if isinstance(candidates[0], (list, tuple)):
            return [
                {"domain": d, "confidence": round(c / max(1, sum(x[1] for x in candidates)), 2)}
                for d, c in candidates
            ]
        return [{"domain": result["domain"], "confidence": result["confidence"]}]


# ─── SINGLETON ────────────────────────────────────────────────────────────────
_router: Optional[Router] = None

def get_router() -> Router:
    global _router
    if _router is None:
        _router = Router()
    return _router


def classify(query: str) -> dict:
    """Shortcut : classifie une query, retourne le résultat complet."""
    return get_router().classify(query)


def get_domain(query: str) -> str:
    """Shortcut ultra-simple : retourne juste le domaine."""
    return classify(query)["domain"]


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    TEST_CASES = [
        # (query, expected_domain)
        # --- geo_data ---
        ("Restaurants à Nantes",                          "geo_data"),
        ("Nantes restaurants",                            "geo_data"),   # inversé fr→en
        ("Affiche les bâtiments autour du château",       "geo_data"),
        ("Réseau routier de Dakar",                       "geo_data"),
        ("Communes de Loire-Atlantique",                  "geo_data"),
        ("Pharmacies près de moi",                        "geo_data"),
        # --- satellite ---
        ("NDVI de Dakar avec Sentinel-2",                 "satellite"),
        ("Température de surface à Paris en été 2023",    "satellite"),
        ("Occupation du sol ESA WorldCover",              "satellite"),
        ("Relief et pente du Sénégal SRTM",               "satellite"),
        ("Satellite imagery Dakar",                       "satellite"),  # anglais
        ("Show me the NDVI",                              "satellite"),  # anglais
        # --- database ---
        ("Connecte-toi à ma base PostGIS",                "database"),
        ("Requête SQL sur mes parcelles",                 "database"),
        ("Quelles tables dans ma base ?",                 "database"),
        # --- routing ---
        ("Itinéraire vélo de la gare au château",         "routing"),
        ("Zone accessible à pied en 10 min",              "routing"),
        ("Isochrone 15 minutes en voiture depuis Paris",  "routing"),
        ("How to get from A to B by bike",                "routing"),   # anglais
        # --- spatial ---
        ("Buffer 500m autour des restaurants",            "spatial"),
        ("Intersection entre l'isochrone et les POI",     "spatial"),
        ("Dissolve les communes par département",         "spatial"),
        ("Clustering DBSCAN sur les commerces",           "spatial"),
        # --- chat ---
        ("Comment fonctionne la carte ?",                 "chat"),
        ("Merci beaucoup",                                "chat"),
        ("Qu'est-ce que Overture Maps ?",                 "chat"),
        # --- ambigus ---
        ("Restaurants dans l'isochrone",                  "spatial"),   # spatial > geo_data
        ("Buildings near the castle route",               "geo_data"),  # geo_data > routing
    ]

    use_llm = "--llm" in sys.argv  # python router.py --llm pour tester avec Haiku

    router = Router()
    ok = 0
    total = len(TEST_CASES)

    print(f"\n{'='*70}")
    print(f"TEST ROUTER OpenMapAgents {'(avec LLM Haiku)' if use_llm else '(règles uniquement)'}")
    print(f"{'='*70}\n")

    for query, expected in TEST_CASES:
        if use_llm:
            result = router.classify(query)
        else:
            # Test règles seulement
            rule_result = _rule_based_classify(query)
            if rule_result:
                result = rule_result
                result.setdefault("latency_ms", 0)
            else:
                result = _rag_fallback_classify(query)
                result.setdefault("latency_ms", 0)

        domain     = result["domain"]
        confidence = result["confidence"]
        method     = result.get("method", "?")
        latency    = result.get("latency_ms", 0)
        match      = "✅" if domain == expected else "❌"
        if domain == expected:
            ok += 1

        print(
            f"{match} [{confidence:.2f}] {domain:12s} "
            f"({method:10s} {latency:3d}ms) "
            f"'{query[:50]}'"
        )
        if domain != expected:
            print(f"         ↳ attendu: {expected}")

    print(f"\n{'='*70}")
    print(f"Score : {ok}/{total} ({100*ok/total:.0f}%)")
    print(f"{'='*70}\n")
