"""
intent_parser.py — Interprétation LLM de l'intention utilisateur
=================================================================
Première étape du pipeline OpenMapAgents.

Le LLM analyse la requête et retourne un JSON structuré qui :
  - Identifie l'intention (geo_search, isochrone, gee, chat...)
  - Reformule le lieu pour Nominatim (avec contexte géographique)
  - Extrait les paramètres de l'outil à appeler
  - Répond directement si c'est conversationnel (chat)

Zéro regex. Le LLM fait tout.
Fallback structuré si LLM indisponible.
"""

import os
import json
import logging
import time
from typing import Optional

log = logging.getLogger("intent_parser")

# ── Config LLM ────────────────────────────────────────────────
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openrouter")
_MODEL_MAP = {
    "claude":     os.getenv("CLAUDE_MODEL",     "claude-haiku-4-5-20251001"),
    "openai":     os.getenv("OPENAI_MODEL",     "gpt-4o-mini"),
    "openrouter": os.getenv("OPENROUTER_MODEL", "openrouter/google/gemma-4-26b-a4b-it:free"),
    "deepseek":   os.getenv("DEEPSEEK_MODEL",   "deepseek/deepseek-chat"),
    "mistral":    os.getenv("MISTRAL_MODEL",    "mistral/mistral-small-latest"),
    "ollama":     os.getenv("OLLAMA_MODEL",     "ollama/llama3.1"),
}
# INTENT_MODEL peut être surchargé dans .env pour utiliser un modèle plus stable
INTENT_MODEL = os.getenv("INTENT_MODEL", _MODEL_MAP.get(LLM_PROVIDER, "claude-haiku-4-5-20251001"))

# ── Prompt système ────────────────────────────────────────────
_SYSTEM = """Tu es un interpréteur de requêtes pour une application cartographique SIG.
Analyse la requête et retourne UNIQUEMENT un objet JSON valide, rien d'autre.

Structure de réponse :
{
  "intent": "<voir liste>",
  "geo": {
    "place": "<lieu reformulé pour Nominatim, avec pays/ville si possible>",
    "type": "poi|address|city|region|country|none",
    "context": "<contexte géographique déduit>"
  },
  "params": {
    "time_minutes": <entier ou null>,
    "profile": "foot|bike|car|null",
    "category": "<catégorie Overture ou null>",
    "index": "<indice GEE: NDVI|LST|NDWI|EVI|SAR|WorldCover|SRTM ou null>",
    "indicator": "<code World Bank ou null>",
    "layer_name": "<nom couche existante mentionnée ou null>",
    "clip_to": "<nom couche pour clip spatial ou null>",
    "date_start": "<YYYY-MM-DD ou null>",
    "date_end": "<YYYY-MM-DD ou null>"
  },
  "response_text": "<réponse directe si chat, null sinon>",
  "confidence": <0.0-1.0>
}

Valeurs de intent :
- geo_search  : chercher des POI/lieux (restaurants, pharmacies, bâtiments...)
- isochrone   : zone accessible en X minutes depuis un lieu
- route       : itinéraire entre deux points
- gee         : analyse satellite GEE (NDVI, LST, WorldCover, SRTM...)
- worldbank   : données mondiales World Bank (PIB, population, CO2...)
- thematic    : analyse thématique sur une couche existante (choroplèthe, symboles)
- clip        : trouver des données DANS une zone/isochrone/buffer existante
- spatial     : opération spatiale (buffer, intersection, union, clip entre couches)
- chat        : conversation, salutation, hors-sujet cartographique → remplir response_text

Règles strictes :
1. geo.place = lieu reformulé pour Nominatim COMPLET. Ex:
   - "beaulieu" → "centre commercial Beaulieu, Nantes, France"
   - "gare" → "gare de Nantes, France" (si contexte Nantes connu)
   - "ici" / "depuis ici" / "la carte" → place = "" (utiliser position carte)
   - "10 minutes" / "à pied" / "vélo" → JAMAIS un lieu, place = ""
2. params.profile: foot=marche/pied/piéton, bike=vélo/cyclable, car=voiture/auto
3. response_text: rempli UNIQUEMENT pour intent=chat. Ex: salut→"Bonjour ! Je suis votre assistant cartographique..."
4. Pour chat: si hors-sujet (recette, actu, blague...) → response_text = explication courte que tu es un assistant cartographique
5. confidence: 0.95+ si très clair, 0.7-0.95 si probable, <0.7 si ambigu"""

_EXAMPLES = [
    {"role": "user", "content": "isochrones de 10 minutes à pied du centre commercial Beaulieu à Nantes"},
    {"role": "assistant", "content": '{"intent":"isochrone","geo":{"place":"centre commercial Beaulieu, Nantes, France","type":"poi","context":"Nantes, France"},"params":{"time_minutes":10,"profile":"foot","category":null,"index":null,"indicator":null,"layer_name":null,"clip_to":null,"date_start":null,"date_end":null},"response_text":null,"confidence":0.97}'},

    {"role": "user", "content": "restaurants dans l'isochrone"},
    {"role": "assistant", "content": '{"intent":"clip","geo":{"place":"","type":"none","context":""},"params":{"time_minutes":null,"profile":null,"category":"restaurant","index":null,"indicator":null,"layer_name":null,"clip_to":"isochrone","date_start":null,"date_end":null},"response_text":null,"confidence":0.96}'},

    {"role": "user", "content": "NDVI sur Dakar entre janvier et juin 2024"},
    {"role": "assistant", "content": '{"intent":"gee","geo":{"place":"Dakar, Sénégal","type":"city","context":"Dakar, Sénégal"},"params":{"time_minutes":null,"profile":null,"category":null,"index":"NDVI","indicator":null,"layer_name":null,"clip_to":null,"date_start":"2024-01-01","date_end":"2024-06-30"},"response_text":null,"confidence":0.99}'},

    {"role": "user", "content": "pharmacies à Nantes centre"},
    {"role": "assistant", "content": '{"intent":"geo_search","geo":{"place":"Nantes centre, France","type":"city","context":"Nantes, France"},"params":{"time_minutes":null,"profile":null,"category":"pharmacy","index":null,"indicator":null,"layer_name":null,"clip_to":null,"date_start":null,"date_end":null},"response_text":null,"confidence":0.97}'},

    {"role": "user", "content": "salut"},
    {"role": "assistant", "content": '{"intent":"chat","geo":{"place":"","type":"none","context":""},"params":{"time_minutes":null,"profile":null,"category":null,"index":null,"indicator":null,"layer_name":null,"clip_to":null,"date_start":null,"date_end":null},"response_text":"Bonjour ! Je suis OpenMapAgents, votre assistant cartographique. Demandez-moi des restaurants, une isochrone, du NDVI ou une analyse spatiale.","confidence":0.99}'},

    {"role": "user", "content": "recette de pizza"},
    {"role": "assistant", "content": '{"intent":"chat","geo":{"place":"","type":"none","context":""},"params":{"time_minutes":null,"profile":null,"category":null,"index":null,"indicator":null,"layer_name":null,"clip_to":null,"date_start":null,"date_end":null},"response_text":"Je suis spécialisé en cartographie. Je ne peux pas aider avec les recettes, mais je peux afficher des restaurants sur la carte !","confidence":0.99}'},

    {"role": "user", "content": "itinéraire vélo de la gare à l'aéroport de Nantes"},
    {"role": "assistant", "content": '{"intent":"route","geo":{"place":"gare de Nantes, France","type":"address","context":"Nantes, France"},"params":{"time_minutes":null,"profile":"bike","category":null,"index":null,"indicator":null,"layer_name":null,"clip_to":null,"date_start":null,"date_end":null},"response_text":null,"confidence":0.97}'},

    {"role": "user", "content": "population mondiale par pays"},
    {"role": "assistant", "content": '{"intent":"worldbank","geo":{"place":"","type":"none","context":"mondial"},"params":{"time_minutes":null,"profile":null,"category":null,"index":null,"indicator":"SP.POP.TOTL","layer_name":null,"clip_to":null,"date_start":null,"date_end":null},"response_text":null,"confidence":0.96}'},

    {"role": "user", "content": "choroplèthe de la population sur la couche Africa_Cities"},
    {"role": "assistant", "content": '{"intent":"thematic","geo":{"place":"","type":"none","context":""},"params":{"time_minutes":null,"profile":null,"category":null,"index":null,"indicator":null,"layer_name":"Africa_Cities","clip_to":null,"date_start":null,"date_end":null},"response_text":null,"confidence":0.95}'},
    {"role": "user", "content": "rennes dinard en voiture"},
    {"role": "assistant", "content": '{"intent":"route","geo":{"place":"Rennes, France","type":"city","context":"France"},"params":{"time_minutes":null,"profile":"car","category":null,"index":null,"indicator":null,"layer_name":null,"clip_to":null,"date_start":null,"date_end":null},"response_text":null,"confidence":0.95}'},

    {"role": "user", "content": "paris lyon à vélo"},
    {"role": "assistant", "content": '{"intent":"route","geo":{"place":"Paris, France","type":"city","context":"France"},"params":{"time_minutes":null,"profile":"bike","category":null,"index":null,"indicator":null,"layer_name":null,"clip_to":null,"date_start":null,"date_end":null},"response_text":null,"confidence":0.95}'},
]


# ── Schéma de validation ──────────────────────────────────────
_VALID_INTENTS = {
    "geo_search","isochrone","route","gee","worldbank",
    "thematic","clip","spatial","chat","unknown"
}
_VALID_PROFILES = {"foot","bike","car",None}
_VALID_GEO_TYPES = {"poi","address","city","region","country","none"}


def _validate_and_fix(parsed: dict) -> dict:
    """Valide et corrige la structure JSON retournée par le LLM."""
    # Intent
    if parsed.get("intent") not in _VALID_INTENTS:
        parsed["intent"] = "unknown"

    # Geo
    if "geo" not in parsed or not isinstance(parsed["geo"], dict):
        parsed["geo"] = {"place": "", "type": "none", "context": ""}
    geo = parsed["geo"]
    if geo.get("type") not in _VALID_GEO_TYPES:
        geo["type"] = "none"
    # Nettoyer les mots de transport dans le lieu
    _transport = {"à pied","à vélo","en voiture","pied","vélo","velo","voiture",
                  "foot","bike","car","ici","here","là","la carte","10","15","20"}
    place = (geo.get("place") or "").strip()
    if place.lower() in _transport or place.isdigit():
        geo["place"] = ""
    geo["place"] = place

    # Params
    if "params" not in parsed or not isinstance(parsed["params"], dict):
        parsed["params"] = {}
    p = parsed["params"]
    # time_minutes doit être entier
    if p.get("time_minutes") is not None:
        try:
            p["time_minutes"] = int(p["time_minutes"])
        except (TypeError, ValueError):
            p["time_minutes"] = None
    # profile valide
    if p.get("profile") not in _VALID_PROFILES:
        p["profile"] = None
    # response_text uniquement pour chat
    if parsed["intent"] != "chat":
        parsed["response_text"] = None

    # confidence
    try:
        parsed["confidence"] = float(parsed.get("confidence", 0.7))
    except (TypeError, ValueError):
        parsed["confidence"] = 0.7

    return parsed


def _fallback_parse(query: str) -> dict:
    """
    Fallback déterministe si LLM indisponible.
    Couvre les cas les plus fréquents sans appel API.
    """
    import re
    q = query.lower().strip()

    base = {
        "intent": "unknown",
        "geo": {"place": "", "type": "none", "context": ""},
        "params": {
            "time_minutes": None, "profile": None, "category": None,
            "index": None, "indicator": None, "layer_name": None,
            "clip_to": None, "date_start": None, "date_end": None,
        },
        "response_text": None,
        "confidence": 0.6,
    }

    # Chat / hors-sujet
    _chat = ["salut","bonjour","bonsoir","hello","hi ","merci","bye","au revoir",
             "recette","cuisine","cuire","actualit","news ","blague"]
    if any(c in q for c in _chat):
        base["intent"] = "chat"
        base["confidence"] = 0.85
        if any(c in q for c in ["salut","bonjour","bonsoir","hello","hi "]):
            base["response_text"] = "Bonjour ! Je suis OpenMapAgents, votre assistant cartographique."
        elif any(c in q for c in ["merci","bye","au revoir"]):
            base["response_text"] = "Avec plaisir ! À bientôt."
        else:
            base["response_text"] = "Je suis spécialisé en cartographie et analyse spatiale."
        return base

    # Clip
    if re.search(r"dans\s+l.{0,2}isochrone|dans\s+la\s+zone|dans\s+le\s+buffer|dans\s+la\s+couche", q):
        base["intent"] = "clip"
        base["confidence"] = 0.90
        return base

    # Route implicite — "ville1 ville2 en voiture/vélo/pied"
    import re as _re_ri
    if _re_ri.search(
        r"[a-zA-ZÀ-ÿ]{3,}\s+[a-zA-ZÀ-ÿ]{3,}\s+(?:en\s+(?:voiture|vélo|velo)|à\s+(?:pied|vélo|velo))",
        q
    ):
        base["intent"] = "route"
        if any(w in q for w in ["voiture","car"]): base["params"]["profile"] = "car"
        elif any(w in q for w in ["vélo","velo","bike"]): base["params"]["profile"] = "bike"
        else: base["params"]["profile"] = "foot"
        base["confidence"] = 0.85
        return base
    # Isochrone
    if re.search(r"\bisochrone|\bzone\s+accessible|\bminutes?\s+(?:à\s+pied|en\s+voiture|à\s+vélo)", q):
        base["intent"] = "isochrone"
        m = re.search(r"(\d+)\s*(?:min|mn|minute)", q)
        if m:
            base["params"]["time_minutes"] = int(m.group(1))
        if any(w in q for w in ["voiture","car","auto"]): base["params"]["profile"] = "car"
        elif any(w in q for w in ["vélo","velo","bike"]): base["params"]["profile"] = "bike"
        else: base["params"]["profile"] = "foot"

        # ── Extraire le lieu depuis la query ──────────────────
        _TRANSPORT_STOP = {
            "pied","vélo","velo","voiture","car","bike","foot","walking",
            "ici","là","la","moi","nous","carte","map","minutes","minute","min",
            "isochrone","accessible","zone"
        }
        _lieu_patterns = [
            # "depuis Rennes", "depuis la gare de Rennes"
            r"(?:depuis|from)\s+([a-zA-ZÀ-ÿ0-9][a-zA-ZÀ-ÿ0-9\s\,\-]{2,60}?)(?=\s*$)",
            # "à pied de la rue 8 rue de Paris, Rennes"
            r"[àa]\s+pied\s+(?:de\s+la\s+rue|de\s+la|du|de\s+l\.|d\.|de|depuis)\s+([a-zA-ZÀ-ÿ0-9][a-zA-ZÀ-ÿ0-9\s\,\-]{2,60}?)(?=\s*$)",
            # "de la rue X", "du château", "de Rennes"
            r"(?:de\s+la\s+rue|de\s+la|du|de\s+l\.|d\.|de)\s+([a-zA-ZÀ-ÿ0-9][a-zA-ZÀ-ÿ0-9\s\,\-]{2,60}?)(?=\s*$)",
            # "autour de X"
            r"autour\s+(?:du|de\s+la|de\s+l\.|d\.|de)\s+([a-zA-ZÀ-ÿ0-9][a-zA-ZÀ-ÿ0-9\s\,\-]{2,60}?)(?=\s*$)",
        ]
        for _pat in _lieu_patterns:
            _m = re.search(_pat, query, re.IGNORECASE | re.UNICODE)
            if _m:
                _cand = _m.group(1).strip().rstrip(",").strip()
                # Nettoyer les mots transport en fin
                _cand = re.sub(
                    r"\s+(à\s+pied|en\s+voiture|à\s+vélo|pied|vélo|velo|voiture|foot|bike|car)\s*$",
                    "", _cand, flags=re.IGNORECASE
                ).strip()
                _words = [w for w in _cand.split() if w.lower() not in _TRANSPORT_STOP]
                if _words and len(_cand) > 2:
                    base["geo"]["place"] = _cand
                    base["geo"]["type"]  = "address" if re.search(r"\d", _cand) else "city"
                    log.info(f"[IntentParser] fallback lieu extrait: '{_cand}'")
                    base["confidence"] = 0.85
                    break
        # ─────────────────────────────────────────────────────
        return base

    # GEE
    _gee_idx = {"ndvi":"NDVI","evi":"EVI","ndwi":"NDWI","lst":"LST","sar":"SAR",
                "srtm":"SRTM","worldcover":"WorldCover","sentinel":"NDVI"}
    for kw, idx in _gee_idx.items():
        if kw in q:
            base["intent"] = "gee"
            base["params"]["index"] = idx
            base["confidence"] = 0.85
            return base

    # Route
    # Route — itinéraire entre deux points
    if re.search(r"\bitinéraire|\btrajet|\baller\s+de|\bcomment\s+aller|\bdirections\b", q):
        base["intent"] = "route"
        if any(w in q for w in ["voiture","car"]): base["params"]["profile"] = "car"
        elif any(w in q for w in ["vélo","velo","bike"]): base["params"]["profile"] = "bike"
        else: base["params"]["profile"] = "foot"
        base["confidence"] = 0.80
        return base

    # Route implicite — "ville1 ville2 en voiture/vélo/pied" sans mot clé itinéraire
    # Ex: "rennes dinard en voiture", "paris lyon à vélo"
    _route_implicit = re.search(
        r"([a-zA-ZÀ-ÿ][a-zA-ZÀ-ÿ\s\-]{1,30}?)\s+([a-zA-ZÀ-ÿ][a-zA-ZÀ-ÿ\s\-]{1,30}?)\s+"
        r"(?:en\s+(?:voiture|vélo|velo|train|bus)|à\s+(?:pied|vélo|velo))",
        q
    )
    if _route_implicit:
        base["intent"] = "route"
        if any(w in q for w in ["voiture","car"]): base["params"]["profile"] = "car"
        elif any(w in q for w in ["vélo","velo","bike"]): base["params"]["profile"] = "bike"
        else: base["params"]["profile"] = "foot"
        base["confidence"] = 0.75
        return base

    # Geo search (défaut)
    base["intent"] = "geo_search"
    base["confidence"] = 0.55
    return base


class IntentParser:
    """
    Interprète une requête utilisateur via LLM.
    Retourne une IntentResult structurée.
    """

    def parse(self, query: str, context: dict = None) -> dict:
        """
        Parse une requête et retourne un dict structuré.

        Args:
            query   : requête utilisateur
            context : contexte optionnel (last_place, active_layers...)

        Returns:
            dict avec intent, geo, params, response_text, confidence
        """
        t0 = time.time()

        # Enrichir la query avec le contexte si disponible
        _ctx_hint = ""
        if context:
            last_place = context.get("last_geo_place","")
            layers     = context.get("active_layers",[])
            iso_layers = [l for l in layers if isinstance(l,dict)
                          and "sochrone" in l.get("name","").lower()]
            if last_place:
                _ctx_hint += f"\n[Contexte: dernier lieu mentionné = {last_place}]"
            if iso_layers:
                names = ", ".join(l["name"] for l in iso_layers[:3])
                _ctx_hint += f"\n[Couches isochrone sur la carte: {names}]"

        user_content = query + _ctx_hint

        # ── Tentative LLM ─────────────────────────────────────
        parsed = None
        try:
            from litellm import completion
            resp = completion(
                model=INTENT_MODEL,
                messages=[
                    {"role": "system",    "content": _SYSTEM},
                    *_EXAMPLES,
                    {"role": "user",      "content": user_content},
                ],
                max_tokens=300,
                temperature=0.0,
                timeout=8,
            )
            raw = (resp.choices[0].message.content or "{}").strip()
            raw = raw.lstrip("```json").lstrip("```").rstrip("```").strip()
            data = json.loads(raw)
            if isinstance(data, list):
                data = data[0] if data else {}
            parsed = _validate_and_fix(data)
            method = "llm"
        except Exception as e:
            log.warning(f"[IntentParser] LLM indisponible ({type(e).__name__}) → fallback")
            parsed = _fallback_parse(query)
            method = "fallback"

        ms = int((time.time() - t0) * 1000)
        log.info(
            f"[IntentParser] '{query[:60]}' → intent={parsed['intent']} "
            f"place='{parsed['geo'].get('place','')}' "
            f"conf={parsed['confidence']:.2f} [{method}] {ms}ms"
        )
        parsed["_method"] = method
        parsed["_latency_ms"] = ms
        return parsed


# Singleton
_parser: Optional[IntentParser] = None

def get_intent_parser() -> IntentParser:
    global _parser
    if _parser is None:
        _parser = IntentParser()
    return _parser
