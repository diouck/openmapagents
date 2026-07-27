"""
agents/routing_agent.py — Agent Routing (autonome, sans BaseAgent)
===================================================================
Pipeline en 3 étapes :
  1. LLM (ou fallback regex) extrait : type, place, time_minutes, profile
  2. Geocode via mcp_nominatim ou Nominatim direct
  3. mcp_ors.compute_isochrone / compute_route / compute_matrix
"""

import re
import json
import logging
import os
import time

log = logging.getLogger("routing_agent")

# ── Config LLM ────────────────────────────────────────────────
# Chaîne de modèles : on essaie dans l'ordre jusqu'au premier qui répond
# ROUTING_MODEL dans .env surcharge tout
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openrouter")

# Modèle principal configuré dans .env
_PRIMARY_MAP = {
    "claude":     os.getenv("CLAUDE_MODEL",    "claude-haiku-4-5-20251001"),
    "openai":     os.getenv("OPENAI_MODEL",    "gpt-4o-mini"),
    "openrouter": os.getenv("OPENROUTER_MODEL","openrouter/google/gemma-4-26b-a4b-it:free"),
    "deepseek":   os.getenv("DEEPSEEK_MODEL",  "deepseek/deepseek-chat"),
    "mistral":    os.getenv("MISTRAL_MODEL",   "mistral/mistral-small-latest"),
    "ollama":     os.getenv("OLLAMA_MODEL",    "ollama/llama3.1"),
}
_PRIMARY_MODEL = os.getenv("ROUTING_MODEL", _PRIMARY_MAP.get(LLM_PROVIDER, "claude-haiku-4-5-20251001"))

# Chaîne de fallback — modèles alternatifs si le principal est rate-limited
# Seuls les modèles configurés dans .env sont tentés
_FALLBACK_MODELS = []
for _env, _default in [
    ("CLAUDE_MODEL",    "claude-haiku-4-5-20251001"),
    ("DEEPSEEK_MODEL",  "deepseek/deepseek-chat"),
    ("OPENAI_MODEL",    "gpt-4o-mini"),
    ("MISTRAL_MODEL",   "mistral/mistral-small-latest"),
]:
    _m = os.getenv(_env, "")
    if _m and _m != _PRIMARY_MODEL:
        _FALLBACK_MODELS.append(_m)
# Ajouter deepseek free comme dernier recours
if "deepseek" not in _PRIMARY_MODEL and not any("deepseek" in m for m in _FALLBACK_MODELS):
    _FALLBACK_MODELS.append("openrouter/deepseek/deepseek-chat:free")

# ── Stopwords transport — jamais des lieux ──────────────────────
_TRANSPORT_WORDS = {
    "pied","vélo","velo","voiture","car","bike","foot","walking",
    "ici","here","là","la","moi","nous","carte","map",
}

# ── Profils ─────────────────────────────────────────────────────
_PROF_LABEL = {"foot":"à pied","bike":"à vélo","car":"en voiture"}


class RoutingAgent:
    """Agent routing autonome — pas de dépendance à BaseAgent."""

    async def run(
        self,
        query:       str,
        context:     dict,
        rag_tools:   list = None,
        agent_config: dict = None,
    ) -> dict:

        from mcp_client import get_mcp_client
        client = get_mcp_client()

        # ══════════════════════════════════════════════════════
        # ÉTAPE 1 — Comprendre la requête (LLM ou fallback regex)
        # ══════════════════════════════════════════════════════
        params = await self._extract_params(query)
        log.info(f"[RoutingAgent] params={params}")

        rtype        = params.get("type", "isochrone")
        place        = (params.get("place") or "").strip()
        time_minutes = int(params.get("time_minutes") or 10)
        profile      = params.get("profile", "foot")
        from_place   = (params.get("from_place") or "").strip()
        to_place     = (params.get("to_place") or "").strip()

        # ── Dispatcher ───────────────────────────────────────
        if any(w in query.lower() for w in ["matrice","matrix","distance entre"]):
            return await self._matrix(client, query, profile, context)

        if rtype == "route" or any(w in query.lower() for w in
                                    ["itinéraire","direction","aller de","comment aller","trajet"]):
            return await self._route(client, query, profile, from_place, to_place, context)

        # Construire les intervalles automatiquement par tranches de 5 min
        # Ex: 10 min → [5, 10]  |  15 min → [5, 10, 15]  |  30 min → [5,10,15,20,25,30]
        explicit = re.findall(r'\b(\d+)\s*(?:min|minute|mn)', query.lower())
        if len(explicit) > 1:
            intervals = sorted(set(int(m) for m in explicit[:5]))
        else:
            intervals = list(range(5, time_minutes + 1, 5))
            if not intervals or intervals[-1] != time_minutes:
                intervals.append(time_minutes)
            intervals = sorted(set(intervals))

        center = await self._resolve_center(client, place, context)

        if len(intervals) > 1:
            return await self._isochrones_multi(client, intervals, profile, center, place)
        return await self._isochrone(client, time_minutes, profile, center, place)

    # ══════════════════════════════════════════════════════════
    # ÉTAPE 1 — Extraction des paramètres
    # ══════════════════════════════════════════════════════════

    async def _extract_params(self, query: str) -> dict:
        """
        Extrait les paramètres routing par regex déterministe.
        Pas de LLM — fiable même sans clé API.
        Le lieu est extrait proprement puis géocodé par Nominatim.
        """

        # ── Fallback regex ────────────────────────────────────
        q = query.lower()

        # Profil
        if any(w in q for w in ["voiture","auto","car","driving"]):
            profile = "car"
        elif any(w in q for w in ["vélo","velo","bike","cyclable"]):
            profile = "bike"
        else:
            profile = "foot"

        # Durée
        m_dur = re.search(r"(\d+)\s*(?:min|mn|minute)", q)
        time_minutes = int(m_dur.group(1)) if m_dur else 10

        # Type
        rtype = "route" if any(w in q for w in [
            "itinéraire","trajet","aller de","comment aller","direction"
        ]) else "isochrone"

        # Lieu : capturer le nom complet après préposition spatiale
        # Priorité aux patterns les plus précis (depuis > de la > du)
        place = ""
        _lieu_patterns = [
            # Adresse avec numéro : "de 22 allée commandant charcot nantes"
            r"(?:depuis|de)\s+(\d+\s+.+?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
            # "depuis la gare de Nantes", "from the station"
            r"(?:depuis|from)\s+(.+?)(?=\s+(?:en\s+(?:voiture|velo|train)|a\s+(?:pied|velo))|\s*$)",
            # "autour du Château des Ducs de Bretagne"
            r"autour\s+du\s+(.+?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
            r"autour\s+de\s+la\s+(.+?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
            r"autour\s+de\s+(.+?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
            # "isochrone 10 min à pied de la gare de Nantes"
            r"isochrone\s+\d+\s*(?:min|mn)\s+\w+\s+(?:de\s+la|du|de\s+l.)\s+(.+?)(?=\s*$)",
            # "de la gare", "du château"
            r"(?:de\s+la|du|de\s+l.)\s+([A-Za-z][A-Za-z\s\-]{3,50}?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
        ]
        for _pat in _lieu_patterns:
            _m = re.search(_pat, query, re.IGNORECASE | re.UNICODE)
            if _m:
                _cand = _m.group(1).strip()
                # Nettoyer les mots de transport en fin
                _cand = re.sub(
                    r"\s+(à\s+pied|en\s+voiture|à\s+vélo|pied|vélo|velo|voiture|foot|bike|car)\s*$",
                    "", _cand, flags=re.IGNORECASE
                ).strip()
                if _cand and _cand.lower() not in _TRANSPORT_WORDS and len(_cand) > 2:
                    place = _cand
                    break

        # Waypoints pour route
        from_place, to_place = "", ""
        m_wp = re.search(
            r"(?:de|from)\s+(.+?)\s+(?:à|to|vers)\s+(.+?)(?:\s+(?:en|par|with)|$)",
            query, re.IGNORECASE
        )
        if m_wp:
            from_place = m_wp.group(1).strip()
            to_place   = m_wp.group(2).strip()

        return {
            "type":         rtype,
            "place":        place,
            "time_minutes": time_minutes,
            "profile":      profile,
            "from_place":   from_place,
            "to_place":     to_place,
        }

    # ══════════════════════════════════════════════════════════
    # ÉTAPE 2 — Résolution du center
    # ══════════════════════════════════════════════════════════

    async def _resolve_center(
        self, client, place: str, context: dict
    ) -> list | None:
        """
        Priorité :
          1. Lieu explicite → geocoder
          2. session.map_center (dernier lieu connu)
          3. map_context.center (vue carte)
        """
        # 1. Lieu explicite dans la query
        if place and place.lower() not in _TRANSPORT_WORDS:
            try:
                r = await client.call_tool(
                    "geocode", {"query": place}, server_name="nominatim"
                )
                if "error" not in r and r.get("longitude"):
                    center = [r["longitude"], r["latitude"]]
                    log.info(f"[RoutingAgent] Geocodé '{place}' → {center}")
                    return center
            except Exception as e:
                log.debug(f"[RoutingAgent] geocode '{place}': {e}")

            # Fallback Nominatim direct (si mcp_nominatim indispo)
            try:
                import requests
                resp = requests.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": place, "format": "json", "limit": 1},
                    headers={"User-Agent": "OpenMapAgents/1.0"},
                    timeout=6,
                )
                results = resp.json()
                if results:
                    center = [float(results[0]["lon"]), float(results[0]["lat"])]
                    log.info(f"[RoutingAgent] Nominatim direct '{place}' → {center}")
                    return center
            except Exception as e:
                log.debug(f"[RoutingAgent] Nominatim direct '{place}': {e}")

        # 2. Session (dernier lieu geocodé)
        mc = context.get("map_center") or context.get("last_center")
        if mc and len(mc) == 2:
            log.info(f"[RoutingAgent] Center depuis session: {mc}")
            return list(mc)

        log.warning(f"[RoutingAgent] Aucun center résolu pour '{place}'")
        return None

    # ══════════════════════════════════════════════════════════
    # ÉTAPE 3 — Appels ORS
    # ══════════════════════════════════════════════════════════

    async def _isochrone(self, client, minutes, profile, center, place) -> dict:
        if not center:
            return {
                "text": (
                    "Je n'ai pas trouvé de point de départ. "
                    "Précisez un lieu (ex: 'isochrone 10 min à pied depuis la Tour Eiffel') "
                    "ou centrez la carte sur votre zone."
                ),
                "tool_calls": [], "tool_results": [],
            }

        prof_label = _PROF_LABEL.get(profile, profile)
        loc_label  = f" — {place}" if place else ""
        layer_name = f"Isochrone {minutes}min {prof_label}{loc_label}"

        # Déléguer au frontend — App.jsx appelle computeIsochrone() via token Mapbox
        tr = {
            "action":       "compute_isochrone",
            "center":       center,
            "time_minutes": minutes,
            "profile":      profile,
            "layer_name":   layer_name,
        }
        text = (
            f"**{layer_name}** affichée.\n"
            f"_Zones accessibles en {minutes} min {prof_label}"
            + (f" depuis {place}" if place else "")
            + ". Suggestions : 'restaurants dans l\\'isochrone', 'pharmacies dans la zone'_"
        )
        return {
            "text":         text,
            "tool_calls":   [{"name":"compute_isochrone","args":{"center":center,"time_minutes":minutes,"profile":profile}}],
            "tool_results": [tr],
        }

    async def _isochrones_multi(self, client, intervals, profile, center, place) -> dict:
        if not center:
            return {"text": "Point de départ non trouvé.",
                    "tool_calls": [], "tool_results": []}

        prof_label = _PROF_LABEL.get(profile, profile)
        loc_label  = f" — {place}" if place else ""
        layer_name = f"Isochrones {intervals[0]}–{intervals[-1]}min {prof_label}{loc_label}"

        # Passer les intervalles au frontend via compute_isochrone multi
        tr = {
            "action":       "compute_isochrone",
            "center":       center,
            "time_minutes": intervals[-1],
            "intervals":    intervals,
            "profile":      profile,
            "layer_name":   layer_name,
        }
        return {
            "text":         f"**{layer_name}** affichées.",
            "tool_calls":   [{"name":"compute_isochrone","args":{"center":center,"intervals":intervals,"profile":profile}}],
            "tool_results": [tr],
        }

    async def _route(self, client, query, profile, from_place, to_place, context) -> dict:
        # Geocoder les waypoints
        waypoints = []
        labels    = []
        for lieu in [from_place, to_place]:
            if not lieu:
                continue
            try:
                r = await client.call_tool(
                    "geocode", {"query": lieu}, server_name="nominatim"
                )
                if "error" not in r and r.get("longitude"):
                    waypoints.append([r["longitude"], r["latitude"]])
                    labels.append(lieu)
            except Exception:
                pass

        if len(waypoints) < 2:
            mc = context.get("map_center") or context.get("last_center")
            if mc and len(waypoints) == 1:
                waypoints = [list(mc)] + waypoints
            else:
                return {
                    "text": "Pour un itinéraire, précisez départ et destination. Ex: 'de la gare à l\\'aéroport'.",
                    "tool_calls": [], "tool_results": [],
                }

        args = {"waypoints": waypoints, "profile": profile}
        r    = await client.call_tool("compute_route", args, server_name="ors")

        if "error" in r:
            return {"text": f"Erreur routing : {r['error']}",
                    "tool_calls": [{"name":"compute_route","args":args}],
                    "tool_results": [r]}

        dist       = r.get("distance_km", 0)
        dur        = r.get("duration_min", 0)
        prof_label = _PROF_LABEL.get(profile, profile)
        loc        = f"{labels[0]} → {labels[-1]}" if labels else ""
        r["layer_name"] = f"Itinéraire {prof_label}" + (f" — {loc}" if loc else "")
        return {
            "text": (
                f"**Itinéraire {prof_label}** : **{dist:.1f} km** en **{int(dur)} min**.\n"
                + (f"_De {loc}_" if loc else "")
            ),
            "tool_calls":   [{"name":"compute_route","args":args}],
            "tool_results": [r],
        }

    async def _matrix(self, client, query, profile, context) -> dict:
        layers = context.get("active_layers", [])
        locations = [
            l["bbox"][0:2] if isinstance(l, dict) and l.get("bbox") else None
            for l in layers[:10]
        ]
        locations = [l for l in locations if l]
        if len(locations) < 2:
            return {"text": "Ajoutez au moins 2 couches de points pour la matrice.",
                    "tool_calls": [], "tool_results": []}

        args = {"locations": locations, "profile": profile, "metric": "both"}
        r    = await client.call_tool("compute_matrix", args, server_name="ors")
        return {
            "text":       f"Matrice distances {profile} calculée ({len(locations)} points).",
            "tool_calls": [{"name":"compute_matrix","args":args}],
            "tool_results": [r],
        }
