"""
agents/routing_agent.py — Agent Routing (autonome, sans BaseAgent)
===================================================================
Pipeline en 3 étapes :
  1. Regex déterministe extrait : type, place, time_minutes, profile
  2. Geocode via mcp_nominatim ou Nominatim direct (countrycodes=fr)
  3. mcp_ors.compute_isochrone / compute_route / compute_matrix

Règles texte :
  - layer_name toujours avec _ comme séparateur
  - Résumé itinéraire : km + durée formatée
  - Isochrone : intervalles + lieu + suggestions
  - Jamais de fallback silencieux si lieu fourni mais introuvable
"""

import re
import json
import logging
import os
import time

log = logging.getLogger("routing_agent")

# ── Config LLM ────────────────────────────────────────────────
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openrouter")

_PRIMARY_MAP = {
    "claude":     os.getenv("CLAUDE_MODEL",    "claude-haiku-4-5-20251001"),
    "openai":     os.getenv("OPENAI_MODEL",    "gpt-4o-mini"),
    "openrouter": os.getenv("OPENROUTER_MODEL","openrouter/google/gemma-4-26b-a4b-it:free"),
    "deepseek":   os.getenv("DEEPSEEK_MODEL",  "deepseek/deepseek-chat"),
    "mistral":    os.getenv("MISTRAL_MODEL",   "mistral/mistral-small-latest"),
    "ollama":     os.getenv("OLLAMA_MODEL",    "ollama/llama3.1"),
}
_PRIMARY_MODEL = os.getenv(
    "ROUTING_MODEL",
    _PRIMARY_MAP.get(LLM_PROVIDER, "claude-haiku-4-5-20251001")
)

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
if "deepseek" not in _PRIMARY_MODEL and not any("deepseek" in m for m in _FALLBACK_MODELS):
    _FALLBACK_MODELS.append("openrouter/deepseek/deepseek-chat:free")

# ── Stopwords transport — jamais des lieux ──────────────────────
_TRANSPORT_WORDS = {
    "pied","vélo","velo","voiture","car","bike","foot","walking",
    "ici","here","là","la","moi","nous","carte","map",
}

# ── Mots-clés routing — jamais des noms de villes ───────────────
_ROUTING_WORDS = {
    "itineraire","itinéraire","trajet","route","direction","aller",
    "directions","routing","navigation","chemin",
}

# ── Profils ─────────────────────────────────────────────────────
_PROF_LABEL = {"foot": "à pied", "bike": "à vélo", "car": "en voiture"}


def _slugify(text: str) -> str:
    """Convertit un texte en slug avec _ comme séparateur."""
    return (
        text.strip()
        .replace(" ", "_")
        .replace(",", "")
        .replace("'", "")
        .replace(".", "")
        .replace("-", "_")
    )


def _format_duration(dur_min: float) -> str:
    """Formate une durée en minutes → '1h05' ou '45 min'."""
    total = int(dur_min)
    h, m  = total // 60, total % 60
    return f"{h}h{m:02d}" if h else f"{total} min"


class RoutingAgent:
    """Agent routing autonome — pas de dépendance à BaseAgent."""

    async def run(
        self,
        query:        str,
        context:      dict,
        rag_tools:    list = None,
        agent_config: dict = None,
    ) -> dict:

        from mcp_client import get_mcp_client
        client = get_mcp_client()

        # ══════════════════════════════════════════════════════
        # ÉTAPE 1 — Comprendre la requête (regex déterministe)
        # ══════════════════════════════════════════════════════
        params = await self._extract_params(query)
        log.info(f"[RoutingAgent] params={params}")

        rtype        = params.get("type", "isochrone")
        place        = (params.get("place") or "").strip()
        time_minutes = int(params.get("time_minutes") or 10)
        profile      = params.get("profile", "foot")
        from_place   = (params.get("from_place") or "").strip()
        to_place     = (params.get("to_place") or "").strip()

        # ── Override depuis context si orchestrateur a pré-détecté route implicite ──
        if context.get("_intent") == "route" and context.get("_from_place"):
            _ctx_from = context["_from_place"].strip()
            _ctx_to   = context.get("_to_place", "").strip()
            # Vérifier que ce ne sont pas des mots-clés routing
            if (_ctx_from.lower() not in _ROUTING_WORDS
                    and _ctx_to.lower() not in _ROUTING_WORDS):
                rtype      = "route"
                from_place = _ctx_from
                to_place   = _ctx_to
                log.info(
                    f"[RoutingAgent] override route depuis context: "
                    f"'{from_place}' → '{to_place}'"
                )
            else:
                log.warning(
                    f"[RoutingAgent] override ignoré (mot-clé routing): "
                    f"'{_ctx_from}' / '{_ctx_to}'"
                )

        # ── Dispatcher ───────────────────────────────────────
        if any(w in query.lower() for w in ["matrice", "matrix", "distance entre"]):
            return await self._matrix(client, query, profile, context)

        # Route explicite (mots clés) OU deux waypoints détectés
        if (rtype == "route"
                or any(w in query.lower() for w in
                       ["itinéraire", "direction", "aller de", "comment aller", "trajet"])
                or (from_place and to_place)):
            return await self._route(client, query, profile, from_place, to_place, context)

        # ── Isochrone ─────────────────────────────────────────
        # Construire les intervalles par tranches de 5 min
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
        """
        q = query.lower()

        # ── Profil ────────────────────────────────────────────
        if any(w in q for w in ["voiture", "auto", "car", "driving"]):
            profile = "car"
        elif any(w in q for w in ["vélo", "velo", "bike", "cyclable"]):
            profile = "bike"
        else:
            profile = "foot"

        # ── Durée ─────────────────────────────────────────────
        m_dur = re.search(r"(\d+)\s*(?:min|mn|minute)", q)
        time_minutes = int(m_dur.group(1)) if m_dur else 10

        # ── Waypoints ─────────────────────────────────────────
        from_place, to_place = "", ""

        # Pattern explicite : "de X à Y" / "from X to Y"
        m_wp = re.search(
            r"(?:de|from)\s+(.+?)\s+(?:à|to|vers|jusqu'à|jusqua)\s+(.+?)"
            r"(?:\s+(?:en\s+\w{3,}|à\s+\w{3,}|par\b|with\b)|$)",
            query, re.IGNORECASE
        )
        if m_wp:
            from_place = m_wp.group(1).strip()
            to_place   = m_wp.group(2).strip()

        # Pattern implicite : "ville1 ville2 en voiture/à vélo/à pied"
        # IMPORTANT : chaque ville = 1 seul mot strict
        if not from_place:
            _m_implicit = re.search(
                r"^(?:itinéraire\s+|itineraire\s+)?"   # préfixe optionnel ignoré
                r"([a-zA-ZÀ-ÿ]{3,25})\s+"              # ville1 = 1 mot
                r"([a-zA-ZÀ-ÿ]{3,25})\s+"              # ville2 = 1 mot
                r"(?:en\s+(?:voiture|v[eé]lo)|[àa]\s+(?:pied|v[eé]lo))",
                query, re.IGNORECASE
            )
            if _m_implicit:
                _g1 = _m_implicit.group(1).strip()
                _g2 = _m_implicit.group(2).strip()
                # Rejeter si l'un des groupes est un mot-clé routing ou transport
                if (_g1.lower() not in _ROUTING_WORDS
                        and _g2.lower() not in _ROUTING_WORDS
                        and _g1.lower() not in _TRANSPORT_WORDS
                        and _g2.lower() not in _TRANSPORT_WORDS):
                    from_place = _g1
                    to_place   = _g2
                    log.info(
                        f"[RoutingAgent] route implicite: "
                        f"'{from_place}' → '{to_place}'"
                    )
                else:
                    log.debug(
                        f"[RoutingAgent] route implicite ignorée "
                        f"(mot-clé): '{_g1}' / '{_g2}'"
                    )

        # ── Type ──────────────────────────────────────────────
        _route_keywords = ["itinéraire", "trajet", "aller de", "comment aller", "direction"]
        if from_place and to_place:
            rtype = "route"
        elif any(w in q for w in _route_keywords):
            rtype = "route"
        else:
            rtype = "isochrone"

        # ── Lieu pour isochrone ───────────────────────────────
        place = ""
        if rtype == "isochrone":
            _lieu_patterns = [
                r"(?:depuis|de)\s+(\d+\s+.+?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
                r"(?:depuis|from)\s+(.+?)(?=\s+(?:en\s+(?:voiture|velo|train)|a\s+(?:pied|velo))|\s*$)",
                r"autour\s+du\s+(.+?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
                r"autour\s+de\s+la\s+(.+?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
                r"autour\s+de\s+(.+?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
                r"isochrone\s+\d+\s*(?:min|mn)\s+\w+\s+(?:de\s+la|du|de\s+l.)\s+(.+?)(?=\s*$)",
                r"(?:de\s+la|du|de\s+l.)\s+([A-Za-z][A-Za-z\s\-]{3,50}?)(?=\s+(?:en\s+\w{4,}|a\s+\w{4,})|\s*$)",
            ]
            for _pat in _lieu_patterns:
                _m = re.search(_pat, query, re.IGNORECASE | re.UNICODE)
                if _m:
                    _cand = _m.group(1).strip()
                    _cand = re.sub(
                        r"\s+(à\s+pied|en\s+voiture|à\s+vélo|pied|vélo|velo|voiture|foot|bike|car)\s*$",
                        "", _cand, flags=re.IGNORECASE
                    ).strip()
                    if _cand and _cand.lower() not in _TRANSPORT_WORDS and len(_cand) > 2:
                        place = _cand
                        break

        return {
            "type":         rtype,
            "place":        place,
            "time_minutes": time_minutes,
            "profile":      profile,
            "from_place":   from_place,
            "to_place":     to_place,
        }

    # ══════════════════════════════════════════════════════════
    # GEOCODE HELPER
    # ══════════════════════════════════════════════════════════

    async def _geocode_lieu(self, client, lieu: str) -> list | None:
        """
        Geocode un lieu avec fallback Nominatim direct + countrycodes=fr.
        Retourne [lon, lat] ou None si introuvable.
        Jamais de fallback silencieux.
        """
        if not lieu or not lieu.strip():
            return None

        # Enrichir avec le pays si ville courte sans contexte
        _lieu_query = lieu.strip()
        if len(_lieu_query.split()) <= 2 and "," not in _lieu_query:
            _lieu_query = f"{_lieu_query}, France"

        # Tentative via MCP nominatim
        try:
            r = await client.call_tool(
                "geocode", {"query": _lieu_query}, server_name="nominatim"
            )
            if "error" not in r and r.get("longitude"):
                log.info(
                    f"[RoutingAgent] Geocodé '{_lieu_query}' "
                    f"→ [{r['longitude']},{r['latitude']}]"
                )
                return [r["longitude"], r["latitude"]]
        except Exception:
            pass

        # Fallback Nominatim direct avec countrycodes=fr
        try:
            import requests
            resp = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={
                    "q":            _lieu_query,
                    "format":       "json",
                    "limit":        1,
                    "countrycodes": "fr",
                },
                headers={"User-Agent": "OpenMapAgents/1.0"},
                timeout=6,
            )
            results = resp.json()
            if results:
                log.info(
                    f"[RoutingAgent] Nominatim '{_lieu_query}' "
                    f"→ [{results[0]['lon']},{results[0]['lat']}]"
                )
                return [float(results[0]["lon"]), float(results[0]["lat"])]
            else:
                log.warning(f"[RoutingAgent] Lieu introuvable: '{_lieu_query}'")
        except Exception as e:
            log.warning(f"[RoutingAgent] geocode '{_lieu_query}' échoué: {e}")

        return None

    # ══════════════════════════════════════════════════════════
    # ÉTAPE 2 — Résolution du center
    # ══════════════════════════════════════════════════════════

    async def _resolve_center(
        self, client, place: str, context: dict
    ) -> list | None:
        """
        Priorité :
          0. _resolved_center déjà geocodé par l'orchestrateur
          1. Lieu explicite → geocoder (jamais de fallback silencieux si lieu fourni)
          2. session.map_center (si aucun lieu fourni)
        """
        # ── PRIORITÉ 0 : center déjà résolu par l'orchestrateur ──
        if context.get("_resolved_center"):
            center = context["_resolved_center"]
            log.info(f"[RoutingAgent] Center pré-résolu: {center}")
            return list(center)

        # ── PRIORITÉ 1 : lieu explicite dans la query ──────────
        if place and place.lower() not in _TRANSPORT_WORDS:
            coords = await self._geocode_lieu(client, place)
            if coords:
                return coords
            # Lieu fourni mais introuvable → pas de fallback silencieux
            log.warning(
                f"[RoutingAgent] Lieu '{place}' introuvable → "
                f"pas de fallback sur session"
            )
            return None

        # ── PRIORITÉ 2 : session / map_center (aucun lieu fourni) ──
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
        """Isochrone simple : 1 seule durée max, décomposée en intervalles de 5 min."""
        if not center:
            return {
                "text": (
                    "Je n'ai pas trouvé de point de départ.\n"
                    "Précisez un lieu (ex: _'isochrone 10 min à pied depuis la Tour Eiffel'_) "
                    "ou centrez la carte sur votre zone."
                ),
                "tool_calls": [], "tool_results": [],
            }

        # Intervalles par tranches de 5 min jusqu'à la durée demandée
        intervals = list(range(5, minutes + 1, 5))
        if not intervals or intervals[-1] != minutes:
            intervals.append(minutes)
        intervals = sorted(set(intervals))

        prof_label = _PROF_LABEL.get(profile, profile)
        loc_slug   = f"_{_slugify(place)}" if place else ""
        layer_name = (
            f"Isochrone_{intervals[0]}–{intervals[-1]}min_{prof_label}{loc_slug}"
            if len(intervals) > 1 else
            f"Isochrone_{minutes}min_{prof_label}{loc_slug}"
        )

        tr = {
            "action":       "compute_isochrone",
            "center":       center,
            "time_minutes": minutes,
            "intervals":    intervals,
            "profile":      profile,
            "layer_name":   layer_name,
        }

        text = (
            f"**Isochrone {minutes} min {prof_label}**"
            + (f" — {place}" if place else "")
            + f"\n_Zones accessibles en {minutes} min {prof_label}"
            + (f" depuis **{place}**" if place else "")
            + ". Suggestions : 'restaurants dans l\\'isochrone', 'pharmacies dans la zone'_"
        )

        return {
            "text":         text,
            "tool_calls":   [{"name": "compute_isochrone", "args": {
                "center":       center,
                "time_minutes": minutes,
                "intervals":    intervals,
                "profile":      profile,
            }}],
            "tool_results": [tr],
        }

    async def _isochrones_multi(self, client, intervals, profile, center, place) -> dict:
        """Isochrones multiples : plusieurs intervalles de durée explicites."""
        if not center:
            return {
                "text": (
                    "Je n'ai pas trouvé de point de départ.\n"
                    "Précisez un lieu ou centrez la carte sur votre zone."
                ),
                "tool_calls": [], "tool_results": [],
            }

        prof_label     = _PROF_LABEL.get(profile, profile)
        loc_slug       = f"_{_slugify(place)}" if place else ""
        layer_name     = f"Isochrones_{intervals[0]}–{intervals[-1]}min_{prof_label}{loc_slug}"
        _intervals_str = " / ".join(f"{i} min" for i in intervals)

        tr = {
            "action":       "compute_isochrone",
            "center":       center,
            "time_minutes": intervals[-1],
            "intervals":    intervals,
            "profile":      profile,
            "layer_name":   layer_name,
        }

        text = (
            f"**Isochrones {intervals[0]}–{intervals[-1]} min {prof_label}**"
            + (f" — {place}" if place else "")
            + f"\n_Intervalles : {_intervals_str} {prof_label}"
            + (f" depuis **{place}**" if place else "")
            + ". Suggestions : 'restaurants dans l\\'isochrone'_"
        )

        return {
            "text":         text,
            "tool_calls":   [{"name": "compute_isochrone", "args": {
                "center":    center,
                "intervals": intervals,
                "profile":   profile,
            }}],
            "tool_results": [tr],
        }

    async def _route(self, client, query, profile, from_place, to_place, context) -> dict:
        """Calcule un itinéraire entre deux points."""
        waypoints = []
        labels    = []
        not_found = []

        for lieu in [from_place, to_place]:
            if not lieu:
                continue
            coords = await self._geocode_lieu(client, lieu)
            if coords:
                waypoints.append(coords)
                labels.append(lieu)
            else:
                not_found.append(lieu)

        # ── Lieu introuvable → demander précision (jamais de fallback) ──
        if not_found:
            _missing_str = ", ".join(f"**\"{l}\"**" for l in not_found)
            return {
                "text": (
                    f"Je n'ai pas pu localiser {_missing_str}.\n\n"
                    f"Pouvez-vous préciser :\n"
                    f"- Le nom complet avec département "
                    f"(ex: *Dinard, Ille-et-Vilaine* ou *Dinard 35800*)\n"
                    f"- Ou l'adresse exacte "
                    f"(ex: *place du Général de Gaulle, Dinard*)"
                ),
                "tool_calls":   [],
                "tool_results": [],
                "_needs_clarification": True,
            }

        # ── Pas assez de waypoints ────────────────────────────
        if len(waypoints) < 2:
            return {
                "text": (
                    "Pour un itinéraire, précisez le départ et la destination.\n"
                    "Ex: _'de la gare de Rennes à Dinard en voiture'_"
                ),
                "tool_calls":   [],
                "tool_results": [],
            }

        args = {"waypoints": waypoints, "profile": profile}
        r    = await client.call_tool("compute_route", args, server_name="ors")

        if "error" in r:
            return {
                "text": f"Erreur routing : {r['error']}",
                "tool_calls":   [{"name": "compute_route", "args": args}],
                "tool_results": [r],
            }

        dist       = r.get("distance_km", 0)
        dur        = r.get("duration_min", 0)
        prof_label = _PROF_LABEL.get(profile, profile)

        # ── Nom de couche avec _ comme séparateur ────────────
        _from_slug      = _slugify(labels[0])  if labels           else ""
        _to_slug        = _slugify(labels[-1]) if len(labels) >= 2 else ""
        _layer_name = f"Itinéraire_{_from_slug}_vers_{_to_slug}_{profile}"

        # Injecter action + layer_name pour que ChatPanel dispatchToolResult() fonctionne
        r["action"]     = "compute_route"
        r["layer_name"] = _layer_name
        r["waypoints"]  = waypoints
        r["profile"]    = profile

        # ── Résumé structuré ──────────────────────────────────
        _dur_str = _format_duration(dur)
        _loc_str = f"{labels[0]} → {labels[-1]}" if len(labels) >= 2 else ""

        text = (
            f"**Itinéraire {prof_label}**"
            + (f" — {_loc_str}" if _loc_str else "")
            + f"\n📍 **{dist:.1f} km** · ⏱ **{_dur_str}**"
        )

        return {
            "text":         text,
            "tool_calls":   [{"name": "compute_route", "args": args}],
            "tool_results": [r],
        }

    async def _matrix(self, client, query, profile, context) -> dict:
        """Matrice de distances entre plusieurs points actifs sur la carte."""
        layers    = context.get("active_layers", [])
        locations = [
            l["bbox"][0:2] if isinstance(l, dict) and l.get("bbox") else None
            for l in layers[:10]
        ]
        locations = [l for l in locations if l]

        if len(locations) < 2:
            return {
                "text": (
                    "Ajoutez au moins 2 couches de points sur la carte "
                    "pour calculer une matrice de distances."
                ),
                "tool_calls": [], "tool_results": [],
            }

        args = {"locations": locations, "profile": profile, "metric": "both"}
        r    = await client.call_tool("compute_matrix", args, server_name="ors")

        return {
            "text": (
                f"**Matrice de distances {profile}** calculée "
                f"({len(locations)} points).\n"
                f"_Distances et durées entre tous les points affichées._"
            ),
            "tool_calls":   [{"name": "compute_matrix", "args": args}],
            "tool_results": [r],
        }