"""
debate/moderator.py — Modérateur de la couche débat
=====================================================
Reçoit les 5 outputs des agents de débat.
Arbitre les contradictions selon une hiérarchie de priorité.
Produit le plan d'exécution final avec confidence_score.

Règles d'arbitrage :
  Critique   > Opérationnel > Stratège > Analyste > Synthétiseur
  (sécurité > faisabilité > stratégie > paramètres > synthèse)

RÈGLES PAR DOMAINE — ce qui est obligatoire :
  GEE temporel  → bbox (max 500km²) + dates + cloud_cover
  GEE statique  → bbox uniquement  (WorldCover, Hansen, SRTM, Canopée)
  Overture/OSM  → bbox OPTIONNELLE (geocodable depuis le lieu mentionné)
  WorldBank     → RIEN d'obligatoire (données mondiales)
  Routing       → point de départ (geocodable)
  Spatial/Elev  → pas de dates

Clarification demandée UNIQUEMENT si GEE temporel sans bbox ET sans lieu
geocodable. JAMAIS pour Overture / OSM / WorldBank / spatial / routing.
"""

import os
import logging
from typing import Optional

log = logging.getLogger("moderator")

# ── Seuils ────────────────────────────────────────────────────
CONFIDENCE_MIN_EXECUTE = float(os.getenv("DEBATE_MIN_CONFIDENCE", "0.55"))
CONFIDENCE_HIGH        = float(os.getenv("ROUTER_CONFIDENCE_HIGH", "0.85"))

# ── Limites bbox ──────────────────────────────────────────────
GEE_BBOX_MAX_KM2 = 231542006     # Rasters GEE uniquement
MAX_DURATION_MIN = 280     # Isochrone max 120 min

# ── Servers raster — seuls soumis à la restriction 500km² ────
_RASTER_SERVERS = {"gee", "stac"}

# ── Tools GEE statiques — pas de dates, cloud_cover, composite
# Ces tools n'ont PAS de limite bbox (données mondiales / statiques)
_STATIC_GEE_TOOLS = {
    # WorldCover ESA — toutes variantes de nommage
    "compute_worldcover", "compute_esa_worldcover",
    # Canopée / forêt
    "compute_canopy_height", "compute_canopy",
    "compute_forest_watch", "compute_forest_cover",
    # Terrain
    "compute_srtm", "compute_dem", "compute_elevation",
    # Autres statiques
    "compute_hansen", "compute_global_surface_water",
}

# ── Domaines sans obligation de dates ─────────────────────────
_DOMAINS_NO_DATES = {
    "overture", "osm", "worldbank", "nominatim",
    "spatial", "routing", "elevation",
    "local", "layer",   # couches locales déjà chargées
}

# ── Domaines sans obligation de bbox ──────────────────────────
# "local" / "layer" = opérations sur couches déjà chargées (symboles proportionnels,
# classification, statistiques, style…) — jamais de restriction géographique
_DOMAINS_NO_BBOX = {"worldbank", "local", "layer"}

# ── Domaines où bbox est optionnelle (geocodable) ─────────────
_DOMAINS_BBOX_OPTIONAL = {"overture", "osm", "nominatim", "routing", "spatial", "elevation"}


# ══════════════════════════════════════════════════════════════
# DATA CLASS RÉSULTAT
# ══════════════════════════════════════════════════════════════

class ModerationResult:
    __slots__ = (
        "plan", "confidence", "needs_clarification",
        "clarification_question", "corrections_applied",
        "warnings", "message_utilisateur",
    )

    def __init__(self):
        self.plan:                   dict  = {}
        self.confidence:             float = 0.0
        self.needs_clarification:    bool  = False
        self.clarification_question: str   = ""
        self.corrections_applied:    list  = []
        self.warnings:               list  = []
        self.message_utilisateur:    str   = ""

    def to_dict(self) -> dict:
        return {
            "plan":                   self.plan,
            "confidence":             round(self.confidence, 3),
            "needs_clarification":    self.needs_clarification,
            "clarification_question": self.clarification_question,
            "corrections_applied":    self.corrections_applied,
            "warnings":               self.warnings,
            "message_utilisateur":    self.message_utilisateur,
        }


# ══════════════════════════════════════════════════════════════
# HELPERS GÉOGRAPHIQUES
# ══════════════════════════════════════════════════════════════

def _bbox_area_km2(bbox: list) -> float:
    if not bbox or len(bbox) != 4:
        return 0.0
    return (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) * 111 * 111


def _clip_bbox_gee(bbox: list) -> tuple[list, str]:
    """
    Réduit une bbox GEE trop grande en gardant le centre.
    UNIQUEMENT pour rasters GEE/STAC — jamais pour vecteurs.
    """
    area = _bbox_area_km2(bbox)
    if area <= GEE_BBOX_MAX_KM2:
        return bbox, ""
    cx = (bbox[0] + bbox[2]) / 2
    cy = (bbox[1] + bbox[3]) / 2
    d  = 0.20   # ≈ racine(500km²) / 111 ≈ 0.20°
    clipped = [cx - d, cy - d, cx + d, cy + d]
    return clipped, (
        f"bbox réduite de {area:.0f}km² → ~{GEE_BBOX_MAX_KM2}km² "
        f"(centré sur [{cx:.3f},{cy:.3f}])"
    )


def _validate_dates(start: Optional[str], end: Optional[str]) -> tuple[str, str, str]:
    """
    Valide et corrige les dates. Retourne (start, end, message).
    Appelé UNIQUEMENT pour GEE temporel.
    """
    from datetime import datetime, timedelta
    today = datetime.now().strftime("%Y-%m-%d")

    if not start: start = "2024-01-01"
    if not end:   end   = "2024-12-31"

    try:
        datetime.strptime(start, "%Y-%m-%d")
        datetime.strptime(end,   "%Y-%m-%d")
    except ValueError:
        return "2024-01-01", "2024-12-31", "dates invalides → reset 2024"

    msg = ""
    if end > today:
        end  = today
        msg += f"end_date futur → corrigé à {today} "
    if start >= end:
        ds    = datetime.strptime(end, "%Y-%m-%d") - timedelta(days=180)
        start = ds.strftime("%Y-%m-%d")
        msg  += f"start ≥ end → corrigé à {start} "

    return start, end, msg.strip()


def _is_gee_temporal(server: str, tool: str) -> bool:
    """True si tool GEE temporel : nécessite dates + bbox.
    Robuste aux variantes de nommage (esa_, global_, etc.)
    via correspondance partielle sur mots-clés statiques.
    """
    if server not in _RASTER_SERVERS:
        return False
    if tool in _STATIC_GEE_TOOLS:
        return False
    _STATIC_KEYWORDS = (
        "worldcover", "canopy", "forest_watch", "srtm", "dem",
        "hansen", "surface_water",
    )
    if any(kw in tool for kw in _STATIC_KEYWORDS):
        return False
    return True


# ══════════════════════════════════════════════════════════════
# FILTRAGE RISQUES / MANQUANTS HORS-DOMAINE
# ══════════════════════════════════════════════════════════════

def _risque_hors_domaine(risque: str, server: str, tool: str, gee_temporal: bool) -> bool:
    """
    Retourne True si le risque est non pertinent pour ce domaine.

    Règles :
    - "absence de dates / période / satellite / cloud"
        → hors-domaine sauf GEE temporel
    - "absence de bbox / zone / géographique"
        → hors-domaine pour WorldBank, Overture, OSM, routing, spatial
          (ces domaines geocodent le lieu ou n'ont pas besoin de bbox)
    - risques GEE spécifiques (tuile, raster, ndvi...)
        → hors-domaine si server pas GEE
    """
    r = risque.lower()

    # ── Dates / collection / satellite ───────────────────────
    date_kw = ("date", "periode", "période", "temporel", "start_date", "end_date",
               "collection", "satellite", "sentinel", "landsat", "cloud",
               "nuage", "composite")
    if any(k in r for k in date_kw):
        return not gee_temporal   # pertinent seulement pour GEE temporel

    # ── Bbox / zone géographique ──────────────────────────────
    bbox_kw = ("bbox", "zone", "geographique", "géographique", "emprise",
               "extent", "region", "localisation", "geospatiale", "géospatiale")
    if any(k in r for k in bbox_kw):
        # local/layer/WorldBank : pas de bbox du tout
        if server in _DOMAINS_NO_BBOX:
            return True
        # Overture/OSM/routing/spatial : bbox geocodable → pas bloquant
        if server in _DOMAINS_BBOX_OPTIONAL:
            return True
        # GEE statique : pas bloquant (données mondiales)
        if server in _RASTER_SERVERS and not gee_temporal:
            return True
        # GEE temporel : bbox vraiment requise
        return False

    # ── Risques GEE spécifiques ───────────────────────────────
    gee_kw = ("gee", "earth engine", "raster", "tuile", "tile_url",
              "ndvi", "lst", "ndwi", "evi", "worldcover")
    if any(k in r for k in gee_kw):
        return server not in _RASTER_SERVERS

    return False


def _manquant_hors_domaine(manquant: str, server: str, gee_temporal: bool) -> bool:
    """Retourne True si l'élément manquant n'est pas requis pour ce domaine."""
    m = manquant.lower()

    date_kw = ("start_date", "end_date", "date", "collection", "cloud", "satellite", "periode", "période")
    bbox_kw = ("bbox", "zone", "lieu", "emprise", "localisation")

    if any(k in m for k in date_kw):
        return not gee_temporal                         # dates inutiles hors GEE temporel

    if any(k in m for k in bbox_kw):
        if server in _DOMAINS_NO_BBOX:      return True # WorldBank : pas de bbox
        if server in _DOMAINS_BBOX_OPTIONAL: return True # Overture/OSM : geocodable

    return False


# ══════════════════════════════════════════════════════════════
# MODÉRATEUR PRINCIPAL
# ══════════════════════════════════════════════════════════════

class Moderator:

    def moderate(self, debate_outputs: dict) -> ModerationResult:
        result = ModerationResult()

        analyste     = debate_outputs.get("analyste",     {})
        stratege     = debate_outputs.get("stratege",     {})
        critique     = debate_outputs.get("critique",     {})
        operationnel = debate_outputs.get("operationnel", {})
        synthetiseur = debate_outputs.get("synthetiseur", {})
        rag_tools    = debate_outputs.get("rag_tools",    [])
        query        = debate_outputs.get("query",        "")

        # Résoudre server/tool dès le début (nécessaire pour le filtrage)
        synth_plan   = synthetiseur.get("plan", {})
        server       = synth_plan.get("server") or stratege.get("primary_server", "overture")
        tool         = synth_plan.get("tool")   or stratege.get("primary_tool",   "query_places")
        args         = dict(synth_plan.get("args", {}))
        output_act   = synth_plan.get("output_action") or stratege.get("output_action", "add_markers")

        gee_temporal  = _is_gee_temporal(server, tool)
        raster_server = server in _RASTER_SERVERS

        # ══════════════════════════════════════════════════════
        # ÉTAPE 1 — Blocages critiques (filtrés par domaine)
        # ══════════════════════════════════════════════════════
        if critique.get("bloquant") and critique.get("severite") == "high":
            risques = critique.get("risques", [])

            # Garder uniquement les risques pertinents pour CE domaine
            risques_pertinents = [
                r for r in risques
                if not _risque_hors_domaine(r, server, tool, gee_temporal)
            ]

            # Cas particulier : seules les dates manquent mais bbox déjà présente
            # → l'orchestrateur injectera les dates par défaut, pas bloquant
            synth_bbox = synth_plan.get("args", {}).get("bbox") or analyste.get("bbox")
            only_date_risks = risques_pertinents and all(
                any(k in r.lower() for k in
                    ("date", "collection", "cloud", "satellite", "sentinel", "landsat"))
                for r in risques_pertinents
            )
            if only_date_risks and synth_bbox and gee_temporal:
                log.info(f"[Modérateur] Critique 'dates manquantes' ignoré (bbox présente)")
                risques_pertinents = []

            if risques_pertinents:
                result.needs_clarification    = True
                result.clarification_question = self._build_clarification(
                    risques_pertinents, analyste, server, tool
                )
                result.confidence          = 0.2
                result.warnings            = risques_pertinents
                result.message_utilisateur = (
                    f"Je n'ai pas pu traiter votre requête : "
                    f"{'; '.join(risques_pertinents[:2])}. "
                    f"{result.clarification_question}"
                )
                log.warning(f"[Modérateur] Bloqué par Critique: {risques_pertinents}")
                return result
            else:
                log.info(
                    f"[Modérateur] Critique ignoré "
                    f"(risques hors-domaine {server}): {risques}"
                )

        # ══════════════════════════════════════════════════════
        # ÉTAPE 2 — Plan depuis Synthétiseur (déjà résolu)
        # ══════════════════════════════════════════════════════
        # server, tool, args, output_act initialisés en haut

        # ══════════════════════════════════════════════════════
        # ÉTAPE 3 — Arbitrage Critique sur bbox
        # ══════════════════════════════════════════════════════
        corr_bbox = critique.get("corrections", {}).get("bbox")
        if corr_bbox:
            args["bbox"] = corr_bbox
            result.corrections_applied.append(f"bbox corrigée: {corr_bbox}")

        # Compléter depuis Analyste si absent
        if not args.get("bbox"):
            anal_bbox = analyste.get("bbox")
            if anal_bbox:
                args["bbox"] = anal_bbox

        # ══════════════════════════════════════════════════════
        # ÉTAPE 4 — Limites bbox
        # Règle stricte :
        #   GEE temporel  → clip si > GEE_BBOX_MAX_KM2
        #   GEE statique  → AUCUNE restriction (données mondiales)
        #   Overture/OSM  → avertissement si très large (>50 000 km²), jamais de clip
        #   local/layer/worldbank → JAMAIS de restriction bbox
        # ══════════════════════════════════════════════════════
        if args.get("bbox") and len(args["bbox"]) == 4:

            if raster_server and gee_temporal:
                # GEE temporel uniquement : réduire si trop grande
                clipped, msg = _clip_bbox_gee(args["bbox"])
                if msg:
                    args["bbox"] = clipped
                    result.corrections_applied.append(msg)
                    result.warnings.append(msg)
                    log.info(f"[Modérateur] GEE temporel bbox clippée: {msg}")
            # GEE statique : rien à faire — WorldCover, SRTM, Canopy sont mondiaux

            elif server in ("overture", "osm"):
                area = _bbox_area_km2(args["bbox"])
                if area > 50_000:
                    # Avertissement seulement — pas de clip pour les vecteurs
                    result.warnings.append(
                        f"Zone large ({area:.0f}km²) pour {server} — "
                        f"résultats limités à {args.get('limit', 500)} features"
                    )

            # Toujours synchroniser xmin/ymin/xmax/ymax depuis bbox
            b = args["bbox"]
            args.setdefault("xmin", b[0])
            args.setdefault("ymin", b[1])
            args.setdefault("xmax", b[2])
            args.setdefault("ymax", b[3])

        # ══════════════════════════════════════════════════════
        # ÉTAPE 5 — Dates (GEE temporel UNIQUEMENT)
        # ══════════════════════════════════════════════════════
        if gee_temporal:
            # Récupérer dates depuis Critique > args > Analyste > Synthétiseur
            corr_start = critique.get("corrections", {}).get("start_date")
            corr_end   = critique.get("corrections", {}).get("end_date")
            start = (corr_start
                     or args.get("start_date") or args.get("date_start")
                     or analyste.get("start_date")
                     or synth_plan.get("args", {}).get("start_date"))
            end   = (corr_end
                     or args.get("end_date") or args.get("date_end")
                     or analyste.get("end_date")
                     or synth_plan.get("args", {}).get("end_date"))

            if start or end:
                start, end, date_msg = _validate_dates(start, end)
                args["start_date"] = start
                args["end_date"]   = end
                if date_msg:
                    result.corrections_applied.append(f"dates: {date_msg}")
        else:
            # Non-GEE ou GEE statique : supprimer dates si présentes par erreur
            for k in ("start_date", "end_date", "date_start", "date_end"):
                args.pop(k, None)

        # ══════════════════════════════════════════════════════
        # ÉTAPE 6 — Params Opérationnel
        # ══════════════════════════════════════════════════════
        params_op = operationnel.get("params_finaux", {})
        for k, v in params_op.items():
            if k not in args or args[k] is None:
                args[k] = v

        # cloud_cover seulement pour GEE temporel
        if gee_temporal and "cloud_cover" not in args:
            args["cloud_cover"] = 20

        # Nettoyer cloud_cover / collection / composite pour GEE statique
        if raster_server and not gee_temporal:
            for k in ("cloud_cover", "collection", "composite"):
                args.pop(k, None)

        # ══════════════════════════════════════════════════════
        # ÉTAPE 7 — Secondary steps
        # ══════════════════════════════════════════════════════
        secondary = synth_plan.get("secondary_steps") or stratege.get("secondary_steps", [])

        # ══════════════════════════════════════════════════════
        # ÉTAPE 8 — Calcul confidence score (adapté au domaine)
        # ══════════════════════════════════════════════════════
        confidence = self._compute_confidence(
            synthetiseur=synthetiseur,
            critique=critique,
            operationnel=operationnel,
            rag_tools=rag_tools,
            args=args,
            server=server,
            tool=tool,
            gee_temporal=gee_temporal,
        )
        result.confidence = confidence

        # ══════════════════════════════════════════════════════
        # ÉTAPE 9 — Décision clarification (domaine-aware)
        # ══════════════════════════════════════════════════════
        if confidence < CONFIDENCE_MIN_EXECUTE:
            should_clarify = self._needs_clarification(
                server, tool, args, gee_temporal, query
            )
            if should_clarify:
                manquant = analyste.get("manquant", [])
                # Filtrer les manquants non pertinents pour ce domaine
                manquant_ok = [
                    m for m in manquant
                    if not _manquant_hors_domaine(m, server, gee_temporal)
                ]
                if manquant_ok:
                    result.needs_clarification    = True
                    result.clarification_question = self._build_clarification(
                        manquant_ok, analyste, server, tool
                    )
                    result.warnings.append(
                        f"Confidence faible ({confidence:.2f}) → clarification"
                    )
                    log.info(
                        f"[Modérateur] Confidence {confidence:.2f} < "
                        f"{CONFIDENCE_MIN_EXECUTE} → clarification"
                    )

        # ══════════════════════════════════════════════════════
        # ÉTAPE 10 — Plan final
        # ══════════════════════════════════════════════════════
        result.plan = {
            "server":          server,
            "tool":            tool,
            "args":            args,
            "output_action":   output_act,
            "secondary_steps": secondary,
        }

        result.message_utilisateur = (
            synthetiseur.get("message_utilisateur")
            or analyste.get("resume")
            or query
        )

        for w in operationnel.get("avertissements", []):
            if w and w not in result.warnings:
                result.warnings.append(w)

        log.info(
            f"[Modérateur] Plan: {server}.{tool} "
            f"| action={output_act} "
            f"| confidence={confidence:.2f} "
            f"| corrections={len(result.corrections_applied)}"
        )
        return result

    # ══════════════════════════════════════════════════════════
    # CONFIDENCE SCORE (domaine-aware)
    # ══════════════════════════════════════════════════════════

    def _compute_confidence(
        self,
        synthetiseur: dict,
        critique:     dict,
        operationnel: dict,
        rag_tools:    list,
        args:         dict,
        server:       str,
        tool:         str,
        gee_temporal: bool,
    ) -> float:
        """
        Score 0-1. Pondérations adaptées au domaine :
          RAG score          → 0.35
          Faisabilité op.    → 0.20
          Score synthétiseur → 0.25
          Bbox (si requise)  → 0.15
          Dates (si GEE)     → 0.05
        """
        score = 0.0

        # RAG (0-0.35)
        if rag_tools:
            top = float(rag_tools[0].get("score", 0.0))
            score += min(top * 0.35, 0.35)
        else:
            score += 0.10  # fallback faible si pas de RAG

        # Faisabilité (0-0.20)
        if operationnel.get("faisable", True):
            score += 0.20

        # Synthétiseur (0-0.25)
        synth_conf = float(synthetiseur.get("confidence", 0.5))
        score += synth_conf * 0.25

        # Bbox (0-0.15) — adapté au domaine
        has_bbox = bool(args.get("bbox") and len(args["bbox"]) == 4)
        if server in _DOMAINS_NO_BBOX:
            score += 0.15          # local/layer/WorldBank : bbox non requise → bonus gratuit
        elif server in _DOMAINS_BBOX_OPTIONAL:
            score += 0.15 if has_bbox else 0.08  # Overture/OSM : bonus partiel sans bbox
        elif raster_server := server in _RASTER_SERVERS:
            if has_bbox:
                score += 0.15
            elif gee_temporal:
                score -= 0.10      # GEE temporel sans bbox : pénalité
            # GEE statique sans bbox : ni bonus ni pénalité

        # Dates (0-0.05) — seulement GEE temporel
        if gee_temporal:
            if args.get("start_date") and args.get("end_date"):
                score += 0.05
            # else : pas de pénalité (l'orchestrateur injecte des dates par défaut)
        else:
            score += 0.05  # Non-GEE : bonus gratuit (dates non requises)

        # Pénalités Critique — uniquement sur risques pertinents
        risques = critique.get("risques", [])
        risques_pertinents = [
            r for r in risques
            if not _risque_hors_domaine(r, server, tool, gee_temporal)
        ]
        severite = critique.get("severite", "low")
        if severite == "high"   and risques_pertinents: score -= 0.25
        elif severite == "medium" and risques_pertinents: score -= 0.10
        elif severite == "low":                           score -= 0.03
        score -= len(risques_pertinents) * 0.04

        return round(max(0.0, min(score, 1.0)), 3)

    # ══════════════════════════════════════════════════════════
    # NEEDS CLARIFICATION (domaine-aware)
    # ══════════════════════════════════════════════════════════

    def _needs_clarification(
        self,
        server:       str,
        tool:         str,
        args:         dict,
        gee_temporal: bool,
        query:        str,
    ) -> bool:
        """
        Clarification vraiment nécessaire ?
        OUI : GEE temporel + pas de bbox + pas de lieu geocodable dans la query.
        NON : tout autre domaine.
        """
        # Jamais pour ces domaines
        if server in _DOMAINS_NO_BBOX or server in _DOMAINS_BBOX_OPTIONAL:
            return False
        # local / layer : opérations sur couches chargées → jamais de clarification
        if server in ("local", "layer"):
            return False
        # GEE statique → pas de clarification
        if not gee_temporal:
            return False
        # GEE temporel → seulement si pas de bbox
        if args.get("bbox") and len(args["bbox"]) == 4:
            return False
        # Vérifier si un lieu geocodable est dans la query
        _skip = {
            "ndvi","evi","savi","ndwi","lst","gee","sar","sentinel","landsat",
            "modis","era5","sur","avec","pour","dans","en","de","du","la","le",
            "les","des","un","une","et","ou","au","aux","que","qui",
        }
        tokens = [w for w in query.lower().split() if len(w) >= 3 and w not in _skip]
        return len(tokens) == 0  # pas de lieu → clarification

    # ══════════════════════════════════════════════════════════
    # BUILD CLARIFICATION (domaine-aware)
    # ══════════════════════════════════════════════════════════

    def _build_clarification(
        self,
        manquant: list,
        analyste: dict,
        server:   str = "",
        tool:     str = "",
    ) -> str:
        """Question de clarification adaptée au domaine."""
        if not manquant:
            return "Pouvez-vous préciser la zone géographique ?"

        gee_temporal = _is_gee_temporal(server, tool)

        # Questions de base (toujours disponibles)
        questions = {
            "bbox":        "Quelle zone géographique souhaitez-vous analyser ?",
            "lieu":        "Sur quelle ville ou région souhaitez-vous travailler ?",
            "indicator":   "Quel indicateur économique souhaitez-vous afficher ?",
        }
        # Questions GEE uniquement si pertinent
        if gee_temporal:
            questions.update({
                "start_date": "Quelle année / période vous intéresse ?",
                "end_date":   "Jusqu'à quelle date ?",
                "collection": "Quelle source satellite (Sentinel-2 ou Landsat) ?",
            })

        # Filtrer les manquants non pertinents
        pertinents = [
            m for m in manquant
            if not _manquant_hors_domaine(m, server, gee_temporal)
        ]
        if not pertinents:
            pertinents = [manquant[0]] if manquant else ["bbox"]

        q_list = [questions.get(m, f"Précisez : {m}") for m in pertinents[:2]]
        return " / ".join(q_list)


# ══════════════════════════════════════════════════════════════
# SINGLETON + INTERFACE PUBLIQUE
# ══════════════════════════════════════════════════════════════

_moderator: Optional[Moderator] = None


def get_moderator() -> Moderator:
    global _moderator
    if _moderator is None:
        _moderator = Moderator()
    return _moderator


def moderate(debate_outputs: dict) -> dict:
    """Interface publique — appelée par l'orchestrateur."""
    result = get_moderator().moderate(debate_outputs)
    return result.to_dict()