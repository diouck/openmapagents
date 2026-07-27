"""
memory/session_memory.py — Mémoire contextuelle de session
===========================================================
Adapté au .env existant :
  REDIS_URL=redis://localhost:6379  (optionnel)
  PG_HOST=geoafrica.fr / PG_PORT=5435 / PG_DB=openmapagents

Ce que la mémoire retient par session :
  - bbox courante (zone affichée sur la carte)
  - layers actifs (nom, type, bbox, source)
  - dernier domaine/agent/tool utilisé
  - dates courantes (start_date / end_date / year)
  - collection satellite en cours
  - indicateur WorldBank en cours
  - profil de transport en cours
  - préférences utilisateur
  - historique des 10 derniers tours de conversation

Résolution automatique des références implicites :
  "même zone" / "ici"         → réutilise current_bbox
  "maintenant en 2023"        → garde bbox, change dates
  "ajoute aussi"              → conserve layers existants
  "en voiture" / "à vélo"     → met à jour current_transport
  "le même indicateur"        → réutilise current_indicator

Intégration dans agent.py :
    from memory.session_memory import SessionMemory
    _mem = SessionMemory()

    @app.post("/api/chat")
    def chat(req: ChatRequest, session_id: str = Header(None)):
        sid     = session_id or req.map_context.get("session_id","anon")
        session = _mem.load(sid)
        _mem.update_from_map_context(session, req.map_context)

        # Injecter dans le system prompt
        context_block = _mem.build_system_context_block(session)
        # → ajouter context_block à SYSTEM_PROMPT avant call_llm()

        result = call_llm(req.messages, req.map_context)

        # Mettre à jour après la réponse
        user_msg = next((m["content"] for m in reversed(req.messages)
                         if m.get("role")=="user"), "")
        _mem.update_from_response(session, result, user_msg)
        _mem.save(session)
        return result
"""

import os
import json
import time
import logging
from typing import Optional
from dataclasses import dataclass, field, asdict

log = logging.getLogger("session_memory")

# ── Config — depuis le .env existant ──────────────────────────
REDIS_URL         = os.getenv("REDIS_URL", "")
SESSION_TTL       = int(os.getenv("SESSION_TTL_SECONDS", str(2 * 3600)))  # 2h
MAX_HISTORY_TURNS = int(os.getenv("SESSION_MAX_HISTORY", "10"))
SESSION_PREFIX    = "oma:session:"


# ═══════════════════════════════════════════════════════════════
# MODÈLES DE DONNÉES
# ═══════════════════════════════════════════════════════════════

@dataclass
class LayerInfo:
    name:          str
    type:          str  = ""
    feature_count: int  = 0
    bbox:          list = field(default_factory=list)
    geom_types:    list = field(default_factory=list)
    source:        str  = ""
    added_at:      float = field(default_factory=time.time)


@dataclass
class SessionData:
    session_id:  str
    user_id:     str   = ""
    created_at:  float = field(default_factory=time.time)
    updated_at:  float = field(default_factory=time.time)

    # ── Contexte carte ────────────────────────────────────────
    current_bbox:   list  = field(default_factory=list)
    map_center:     list  = field(default_factory=list)
    map_zoom:       float = 10.0
    active_layers:  list  = field(default_factory=list)  # list[LayerInfo]

    # ── Contexte agent ────────────────────────────────────────
    last_domain:  str = ""
    last_agent:   str = ""
    last_tool:    str = ""

    # ── Paramètres carto courants ─────────────────────────────
    current_dates:      dict = field(default_factory=dict)
    current_collection: str  = "sentinel2"
    current_indicator:  str  = ""
    current_transport:  str  = "foot"

    # ── Préférences utilisateur ───────────────────────────────
    preferred_palette:  str = ""
    preferred_language: str = "fr"

    # ── Historique conversation ───────────────────────────────
    history: list = field(default_factory=list)
    # [{"role": "user"|"assistant", "content": str, "ts": float}]

    # ── Stats ─────────────────────────────────────────────────
    message_count: int = 0


def _to_dict(s: SessionData) -> dict:
    d = asdict(s)
    return d


def _from_dict(d: dict) -> SessionData:
    # Reconstruire les LayerInfo
    raw_layers = d.pop("active_layers", [])
    layers = []
    for l in raw_layers:
        try:
            layers.append(LayerInfo(**{
                k: v for k, v in l.items()
                if k in LayerInfo.__dataclass_fields__
            }))
        except Exception:
            pass
    d["active_layers"] = layers

    return SessionData(**{
        k: v for k, v in d.items()
        if k in SessionData.__dataclass_fields__
    })


# ═══════════════════════════════════════════════════════════════
# BACKENDS REDIS / RAM
# ═══════════════════════════════════════════════════════════════

class _RamBackend:
    def __init__(self):
        self._store: dict[str, tuple] = {}

    def get(self, key: str) -> Optional[dict]:
        entry = self._store.get(key)
        if not entry: return None
        data, expires = entry
        if time.time() > expires:
            del self._store[key]; return None
        return data

    def set(self, key: str, data: dict, ttl: int):
        self._store[key] = (data, time.time() + ttl)

    def delete(self, key: str):
        self._store.pop(key, None)

    def count(self) -> int:
        now = time.time()
        return sum(1 for _,(_, e) in self._store.items() if e > now)


class _RedisBackend:
    def __init__(self, url: str):
        import redis
        self._r = redis.from_url(url, decode_responses=True)
        self._r.ping()
        log.info("✓ Session memory Redis connecté")

    def get(self, key: str) -> Optional[dict]:
        try:
            raw = self._r.get(key)
            return json.loads(raw) if raw else None
        except Exception as e:
            log.warning(f"Redis session get: {e}"); return None

    def set(self, key: str, data: dict, ttl: int):
        try:
            self._r.setex(key, ttl, json.dumps(data, default=str))
        except Exception as e:
            log.warning(f"Redis session set: {e}")

    def delete(self, key: str):
        try: self._r.delete(key)
        except Exception: pass

    def count(self) -> int:
        try: return len(list(self._r.scan_iter(f"{SESSION_PREFIX}*")))
        except Exception: return 0


# ═══════════════════════════════════════════════════════════════
# SESSION MEMORY
# ═══════════════════════════════════════════════════════════════

class SessionMemory:
    """Mémoire contextuelle de session pour OpenMapAgents."""

    def __init__(self):
        self._backend = None

    def _get_backend(self):
        if self._backend is None:
            if REDIS_URL:
                try:
                    self._backend = _RedisBackend(REDIS_URL)
                except Exception as e:
                    log.warning(f"Redis indispo ({e}) → RAM")
                    self._backend = _RamBackend()
            else:
                self._backend = _RamBackend()
        return self._backend

    def _key(self, sid: str) -> str:
        return f"{SESSION_PREFIX}{sid}"

    # ─── LOAD / SAVE ──────────────────────────────────────────

    def load(self, session_id: str, user_id: str = "") -> SessionData:
        """Charge ou crée une session."""
        raw = self._get_backend().get(self._key(session_id))
        if raw:
            try:
                s = _from_dict(raw)
                log.debug(f"Session {session_id[:12]} chargée "
                          f"({s.message_count} msgs, "
                          f"domain={s.last_domain})")
                return s
            except Exception as e:
                log.warning(f"Session corrompue ({e}) → nouvelle")
        return SessionData(session_id=session_id, user_id=user_id)

    def save(self, session: SessionData):
        """Sauvegarde avec reset du TTL."""
        session.updated_at = time.time()
        self._get_backend().set(
            self._key(session.session_id),
            _to_dict(session),
            SESSION_TTL,
        )

    def delete(self, session_id: str):
        self._get_backend().delete(self._key(session_id))

    # ─── CONTEXT POUR LE LLM ─────────────────────────────────

    def get_context(self, session: SessionData) -> dict:
        """Dict de contexte à injecter dans le system prompt."""
        ctx: dict = {}

        if session.current_bbox:
            ctx["current_bbox"] = session.current_bbox
            x1,y1,x2,y2 = session.current_bbox
            ctx["current_bbox_str"] = f"[{x1:.4f},{y1:.4f},{x2:.4f},{y2:.4f}]"

        if session.active_layers:
            ctx["active_layers"] = [
                {"name": l.name, "type": l.type,
                 "bbox": l.bbox, "source": l.source}
                for l in session.active_layers[-5:]
            ]
            ctx["layers_summary"] = ", ".join(
                f'"{l.name}"' for l in session.active_layers[-3:]
            )

        for attr in ("current_dates","current_collection",
                     "current_indicator","current_transport",
                     "last_domain","last_tool"):
            v = getattr(session, attr, None)
            if v: ctx[attr] = v

        ctx["message_count"] = session.message_count
        return ctx

    def build_system_context_block(self, session: SessionData) -> str:
        """
        Bloc texte à ajouter à SYSTEM_PROMPT dans agent.py.
        Insérer juste avant full_messages = [{"role":"system"...}]
        """
        ctx   = self.get_context(session)
        if not ctx or session.message_count == 0:
            return ""   # première requête : pas de contexte à injecter

        lines = ["\n\n=== CONTEXTE SESSION CARTOGRAPHIQUE ==="]

        if ctx.get("current_bbox_str"):
            lines.append(f"Zone courante (bbox): {ctx['current_bbox_str']}")
            lines.append(
                '→ "même zone"/"ici"/"cette zone" = utiliser cette bbox'
            )

        if ctx.get("layers_summary"):
            lines.append(f"Couches actives: {ctx['layers_summary']}")

        if ctx.get("current_dates"):
            d = ctx["current_dates"]
            if "year" in d:
                lines.append(f"Année courante: {d['year']}")
            else:
                lines.append(
                    f"Période courante: "
                    f"{d.get('start_date','')} → {d.get('end_date','')}"
                )
            lines.append(
                '→ "maintenant en 2023"/"pour 2022" = '
                'garder bbox, changer les dates'
            )

        if ctx.get("current_collection"):
            lines.append(f"Collection satellite: {ctx['current_collection']}")

        if ctx.get("current_indicator"):
            lines.append(f"Indicateur WorldBank: {ctx['current_indicator']}")
            lines.append('→ "le même indicateur" = réutiliser ce code')

        if ctx.get("current_transport"):
            lines.append(f"Profil transport: {ctx['current_transport']}")

        if ctx.get("last_domain"):
            lines.append(f"Dernier domaine: {ctx['last_domain']}")

        lines.append("=== FIN CONTEXTE ===")
        return "\n".join(lines)

    # ─── UPDATE DEPUIS MAP_CONTEXT ────────────────────────────

    def update_from_map_context(
        self, session: SessionData, map_context: Optional[dict]
    ):
        """
        Met à jour depuis map_context={layers,center,zoom,bbox} du frontend.
        Appelé en début de /api/chat avant call_llm().
        Compatible avec le ChatRequest de agent.py existant.
        """
        if not map_context:
            return

        # Bbox — essayer plusieurs sources
        bbox = map_context.get("bbox")
        if bbox and len(bbox) == 4:
            session.current_bbox = [float(v) for v in bbox]
        elif map_context.get("center") and map_context.get("zoom"):
            c    = map_context["center"]
            z    = float(map_context.get("zoom", 10))
            # Approximation de la bbox depuis center+zoom
            delta = 360 / (2 ** z)
            session.current_bbox = [
                c[0]-delta,   c[1]-delta/2,
                c[0]+delta,   c[1]+delta/2,
            ]

        if map_context.get("center"):
            session.map_center = map_context["center"]
        if map_context.get("zoom"):
            session.map_zoom = float(map_context["zoom"])

        # Layers — compatible avec le format de agent.py
        # {name, featureCount, geomTypes, bbox, type}
        raw_layers = map_context.get("layers", [])
        if raw_layers is not None:
            session.active_layers = [
                LayerInfo(
                    name=          l.get("name", ""),
                    type=          l.get("type") or l.get("layer_type",""),
                    feature_count= l.get("featureCount") or l.get("feature_count",0),
                    bbox=          l.get("bbox", []),
                    geom_types=    l.get("geomTypes") or l.get("geom_types",[]),
                    source=        l.get("source",""),
                )
                for l in raw_layers
                if l.get("name")
            ]

    # ─── UPDATE DEPUIS RÉPONSE ────────────────────────────────

    def update_from_response(
        self,
        session:      SessionData,
        response:     dict,
        user_message: str = "",
    ):
        """
        Met à jour après réponse de call_llm() / orchestrate().
        response = {"text":..., "tool_calls":[...], "tool_results":[...]}
        """
        session.message_count += 1

        # Historique
        if user_message:
            session.history.append({
                "role": "user", "content": user_message[:400],
                "ts": time.time()
            })
        if response.get("text"):
            session.history.append({
                "role": "assistant", "content": response["text"][:400],
                "ts": time.time()
            })
        # Garder les N derniers tours
        max_entries = MAX_HISTORY_TURNS * 2
        if len(session.history) > max_entries:
            session.history = session.history[-max_entries:]

        # Extraire infos des tool_calls
        for tc in response.get("tool_calls", []):
            self._parse_tool_call(session, tc)

        # Extraire infos des tool_results (bbox retournée, layers ajoutés)
        for tr in response.get("tool_results", []):
            self._parse_tool_result(session, tr)

    # ─── PARSING INTERNE ──────────────────────────────────────

    # Mapping tool → domaine (doit rester sync avec TOOL_TO_SERVER de mcp_client.py)
    _TOOL_DOMAIN = {
        # GEE
        **{t:"gee" for t in [
            "compute_ndvi","compute_rgb","compute_evi","compute_ndwi",
            "compute_savi","compute_timelapse","compute_change_detection",
            "compute_sar_vv","compute_sar_vh","compute_sar_vv_vh","compute_sar_rgb",
            "compute_lst_modis","compute_lst_landsat",
            "compute_era5_temp","compute_era5_precip","compute_era5_humidity",
            "compute_modis_ndvi","compute_esa_worldcover","compute_forest_watch",
            "compute_canopy_height","compute_elevation","compute_slope",
            "compute_hillshade","get_available_dates",
        ]},
        # Routing
        **{t:"routing" for t in [
            "compute_isochrone","compute_isochrones_multi",
            "compute_route","compute_matrix",
        ]},
        # WorldBank
        **{t:"worldbank" for t in [
            "get_indicator","world_bank_indicator","list_indicators",
            "get_country_profile","compare_countries","get_indicator_timeseries",
        ]},
        # Overture
        **{t:"overture" for t in [
            "query_overture","query_places","query_buildings",
            "query_roads","query_divisions","query_addresses",
        ]},
        # OSM
        **{t:"osm" for t in [
            "query_overpass","get_amenities","get_street_network",
            "get_water_features","get_green_spaces","get_public_transport",
            "get_landuse","get_buildings_osm",
        ]},
        # Spatial
        **{t:"spatial" for t in [
            "spatial_buffer","spatial_join","spatial_intersect",
            "spatial_union","spatial_difference","spatial_clip",
            "points_in_polygon","spatial_analysis",
        ]},
        # Nominatim
        **{t:"nominatim" for t in [
            "geocode","reverse_geocode","autocomplete",
            "geocode_batch","get_bbox_for_place",
        ]},
        # Elevation
        **{t:"elevation" for t in [
            "get_elevation_profile","get_elevation_point","get_elevation_grid",
            "get_contours","get_hillshade_url","get_slope_analysis",
        ]},
    }

    def _parse_tool_call(self, session: SessionData, tc: dict):
        """Extrait paramètres utiles d'un tool_call."""
        name = tc.get("name","")
        args = tc.get("args",{})

        # Domaine
        domain = self._TOOL_DOMAIN.get(name)
        if domain:
            session.last_domain = domain
        session.last_tool = name

        # Dates
        if args.get("start_date") and args.get("end_date"):
            session.current_dates = {
                "start_date": args["start_date"],
                "end_date":   args["end_date"],
            }
        elif args.get("year"):
            session.current_dates = {"year": str(args["year"])}

        # Collection satellite
        if args.get("collection"):
            session.current_collection = args["collection"]

        # Indicateur WorldBank
        if args.get("indicator"):
            session.current_indicator = args["indicator"]

        # Profil transport
        if args.get("profile"):
            session.current_transport = args["profile"]

        # Bbox depuis les args du tool
        bbox = args.get("bbox")
        if bbox and len(bbox) == 4:
            session.current_bbox = [float(v) for v in bbox]
        elif all(k in args for k in ["xmin","ymin","xmax","ymax"]):
            try:
                session.current_bbox = [
                    float(args["xmin"]), float(args["ymin"]),
                    float(args["xmax"]), float(args["ymax"]),
                ]
            except (TypeError, ValueError):
                pass

    def _parse_tool_result(self, session: SessionData, tr: dict):
        """Extrait infos du résultat d'un tool."""
        if not isinstance(tr, dict):
            return

        action = tr.get("action","")

        # Nouveau layer → l'enregistrer dans active_layers
        if action in ("add_layer","add_markers","add_isochrone",
                      "add_choropleth","add_timelapse","add_route",
                      "add_multiple_layers"):

            if action == "add_multiple_layers":
                for sub in tr.get("layers",[]):
                    self._register_layer(session, sub)
            else:
                self._register_layer(session, tr)

        # Géocodage → mise à jour bbox
        if action == "geocode_result":
            bbox = tr.get("bbox",[])
            if bbox and len(bbox) == 4:
                session.current_bbox = bbox

        # fly_to_place → mise à jour centre
        if action == "fly_to_place":
            lng = tr.get("longitude")
            lat = tr.get("latitude")
            if lng and lat:
                session.map_center = [lng, lat]
            bbox = tr.get("bbox",[])
            if bbox and len(bbox) == 4:
                session.current_bbox = bbox

    def _register_layer(self, session: SessionData, layer_data: dict):
        """Enregistre un layer dans active_layers de la session."""
        name = layer_data.get("layer_name","")
        if not name:
            return

        # Éviter les doublons
        existing = [l for l in session.active_layers if l.name == name]
        if existing:
            return

        action = layer_data.get("action","")
        ltype  = {
            "add_layer":      "raster",
            "add_markers":    "vector_points",
            "add_isochrone":  "vector_polygon",
            "add_choropleth": "choropleth",
            "add_timelapse":  "timelapse",
            "add_route":      "vector_line",
        }.get(action, "unknown")

        # Garder max 10 layers en mémoire
        if len(session.active_layers) >= 10:
            session.active_layers.pop(0)

        session.active_layers.append(LayerInfo(
            name=   name,
            type=   ltype,
            source= layer_data.get("provider",""),
            bbox=   layer_data.get("bbox", session.current_bbox or []),
        ))
        log.debug(f"Session {session.session_id[:10]} → layer: {name}")

    # ─── STATS ────────────────────────────────────────────────

    def stats(self) -> dict:
        return {
            "backend":         type(self._get_backend()).__name__,
            "active_sessions": self._get_backend().count(),
            "session_ttl_s":   SESSION_TTL,
            "max_history":     MAX_HISTORY_TURNS,
            "redis_configured":bool(REDIS_URL),
        }


# ═══════════════════════════════════════════════════════════════
# SINGLETON
# ═══════════════════════════════════════════════════════════════

_memory: Optional[SessionMemory] = None

def get_session_memory() -> SessionMemory:
    global _memory
    if _memory is None:
        _memory = SessionMemory()
    return _memory


# ═══════════════════════════════════════════════════════════════
# FASTAPI ROUTER
# ═══════════════════════════════════════════════════════════════

from fastapi import APIRouter, Header
memory_router = APIRouter(prefix="/api/session", tags=["session"])

@memory_router.get("/stats")
def session_stats():
    return get_session_memory().stats()

@memory_router.delete("/{session_id}")
def session_delete(session_id: str):
    get_session_memory().delete(session_id)
    return {"session_id": session_id, "deleted": True}

@memory_router.get("/{session_id}/context")
def session_context(session_id: str):
    mem = get_session_memory()
    s   = mem.load(session_id)
    return mem.get_context(s)
