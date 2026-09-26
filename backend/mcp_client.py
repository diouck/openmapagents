"""
mcp_client.py — Client MCP unifié pour OpenMapAgents
=====================================================
Remplace tous les appels directs aux APIs (GEE, ORS, WorldBank, DuckDB, Nominatim...)
par un client unique qui route vers les MCP servers appropriés.

Compatible avec l'agent.py existant : fonctionne en parallèle via
le flag MCP_ENABLED dans .env. Si MCP_ENABLED=false, l'ancien execute_tool()
reste actif (fallback transparent).

Usage dans agent.py / orchestrator.py :
    from mcp_client import MCPClient
    client = MCPClient()
    result = await client.call_tool("overture", "query_buildings", {"bbox": [...]})
"""

import os
import json
import hashlib
import logging
import asyncio
import time
from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum

log = logging.getLogger("mcp_client")

# ═══════════════════════════════════════════════════════════════
# REGISTRE DES SERVERS MCP
# Mapping nom logique → classe MCP server
# Chaque entry correspond à un fichier mcp_servers/mcp_*.py
# ═══════════════════════════════════════════════════════════════

MCP_SERVER_REGISTRY = {
    # Nom logique      → module Python à importer
    "gee":             "mcp_servers.mcp_gee",
    "ors":             "mcp_servers.mcp_ors",
    "worldbank":       "mcp_servers.mcp_worldbank",
    "overture":        "mcp_servers.mcp_overture",
    "postgis":         "mcp_servers.mcp_postgis",
    "nominatim":       "mcp_servers.mcp_nominatim",
    "osm":             "mcp_servers.mcp_osm",
    "stac":            "mcp_servers.mcp_stac",
    "maptiler":        "mcp_servers.mcp_maptiler",
    "cache":           "mcp_servers.mcp_cache",
}

# Mapping tool_name → server logique
# Permet d'appeler call_tool("compute_ndvi", args) sans préciser le server
TOOL_TO_SERVER = {
    # GEE
    "compute_ndvi":             "gee",
    "compute_timelapse":        "gee",
    "compute_rgb":              "gee",
    "compute_lst":              "gee",
    "compute_change_detection": "gee",
    # ORS
    "compute_isochrone":        "ors",
    "compute_route":            "ors",
    "compute_matrix":           "ors",
    # WorldBank — remplace world_bank_indicator de l'ancien agent.py
    "get_indicator":            "worldbank",
    "list_indicators":          "worldbank",
    "get_country_profile":      "worldbank",
    "world_bank_indicator":     "worldbank",   # alias rétrocompatibilité
    # Overture — remplace query_overture + DuckDB inline
    "query_buildings":          "overture",
    "query_roads":              "overture",
    "query_divisions":          "overture",
    "query_places":             "overture",
    "query_overture":           "overture",    # alias rétrocompatibilité
    # PostGIS — remplace spatial_analysis turf.js backend
    "spatial_buffer":           "postgis",
    "spatial_join":             "postgis",
    "spatial_intersect":        "postgis",
    "query_table":              "postgis",
    "spatial_analysis":         "postgis",     # alias rétrocompatibilité
    # Nominatim — remplace geocode inline
    "geocode":                  "nominatim",
    "reverse_geocode":          "nominatim",
    "autocomplete":             "nominatim",
    # OSM
    "query_overpass":           "osm",
    "get_street_network":       "osm",
    "get_amenities":            "osm",
    # STAC
    "search_catalog":           "stac",
    "get_available_dates":      "stac",
    "get_scene_preview":        "stac",
    # MapTiler
    "get_elevation_profile":    "maptiler",
    "get_contours":             "maptiler",
    "get_hillshade":            "maptiler",
    # Cache (interne)
    "cache_get":                "cache",
    "cache_set":                "cache",
    "cache_invalidate":         "cache",
}

# TTL cache par server (secondes)
CACHE_TTL = {
    "gee":        86_400,    # 24h — URLs GEE valables 24h
    "worldbank":  604_800,   # 7 jours — données économiques stables
    "nominatim":  2_592_000, # 30 jours — géocodage très stable
    "overture":   3_600,     # 1h — données OSM/Overture
    "ors":        3_600,     # 1h — isochrones/routes
    "stac":       3_600,     # 1h — catalog scenes
    "maptiler":   86_400,    # 24h — élévation stable
    "osm":        3_600,     # 1h
    "postgis":    300,        # 5min — données locales changeantes
}

# Timeout par server (secondes)
SERVER_TIMEOUT = {
    "gee":       300,  # 5 min
    "ors":       10,
    "worldbank": 15,
    "overture":  300,  # 5 min — DuckDB peut être lent sur large bbox
    "postgis":   10,
    "nominatim": 5,
    "osm":       15,
    "stac":      10,
    "maptiler":  10,
    "cache":     2,
}


# ═══════════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ═══════════════════════════════════════════════════════════════

class CircuitState(Enum):
    CLOSED   = "closed"    # normal
    OPEN     = "open"      # server down, rejette les appels
    HALF_OPEN = "half_open" # test si server est remonté


@dataclass
class CircuitBreaker:
    server:         str
    failure_threshold: int   = 5
    reset_timeout:  float    = 60.0   # secondes avant retry
    state:          CircuitState = CircuitState.CLOSED
    failure_count:  int      = 0
    last_failure:   float    = 0.0

    def call_allowed(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure > self.reset_timeout:
                self.state = CircuitState.HALF_OPEN
                log.info(f"[CB:{self.server}] HALF_OPEN — test si server remonté")
                return True
            return False
        # HALF_OPEN : on laisse passer un seul appel test
        return True

    def record_success(self):
        self.failure_count = 0
        if self.state != CircuitState.CLOSED:
            log.info(f"[CB:{self.server}] CLOSED — server opérationnel")
        self.state = CircuitState.CLOSED

    def record_failure(self, error: str):
        self.failure_count += 1
        self.last_failure = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            log.warning(
                f"[CB:{self.server}] OPEN après {self.failure_count} échecs — "
                f"dernière erreur: {error}"
            )


# ═══════════════════════════════════════════════════════════════
# CACHE SIMPLE (RAM + optionnel Redis)
# Phase 4 ajoutera le cache Redis complet.
# Pour l'instant : cache RAM avec TTL, compatible avec l'ancien
# cache fichier CACHE_DIR de agent.py
# ═══════════════════════════════════════════════════════════════

@dataclass
class CacheEntry:
    value:      Any
    expires_at: float


class MCPCache:
    """Cache RAM avec TTL. Redis optionnel (activé si REDIS_URL présent)."""

    def __init__(self):
        self._store: dict[str, CacheEntry] = {}
        self._redis = None
        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            try:
                import redis
                self._redis = redis.from_url(redis_url, decode_responses=True)
                self._redis.ping()
                log.info("✓ MCPCache Redis connecté")
            except Exception as e:
                log.warning(f"MCPCache Redis indisponible, fallback RAM: {e}")
                self._redis = None

    def _key(self, server: str, tool: str, args: dict) -> str:
        payload = json.dumps({"server": server, "tool": tool, "args": args},
                             sort_keys=True, default=str)
        return f"mcp:{hashlib.md5(payload.encode()).hexdigest()}"

    def get(self, server: str, tool: str, args: dict) -> Optional[Any]:
        key = self._key(server, tool, args)
        # Redis en priorité
        if self._redis:
            try:
                raw = self._redis.get(key)
                if raw:
                    log.debug(f"[Cache HIT Redis] {server}.{tool}")
                    return json.loads(raw)
            except Exception:
                pass
        # RAM
        entry = self._store.get(key)
        if entry and time.time() < entry.expires_at:
            log.debug(f"[Cache HIT RAM] {server}.{tool}")
            return entry.value
        return None

    def set(self, server: str, tool: str, args: dict,
            value: Any, ttl: Optional[int] = None):
        key = self._key(server, tool, args)
        ttl = ttl or CACHE_TTL.get(server, 3600)
        # Redis
        if self._redis:
            try:
                self._redis.setex(key, ttl, json.dumps(value, default=str))
            except Exception as e:
                log.warning(f"Cache Redis set failed: {e}")
        # RAM
        self._store[key] = CacheEntry(
            value=value,
            expires_at=time.time() + ttl
        )

    def invalidate(self, pattern: str):
        """Invalide les entrées RAM dont la clé contient le pattern."""
        keys_to_del = [k for k in self._store if pattern in k]
        for k in keys_to_del:
            del self._store[k]
        if self._redis:
            try:
                keys = self._redis.keys(f"mcp:*{pattern}*")
                if keys:
                    self._redis.delete(*keys)
            except Exception:
                pass
        log.info(f"Cache invalidé: {len(keys_to_del)} entrées RAM ({pattern})")


# ═══════════════════════════════════════════════════════════════
# MCP CLIENT PRINCIPAL
# ═══════════════════════════════════════════════════════════════

class MCPClient:
    """
    Client MCP unifié — point d'entrée unique pour tous les outils externes.

    Remplace progressivement les appels directs dans agent.py :
        AVANT : execute_tool("geocode", args)
        APRÈS : await mcp_client.call_tool("geocode", args)

    Compatible avec le mode synchrone via call_tool_sync() pour
    les endpoints FastAPI non-async de l'agent.py existant.
    """

    def __init__(self):
        self._servers:  dict[str, Any]           = {}   # instances MCP chargées
        self._breakers: dict[str, CircuitBreaker] = {}
        self._cache = MCPCache()
        self._enabled = os.getenv("MCP_ENABLED", "true").lower() == "true"
        log.info(f"MCPClient init — MCP_ENABLED={self._enabled}")

    # ─── Chargement lazy des servers ─────────────────────────────

    def _get_server(self, server_name: str) -> Any:
        """Charge le MCP server si pas encore instancié (lazy loading)."""
        if server_name not in self._servers:
            module_path = MCP_SERVER_REGISTRY.get(server_name)
            if not module_path:
                raise ValueError(f"MCP server inconnu: '{server_name}'. "
                                 f"Disponibles: {list(MCP_SERVER_REGISTRY.keys())}")
            try:
                import importlib
                mod = importlib.import_module(module_path)
                # Convention : chaque mcp_*.py expose une classe NomServer
                # ex: mcp_gee.py → class GEEServer
                class_name = "".join(
                    p.capitalize() for p in server_name.split("_")
                ) + "Server"
                server_cls = getattr(mod, class_name)
                self._servers[server_name] = server_cls()
                log.info(f"✓ MCP server chargé: {server_name} ({class_name})")
            except ImportError as e:
                log.warning(f"⚠ MCP server '{server_name}' non disponible: {e}")
                raise RuntimeError(
                    f"MCP server '{server_name}' non implémenté. "
                    f"Créer mcp_servers/{module_path.split('.')[-1]}.py"
                ) from e
        return self._servers[server_name]

    def _get_breaker(self, server_name: str) -> CircuitBreaker:
        if server_name not in self._breakers:
            self._breakers[server_name] = CircuitBreaker(server=server_name)
        return self._breakers[server_name]

    # ─── Résolution server depuis tool_name ──────────────────────

    def resolve_server(self, tool_name: str) -> str:
        """Retourne le server logique pour un tool_name donné."""
        server = TOOL_TO_SERVER.get(tool_name)
        if not server:
            raise ValueError(
                f"Tool '{tool_name}' non mappé. "
                f"Ajouter dans TOOL_TO_SERVER dans mcp_client.py"
            )
        return server

    # ─── Appel MCP async principal ───────────────────────────────

    async def call_tool(
        self,
        tool_name:   str,
        args:        dict,
        server_name: Optional[str] = None,
        use_cache:   bool = True,
        ttl:         Optional[int] = None,
    ) -> dict:
        """
        Appelle un tool MCP avec cache + circuit breaker + timeout.

        Args:
            tool_name:   Nom du tool (ex: "compute_ndvi", "geocode")
            args:        Arguments du tool
            server_name: Server MCP (auto-détecté depuis TOOL_TO_SERVER si absent)
            use_cache:   Utiliser le cache (défaut True)
            ttl:         TTL cache custom en secondes

        Returns:
            dict avec le résultat, toujours avec une clé "action" ou "error"
        """
        # Résoudre le server
        server = server_name or self.resolve_server(tool_name)
        timeout = SERVER_TIMEOUT.get(server, 20)

        # 1. Cache hit ?
        if use_cache:
            cached = self._cache.get(server, tool_name, args)
            if cached is not None:
                return {**cached, "_cache_hit": True}

        # 2. Circuit breaker
        breaker = self._get_breaker(server)
        if not breaker.call_allowed():
            log.warning(f"[CB:{server}] Circuit OPEN — appel rejeté")
            return {
                "error": f"Service '{server}' temporairement indisponible "
                         f"(trop d'erreurs récentes). Réessayez dans 60s.",
                "server": server,
                "tool":   tool_name,
            }

        # 3. Appel avec timeout
        start = time.time()
        try:
            srv = self._get_server(server)

            # Appel async avec timeout
            result = await asyncio.wait_for(
                srv.call(tool_name, args),
                timeout=timeout
            )

            latency = int((time.time() - start) * 1000)
            breaker.record_success()
            log.info(
                f"[MCP] {server}.{tool_name} ✓ {latency}ms "
                f"cache={use_cache} args_keys={list(args.keys())}"
            )

            # 4. Mise en cache — jamais les erreurs ni les résultats sans tile_url GEE
            should_cache = (
                use_cache
                and "error" not in result
                and not result.get("_cache_hit")
                # Pour GEE : ne cacher que si tile_url présent (évite de cacher les erreurs GEE)
                and (server != "gee" or bool(result.get("tile_url")))
            )
            if should_cache:
                self._cache.set(server, tool_name, args, result, ttl=ttl)

            return result

        except asyncio.TimeoutError:
            breaker.record_failure(f"timeout>{timeout}s")
            log.error(f"[MCP] {server}.{tool_name} TIMEOUT >{timeout}s")
            return {
                "error":   f"Timeout: '{server}.{tool_name}' a dépassé {timeout}s",
                "server":  server,
                "tool":    tool_name,
            }
        except RuntimeError as e:
            # Server non implémenté — fallback vers legacy execute_tool
            log.warning(f"[MCP] {server}.{tool_name} non implémenté → fallback legacy")
            return await self._legacy_fallback(tool_name, args)
        except Exception as e:
            breaker.record_failure(str(e))
            log.error(f"[MCP] {server}.{tool_name} ERROR: {e}")
            return {
                "error":  str(e),
                "server": server,
                "tool":   tool_name,
            }

    # ─── Version synchrone (pour endpoints FastAPI non-async) ────

    def call_tool_sync(
        self,
        tool_name:   str,
        args:        dict,
        server_name: Optional[str] = None,
        use_cache:   bool = True,
    ) -> dict:
        """
        Version synchrone de call_tool() — compatible AnyIO (FastAPI).
        Toujours via ThreadPoolExecutor pour éviter get_event_loop() dans AnyIO.
        """
        import concurrent.futures
        timeout = SERVER_TIMEOUT.get(server_name or TOOL_TO_SERVER.get(tool_name, "default"), 30)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    asyncio.run,
                    self.call_tool(tool_name, args, server_name, use_cache)
                )
                return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            log.error(f"call_tool_sync timeout ({timeout}s): {tool_name}")
            return {"error": f"Timeout {timeout}s: {tool_name}", "tool": tool_name}
        except Exception as e:
            log.error(f"call_tool_sync error: {e}")
            return {"error": str(e), "tool": tool_name}

    # ─── Fallback legacy (tant que MCP servers pas tous implémentés) ──

    async def _legacy_fallback(self, tool_name: str, args: dict) -> dict:
        """
        Fallback vers l'ancien execute_tool() de agent.py.
        Utilisé automatiquement si le MCP server n'est pas encore créé.
        Permet une migration progressive sans casser l'existant.
        """
        log.info(f"[Fallback legacy] {tool_name}")
        try:
            # Import circulaire évité par import local
            from agent import execute_tool
            return execute_tool(tool_name, args)
        except ImportError:
            return {"error": f"Tool '{tool_name}' non disponible (MCP ni legacy)"}

    # ─── Introspection ───────────────────────────────────────────

    def list_tools(self, server_name: Optional[str] = None) -> list[dict]:
        """
        Liste les tools disponibles.
        Si server_name fourni : tools de ce server uniquement.
        Sinon : tous les tools de tous les servers.
        """
        if server_name:
            return [
                {"tool": t, "server": s}
                for t, s in TOOL_TO_SERVER.items()
                if s == server_name
            ]
        return [
            {"tool": t, "server": s}
            for t, s in TOOL_TO_SERVER.items()
        ]

    def health(self) -> dict:
        """État de santé de tous les circuit breakers."""
        return {
            "mcp_enabled": self._enabled,
            "servers": {
                name: {
                    "state":         cb.state.value,
                    "failure_count": cb.failure_count,
                    "loaded":        name in self._servers,
                }
                for name, cb in self._breakers.items()
            },
            "cache": {
                "ram_entries": len(self._cache._store),
                "redis":       self._cache._redis is not None,
            }
        }

    def invalidate_cache(self, server: Optional[str] = None):
        """Invalide le cache d'un server ou de tous."""
        pattern = server or ""
        self._cache.invalidate(pattern)


# ═══════════════════════════════════════════════════════════════
# INSTANCE GLOBALE SINGLETON
# Importée par agent.py, orchestrator.py, les agents spécialisés
# ═══════════════════════════════════════════════════════════════

_mcp_client: Optional[MCPClient] = None


def get_mcp_client() -> MCPClient:
    """Retourne l'instance singleton MCPClient (lazy init)."""
    global _mcp_client
    if _mcp_client is None:
        _mcp_client = MCPClient()
    return _mcp_client


# ═══════════════════════════════════════════════════════════════
# INTEGRATION DANS agent.py EXISTANT
# Ajouter ces 3 lignes dans execute_tool() de agent.py pour
# activer MCP progressivement sans rien casser :
#
#   from mcp_client import get_mcp_client
#   client = get_mcp_client()
#   if os.getenv("MCP_ENABLED", "true") == "true":
#       return client.call_tool_sync(name, args)
#   # fallback legacy existant...
# ═══════════════════════════════════════════════════════════════


# ─── FastAPI router optionnel (endpoints MCP admin) ──────────
from fastapi import APIRouter

mcp_router = APIRouter(prefix="/api/mcp", tags=["mcp"])


@mcp_router.get("/health")
def mcp_health():
    """État de santé des MCP servers et circuit breakers."""
    return get_mcp_client().health()


@mcp_router.get("/tools")
def mcp_tools(server: Optional[str] = None):
    """Liste tous les tools MCP disponibles."""
    return {"tools": get_mcp_client().list_tools(server)}


@mcp_router.post("/invalidate-cache")
def mcp_invalidate(server: Optional[str] = None):
    """Invalide le cache d'un server ou de tous."""
    get_mcp_client().invalidate_cache(server)
    return {"status": "ok", "server": server or "all"}


@mcp_router.post("/call/{tool_name}")
async def mcp_call(tool_name: str, args: dict):
    """Appel direct d'un tool MCP (debug/test)."""
    client = get_mcp_client()
    result = await client.call_tool(tool_name, args)
    return result
