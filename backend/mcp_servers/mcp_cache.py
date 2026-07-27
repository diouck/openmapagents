"""
mcp_servers/mcp_cache.py — MCP Server Cache
============================================
Cache centralisé Redis + RAM pour tous les MCP servers.
Intercepte les appels répétés avant qu'ils arrivent aux APIs externes.

Backend par priorité :
  1. Redis (si REDIS_URL configuré)
  2. RAM dict (fallback — perdu au redémarrage)

TTL par défaut par server :
  gee        → 86400s  (24h)   URLs GEE valides 24h
  worldbank  → 604800s (7j)    données économiques stables
  nominatim  → 2592000s(30j)   géocodages très stables
  stac       → 3600s   (1h)    catalog satellite
  overture   → 3600s   (1h)    données OSM/Overture
  ors        → 3600s   (1h)    isochrones/routes
  osm        → 3600s   (1h)    données OSM
  maptiler   → 86400s  (24h)   élévation stable
  postgis    → 300s    (5min)  données locales changeantes
  default    → 3600s   (1h)

Tools exposés :
    cache_get           → lire une valeur du cache
    cache_set           → écrire une valeur dans le cache
    cache_delete        → supprimer une clé
    cache_invalidate    → supprimer par pattern (préfixe)
    cache_stats         → statistiques du cache
    cache_clear_server  → vider le cache d'un server MCP spécifique
    cache_warm          → préchauffer le cache avec des requêtes fréquentes
"""

import os
import json
import time
import hashlib
import logging
import asyncio
from typing import Any, Optional
from dataclasses import dataclass, field

log = logging.getLogger("mcp_cache")

# ── TTL par server MCP (secondes) ────────────────────────────
TTL_BY_SERVER = {
    "gee":       86_400,
    "worldbank": 604_800,
    "nominatim": 2_592_000,
    "stac":      3_600,
    "overture":  3_600,
    "ors":       3_600,
    "osm":       3_600,
    "maptiler":  86_400,
    "postgis":   300,
    "default":   3_600,
}

# ── Taille max cache RAM ───────────────────────────────────────
MAX_RAM_ENTRIES = int(os.getenv("CACHE_MAX_RAM_ENTRIES", "10000"))


# ═══════════════════════════════════════════════════════════════
# BACKENDS
# ═══════════════════════════════════════════════════════════════

@dataclass
class _RamEntry:
    value:      Any
    expires_at: float
    server:     str = ""
    tool:       str = ""
    hits:       int = 0


class _RamBackend:
    """Cache RAM simple avec TTL et LRU basique."""

    def __init__(self):
        self._store: dict[str, _RamEntry] = {}
        self._hits   = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if entry is None:
            self._misses += 1
            return None
        if time.time() > entry.expires_at:
            del self._store[key]
            self._misses += 1
            return None
        entry.hits += 1
        self._hits += 1
        return entry.value

    def set(self, key: str, value: Any, ttl: int,
            server: str = "", tool: str = ""):
        # LRU basique : si plein, supprimer les entrées expirées d'abord
        if len(self._store) >= MAX_RAM_ENTRIES:
            self._evict()
        self._store[key] = _RamEntry(
            value=value,
            expires_at=time.time() + ttl,
            server=server,
            tool=tool,
        )

    def delete(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def keys_by_pattern(self, pattern: str) -> list:
        return [k for k in self._store if pattern in k]

    def _evict(self):
        """Supprime les entrées expirées ou les plus anciennes."""
        now  = time.time()
        dead = [k for k, v in self._store.items() if v.expires_at < now]
        for k in dead:
            del self._store[k]
        # Si encore plein, supprimer 10% des entrées les moins utilisées
        if len(self._store) >= MAX_RAM_ENTRIES:
            sorted_keys = sorted(
                self._store.keys(),
                key=lambda k: self._store[k].hits
            )
            for k in sorted_keys[:MAX_RAM_ENTRIES // 10]:
                del self._store[k]

    def stats(self) -> dict:
        now     = time.time()
        active  = sum(1 for v in self._store.values() if v.expires_at > now)
        expired = len(self._store) - active
        by_server: dict = {}
        for v in self._store.values():
            by_server[v.server] = by_server.get(v.server, 0) + 1
        return {
            "backend":        "ram",
            "total_entries":  len(self._store),
            "active_entries": active,
            "expired_entries": expired,
            "hits":           self._hits,
            "misses":         self._misses,
            "hit_rate_pct":   round(
                self._hits / max(self._hits + self._misses, 1) * 100, 1
            ),
            "by_server":      by_server,
        }


class _RedisBackend:
    """Cache Redis avec TTL natif."""

    def __init__(self, redis_url: str):
        import redis
        self._r = redis.from_url(redis_url, decode_responses=True)
        self._r.ping()
        self._hits   = 0
        self._misses = 0
        log.info(f"✓ Cache Redis connecté: {redis_url.split('@')[-1]}")

    def get(self, key: str) -> Optional[Any]:
        try:
            raw = self._r.get(key)
            if raw is None:
                self._misses += 1
                return None
            self._hits += 1
            # Incrémenter compteur hits
            self._r.hincrby(f"{key}:meta", "hits", 1)
            return json.loads(raw)
        except Exception as e:
            log.warning(f"Redis get error: {e}")
            self._misses += 1
            return None

    def set(self, key: str, value: Any, ttl: int,
            server: str = "", tool: str = ""):
        try:
            self._r.setex(key, ttl, json.dumps(value, default=str))
            # Métadonnées
            self._r.hset(f"{key}:meta", mapping={
                "server": server,
                "tool":   tool,
                "set_at": int(time.time()),
                "hits":   0,
            })
            self._r.expire(f"{key}:meta", ttl)
        except Exception as e:
            log.warning(f"Redis set error: {e}")

    def delete(self, key: str) -> bool:
        try:
            deleted = self._r.delete(key, f"{key}:meta")
            return deleted > 0
        except Exception:
            return False

    def keys_by_pattern(self, pattern: str) -> list:
        try:
            return [
                k for k in self._r.scan_iter(f"*{pattern}*")
                if not k.endswith(":meta")
            ]
        except Exception:
            return []

    def stats(self) -> dict:
        try:
            info      = self._r.info("memory")
            keyspace  = self._r.info("keyspace")
            # Compter les clés par server
            by_server: dict = {}
            for key in self._r.scan_iter("mcp:*"):
                if ":meta" in key:
                    continue
                meta = self._r.hgetall(f"{key}:meta")
                srv  = meta.get("server", "unknown")
                by_server[srv] = by_server.get(srv, 0) + 1

            db_keys = sum(
                v.get("keys", 0) for v in keyspace.values()
                if isinstance(v, dict)
            )
            return {
                "backend":      "redis",
                "total_keys":   db_keys // 2,  # divisé par 2 (clé + meta)
                "memory_mb":    round(
                    info.get("used_memory", 0) / 1024 / 1024, 2
                ),
                "hits":         self._hits,
                "misses":       self._misses,
                "hit_rate_pct": round(
                    self._hits / max(self._hits + self._misses, 1) * 100, 1
                ),
                "by_server":    by_server,
            }
        except Exception as e:
            return {"backend": "redis", "error": str(e)}


# ═══════════════════════════════════════════════════════════════
# CACHE MANAGER
# ═══════════════════════════════════════════════════════════════

class _CacheManager:
    """
    Gestionnaire de cache unifié.
    Choisit Redis ou RAM selon REDIS_URL.
    """

    def __init__(self):
        self._backend = None

    def _get_backend(self):
        if self._backend is None:
            redis_url = os.getenv("REDIS_URL", "")
            if redis_url:
                try:
                    self._backend = _RedisBackend(redis_url)
                except Exception as e:
                    log.warning(f"Redis indisponible ({e}) → fallback RAM")
                    self._backend = _RamBackend()
            else:
                self._backend = _RamBackend()
                log.info("Cache RAM actif (configurez REDIS_URL pour Redis)")
        return self._backend

    @staticmethod
    def build_key(server: str, tool: str, args: dict) -> str:
        """Construit une clé de cache déterministe."""
        payload = json.dumps(
            {"server": server, "tool": tool, "args": args},
            sort_keys=True, default=str
        )
        return f"mcp:{server}:{tool}:{hashlib.md5(payload.encode()).hexdigest()}"

    def get(self, key: str) -> Optional[Any]:
        return self._get_backend().get(key)

    def set(self, key: str, value: Any, ttl: int,
            server: str = "", tool: str = ""):
        self._get_backend().set(key, value, ttl, server, tool)

    def delete(self, key: str) -> bool:
        return self._get_backend().delete(key)

    def keys_by_pattern(self, pattern: str) -> list:
        return self._get_backend().keys_by_pattern(pattern)

    def stats(self) -> dict:
        return self._get_backend().stats()

    def ttl_for(self, server: str) -> int:
        return TTL_BY_SERVER.get(server, TTL_BY_SERVER["default"])


# Instance globale
_manager = _CacheManager()


def get_cache_manager() -> _CacheManager:
    """Retourne l'instance singleton du CacheManager."""
    return _manager


# ═══════════════════════════════════════════════════════════════
# CACHE SERVER (MCP)
# ═══════════════════════════════════════════════════════════════

class CacheServer:
    """
    MCP Server Cache.
    Expose les opérations de cache comme tools MCP.
    Utilisé par MCPClient pour l'interception automatique.
    """

    async def call(self, tool: str, args: dict) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._run, tool, args)

    def _run(self, tool: str, args: dict) -> dict:
        fn = {
            "cache_get":          self.cache_get,
            "cache_set":          self.cache_set,
            "cache_delete":       self.cache_delete,
            "cache_invalidate":   self.cache_invalidate,
            "cache_stats":        self.cache_stats,
            "cache_clear_server": self.cache_clear_server,
            "cache_warm":         self.cache_warm,
        }.get(tool)
        if not fn:
            return {"error": f"Cache tool inconnu: '{tool}'"}
        try:
            return fn(args)
        except Exception as e:
            log.error(f"Cache {tool}: {e}")
            return {"error": f"Erreur cache: {e}"}

    # ─── CACHE GET ────────────────────────────────────────────

    def cache_get(self, a: dict) -> dict:
        """
        Lit une valeur du cache.

        Args:
            key:    clé exacte
            server: server MCP (optionnel — pour construire la clé)
            tool:   tool name (optionnel)
            args:   args du tool (optionnel — pour construire la clé)

        Returns:
            value si trouvé, null sinon
        """
        key = a.get("key")
        if not key:
            # Construire la clé depuis server/tool/args
            server = a.get("server", "")
            tool   = a.get("tool",   "")
            args   = a.get("args",   {})
            if server and tool:
                key = _manager.build_key(server, tool, args)
            else:
                return {"error": "key ou (server + tool + args) requis"}

        value = _manager.get(key)
        return {
            "key":   key,
            "found": value is not None,
            "value": value,
        }

    # ─── CACHE SET ────────────────────────────────────────────

    def cache_set(self, a: dict) -> dict:
        """
        Écrit une valeur dans le cache.

        Args:
            key:    clé (ou construite depuis server/tool/args)
            value:  valeur à stocker (sérialisable JSON)
            ttl:    durée en secondes (défaut: TTL du server)
            server: server MCP (pour TTL auto + métadonnées)
            tool:   tool name
            args:   args du tool (pour construire la clé)
        """
        server = a.get("server", "")
        tool   = a.get("tool",   "")
        args   = a.get("args",   {})
        value  = a.get("value")
        ttl    = int(a.get("ttl", _manager.ttl_for(server)))

        key = a.get("key")
        if not key and server and tool:
            key = _manager.build_key(server, tool, args)
        if not key:
            return {"error": "key ou (server + tool + args) requis"}
        if value is None:
            return {"error": "value requis"}

        _manager.set(key, value, ttl, server, tool)
        return {
            "key":    key,
            "stored": True,
            "ttl":    ttl,
            "server": server,
        }

    # ─── CACHE DELETE ─────────────────────────────────────────

    def cache_delete(self, a: dict) -> dict:
        """
        Supprime une clé du cache.

        Args:
            key:    clé exacte
        """
        key = a.get("key", "")
        if not key:
            return {"error": "key requis"}
        deleted = _manager.delete(key)
        return {"key": key, "deleted": deleted}

    # ─── CACHE INVALIDATE ─────────────────────────────────────

    def cache_invalidate(self, a: dict) -> dict:
        """
        Supprime toutes les clés contenant un pattern.

        Args:
            pattern:  sous-chaîne à chercher dans les clés
                      ex: "gee" → invalide tout le cache GEE
                      ex: "ndvi" → invalide tous les NDVI

        Returns:
            nombre de clés supprimées
        """
        pattern = a.get("pattern", "")
        if not pattern:
            return {"error": "pattern requis"}

        keys    = _manager.keys_by_pattern(pattern)
        deleted = 0
        for k in keys:
            if _manager.delete(k):
                deleted += 1

        log.info(f"Cache invalidate '{pattern}': {deleted} clés supprimées")
        return {
            "pattern": pattern,
            "deleted": deleted,
            "keys":    keys[:20],  # max 20 dans la réponse
        }

    # ─── CACHE STATS ──────────────────────────────────────────

    def cache_stats(self, a: dict) -> dict:
        """
        Statistiques complètes du cache.

        Returns:
            backend, taille, hits/misses, taux de hit, répartition par server
        """
        stats = _manager.stats()
        stats.update({
            "ttl_by_server": TTL_BY_SERVER,
            "max_ram_entries": MAX_RAM_ENTRIES,
            "redis_configured": bool(os.getenv("REDIS_URL")),
        })
        return stats

    # ─── CACHE CLEAR SERVER ───────────────────────────────────

    def cache_clear_server(self, a: dict) -> dict:
        """
        Vide le cache d'un server MCP spécifique.

        Args:
            server:  nom du server (gee | worldbank | overture | ors | ...)
                     "all" pour tout vider

        Returns:
            nombre de clés supprimées
        """
        server = a.get("server", "")
        if not server:
            return {"error": "server requis (ou 'all')"}

        if server == "all":
            pattern = "mcp:"
        else:
            pattern = f"mcp:{server}:"

        keys    = _manager.keys_by_pattern(pattern)
        deleted = 0
        for k in keys:
            if _manager.delete(k):
                deleted += 1

        log.info(f"Cache clear server '{server}': {deleted} clés supprimées")
        return {
            "server":  server,
            "deleted": deleted,
        }

    # ─── CACHE WARM ───────────────────────────────────────────

    def cache_warm(self, a: dict) -> dict:
        """
        Préchauffage du cache avec des requêtes fréquentes.
        Lance les requêtes et stocke les résultats en cache.

        Args:
            server:   server MCP à préchauffer
            queries:  liste de dicts {tool, args} à préchauffer

        Returns:
            résultats du préchauffage
        """
        server  = a.get("server", "")
        queries = a.get("queries", [])

        if not server or not queries:
            return {"error": "server et queries requis"}

        results = []
        warmed  = 0

        for q in queries[:20]:   # max 20 queries de warm
            tool   = q.get("tool", "")
            args   = q.get("args", {})
            key    = _manager.build_key(server, tool, args)

            # Vérifier si déjà en cache
            existing = _manager.get(key)
            if existing is not None:
                results.append({
                    "tool":   tool,
                    "status": "already_cached",
                    "key":    key,
                })
                continue

            # Appeler le server MCP pour préchauffer
            try:
                from mcp_client import get_mcp_client
                client = get_mcp_client()
                result = client.call_tool_sync(
                    tool, args, server_name=server, use_cache=False
                )
                if "error" not in result:
                    ttl = _manager.ttl_for(server)
                    _manager.set(key, result, ttl, server, tool)
                    warmed += 1
                    results.append({
                        "tool":   tool,
                        "status": "warmed",
                        "key":    key,
                    })
                else:
                    results.append({
                        "tool":   tool,
                        "status": "error",
                        "error":  result.get("error"),
                    })
            except Exception as e:
                results.append({
                    "tool":   tool,
                    "status": "error",
                    "error":  str(e),
                })

        return {
            "server":         server,
            "total_queries":  len(queries),
            "warmed":         warmed,
            "already_cached": len([r for r in results if r["status"] == "already_cached"]),
            "errors":         len([r for r in results if r["status"] == "error"]),
            "results":        results,
        }
