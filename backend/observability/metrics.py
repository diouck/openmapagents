"""
observability/metrics.py — Dashboard métriques FastAPI
=======================================================
GET /admin/metrics → JSON avec toutes les métriques agrégées.

Métriques calculées depuis les logs en mémoire :
  - Taux cache hit par MCP server
  - Latence p50/p95 par agent/domaine
  - Modèles LLM utilisés et leur fréquence
  - Top requêtes par domaine
  - Taux d'erreur par MCP server
  - Alertes récentes (HALLUCINATION_RISK, LOW_CONFIDENCE, MCP_FAILURE)
  - LLM circuit breaker status
  - Cache stats
"""

import os
import time
import math
import logging
from collections import defaultdict, Counter
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

log = logging.getLogger("oma.metrics")

# ── Auth basique pour /admin ──────────────────────────────────
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
security    = HTTPBearer(auto_error=False)


def _check_admin(creds: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    if not ADMIN_TOKEN:
        return True   # pas d'auth configurée
    if not creds or creds.credentials != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Token admin requis")
    return True


# ── Helpers stats ─────────────────────────────────────────────

def _percentile(sorted_values: list, pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = math.ceil(pct / 100 * len(sorted_values)) - 1
    return sorted_values[max(0, min(idx, len(sorted_values)-1))]


def _compute_mcp_stats(records: list) -> dict:
    """Taux cache hit, latence et taux d'erreur par MCP server."""
    by_server: dict = defaultdict(lambda: {
        "calls": 0, "cache_hits": 0, "errors": 0,
        "latencies": [], "tools": Counter(),
    })

    for rec in records:
        for call in rec.get("mcp_calls", []):
            srv = call.get("server","unknown")
            s   = by_server[srv]
            s["calls"]     += 1
            s["cache_hits"] += 1 if call.get("cache_hit") else 0
            s["errors"]    += 0 if call.get("success", True) else 1
            lat = call.get("latency_ms", 0)
            if lat > 0:
                s["latencies"].append(lat)
            tool = call.get("tool","")
            if tool:
                s["tools"][tool] += 1

    result = {}
    for srv, s in by_server.items():
        lats = sorted(s["latencies"])
        result[srv] = {
            "calls":           s["calls"],
            "cache_hit_rate":  round(s["cache_hits"] / max(s["calls"],1) * 100, 1),
            "error_rate":      round(s["errors"]     / max(s["calls"],1) * 100, 1),
            "latency_p50_ms":  int(_percentile(lats, 50)),
            "latency_p95_ms":  int(_percentile(lats, 95)),
            "top_tools":       dict(s["tools"].most_common(5)),
        }

    return dict(result)


def _compute_domain_stats(records: list) -> dict:
    """Top requêtes et latence p50/p95 par domaine."""
    by_domain: dict = defaultdict(lambda: {
        "count": 0, "latencies": [], "queries": [], "errors": 0,
    })

    for rec in records:
        domain = rec.get("domain","unknown") or "unknown"
        d      = by_domain[domain]
        d["count"]     += 1
        d["errors"]    += 1 if rec.get("status") == "error" else 0
        lat = rec.get("latency_ms", 0)
        if lat > 0:
            d["latencies"].append(lat)
        q = rec.get("query","")[:60]
        if q:
            d["queries"].append(q)

    result = {}
    for domain, d in sorted(by_domain.items(), key=lambda x: -x[1]["count"]):
        lats = sorted(d["latencies"])
        result[domain] = {
            "count":          d["count"],
            "error_rate":     round(d["errors"] / max(d["count"],1) * 100, 1),
            "latency_p50_ms": int(_percentile(lats, 50)),
            "latency_p95_ms": int(_percentile(lats, 95)),
            "sample_queries": list(dict.fromkeys(d["queries"]))[:5],
        }

    return result


def _compute_llm_stats(records: list) -> dict:
    """Modèles LLM utilisés, fréquence et tokens moyens."""
    models:  Counter = Counter()
    tokens_by_model: dict = defaultdict(list)

    for rec in records:
        model = rec.get("model_used","")
        if model:
            models[model] += 1
            t = rec.get("tokens", 0)
            if t > 0:
                tokens_by_model[model].append(t)

    result = {}
    for model, count in models.most_common():
        toks = tokens_by_model.get(model,[])
        result[model] = {
            "calls":      count,
            "pct":        round(count / max(sum(models.values()),1) * 100, 1),
            "avg_tokens": int(sum(toks)/len(toks)) if toks else 0,
        }
    return result


def _compute_global_stats(records: list) -> dict:
    """KPIs globaux."""
    if not records:
        return {"total_requests": 0}

    total      = len(records)
    errors     = sum(1 for r in records if r.get("status") == "error")
    no_tools   = sum(1 for r in records if not r.get("has_tool_calls") and r.get("query"))
    cache_hits = sum(
        1 for r in records
        for c in r.get("mcp_calls",[])
        if c.get("cache_hit")
    )
    total_mcp  = sum(len(r.get("mcp_calls",[])) for r in records)

    lats = sorted(r.get("latency_ms",0) for r in records if r.get("latency_ms",0) > 0)
    confs = [r.get("confidence",0) for r in records if r.get("confidence",0) > 0]

    alerts = defaultdict(int)
    for r in records:
        for a in r.get("alerts",[]):
            alerts[a.get("type","UNKNOWN")] += 1

    return {
        "total_requests":     total,
        "error_rate_pct":     round(errors / max(total,1) * 100, 1),
        "hallucination_risk_pct": round(no_tools / max(total,1) * 100, 1),
        "cache_hit_rate_pct": round(cache_hits / max(total_mcp,1) * 100, 1),
        "latency_p50_ms":     int(_percentile(lats, 50)),
        "latency_p95_ms":     int(_percentile(lats, 95)),
        "avg_confidence":     round(sum(confs)/len(confs), 3) if confs else 0,
        "alerts_by_type":     dict(alerts),
        "window_minutes":     round((time.time() - records[0].get("ts", time.time())) / 60, 1) if records else 0,
    }


# ═══════════════════════════════════════════════════════════════
# ROUTER FASTAPI
# ═══════════════════════════════════════════════════════════════

metrics_router = APIRouter(prefix="/admin", tags=["admin"])


@metrics_router.get("/metrics")
def get_metrics(_=Depends(_check_admin)):
    """
    Dashboard métriques complet.
    Retourne un JSON avec toutes les métriques agrégées.
    """
    from observability.logger import get_request_logger
    logger  = get_request_logger()
    records = logger.get_recent(n=500)

    # Métriques LLM circuit breakers
    llm_status = {}
    try:
        from resilience.llm_resilience import get_resilient_client
        llm_status = get_resilient_client().status()
    except Exception:
        pass

    # Métriques cache
    cache_stats = {}
    try:
        from mcp_servers.mcp_cache import get_cache_manager
        cache_stats = get_cache_manager().stats()
    except Exception:
        pass

    # RAG embedder stats
    rag_stats = {}
    try:
        from rag.embedder import get_embedder
        rag_stats = get_embedder().stats()
    except Exception:
        pass

    # Session memory stats
    session_stats = {}
    try:
        from memory.session_memory import get_session_memory
        session_stats = get_session_memory().stats()
    except Exception:
        pass

    # Alertes récentes
    recent_alerts = logger.get_alerts(n=20)

    return {
        "generated_at":    time.time(),
        "global":          _compute_global_stats(records),
        "by_domain":       _compute_domain_stats(records),
        "by_mcp_server":   _compute_mcp_stats(records),
        "llm_models":      _compute_llm_stats(records),
        "recent_alerts":   recent_alerts,
        "llm_resilience":  {
            "primary":        llm_status.get("primary_model",""),
            "fallback_chain": llm_status.get("fallback_chain",[]),
            "circuit_breakers": {
                m: cb["state"]
                for m, cb in llm_status.get("circuit_breakers",{}).items()
            },
        },
        "cache":           cache_stats,
        "rag":             rag_stats,
        "sessions":        session_stats,
    }


@metrics_router.get("/logs")
def get_recent_logs(n: int = 20, _=Depends(_check_admin)):
    """Derniers logs bruts."""
    from observability.logger import get_request_logger
    return get_request_logger().get_recent(n=min(n, 100))


@metrics_router.get("/alerts")
def get_alerts(n: int = 20, _=Depends(_check_admin)):
    """Dernières alertes (HALLUCINATION_RISK, LOW_CONFIDENCE, MCP_FAILURE)."""
    from observability.logger import get_request_logger
    return get_request_logger().get_alerts(n=min(n, 50))


@metrics_router.get("/health")
def admin_health():
    """Health check rapide (pas d'auth)."""
    checks = {}

    try:
        from mcp_client import get_mcp_client
        client = get_mcp_client()
        checks["mcp_client"] = "ok"
    except Exception as e:
        checks["mcp_client"] = f"error: {e!s:.40}"

    try:
        from resilience.llm_resilience import get_resilient_client
        st = get_resilient_client().status()
        open_cbs = [m for m, cb in st.get("circuit_breakers",{}).items()
                    if cb.get("state") == "open"]
        checks["llm"] = f"ok | {len(open_cbs)} CB open"
    except Exception as e:
        checks["llm"] = f"error: {e!s:.40}"

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST","geoafrica.fr"),
            port=int(os.getenv("PG_PORT","5435")),
            dbname=os.getenv("PG_DB","openmapagents"),
            user=os.getenv("PG_USER","postgres"),
            password=os.getenv("PG_PASSWORD",""),
            connect_timeout=2,
        )
        conn.close()
        checks["postgis"] = "ok"
    except Exception as e:
        checks["postgis"] = f"error: {e!s:.40}"

    checks["logger"] = "ok"
    status = "ok" if all("ok" in v for v in checks.values()) else "degraded"
    return {"status": status, "checks": checks, "ts": time.time()}
