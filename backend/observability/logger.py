"""
observability/logger.py — Logs JSON structurés pour OpenMapAgents
==================================================================
Chaque requête produit un log JSON complet avec trace_id unique.
Alertes automatiques : HALLUCINATION_RISK, LOW_CONFIDENCE, MCP_FAILURE.

Usage dans agent.py / orchestrator.py :
    from observability.logger import get_request_logger

    log = get_request_logger()
    trace = log.start(query, session_id, user_id)

    log.record_llm(trace, model="openrouter/free", tokens=450, latency_ms=3200)
    log.record_mcp(trace, server="gee", tool="compute_ndvi",
                   latency_ms=8000, cache_hit=False, success=True)
    log.finish(trace, confidence=0.82, tool_calls=tool_calls, text=response_text)
"""

import os
import json
import time
import uuid
import logging
import threading
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Optional

log = logging.getLogger("oma.observability")

# ── Config ────────────────────────────────────────────────────
LOG_FILE              = os.getenv("OMA_LOG_FILE",          "logs/oma_requests.jsonl")
LOG_LEVEL_ALERT       = os.getenv("OMA_LOG_LEVEL_ALERT",   "WARNING")
CONFIDENCE_ALERT_THR  = float(os.getenv("OMA_CONF_ALERT",  "0.60"))
HALLUCINATION_ENABLED = os.getenv("OMA_HALLUC_ALERT",      "true").lower() == "true"
MAX_MEMORY_LOGS       = int(os.getenv("OMA_MAX_MEMORY_LOGS","500"))

# S'assurer que le dossier logs existe
os.makedirs(os.path.dirname(LOG_FILE) if os.path.dirname(LOG_FILE) else ".", exist_ok=True)


# ═══════════════════════════════════════════════════════════════
# DATACLASSES
# ═══════════════════════════════════════════════════════════════

@dataclass
class MCPCallRecord:
    server:      str
    tool:        str
    latency_ms:  int
    cache_hit:   bool
    success:     bool
    error:       Optional[str] = None
    feature_count: Optional[int] = None


@dataclass
class TraceRecord:
    trace_id:         str  = field(default_factory=lambda: uuid.uuid4().hex[:12])
    session_id:       str  = ""
    user_id:          str  = ""
    query:            str  = ""
    domain:           str  = ""
    model_used:       str  = ""
    tokens:           int  = 0
    latency_ms:       int  = 0
    confidence:       float = 0.0
    tool_calls:       list = field(default_factory=list)
    mcp_calls:        list = field(default_factory=list)   # list[MCPCallRecord]
    has_tool_calls:   bool = False
    response_text:    str  = ""
    alerts:           list = field(default_factory=list)
    started_at:       float = field(default_factory=time.time)
    finished_at:      float = 0.0
    status:           str  = "pending"   # pending | ok | error


# ═══════════════════════════════════════════════════════════════
# REQUEST LOGGER
# ═══════════════════════════════════════════════════════════════

class RequestLogger:
    """
    Logs JSON structurés par requête.
    Thread-safe. Écrit dans un fichier JSONL + mémoire circulaire.
    """

    def __init__(self):
        self._lock    = threading.Lock()
        self._memory: deque = deque(maxlen=MAX_MEMORY_LOGS)
        self._file_handle = None
        self._init_file()
        log.info(f"RequestLogger init | file={LOG_FILE} | mem={MAX_MEMORY_LOGS}")

    def _init_file(self):
        try:
            os.makedirs(os.path.dirname(LOG_FILE) if os.path.dirname(LOG_FILE) else ".", exist_ok=True)
            self._file_handle = open(LOG_FILE, "a", encoding="utf-8", buffering=1)
        except Exception as e:
            log.warning(f"Log file non disponible ({e}) → mémoire uniquement")
            self._file_handle = None

    # ── Cycle de vie d'une trace ──────────────────────────────

    def start(
        self,
        query:      str,
        session_id: str = "",
        user_id:    str = "",
    ) -> TraceRecord:
        """Démarre une nouvelle trace. Retourner au début de chaque requête."""
        trace = TraceRecord(
            query=      query[:500],
            session_id= session_id,
            user_id=    user_id,
        )
        return trace

    def record_llm(
        self,
        trace:      TraceRecord,
        model:      str,
        tokens:     int   = 0,
        latency_ms: int   = 0,
        domain:     str   = "",
    ):
        """Enregistre un appel LLM dans la trace."""
        trace.model_used = model
        trace.tokens    += tokens
        if latency_ms:
            trace.latency_ms += latency_ms
        if domain:
            trace.domain = domain

    def record_mcp(
        self,
        trace:         TraceRecord,
        server:        str,
        tool:          str,
        latency_ms:    int  = 0,
        cache_hit:     bool = False,
        success:       bool = True,
        error:         Optional[str] = None,
        feature_count: Optional[int] = None,
    ):
        """Enregistre un appel MCP dans la trace."""
        call = MCPCallRecord(
            server=       server,
            tool=         tool,
            latency_ms=   latency_ms,
            cache_hit=    cache_hit,
            success=      success,
            error=        error,
            feature_count=feature_count,
        )
        trace.mcp_calls.append(call)
        trace.latency_ms += latency_ms

        if not success:
            self._alert(
                trace, "MCP_FAILURE",
                f"server={server} tool={tool} error={error or 'unknown'}",
                level="ERROR",
            )

    def finish(
        self,
        trace:       TraceRecord,
        confidence:  float = 0.0,
        tool_calls:  list  = None,
        text:        str   = "",
        domain:      str   = "",
    ):
        """
        Finalise la trace, détecte les alertes et écrit le log.
        Appelé à la fin de chaque requête.
        """
        trace.finished_at   = time.time()
        trace.latency_ms    = int((trace.finished_at - trace.started_at) * 1000)
        trace.confidence    = round(confidence, 3)
        trace.tool_calls    = tool_calls or []
        trace.has_tool_calls = len(trace.tool_calls) > 0
        trace.response_text = text[:300] if text else ""
        trace.status        = "ok"
        if domain:
            trace.domain = domain

        # ── Alertes ───────────────────────────────────────────
        if HALLUCINATION_ENABLED and not trace.has_tool_calls and trace.query:
            # LLM a répondu en texte sans aucun tool call
            if not any(kw in trace.query.lower()
                       for kw in ["bonjour","merci","aide","help","hello","salut","?", "qu"]):
                self._alert(
                    trace, "HALLUCINATION_RISK",
                    f"LLM n'a pas appelé de tool pour: '{trace.query[:60]}' "
                    f"(model={trace.model_used[:40]})",
                    level="WARNING",
                )

        if confidence > 0 and confidence < CONFIDENCE_ALERT_THR:
            self._alert(
                trace, "LOW_CONFIDENCE",
                f"confidence={confidence:.2f} < {CONFIDENCE_ALERT_THR} "
                f"| query='{trace.query[:50]}' | domain={trace.domain}",
                level="WARNING",
            )

        self._write(trace)
        return trace

    def record_error(
        self,
        trace:  TraceRecord,
        error:  str,
        source: str = "",
    ):
        """Enregistre une erreur dans la trace."""
        trace.status = "error"
        trace.alerts.append({
            "type":    "ERROR",
            "source":  source,
            "message": error[:200],
            "ts":      time.time(),
        })
        log.error(f"[{trace.trace_id}] ERROR {source}: {error[:100]}")

    # ── Alertes ───────────────────────────────────────────────

    def _alert(
        self,
        trace:   TraceRecord,
        kind:    str,
        message: str,
        level:   str = "WARNING",
    ):
        """Enregistre une alerte dans la trace et les logs système."""
        alert = {
            "type":    kind,
            "message": message[:300],
            "ts":      time.time(),
        }
        trace.alerts.append(alert)

        logger_fn = getattr(log, level.lower(), log.warning)
        logger_fn(f"[{trace.trace_id}] 🔔 {kind}: {message[:120]}")

    # ── Écriture ──────────────────────────────────────────────

    def _write(self, trace: TraceRecord):
        """Sérialise et écrit la trace."""
        record = {
            "trace_id":       trace.trace_id,
            "ts":             trace.started_at,
            "session_id":     trace.session_id,
            "user_id":        trace.user_id,
            "query":          trace.query,
            "domain":         trace.domain,
            "model_used":     trace.model_used,
            "tokens":         trace.tokens,
            "latency_ms":     trace.latency_ms,
            "confidence":     trace.confidence,
            "has_tool_calls": trace.has_tool_calls,
            "tool_calls":     [tc.get("name","") if isinstance(tc,dict) else str(tc)
                               for tc in trace.tool_calls],
            "mcp_calls":      [asdict(c) for c in trace.mcp_calls],
            "alerts":         trace.alerts,
            "status":         trace.status,
        }

        with self._lock:
            self._memory.append(record)
            if self._file_handle:
                try:
                    self._file_handle.write(json.dumps(record, default=str) + "\n")
                    self._file_handle.flush()
                except Exception as e:
                    log.warning(f"Log write error: {e}")

    # ── Accès mémoire ─────────────────────────────────────────

    def get_recent(self, n: int = 50) -> list:
        with self._lock:
            return list(self._memory)[-n:]

    def get_alerts(self, n: int = 20) -> list:
        """Retourne les dernières alertes toutes traces confondues."""
        with self._lock:
            alerts = []
            for record in list(self._memory)[-100:]:
                for alert in record.get("alerts",[]):
                    alerts.append({**alert, "trace_id": record["trace_id"],
                                   "query": record["query"][:50]})
            return sorted(alerts, key=lambda a: a.get("ts",0), reverse=True)[:n]

    def close(self):
        if self._file_handle:
            try: self._file_handle.close()
            except Exception: pass


# ── Singleton ─────────────────────────────────────────────────
_logger: Optional[RequestLogger] = None

def get_request_logger() -> RequestLogger:
    global _logger
    if _logger is None:
        _logger = RequestLogger()
    return _logger
