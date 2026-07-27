"""
resilience/llm_resilience.py — Résilience LLM pour OpenMapAgents
=================================================================
Adapté exactement au .env existant :
  LLM_PROVIDER=openrouter
  OPENROUTER_MODEL=openrouter/openrouter/free
  OPENROUTER_API_KEY=sk-or-v1-...
  + fallbacks sur les autres providers configurés (Claude, OpenAI, DeepSeek)

Fonctionnalités :
  - Retry backoff exponentiel (1s→2s→4s + jitter)
  - Fallback automatique sur chaîne de modèles ordonnée
  - Circuit breaker par modèle (5 échecs → OPEN 60s)
  - Timeout par domaine carto (GEE 45s, routing 15s, défaut 30s)
  - Health check au démarrage
  - Logging structuré : modèle réel (openrouter/free révèle son choix),
    latence, tokens, tentative

Intégration dans agent.py :
    from resilience.llm_resilience import get_resilient_client, resilience_router
    app.include_router(resilience_router)

    # Dans call_llm() ligne 603 — remplacer completion(**kwargs) :
    _llm = get_resilient_client()
    response = _llm.complete(domain="default", **kwargs)

    # Dans call_llm() ligne 642 — même chose pour la boucle :
    response = _llm.complete(domain="default", model=LLM_MODEL,
                              messages=current_messages, tools=TOOLS,
                              tool_choice="auto", max_tokens=2000, temperature=0.3)

    # Dans lifespan() :
    get_resilient_client().health_check()
"""

import os
import time
import logging
import asyncio
from enum import Enum
from typing import Optional, Any

log = logging.getLogger("llm_resilience")

# ═══════════════════════════════════════════════════════════════
# CONFIG — lit exactement les mêmes vars que agent.py
# ═══════════════════════════════════════════════════════════════

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openrouter")

# Résolution du modèle primaire — même logique que MODEL_MAP de agent.py
_MODEL_MAP = {
    "claude":      os.getenv("CLAUDE_MODEL",     "claude-sonnet-4-20250514"),
    "openai":      os.getenv("OPENAI_MODEL",      "gpt-4o"),
    "ollama":      os.getenv("OLLAMA_MODEL",      "ollama/llama3.1"),
    "openrouter":  os.getenv("OPENROUTER_MODEL",  "openrouter/openrouter/free"),
    "deepseek":    os.getenv("DEEPSEEK_MODEL",    "deepseek/deepseek-chat"),
    "mistral":     os.getenv("MISTRAL_MODEL",     "mistral/mistral-large-latest"),
}
PRIMARY_MODEL = _MODEL_MAP.get(LLM_PROVIDER, "openrouter/openrouter/free")

# ── Chaîne de fallback ────────────────────────────────────────
# Construite depuis le .env : provider actif → providers alternatifs configurés
def _build_fallback_chain() -> list[str]:
    """
    Construit la chaîne de fallback dans l'ordre :
    1. Modèle primaire configuré (LLM_PROVIDER)
    2. Modèles OpenRouter stables (gratuits)
    3. Autres providers si leurs clés sont présentes
    4. openrouter/free en dernier recours
    """
    chain = [PRIMARY_MODEL]

    # OpenRouter stables (gratuits, bons pour la carto)
    openrouter_fallbacks = [
        "openrouter/meta-llama/llama-3.3-70b-instruct:free",
        "openrouter/deepseek/deepseek-chat:free",
        "openrouter/openrouter/free",
    ]
    for m in openrouter_fallbacks:
        if m not in chain:
            chain.append(m)

    # Claude si clé présente et pas déjà primary
    if os.getenv("ANTHROPIC_API_KEY","").startswith("sk-ant"):
        claude = _MODEL_MAP["claude"]
        if claude not in chain:
            chain.append(claude)

    # DeepSeek si clé présente
    if os.getenv("DEEPSEEK_API_KEY","").startswith("sk-"):
        ds = _MODEL_MAP["deepseek"]
        if ds not in chain:
            chain.append(ds)

    # Dédupliquer en préservant l'ordre
    seen = set(); result = []
    for m in chain:
        if m and m not in seen:
            seen.add(m); result.append(m)
    return result

FALLBACK_CHAIN: list[str] = _build_fallback_chain()

# ── Retry ─────────────────────────────────────────────────────
MAX_RETRIES      = int(os.getenv("LLM_MAX_RETRIES",      "3"))
RETRY_BASE_DELAY = float(os.getenv("LLM_RETRY_BASE_DELAY","1.0"))
RETRY_MAX_DELAY  = float(os.getenv("LLM_RETRY_MAX_DELAY", "8.0"))

# ── Circuit breaker ───────────────────────────────────────────
CB_FAILURE_THRESHOLD = int(os.getenv("LLM_CB_FAILURE_THRESHOLD","5"))
CB_RESET_TIMEOUT     = float(os.getenv("LLM_CB_RESET_TIMEOUT",  "60.0"))

# ── Timeouts par domaine ──────────────────────────────────────
DOMAIN_TIMEOUTS = {
    "gee":       int(os.getenv("LLM_TIMEOUT_GEE",       "45")),
    "satellite": int(os.getenv("LLM_TIMEOUT_SATELLITE",  "45")),
    "worldbank": int(os.getenv("LLM_TIMEOUT_WORLDBANK",  "20")),
    "routing":   int(os.getenv("LLM_TIMEOUT_ROUTING",    "15")),
    "spatial":   int(os.getenv("LLM_TIMEOUT_SPATIAL",    "20")),
    "overture":  int(os.getenv("LLM_TIMEOUT_OVERTURE",   "20")),
    "osm":       int(os.getenv("LLM_TIMEOUT_OSM",        "15")),
    "default":   int(os.getenv("LLM_TIMEOUT_DEFAULT",    "30")),
}

# ── Classification des erreurs ────────────────────────────────
_RETRIABLE = (
    "ratelimit","rate_limit","429","503","529","502",
    "timeout","timed out","overloaded","temporarily",
    "servicenavailable","connection","upstream",
    "provider returned error",
)
_FATAL = (
    "401","403","authentication","invalid api key",
    "400","bad request","invalid request",
)

def _is_retriable(e: Exception) -> bool:
    s = str(e).lower()
    for f in _FATAL:
        if f in s: return False
    for r in _RETRIABLE:
        if r in s: return True
    return True   # défaut : on retry

def _backoff(attempt: int) -> float:
    import random
    d = min(RETRY_BASE_DELAY * (2 ** attempt), RETRY_MAX_DELAY)
    return max(0.1, d + d * 0.2 * (random.random() * 2 - 1))

# ── Provider-specific kwargs ──────────────────────────────────
def _provider_kwargs(model: str) -> dict:
    """Kwargs additionnels selon le provider détecté dans le model string."""
    extra = {}
    if model.startswith("ollama"):
        extra["api_base"] = os.getenv("OLLAMA_API_BASE","http://localhost:11434")
    # OpenRouter : clé injectée via env OPENROUTER_API_KEY → LiteLLM la lit auto
    # Claude/OpenAI/DeepSeek/Mistral : idem, LiteLLM lit les env vars
    return extra


# ═══════════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ═══════════════════════════════════════════════════════════════

class _CBState(Enum):
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"

class _CB:
    def __init__(self, model: str):
        self.model    = model
        self.state    = _CBState.CLOSED
        self.failures = 0
        self.last_fail= 0.0
        self.calls    = 0
        self.errors   = 0
        self.successes= 0

    def allow(self) -> bool:
        self.calls += 1
        if self.state == _CBState.CLOSED:
            return True
        if self.state == _CBState.OPEN:
            if time.time() - self.last_fail > CB_RESET_TIMEOUT:
                self.state = _CBState.HALF_OPEN
                log.info(f"[CB] {self.model[:40]} → HALF_OPEN (test)")
                return True
            return False
        return True   # HALF_OPEN : 1 appel test

    def ok(self):
        self.successes += 1
        self.failures   = 0
        if self.state != _CBState.CLOSED:
            log.info(f"[CB] {self.model[:40]} → CLOSED ✓")
        self.state = _CBState.CLOSED

    def fail(self, err: str):
        self.failures += 1
        self.errors   += 1
        self.last_fail = time.time()
        if self.failures >= CB_FAILURE_THRESHOLD:
            if self.state != _CBState.OPEN:
                log.warning(f"[CB] {self.model[:40]} → OPEN "
                            f"({self.failures} échecs) | {err[:60]}")
            self.state = _CBState.OPEN

    def to_dict(self) -> dict:
        return {
            "model":     self.model,
            "state":     self.state.value,
            "failures":  self.failures,
            "successes": self.successes,
            "calls":     self.calls,
            "errors":    self.errors,
            "error_rate": round(self.errors/max(self.calls,1)*100,1),
        }


# ═══════════════════════════════════════════════════════════════
# RESILIENT CLIENT
# ═══════════════════════════════════════════════════════════════

class ResilientLLMClient:
    """
    Drop-in replacement de litellm.completion() dans agent.py.
    Compatible avec tous les providers du .env.
    """

    def __init__(self):
        self._cbs:   dict[str,_CB]   = {}
        self._stats: dict[str,dict]  = {}
        log.info(
            f"ResilientLLMClient — provider={LLM_PROVIDER} "
            f"primary={PRIMARY_MODEL[:40]} "
            f"chain={len(FALLBACK_CHAIN)} modèles"
        )
        log.info("Fallback: " + " → ".join(m[:35] for m in FALLBACK_CHAIN))

    def _cb(self, model: str) -> _CB:
        if model not in self._cbs:
            self._cbs[model] = _CB(model)
        return self._cbs[model]

    def _timeout(self, domain: str, kwargs: dict) -> int:
        if kwargs.get("request_timeout"):
            return int(kwargs["request_timeout"])
        return DOMAIN_TIMEOUTS.get(domain, DOMAIN_TIMEOUTS["default"])

    def _models_to_try(self, requested: str) -> list[str]:
        """Modèles à essayer : requested en premier, puis la chaîne."""
        seen = set(); out = []
        for m in [requested] + FALLBACK_CHAIN:
            if m and m not in seen:
                seen.add(m); out.append(m)
        return out

    # ─── SYNC ─────────────────────────────────────────────────

    def complete(self, domain: str = "default", **kwargs) -> Any:
        """
        Remplace litellm.completion(**kwargs) dans agent.py.
        Ajouter domain= pour le timeout adapté.
        """
        from litellm import completion

        timeout    = self._timeout(domain, kwargs)
        models     = self._models_to_try(kwargs.get("model", PRIMARY_MODEL))
        last_error = None

        for model in models:
            cb = self._cb(model)
            if not cb.allow():
                log.debug(f"[CB] skip {model[:40]} (OPEN)")
                continue

            for attempt in range(MAX_RETRIES):
                t0 = time.time()
                try:
                    kw = {**kwargs, "model": model,
                          "request_timeout": timeout,
                          **_provider_kwargs(model)}
                    resp    = completion(**kw)
                    latency = int((time.time()-t0)*1000)
                    cb.ok()
                    self._record(model, latency, resp)
                    self._annotate(resp, model, attempt, latency)
                    return resp

                except Exception as e:
                    latency = int((time.time()-t0)*1000)
                    if _is_retriable(e):
                        if attempt < MAX_RETRIES - 1:
                            delay = _backoff(attempt)
                            log.warning(
                                f"[LLM] {model[:35]} "
                                f"tentative {attempt+1}/{MAX_RETRIES} "
                                f"→ retry {delay:.1f}s | {str(e)[:80]}"
                            )
                            time.sleep(delay)
                            last_error = e
                        else:
                            cb.fail(str(e))
                            log.warning(
                                f"[LLM] {model[:35]} épuisé "
                                f"→ fallback suivant"
                            )
                            last_error = e
                            break
                    else:
                        log.error(f"[LLM] erreur fatale {model[:35]}: {e!s:.100}")
                        raise

        if last_error:
            raise last_error
        raise RuntimeError("Aucun modèle LLM disponible — vérifiez votre .env")

    # ─── ASYNC ────────────────────────────────────────────────

    async def acomplete(self, domain: str = "default", **kwargs) -> Any:
        """Version async — utilisée par les agents FastAPI async."""
        from litellm import acompletion

        timeout    = self._timeout(domain, kwargs)
        models     = self._models_to_try(kwargs.get("model", PRIMARY_MODEL))
        last_error = None

        for model in models:
            cb = self._cb(model)
            if not cb.allow():
                continue

            for attempt in range(MAX_RETRIES):
                t0 = time.time()
                try:
                    kw = {**kwargs, "model": model,
                          "request_timeout": timeout,
                          **_provider_kwargs(model)}
                    resp    = await asyncio.wait_for(acompletion(**kw), timeout)
                    latency = int((time.time()-t0)*1000)
                    cb.ok()
                    self._record(model, latency, resp)
                    self._annotate(resp, model, attempt, latency)
                    return resp

                except asyncio.TimeoutError:
                    cb.fail(f"timeout>{timeout}s")
                    last_error = TimeoutError(f"LLM timeout ({timeout}s)")
                    break
                except Exception as e:
                    if _is_retriable(e):
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(_backoff(attempt))
                            last_error = e
                        else:
                            cb.fail(str(e))
                            last_error = e; break
                    else:
                        raise

        if last_error: raise last_error
        raise RuntimeError("Aucun modèle LLM disponible.")

    # ─── HELPERS ──────────────────────────────────────────────

    def _record(self, model: str, latency: int, resp: Any):
        """Stats par modèle."""
        tokens = 0
        try:
            u = getattr(resp, "usage", None)
            if u: tokens = getattr(u, "total_tokens", 0)
        except Exception: pass

        # Modèle réel (openrouter/free révèle le sous-modèle choisi)
        actual = model
        try:
            if getattr(resp, "model", None): actual = resp.model
        except Exception: pass

        log.info(
            f"[LLM] ✓ {actual[:45]} | attempt latency={latency}ms | tokens={tokens}"
            + (f" [via {model[:30]}]" if actual != model else "")
        )

        if model not in self._stats:
            self._stats[model] = {"calls":0,"ms":0,"tokens":0,"errors":0}
        s = self._stats[model]
        s["calls"]  += 1
        s["ms"]     += latency
        s["tokens"] += tokens

    def _annotate(self, resp: Any, model: str, attempt: int, latency: int):
        """Annote la réponse avec les métadonnées de résilience."""
        try:
            if hasattr(resp, "_hidden_params"):
                resp._hidden_params.update({
                    "resilience_model":   model,
                    "resilience_attempt": attempt,
                    "resilience_ms":      latency,
                })
        except Exception: pass

    # ─── HEALTH CHECK ─────────────────────────────────────────

    def health_check(self, quick: bool = True) -> dict:
        """
        Teste les modèles de la chaîne fallback.
        quick=True : teste seulement le primaire + 1 fallback.
        Appelé dans lifespan() de agent.py.
        """
        from litellm import completion as lc

        to_test = FALLBACK_CHAIN[:2] if quick else FALLBACK_CHAIN
        results = {}

        for model in to_test:
            cb = self._cb(model)
            if cb.state == _CBState.OPEN:
                results[model] = {"status":"circuit_open"}
                continue

            t0 = time.time()
            try:
                r = lc(
                    model=model,
                    messages=[{"role":"user","content":"1+1="}],
                    max_tokens=5,
                    request_timeout=10,
                    **_provider_kwargs(model),
                )
                ms = int((time.time()-t0)*1000)
                cb.ok()
                results[model] = {"status":"ok","latency_ms":ms}
                log.info(f"[Health] {model[:40]} ✓ {ms}ms")
            except Exception as e:
                ms = int((time.time()-t0)*1000)
                cb.fail(str(e))
                results[model] = {"status":"error","latency_ms":ms,
                                  "error":str(e)[:100]}
                log.warning(f"[Health] {model[:40]} ✗ {str(e)[:60]}")

        ok_models = [m for m,r in results.items() if r["status"]=="ok"]
        log.info(
            f"Health check: {len(ok_models)}/{len(to_test)} modèles OK | "
            f"primaire={'✓' if PRIMARY_MODEL in ok_models else '✗'}"
        )
        return {
            "primary":       PRIMARY_MODEL,
            "available":     ok_models,
            "unavailable":   [m for m in to_test if m not in ok_models],
            "fallback_chain": FALLBACK_CHAIN,
            "results":       results,
        }

    # ─── STATUS / ADMIN ───────────────────────────────────────

    def status(self) -> dict:
        return {
            "provider":      LLM_PROVIDER,
            "primary_model": PRIMARY_MODEL,
            "fallback_chain": FALLBACK_CHAIN,
            "circuit_breakers": {m:cb.to_dict() for m,cb in self._cbs.items()},
            "model_stats": {
                m: {**s,
                    "avg_ms":     round(s["ms"]/max(s["calls"],1)),
                    "avg_tokens": round(s["tokens"]/max(s["calls"],1)),
                    }
                for m,s in self._stats.items()
            },
            "config": {
                "max_retries":    MAX_RETRIES,
                "base_delay_s":   RETRY_BASE_DELAY,
                "cb_threshold":   CB_FAILURE_THRESHOLD,
                "cb_reset_s":     CB_RESET_TIMEOUT,
                "domain_timeouts":DOMAIN_TIMEOUTS,
            },
        }

    def reset_breaker(self, model: str):
        if model in self._cbs:
            self._cbs[model].state    = _CBState.CLOSED
            self._cbs[model].failures = 0
            log.info(f"[CB] reset: {model[:40]}")


# ═══════════════════════════════════════════════════════════════
# SINGLETON
# ═══════════════════════════════════════════════════════════════

_client: Optional[ResilientLLMClient] = None

def get_resilient_client() -> ResilientLLMClient:
    global _client
    if _client is None:
        _client = ResilientLLMClient()
    return _client


# ═══════════════════════════════════════════════════════════════
# FASTAPI ROUTER
# ═══════════════════════════════════════════════════════════════

from fastapi import APIRouter
resilience_router = APIRouter(prefix="/api/llm", tags=["llm"])

@resilience_router.get("/status")
def llm_status():
    return get_resilient_client().status()

@resilience_router.post("/health-check")
def llm_health(quick: bool = True):
    return get_resilient_client().health_check(quick=quick)

@resilience_router.post("/reset-breaker/{model:path}")
def llm_reset(model: str):
    get_resilient_client().reset_breaker(model)
    return {"model": model, "reset": True}

@resilience_router.get("/models")
def llm_models():
    return {
        "provider":       LLM_PROVIDER,
        "primary":        PRIMARY_MODEL,
        "fallback_chain": FALLBACK_CHAIN,
        "domain_timeouts":DOMAIN_TIMEOUTS,
    }
