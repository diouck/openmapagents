"""
agent_patch.py — Instructions d'intégration de l'orchestrateur dans agent.py

CE FICHIER N'EST PAS EXÉCUTÉ DIRECTEMENT.
Il montre les 3 changements à faire dans agent.py existant.

CHANGEMENT 1 — Import (en haut de agent.py, après les imports existants)
CHANGEMENT 2 — Warmup dans lifespan()
CHANGEMENT 3 — Remplacement de call_llm() dans l'endpoint /api/chat
"""

# ══════════════════════════════════════════════════════════════════════════════
# CHANGEMENT 1 — Ajouter ces imports après les imports existants dans agent.py
# ══════════════════════════════════════════════════════════════════════════════

# À ajouter APRÈS : from litellm import completion
"""
try:
    from orchestrator import get_orchestrator, orchestrate
    ORCHESTRATOR_ENABLED = True
    log.info("✓ Orchestrateur multi-agent activé")
except ImportError as e:
    ORCHESTRATOR_ENABLED = False
    log.warning(f"Orchestrateur non disponible, mode legacy : {e}")
"""


# ══════════════════════════════════════════════════════════════════════════════
# CHANGEMENT 2 — Warmup dans lifespan()
# ══════════════════════════════════════════════════════════════════════════════

# Remplacer lifespan() existant par :
"""
@asynccontextmanager
async def lifespan(app: FastAPI):
    db.connect()
    # Warmup orchestrateur (charge modèle embedding + router)
    if ORCHESTRATOR_ENABLED:
        try:
            get_orchestrator().warmup()
        except Exception as e:
            log.warning(f"Orchestrateur warmup ignoré : {e}")
    yield
    db.close()
"""


# ══════════════════════════════════════════════════════════════════════════════
# CHANGEMENT 3 — Endpoint /api/chat
# ══════════════════════════════════════════════════════════════════════════════

# Remplacer l'endpoint /api/chat existant par :
"""
@app.post("/api/chat")
def chat(req: ChatRequest):
    \"\"\"Main chat endpoint — orchestrateur multi-agent ou fallback legacy.\"\"\"

    if ORCHESTRATOR_ENABLED:
        # ── Nouveau pipeline : RAG → Router → Agent → Validation ──
        result = orchestrate(req.messages, req.map_context)
        return result
    else:
        # ── Fallback legacy (call_llm direct) ──
        result = call_llm(req.messages, req.map_context)
        return result
"""


# ══════════════════════════════════════════════════════════════════════════════
# OPTIONNEL — Endpoint de debug pour inspecter le pipeline
# ══════════════════════════════════════════════════════════════════════════════

# Ajouter dans agent.py pour diagnostiquer le routing :
"""
@app.post("/api/debug/route")
def debug_route(req: ChatRequest):
    \"\"\"Debug : montre le routing sans exécuter les tools.\"\"\"
    from agents.router import classify
    from rag.retriever import retrieve_tools

    query = next(
        (m["content"] for m in reversed(req.messages) if m.get("role") == "user"),
        ""
    )
    rag_tools    = retrieve_tools(query, top_k=5)
    route_result = classify(query)

    return {
        "query":      query,
        "rag_tools":  [{"id": t["id"], "score": t.get("score", 0)} for t in rag_tools],
        "domain":     route_result["domain"],
        "confidence": route_result["confidence"],
        "method":     route_result["method"],
        "latency_ms": route_result.get("latency_ms", 0),
    }


@app.get("/api/debug/orchestrator")
def debug_orchestrator():
    \"\"\"Statut de l'orchestrateur et de ses composants.\"\"\"
    status = {
        "orchestrator_enabled": ORCHESTRATOR_ENABLED,
        "llm_provider":  LLM_PROVIDER,
        "llm_model":     LLM_MODEL,
        "enable_rag":    os.getenv('ENABLE_RAG', 'true'),
        "enable_multi":  os.getenv('ENABLE_MULTI_AGENT', 'true'),
    }

    # Vérifier pgvector
    try:
        from rag.retriever import _pool
        conn = _pool.get()
        conn.cursor().execute("SELECT COUNT(*) FROM rag_tools")
        count = conn.cursor().fetchone()[0]
        status["rag_tools_indexed"] = count
        status["pgvector"] = "ok"
    except Exception as e:
        status["pgvector"] = f"indisponible : {e}"

    # Vérifier GEE
    try:
        import requests
        resp = requests.get("http://localhost:8000/api/gee/health", timeout=3)
        status["gee"] = resp.json().get("status", "unknown")
    except Exception:
        status["gee"] = "indisponible"

    return status
"""
