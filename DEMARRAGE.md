# Guide de démarrage OpenMapAgents v4
## Ordre des opérations : copier les fichiers → RAG → backend → frontend

---

## 1. Structure finale des fichiers

Copier tous les fichiers générés dans le dossier racine du projet
(là où se trouve `agent.py`) :

```
projet/
├── agent.py                       ← existant (modifié ci-dessous)
├── orchestrator.py                ← Phase 7
├── mcp_client.py                  ← Phase 1
│
├── mcp_servers/
│   ├── mcp_gee.py
│   ├── mcp_ors.py
│   ├── mcp_worldbank.py
│   ├── mcp_overture.py
│   ├── mcp_postgis.py
│   ├── mcp_nominatim.py
│   ├── mcp_osm.py
│   ├── mcp_stac.py
│   ├── mcp_maptiler.py
│   └── mcp_cache.py
│
├── resilience/
│   └── llm_resilience.py
│
├── memory/
│   └── session_memory.py
│
├── rag/
│   ├── tool_registry.py
│   ├── embedder.py
│   └── retriever.py
│
├── debate/
│   ├── debate_layer.py
│   └── moderator.py
│
├── agents/
│   ├── base_agent.py
│   ├── satellite_agent.py
│   ├── routing_agent.py
│   ├── worldbank_agent.py
│   ├── geo_data_agent.py
│   └── spatial_agent.py
│
├── validation/
│   ├── geo_validator.py
│   ├── gee_validator.py
│   ├── mcp_validator.py
│   └── contract_validator.py
│
├── observability/
│   ├── logger.py
│   └── metrics.py
│
└── logs/                          ← créer ce dossier
```

---

## 2. Créer les dossiers manquants

```bash
# Depuis la racine du projet
mkdir -p logs data/cache mcp_servers resilience memory rag debate agents validation observability
touch mcp_servers/__init__.py resilience/__init__.py memory/__init__.py
touch rag/__init__.py debate/__init__.py agents/__init__.py
touch validation/__init__.py observability/__init__.py
```

---

## 3. Installer les nouvelles dépendances

```bash
conda activate geoai

# Embeddings multilingues (EMBED_MODEL du .env)
pip install sentence-transformers

# Pydantic v2 pour les validators
pip install "pydantic>=2.0"

# psycopg2 pour PostGIS + cache nominatim
pip install psycopg2-binary

# pgvector pour Python
pip install pgvector

# httpx pour les appels async (Nominatim, STAC, ORS)
pip install httpx

# Redis optionnel (si REDIS_URL configuré)
# pip install redis

# Vérifier que litellm est à jour
pip install --upgrade litellm
```

---

## 4. Préparer la base PostgreSQL (pgvector + table RAG)

### 4a. Activer pgvector sur geoafrica.fr:5435

Se connecter à la base `openmapagents` et exécuter :

```sql
-- Connexion : psql -h geoafrica.fr -p 5435 -U postgres -d openmapagents

-- Activer l'extension pgvector
CREATE EXTENSION IF NOT EXISTS vector;

-- Vérifier
SELECT * FROM pg_extension WHERE extname = 'vector';
```

### 4b. Créer la table rag_tools

La table est créée **automatiquement** par `embedder.setup()` au démarrage,
mais voici le SQL manuel si besoin :

```sql
CREATE TABLE IF NOT EXISTS rag_tools (
    id           SERIAL PRIMARY KEY,
    tool_id      VARCHAR(100) UNIQUE NOT NULL,
    server       VARCHAR(50),
    tool         VARCHAR(100),
    description  TEXT,
    triggers     JSONB,
    output_action VARCHAR(50),
    few_shot     JSONB,
    embedding    vector(768),        -- EMBED_DIM du .env
    embed_model  VARCHAR(200),
    embed_hash   VARCHAR(64),
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- Index HNSW pour similarité cosine
CREATE INDEX IF NOT EXISTS rag_tools_hnsw_idx
ON rag_tools
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

CREATE INDEX IF NOT EXISTS rag_tools_server_idx ON rag_tools (server);

-- Table cache Nominatim
CREATE TABLE IF NOT EXISTS nominatim_cache (
    cache_key   VARCHAR(64) PRIMARY KEY,
    query       TEXT,
    result      JSONB,
    created_at  TIMESTAMP DEFAULT NOW(),
    hit_count   INTEGER DEFAULT 1
);
```

### 4c. Pré-embedder les 45 tools RAG manuellement (optionnel)

Si vous voulez embedder avant de démarrer le backend :

```bash
conda activate geoai
cd /chemin/vers/projet

python - <<'EOF'
import os
os.environ.setdefault("PG_HOST",     "geoafrica.fr")
os.environ.setdefault("PG_PORT",     "5435")
os.environ.setdefault("PG_DB",       "openmapagents")
os.environ.setdefault("PG_USER",     "postgres")
os.environ.setdefault("PG_PASSWORD", "<votre_mot_de_passe>")
os.environ.setdefault("EMBED_MODEL", "sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
os.environ.setdefault("EMBED_DIM",   "768")

from rag.embedder import get_embedder
emb = get_embedder()
emb.setup()
stats = emb.warmup(force=False)
print(f"✓ RAG warmup: {stats}")
EOF
```

Résultat attendu :
```
✓ RAG warmup: {'total': 45, 'embedded': 45, 'skipped': 0, 'errors': 0}
```

---

## 5. Modifier agent.py — 5 ajouts

### 5a. Imports (après les imports existants, vers ligne 20)

```python
# Ajouter après "from litellm import completion"
from resilience.llm_resilience import get_resilient_client, resilience_router
from memory.session_memory     import get_session_memory, memory_router
from observability.metrics     import metrics_router
```

### 5b. Enregistrer les routers (après les routers existants, vers ligne 745)

```python
app.include_router(resilience_router)
app.include_router(memory_router)
app.include_router(metrics_router)
```

### 5c. Modifier call_llm() — remplacer les 2 completion()

**Ligne ~603** (premier appel) :
```python
# AVANT :
response = completion(**kwargs)

# APRÈS :
_llm = get_resilient_client()
response = _llm.complete(domain="default", **kwargs)
```

**Ligne ~642** (appel dans la boucle) :
```python
# AVANT :
response = completion(
    model=LLM_MODEL, messages=current_messages,
    tools=TOOLS, tool_choice="auto",
    max_tokens=2000, temperature=0.3,
    **({"api_base": os.getenv("OLLAMA_API_BASE")} if LLM_PROVIDER == "ollama" else {}),
)

# APRÈS :
response = _llm.complete(
    domain="default",
    model=LLM_MODEL,
    messages=current_messages,
    tools=TOOLS,
    tool_choice="auto",
    max_tokens=2000,
    temperature=0.3,
)
```

### 5d. Modifier /api/chat pour injecter session_id

```python
# AVANT :
@app.post("/api/chat")
def chat(req: ChatRequest):
    if ORCHESTRATOR_ENABLED:
        return orchestrate(req.messages, req.map_context)
    return call_llm(req.messages, req.map_context)

# APRÈS :
from fastapi import Header

@app.post("/api/chat")
def chat(req: ChatRequest, x_session_id: str = Header(None)):
    sid = x_session_id or (req.map_context or {}).get("session_id", "anon")

    # Mise à jour mémoire session
    mem  = get_session_memory()
    sess = mem.load(sid)
    mem.update_from_map_context(sess, req.map_context)

    if ORCHESTRATOR_ENABLED:
        result = orchestrate(req.messages, req.map_context)
    else:
        result = call_llm(req.messages, req.map_context)

    # Sauvegarder le contexte après réponse
    user_msg = next((m["content"] for m in reversed(req.messages)
                     if m.get("role") == "user"), "")
    mem.update_from_response(sess, result, user_msg)
    mem.save(sess)
    return result
```

### 5e. Modifier lifespan() pour ajouter le warmup complet

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Existant
    db.connect()

    # Nouveau : warmup complet
    if ORCHESTRATOR_ENABLED:
        try:
            get_orchestrator().warmup()   # inclut embedder.setup() + warmup()
        except Exception as e:
            log.warning(f"Orchestrateur warmup ignoré : {e}")

    # Health check LLM
    try:
        get_resilient_client().health_check(quick=True)
    except Exception as e:
        log.warning(f"LLM health check : {e}")

    yield

    # Cleanup
    db.close()
    log.info("Backend arrêté proprement")
```

---

## 6. Démarrer le backend

```bash
conda activate geoai
cd E:\DataScience\MCPCartographic

# Mode développement (reload auto)
uvicorn agent:app --host 0.0.0.0 --port 8000 --reload

# Mode production (Windows)
uvicorn agent:app --host 0.0.0.0 --port 8000 --workers 1
```

### Logs attendus au démarrage

```
INFO  DuckDB connected with spatial + httpfs + h3
INFO  ResilientLLMClient — provider=openrouter primary=openrouter/openrouter/free chain=4 modèles
INFO  Fallback: openrouter/openrouter/free → meta-llama/llama-3.3-70b:free → ...
INFO  ✓ Orchestrateur multi-agent activé
INFO  [Health] openrouter/openrouter/free ✓ 1842ms
INFO  RAG warmup: 45 embedés, 0 inchangés   ← 1ère fois ~2 min
INFO  RAG warmup: 0 embedés, 45 inchangés   ← fois suivantes <1s
INFO  ✓ Auth + Maps router chargés
INFO  Application startup complete.
```

---

## 7. Démarrer le frontend (existant)

```bash
cd frontend   # ou le dossier de votre frontend

npm install   # si pas déjà fait
npm run dev   # Vite dev server → http://localhost:5173
```

### Vérifier que ChatPanel.jsx envoie bien le X-Session-Id

Dans votre `ChatPanel.jsx` existant (ou le nouveau généré), s'assurer que
chaque requête `/api/chat` envoie :

```javascript
headers: {
  "Content-Type": "application/json",
  "X-Session-Id": sessionId,   // ← obligatoire pour la mémoire session
}
```

Et que `map_context` inclut la bbox courante MapLibre :

```javascript
// Dans votre composant Map, à chaque moveend :
map.on("moveend", () => {
  const bounds = map.getBounds();
  setMapContext({
    center: [map.getCenter().lng, map.getCenter().lat],
    zoom:   map.getZoom(),
    bbox:   [bounds.getWest(), bounds.getSouth(),
             bounds.getEast(), bounds.getNorth()],
    layers: activeLayers,   // tableau de vos layers actifs
    session_id: sessionId,
  });

  // Notifier ChatPanel de la nouvelle bbox
  window.dispatchEvent(new CustomEvent("oma:map_moved", {
    detail: { sessionId }
  }));
});
```

---

## 8. Vérifier que tout fonctionne

### 8a. Tester le backend

```bash
# Health check
curl http://localhost:8000/admin/health

# Tester le RAG directement
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -H "X-Session-Id: test-session-1" \
  -d '{"messages":[{"role":"user","content":"NDVI sur Dakar"}],
       "map_context":{"bbox":[-17.55,14.63,-17.33,14.82],"center":[-17.44,14.72],"zoom":12}}'

# Vérifier les métriques
curl http://localhost:8000/admin/metrics

# Status LLM
curl http://localhost:8000/api/llm/status

# Vérifier le RAG en base
psql -h geoafrica.fr -p 5435 -U postgres -d openmapagents \
  -c "SELECT tool_id, server, embed_model FROM rag_tools ORDER BY server LIMIT 10;"
```

### 8b. Résultats attendus

```json
// /admin/health
{
  "status": "ok",
  "checks": {
    "mcp_client": "ok",
    "llm": "ok | 0 CB open",
    "postgis": "ok",
    "logger": "ok"
  }
}

// /api/llm/status
{
  "provider": "openrouter",
  "primary_model": "openrouter/openrouter/free",
  "fallback_chain": ["openrouter/openrouter/free", "...llama-3.3-70b:free", "...deepseek:free"]
}
```

---

## 9. Mettre à jour le RAG (après ajout de nouveaux tools)

```bash
# Forcer le re-embedding de tous les tools
python - <<'EOF'
from rag.embedder import get_embedder
stats = get_embedder().warmup(force=True)
print(f"Re-embedded: {stats}")
EOF

# Ou juste les nouveaux (détection auto par hash)
python - <<'EOF'
from rag.embedder import get_embedder
stats = get_embedder().warmup(force=False)
print(f"Updated: {stats}")
EOF
```

### Ajouter un nouveau tool au RAG

1. Ouvrir `rag/tool_registry.py`
2. Ajouter une entrée dans `TOOL_REGISTRY` avec `id`, `server`, `tool`, `description`, `triggers`, `few_shot`
3. Relancer le warmup (ci-dessus)
4. Le tool est immédiatement disponible pour le retriever

---

## 10. Checklist démarrage rapide

```
[ ] conda activate geoai
[ ] mkdir -p logs data/cache + tous les __init__.py
[ ] pip install sentence-transformers pydantic>=2.0 psycopg2-binary pgvector httpx
[ ] CREATE EXTENSION IF NOT EXISTS vector; sur geoafrica.fr:5435
[ ] Copier tous les fichiers générés dans la structure correcte
[ ] Modifier agent.py (5 ajouts décrits section 5)
[ ] uvicorn agent:app --port 8000 --reload
[ ] Vérifier logs : "45 embedés" au premier démarrage
[ ] curl http://localhost:8000/admin/health → "ok"
[ ] npm run dev → tester une requête "NDVI sur Dakar"
```

---

## 11. Dépannage courant

| Erreur | Cause | Solution |
|--------|-------|---------|
| `pgvector not found` | Extension absente | `CREATE EXTENSION IF NOT EXISTS vector;` |
| `sentence_transformers` not found | Pip manquant | `pip install sentence-transformers` |
| `RAG warmup: 0 embedded, 0 skipped` | PG indispo | Vérifier connexion geoafrica.fr:5435 |
| `openrouter/free timeout` | Quota dépassé | Vérifier OPENROUTER_API_KEY dans .env |
| `Orchestrateur warmup ignoré` | Import error | Lancer `python -c "from orchestrator import orchestrate"` pour voir l'erreur exacte |
| `BrokenProcessPool` | Windows multiproc | Normal — l'embedder utilise le thread pool |
| Frontend `X-Session-Id` absent | Header manquant | Ajouter dans les headers de fetch() |
