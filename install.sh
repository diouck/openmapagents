#!/usr/bin/env bash
# =============================================================================
#  install.sh — Overture Maps Explorer (openmapagents)
#  Auteur : Kane Diouck <diouckk@gmail.com>
#  Compatible : Ubuntu 20.04+, Debian 11+, Fedora 38+, Arch Linux
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERREUR]${NC} $*" >&2; exit 1; }
step()    { echo -e "\n${BOLD}${CYAN}▶ $*${NC}"; }

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BOLD}${CYAN}"
cat << 'EOF'
  ___                   __  __              ___          _
 / _ \ _ __  ___ _ __  |  \/  | __ _ _ __ / _ \__   __| |_ ___
| | | | '_ \/ _ \ '_ \ | |\/| |/ _` | '_ \ (_) \ \ / / __/ __|
| |_| | |_) |  __/ | | || |  | | (_| | |_) \__, |\ V /| |_\__ \
 \___/| .__/ \___|_| |_||_|  |_|\__,_| .__/  /_/  \_/  \__|___/
      |_|                             |_|         Explorer
EOF
echo -e "${NC}"
echo -e "  ${BOLD}Overture Maps Explorer — Agent Mode${NC}"
echo -e "  Stack : React + FastAPI + DuckDB + LiteLLM + MCP + GEE"
echo -e "  ──────────────────────────────────────────────────────\n"

# =============================================================================
#  1. Détection distribution
# =============================================================================
step "Détection du système"

if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS_ID="${ID}"; OS_FAMILY="${ID_LIKE:-$ID}"
else
    OS_ID="unknown"; OS_FAMILY="debian"
fi

case "$OS_FAMILY" in
    *debian*|*ubuntu*) PKG_MGR="apt" ;;
    *fedora*|*rhel*|*centos*) PKG_MGR="dnf" ;;
    *arch*|*manjaro*) PKG_MGR="pacman" ;;
    *) PKG_MGR="apt" ;;
esac

SUDO=""
[ "$EUID" -ne 0 ] && command -v sudo &>/dev/null && SUDO="sudo"
info "Distribution : ${OS_ID} | Gestionnaire : ${PKG_MGR}"

# =============================================================================
#  2. Dépendances système
# =============================================================================
step "Installation des dépendances système"

case "$PKG_MGR" in
    apt)
        $SUDO apt-get update -qq
        $SUDO apt-get install -y \
            python3 python3-pip python3-venv python3-dev \
            curl git build-essential \
            libgdal-dev gdal-bin \
            libspatialindex-dev \
            nodejs npm 2>/dev/null || true
        ;;
    dnf)
        $SUDO dnf install -y \
            python3 python3-pip python3-devel \
            curl git gcc gcc-c++ make \
            gdal gdal-devel spatialindex-devel \
            nodejs npm
        ;;
    pacman)
        $SUDO pacman -Sy --noconfirm \
            python python-pip curl git base-devel gdal nodejs npm
        ;;
esac

# Node.js >= 18 requis pour Vite 5
NODE_MAJOR=$(node --version 2>/dev/null | grep -oP '\d+' | head -1 || echo "0")
if [ "$NODE_MAJOR" -lt 18 ]; then
    warn "Node.js ${NODE_MAJOR} détecté, mise à jour vers Node 20..."
    curl -fsSL https://deb.nodesource.com/setup_20.x | $SUDO -E bash - 2>/dev/null
    $SUDO apt-get install -y nodejs 2>/dev/null || true
fi

success "Dépendances système OK."

# =============================================================================
#  3. Organisation du projet
# =============================================================================
step "Organisation du projet"

mkdir -p "${REPO_DIR}/backend"
mkdir -p "${REPO_DIR}/frontend/src/components"
mkdir -p "${REPO_DIR}/frontend/src/pages"
mkdir -p "${REPO_DIR}/frontend/src/utils"
mkdir -p "${REPO_DIR}/frontend/public"
mkdir -p "${REPO_DIR}/data/cache"

# Copier les fichiers Python vers backend/ s'ils sont à la racine
for f in backend.py agent.py mcp_server.py db_routes.py gee_routes.py; do
    if [ -f "${REPO_DIR}/${f}" ] && [ ! -f "${REPO_DIR}/backend/${f}" ]; then
        cp "${REPO_DIR}/${f}" "${REPO_DIR}/backend/${f}"
        info "${f} → backend/"
    fi
done

# Copier les requirements
for req in requirements.txt requirements_agent.txt; do
    if [ -f "${REPO_DIR}/${req}" ] && [ ! -f "${REPO_DIR}/backend/${req}" ]; then
        cp "${REPO_DIR}/${req}" "${REPO_DIR}/backend/${req}"
    fi
done

# Copier les fichiers frontend src (theme.js, config.js, index.css, main.jsx)
for f in theme.js config.js index.css main.jsx; do
    if [ -f "${REPO_DIR}/${f}" ] && [ ! -f "${REPO_DIR}/frontend/src/${f}" ]; then
        cp "${REPO_DIR}/${f}" "${REPO_DIR}/frontend/src/${f}"
        info "${f} → frontend/src/"
    fi
done

# App.jsx : version multi-composants (App.jsx) prioritaire sur overture_explorer.jsx
if [ -f "${REPO_DIR}/App.jsx" ]; then
    cp "${REPO_DIR}/App.jsx" "${REPO_DIR}/frontend/src/App.jsx"
    info "App.jsx (version agent multi-composants) installé"
elif [ -f "${REPO_DIR}/overture_explorer.jsx" ]; then
    cp "${REPO_DIR}/overture_explorer.jsx" "${REPO_DIR}/frontend/src/App.jsx"
    info "overture_explorer.jsx → frontend/src/App.jsx"
fi

success "Structure du projet OK."

# =============================================================================
#  4. Fichiers frontend générés si absents
# =============================================================================
step "Génération des fichiers frontend manquants"

# index.html
if [ ! -f "${REPO_DIR}/frontend/index.html" ]; then
cat > "${REPO_DIR}/frontend/index.html" << 'HTML'
<!DOCTYPE html>
<html lang="fr">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Overture Maps Explorer</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.jsx"></script>
  </body>
</html>
HTML
info "index.html créé"
fi

# package.json — basé sur les imports réels de App.jsx :
#   react-map-gl/maplibre, maplibre-gl, @turf/turf, d3, recharts, lucide-react
if [ ! -f "${REPO_DIR}/frontend/package.json" ]; then
cat > "${REPO_DIR}/frontend/package.json" << 'JSON'
{
  "name": "overture-maps-explorer",
  "private": true,
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "react": "^18.3.1",
    "react-dom": "^18.3.1",
    "react-map-gl": "^7.1.7",
    "maplibre-gl": "^4.5.0",
    "@maplibre/maplibre-gl-style-spec": "^20.3.0",
    "@turf/turf": "^7.1.0",
    "d3": "^7.9.0",
    "recharts": "^2.13.0",
    "lucide-react": "^0.383.0"
  },
  "devDependencies": {
    "@vitejs/plugin-react": "^4.3.0",
    "vite": "^5.4.0"
  }
}
JSON
info "package.json créé (react-map-gl, maplibre-gl, turf, d3, recharts, lucide-react)"
fi

# vite.config.js — proxy /api → backend:8000
if [ ! -f "${REPO_DIR}/frontend/vite.config.js" ]; then
cat > "${REPO_DIR}/frontend/vite.config.js" << 'JS'
import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true
      }
    }
  }
})
JS
info "vite.config.js créé (proxy /api → :8000)"
fi

success "Fichiers frontend OK."

# =============================================================================
#  5. Backend Python — venv + toutes les dépendances
# =============================================================================
step "Configuration du Backend Python"

cd "${REPO_DIR}/backend"

info "Création de l'environnement virtuel Python..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip --quiet

# ── Dépendances core (requirements.txt) ──────────────────────
info "Core : FastAPI + DuckDB + GeoPandas..."
pip install --quiet \
    "fastapi>=0.115.0" \
    "uvicorn>=0.30.0" \
    "duckdb>=1.1.0" \
    "pandas>=2.0.0" \
    "geopandas>=0.14.0" \
    "pyarrow>=14.0.0" \
    "shapely>=2.0.0" \
    "mcp>=1.0.0" \
    "pydantic>=2.0.0"

# ── Dépendances agent (requirements_agent.txt) ───────────────
info "Agent : LiteLLM + python-dotenv + requests..."
pip install --quiet \
    "litellm>=1.50.0" \
    "python-dotenv>=1.0.0" \
    "requests>=2.31.0"

# ── Dépendances DB externe (db_routes.py) ────────────────────
info "DB externe : SQLAlchemy + psycopg2 + pymysql (optionnel)..."
pip install --quiet "sqlalchemy>=2.0.0" || true
pip install --quiet "psycopg2-binary" || warn "psycopg2-binary non installé (PostgreSQL optionnel)"
pip install --quiet "pymysql" || warn "pymysql non installé (MySQL optionnel)"

# ── Google Earth Engine (gee_routes.py) ──────────────────────
info "GEE : earthengine-api + google-auth (optionnel)..."
pip install --quiet \
    "earthengine-api" \
    "google-auth" \
    "google-auth-httplib2" || warn "earthengine-api non installé (GEE optionnel)"

deactivate
success "Backend Python configuré."

# =============================================================================
#  6. Fichier .env
# =============================================================================
step "Création du fichier .env"

ENV_FILE="${REPO_DIR}/backend/.env"
if [ ! -f "$ENV_FILE" ]; then
cat > "$ENV_FILE" << 'ENV'
# ── LLM Provider ──────────────────────────────────────────────
# Valeurs possibles : claude | openai | ollama | openrouter | deepseek | mistral
LLM_PROVIDER=claude

# ── Clés API — décommenter selon le provider choisi ──────────
# ANTHROPIC_API_KEY=sk-ant-...
# OPENAI_API_KEY=sk-...
# OPENROUTER_API_KEY=sk-or-...
# DEEPSEEK_API_KEY=sk-...
# MISTRAL_API_KEY=...

# Ollama local (si LLM_PROVIDER=ollama)
# OLLAMA_API_BASE=http://localhost:11434
# OLLAMA_MODEL=ollama/llama3.1

# ── Modèles (optionnel, sinon valeurs par défaut) ─────────────
# CLAUDE_MODEL=claude-sonnet-4-20250514
# OPENAI_MODEL=gpt-4o

# ── Overture Maps ─────────────────────────────────────────────
OVERTURE_RELEASE=2026-03-18.0
OVERTURE_S3_REGION=us-west-2

# ── DuckDB ────────────────────────────────────────────────────
DUCKDB_MEMORY=4GB
DUCKDB_THREADS=4

# ── Serveur ───────────────────────────────────────────────────
BACKEND_PORT=8000
CORS_ORIGINS=http://localhost:5173,http://localhost:3000
ENV
    success ".env créé → ${ENV_FILE}"
    echo ""
    warn "⚠  IMPORTANT : Renseignez votre clé API dans backend/.env avant de démarrer !"
    warn "   nano ${ENV_FILE}"
else
    info ".env déjà présent, non modifié."
fi

# =============================================================================
#  7. Frontend — npm install
# =============================================================================
step "Installation des dépendances npm"

cd "${REPO_DIR}/frontend"
npm install --silent
success "npm install OK."

# =============================================================================
#  8. Scripts de lancement
# =============================================================================
step "Création des scripts de lancement"

# start_backend.sh — mode agent (LiteLLM)
cat > "${REPO_DIR}/start_backend.sh" << 'SCRIPT'
#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/backend"
source venv/bin/activate
echo "▶  Agent Backend démarré sur http://localhost:8000"
echo "   Docs : http://localhost:8000/docs"
echo "   Provider LLM : $(grep LLM_PROVIDER .env 2>/dev/null | cut -d= -f2 || echo 'voir .env')"
python agent.py
SCRIPT
chmod +x "${REPO_DIR}/start_backend.sh"

# start_backend_simple.sh — DuckDB seul, sans LLM
cat > "${REPO_DIR}/start_backend_simple.sh" << 'SCRIPT'
#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/backend"
source venv/bin/activate
echo "▶  Backend simple (DuckDB, sans LLM) sur http://localhost:8000"
python backend.py
SCRIPT
chmod +x "${REPO_DIR}/start_backend_simple.sh"

# start_frontend.sh
cat > "${REPO_DIR}/start_frontend.sh" << 'SCRIPT'
#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/frontend"
echo "▶  Frontend React démarré sur http://localhost:5173"
npm run dev
SCRIPT
chmod +x "${REPO_DIR}/start_frontend.sh"

# start_mcp.sh — MCP Server pour Claude Desktop / Cursor
cat > "${REPO_DIR}/start_mcp.sh" << 'SCRIPT'
#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/backend"
source venv/bin/activate
echo "▶  MCP Server Overture Maps (stdio)"
python mcp_server.py
SCRIPT
chmod +x "${REPO_DIR}/start_mcp.sh"

# start_all.sh — tout en une commande
cat > "${REPO_DIR}/start_all.sh" << 'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "▶  Démarrage de tous les services..."

bash "$SCRIPT_DIR/start_backend.sh" &
BACKEND_PID=$!
sleep 3

bash "$SCRIPT_DIR/start_frontend.sh" &
FRONTEND_PID=$!

echo ""
echo "✅  Services démarrés :"
echo "   Agent Backend : http://localhost:8000"
echo "   Frontend      : http://localhost:5173"
echo "   API Docs      : http://localhost:8000/docs"
echo ""
echo "   Ctrl+C pour arrêter."

trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; echo 'Services arrêtés.'" INT TERM
wait
SCRIPT
chmod +x "${REPO_DIR}/start_all.sh"

success "Scripts de lancement créés."

# =============================================================================
#  9. Configuration MCP pour Claude Desktop (Linux)
# =============================================================================
step "Configuration MCP pour Claude Desktop"

MCP_DIR="${HOME}/.config/Claude"
MCP_FILE="${MCP_DIR}/claude_desktop_config.json"
VENV_PY="${REPO_DIR}/backend/venv/bin/python"
MCP_PY="${REPO_DIR}/backend/mcp_server.py"

mkdir -p "$MCP_DIR"
[ -f "$MCP_FILE" ] && cp "$MCP_FILE" "${MCP_FILE}.bak" && warn "Config MCP existante sauvegardée."

cat > "$MCP_FILE" << JSON
{
  "mcpServers": {
    "overture-maps": {
      "command": "${VENV_PY}",
      "args": ["${MCP_PY}"],
      "env": {
        "OVERTURE_RELEASE": "2026-03-18.0",
        "DUCKDB_MEMORY": "4GB"
      }
    }
  }
}
JSON
success "Config MCP → ${MCP_FILE}"

# =============================================================================
#  10. Google Earth Engine — instructions authentification
# =============================================================================
step "Google Earth Engine (optionnel)"
echo -e "  Pour activer les couches satellite GEE :"
echo -e "  ${YELLOW}source backend/venv/bin/activate${NC}"
echo -e "  ${YELLOW}earthengine authenticate${NC}"
echo -e "  Credentials sauvegardés dans : ~/.config/earthengine/credentials"

# =============================================================================
#  Résumé final
# =============================================================================
echo ""
echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${GREEN}║        ✅  Installation terminée avec succès !           ║${NC}"
echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BOLD}Étape 1 — Configurer votre clé API LLM :${NC}"
echo -e "  ${YELLOW}nano backend/.env${NC}   # décommenter ANTHROPIC_API_KEY ou autre"
echo ""
echo -e "  ${BOLD}Étape 2 — Démarrer l'application :${NC}"
echo -e "  ${YELLOW}./start_all.sh${NC}                 # tout en une commande"
echo ""
echo -e "  ${CYAN}Ou séparément dans deux terminaux :${NC}"
echo -e "  ${YELLOW}./start_backend.sh${NC}             # Agent (LiteLLM) → :8000"
echo -e "  ${YELLOW}./start_backend_simple.sh${NC}      # DuckDB seul     → :8000"
echo -e "  ${YELLOW}./start_frontend.sh${NC}            # React UI        → :5173"
echo -e "  ${YELLOW}./start_mcp.sh${NC}                 # MCP Server (Claude Desktop)"
echo ""
echo -e "  ${BOLD}API Docs :${NC} http://localhost:8000/docs"
echo ""
