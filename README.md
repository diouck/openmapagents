# Overture Maps Explorer

> Interface cartographique intelligente pour explorer, visualiser et analyser les données **Overture Maps**.
> Agent IA multi-provider avec routing, analyse spatiale, GEE et connexion DB externe.

**Stack :** React 18 + Vite · FastAPI · DuckDB · LiteLLM · MapLibre GL · Turf.js · MCP Server · Google Earth Engine

---

## Architecture réelle du projet

```
openmapagents/
├── backend/
│   ├── agent.py              # ⭐ Backend principal — FastAPI + LiteLLM (multi-LLM)
│   ├── backend.py            # Backend simple — FastAPI + DuckDB seul (sans LLM)
│   ├── mcp_server.py         # MCP Server — 7 tools pour Claude Desktop / Cursor
│   ├── db_routes.py          # Router DB externe — PostgreSQL, MySQL, SQLite → GeoJSON
│   ├── gee_routes.py         # Router GEE — Sentinel-2, Landsat, MODIS, SRTM...
│   ├── requirements.txt      # FastAPI, DuckDB, GeoPandas, MCP...
│   ├── requirements_agent.txt # LiteLLM, python-dotenv, requests
│   └── .env                  # Clés API et configuration (créé à l'installation)
│
├── frontend/
│   ├── src/
│   │   ├── App.jsx           # Composant principal (version agent multi-panneaux)
│   │   ├── main.jsx          # Point d'entrée React + MapLibre CSS
│   │   ├── theme.js          # Thème dark/light avec CSS variables
│   │   ├── config.js         # API URL, styles carte, couleurs, export formats
│   │   ├── index.css         # Styles globaux
│   │   ├── components/       # ChatPanel, LayerPanel, GEEPanel, DBPanel...
│   │   └── utils/            # classification, helpers, spatial, routing, makiLoader
│   ├── package.json
│   └── vite.config.js        # Proxy /api → localhost:8000
│
├── data/cache/               # Cache DuckDB local (JSON)
├── install.sh                # ← Script d'installation Linux
├── setup.ps1                 # Script d'installation Windows
├── install_maplibre.ps1      # Installation MapLibre (Windows)
├── overture_explorer.jsx     # Version standalone (sans composants séparés)
└── README.md
```

---

## Prérequis

| Outil | Version minimale | Rôle |
|-------|-----------------|------|
| Python | 3.10+ | Backend FastAPI + DuckDB |
| Node.js | **18+** | Frontend Vite/React |
| npm | 9+ | Packages frontend |
| Git | 2.x | Clone du repo |

---

## Installation Linux (Ubuntu / Debian / Fedora / Arch)

### Méthode rapide

```bash
git clone https://github.com/diouck/openmapagents.git
cd openmapagents
chmod +x install.sh
./install.sh
```

Le script installe automatiquement :
- Les dépendances système (Python, Node.js 18+, GDAL, spatialindex)
- L'environnement virtuel Python avec tous les packages (core + agent + GEE + DB)
- Les dépendances npm (react-map-gl, maplibre-gl, @turf/turf, d3, recharts, lucide-react)
- Les fichiers frontend manquants (index.html, package.json, vite.config.js)
- Un fichier `.env` à configurer avec votre clé API LLM
- Les scripts `start_*.sh` prêts à l'emploi
- La configuration MCP dans `~/.config/Claude/claude_desktop_config.json`

### Après installation

```bash
# 1. Configurer votre provider LLM
nano backend/.env
# → décommenter ANTHROPIC_API_KEY=sk-ant-... (ou OpenAI, Ollama, etc.)

# 2. Lancer tout en une commande
./start_all.sh
```

### Scripts disponibles

| Script | Description | URL |
|--------|-------------|-----|
| `./start_backend.sh` | Agent LiteLLM multi-LLM | http://localhost:8000 |
| `./start_backend_simple.sh` | DuckDB seul, sans LLM | http://localhost:8000 |
| `./start_frontend.sh` | Interface React | http://localhost:5173 |
| `./start_mcp.sh` | MCP Server (Claude Desktop) | stdio |
| `./start_all.sh` | Tout en une commande | — |

### Installation manuelle étape par étape

#### Backend

```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip

# Core (DuckDB, FastAPI, GeoPandas)
pip install -r requirements.txt

# Agent LiteLLM
pip install -r requirements_agent.txt

# Optionnel — DB externe (PostgreSQL, MySQL)
pip install sqlalchemy psycopg2-binary pymysql

# Optionnel — Google Earth Engine
pip install earthengine-api google-auth google-auth-httplib2

# Lancer l'agent (LiteLLM multi-LLM)
python agent.py
# → http://localhost:8000  |  Docs : http://localhost:8000/docs

# Ou le backend simple (DuckDB seul)
python backend.py
```

#### Frontend

```bash
cd frontend
npm install
npm run dev
# → http://localhost:5173
```

#### MCP Server (Claude Desktop / Cursor)

Créer ou éditer `~/.config/Claude/claude_desktop_config.json` :

```json
{
  "mcpServers": {
    "overture-maps": {
      "command": "/chemin/vers/backend/venv/bin/python",
      "args": ["/chemin/vers/backend/mcp_server.py"],
      "env": {
        "OVERTURE_RELEASE": "2026-03-18.0",
        "DUCKDB_MEMORY": "4GB"
      }
    }
  }
}
```

---

## Installation Windows

```powershell
# Cloner et installer
git clone https://github.com/diouck/openmapagents.git
cd openmapagents
.\setup.ps1

# Installer MapLibre dans le frontend
cd frontend
..\install_maplibre.ps1
```

Ou manuellement :

```powershell
# Backend
cd backend
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt -r requirements_agent.txt
python agent.py

# Frontend (autre terminal)
cd frontend
npm install
npm run dev
```

---

## Configuration LLM (backend/.env)

Le fichier `.env` est créé automatiquement à l'installation. Décommentez le provider de votre choix :

```dotenv
# Provider : claude | openai | ollama | openrouter | deepseek | mistral
LLM_PROVIDER=claude

ANTHROPIC_API_KEY=sk-ant-...
# OPENAI_API_KEY=sk-...
# OPENROUTER_API_KEY=sk-or-...
# OLLAMA_API_BASE=http://localhost:11434

OVERTURE_RELEASE=2026-03-18.0
DUCKDB_MEMORY=4GB
DUCKDB_THREADS=4
BACKEND_PORT=8000
```

---

## Fonctionnalités

### 🗺️ Carte interactive (MapLibre GL)
Fonds de carte OpenFreeMap (dark, liberty, positron) — sans clé API. Turf.js côté client pour l'analyse spatiale.

### 🤖 Agent IA (ChatPanel)
Interrogation en langage naturel via LiteLLM. Tools : `geocode`, `query_overture`, `fly_to`, `spatial_analysis`, `compute_route`, `compute_isochrone`, `set_layer_style`, `remove_layer`, `get_layer_stats`.

### 📡 Données Overture Maps (DuckDB → S3)
Requêtes directes sur les GeoParquet Overture Maps hébergés sur S3 AWS (release `2026-03-18.0`). Thèmes : places, buildings, transportation, divisions, base, addresses.

### 🛰️ Google Earth Engine (GEEPanel)
Sentinel-2, Landsat 8/9, MODIS LST/NDVI, ESA WorldCover, Sentinel-1 SAR, SRTM, ERA5. Indices : NDVI, NDWI, NDBI, EVI, LST, RGB, False Color.

### 🗄️ Connexion DB externe (DBPanel)
PostgreSQL (PostGIS), MySQL spatial, SQLite. Retour GeoJSON avec support WKT, ST_AsGeoJSON, colonnes lat/lon.

### 🔀 Analyse spatiale (SpatialPanel)
Operations Turf.js : intersection, union, difference, clip, buffer, points_in_polygon, spatial_join, clustering DBSCAN, centroid, convex_hull, voronoi, hex_grid, dissolve, simplify.

### 🚗 Routing & Isochrones
Itinéraires (pied, vélo, voiture) et zones d'accessibilité via Mapbox Directions/Isochrone API.

---

## API Endpoints (agent.py)

| Méthode | Route | Description |
|---------|-------|-------------|
| GET | `/` | Info service + provider LLM actif |
| GET | `/api/config` | Config frontend (provider, modèle, thèmes) |
| POST | `/api/chat` | Chat avec l'agent (tool calling LiteLLM) |
| POST | `/api/query/{theme}` | Requête DuckDB directe (bypass LLM) |
| GET | `/api/query` | Requête GET (re-query auto-clip frontend) |
| POST | `/api/export` | Export GeoJSON |
| POST | `/api/db/test` | Test connexion DB externe |
| POST | `/api/db/tables` | Liste des tables |
| POST | `/api/db/query` | Requête SQL → GeoJSON |
| GET | `/api/gee/health` | Statut GEE |
| GET | `/api/gee/datasets` | Catalogue datasets GEE |
| POST | `/api/gee/tiles` | URL tuiles XYZ GEE |
| POST | `/api/gee/dates` | Dates disponibles pour un dataset |

## MCP Tools (mcp_server.py)

| Tool | Description |
|------|-------------|
| `query_places` | POI par bbox, catégorie, nom, confiance |
| `query_buildings` | Bâtiments par bbox et hauteur |
| `query_transport` | Réseau routier par bbox et classe |
| `spatial_stats` | Stats agrégées (count, catégories, hauteurs) |
| `h3_density` | Densité hexagonale H3 (résolution 4-12) |
| `export_overture` | Génération requête DuckDB d'export |
| `raw_duckdb_query` | Requête SQL DuckDB libre |

---

## Dépannage

**GDAL introuvable (Ubuntu) :**
```bash
sudo apt-get install libgdal-dev gdal-bin
pip install gdal==$(gdal-config --version)
```

**Node.js < 18 :**
```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
```

**Erreur DuckDB httpfs / S3 :**
```bash
# Vérifier l'accès réseau S3
python3 -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs; LOAD httpfs;'); print('OK')"
```

**GEE : credentials non trouvés :**
```bash
source backend/venv/bin/activate
earthengine authenticate
# Suivre le lien OAuth et copier le token
```

**Port 8000 déjà utilisé :**
```bash
lsof -i :8000         # trouver le processus
# ou changer BACKEND_PORT dans backend/.env
```






Crée un script de démarrage :

```bash
cat > /var/www/openmapagents/start.sh << 'EOF'
#!/bin/bash

echo "=== Arrêt des processus existants ==="
pkill -f "agent.py" 2>/dev/null
pkill -f "vite" 2>/dev/null
sleep 2
fuser -k 8000/tcp 2>/dev/null
fuser -k 5173/tcp 2>/dev/null
sleep 1

echo "=== Démarrage du backend ==="
cd /var/www/openmapagents/backend
source venv/bin/activate
nohup python agent.py > /var/log/openmapagents-backend.log 2>&1 &
echo "Backend PID: $!"

sleep 3

echo "=== Démarrage du frontend ==="
cd /var/www/openmapagents/frontend
nohup npm run dev -- --host 0.0.0.0 > /var/log/openmapagents-frontend.log 2>&1 &
echo "Frontend PID: $!"

echo "=== Terminé ==="
echo "Logs backend  : tail -f /var/log/openmapagents-backend.log"
echo "Logs frontend : tail -f /var/log/openmapagents-frontend.log"
EOF

chmod +x /var/www/openmapagents/start.sh
```

Lance avec :

```bash
bash /var/www/openmapagents/start.sh
```

Mais comme tu as nginx avec le build statique, **tu n'as pas besoin du frontend Vite** en production. Juste le backend suffit :

```bash
cat > /var/www/openmapagents/start.sh << 'EOF'
#!/bin/bash

echo "=== Arrêt des processus existants ==="
pkill -f "agent.py" 2>/dev/null
sleep 2
fuser -k 8000/tcp 2>/dev/null
sleep 1

echo "=== Démarrage du backend ==="
cd /var/www/openmapagents/backend
source venv/bin/activate
nohup python agent.py > /var/log/openmapagents-backend.log 2>&1 &
echo "Backend PID: $!"

echo "Backend démarré — https://openmapagents.geoafrica.fr"
EOF

chmod +x /var/www/openmapagents/start.sh
bash /var/www/openmapagents/start.sh
```

Nginx sert les fichiers statiques du `dist/` directement — pas besoin de Vite en prod. Et pour que le backend redémarre automatiquement au reboot :

```bash
systemctl enable openmapagents
systemctl start openmapagents
```



---

**Auteur :** Kane Diouck — [diouckk@gmail.com](mailto:diouckk@gmail.com)
**GitHub :** [github.com/diouck](https://github.com/diouck)
