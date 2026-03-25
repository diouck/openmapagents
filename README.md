# Overture Maps Explorer

> Interface complète pour explorer, visualiser, administrer et exporter les données Overture Maps.
> Stack : React + FastAPI + DuckDB + MCP Server

## Architecture

```
overture-maps-explorer/
├── frontend/                    # React app (Vite)
│   ├── src/
│   │   ├── App.jsx              # ← overture_explorer.jsx
│   │   ├── main.jsx
│   │   └── index.css
│   ├── package.json
│   └── vite.config.js
│
├── backend/
│   ├── backend.py               # FastAPI server
│   ├── mcp_server.py            # MCP server pour agents AI
│   └── requirements.txt
│
├── data/
│   └── cache/                   # Cache DuckDB local
│
└── README.md
```

## Installation sur Windows

### 1. Backend Python

```powershell
# Créer l'environnement
cd backend
python -m venv venv
venv\Scripts\activate

# Installer les dépendances
pip install fastapi uvicorn duckdb pandas geopandas pyarrow shapely mcp

# Lancer le serveur API
python backend.py
# → http://localhost:8000
# → http://localhost:8000/docs (Swagger UI)
```

### 2. Frontend React

```powershell
cd frontend

# Créer le projet Vite
npm create vite@latest . -- --template react
npm install
npm install d3 react-map-gl maplibre-gl @maplibre/maplibre-gl-style-spec
npm install recharts lucide-react

# Remplacer src/App.jsx par overture_explorer.jsx
# Lancer le dev server
npm run dev
# → http://localhost:5173
```

### 3. MCP Server (pour Claude Desktop / Cursor)

Ajouter dans `claude_desktop_config.json` :

```json
{
  "mcpServers": {
    "overture-maps": {
      "command": "python",
      "args": ["C:/path/to/backend/mcp_server.py"],
      "env": {
        "OVERTURE_RELEASE": "2026-03-18.0",
        "DUCKDB_MEMORY": "4GB"
      }
    }
  }
}
```

## Pages de l'application

### 🔍 Explorer
- Carte interactive SVG avec projection des features Overture
- Sélection du thème (Places, Buildings, Transport, Divisions, Base, Addresses)
- Filtrage par bbox preset (Nantes, Loire-Atlantique, Dakar, IDF, France)
- Coloration par catégorie, confiance ou hauteur
- Panel de détail au clic sur une feature
- Graphiques de distribution et donut de répartition

### ⚙️ Admin
- Import de données par thème et zone géographique
- Génération automatique de la requête DuckDB
- Historique des imports avec statut (pending/running/done)
- Configuration DuckDB (mémoire, threads, extensions)
- Gestion du cache local

### 📊 DataViz
- Distribution des hauteurs de bâtiments
- Distribution des scores de confiance
- Densité H3 hexagonale (viridis colormap)
- Évolution temporelle des releases Overture
- Scatter plot croisement hauteur × surface
- KPI cards avec métriques clés

### 🎨 Style Editor
- Éditeur visuel de styles MapLibre GL
- Color picker pour remplissage et contour
- Contrôle d'opacité et épaisseur
- Toggle extrusion 3D avec champ de hauteur
- Choix du fond de carte (dark/light/satellite/terrain)
- Preview SVG temps réel
- Export du style JSON complet

### ↓ Export
- Sélection du thème, zone et format
- Formats supportés : GeoJSON, GeoPackage, CSV, GeoParquet, FlatGeobuf, Shapefile
- Sélection des colonnes à exporter
- Limite de features configurable
- Génération de la requête DuckDB SQL
- Historique des exports récents

### ⬡ MCP Server
- Liste des 7 tools MCP disponibles
- Logs en temps réel des appels
- Configuration JSON pour Claude Desktop
- Status des services (MCP, DuckDB, S3, Cache, FastAPI)

## API Endpoints

| Méthode | Route | Description |
|---------|-------|-------------|
| GET | `/api/themes` | Liste des thèmes Overture |
| GET | `/api/query/{theme}` | Requête spatiale par bbox |
| GET | `/api/stats/{theme}` | Statistiques agrégées |
| GET | `/api/h3/{theme}` | Densité H3 |
| POST | `/api/export` | Export multi-format |
| GET | `/api/sql` | Requête SQL brute |

## MCP Tools

| Tool | Description |
|------|-------------|
| `query_places` | Recherche POI par bbox, catégorie, nom |
| `query_buildings` | Recherche bâtiments par bbox et hauteur |
| `query_transport` | Réseau routier par bbox et classe |
| `spatial_stats` | Statistiques spatiales agrégées |
| `h3_density` | Densité hexagonale H3 |
| `export_overture` | Génération de requêtes d'export |
| `raw_duckdb_query` | Requête DuckDB SQL libre |

## Technologies

- **Frontend** : React 18, Vite, D3.js, SVG natif
- **Backend** : FastAPI, DuckDB, Pandas, GeoPandas
- **Data** : Overture Maps GeoParquet sur S3 (release 2026-03-18.0)
- **Carto** : MapLibre GL JS, PyDeck, SVG custom
- **AI** : MCP Server Python (protocole Model Context Protocol)
- **Cache** : SQLite / fichiers JSON locaux

---

**Auteur** : Kane Diouck — diouckk@gmail.com
**GitHub** : github.com/diouck
