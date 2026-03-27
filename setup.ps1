# ============================================================
# Overture Maps Explorer — Setup complet Windows
# ============================================================

$ErrorActionPreference = "Stop"

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Overture Maps Explorer - Installation" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# ─── Vérifier Node.js ─────────────────────────────────────────
try {
    $nodeVersion = node --version
} catch {
    $nodeVersion = $null
}

if (-not $nodeVersion) {
    Write-Host "[ERREUR] Node.js n'est pas installe." -ForegroundColor Red
    Write-Host "Telecharge-le ici : https://nodejs.org/" -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Node.js $nodeVersion detecte" -ForegroundColor Green

# ─── Création des dossiers (robuste) ──────────────────────────
Write-Host ""
Write-Host "[1/6] Creation des dossiers..." -ForegroundColor Yellow

$dirs = @(
    "frontend",
    "frontend/src",
    "frontend/src/components",
    "frontend/src/pages",
    "frontend/src/utils",
    "frontend/public",
    "backend",
    "data/cache"
)

foreach ($dir in $dirs) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    Write-Host "  + $dir" -ForegroundColor DarkGray
}

# ─── Backend files ────────────────────────────────────────────
Write-Host ""
Write-Host "[2/6] Organisation backend..." -ForegroundColor Yellow

if (Test-Path "backend.py") {
    Move-Item "backend.py" "backend/backend.py" -Force
    Write-Host "  -> backend.py deplace" -ForegroundColor DarkGray
}

if (Test-Path "mcp_server.py") {
    Move-Item "mcp_server.py" "backend/mcp_server.py" -Force
    Write-Host "  -> mcp_server.py deplace" -ForegroundColor DarkGray
}

if (Test-Path "requirements.txt") {
    Copy-Item "requirements.txt" "backend/requirements.txt" -Force
    Write-Host "  -> requirements.txt copie" -ForegroundColor DarkGray
}

# ─── Frontend ────────────────────────────────────────────────
Write-Host ""
Write-Host "[3/6] Creation frontend (Vite + React)..." -ForegroundColor Yellow

# package.json
$packageJson = @'
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
    "d3": "^7.9.0"
  },
  "devDependencies": {
    "@vitejs/plugin-react": "^4.3.0",
    "vite": "^5.4.0"
  }
}
'@
Set-Content "frontend/package.json" $packageJson -Encoding UTF8

# vite.config.js
$viteConfig = @'
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
'@
Set-Content "frontend/vite.config.js" $viteConfig -Encoding UTF8

# index.html
$indexHtml = @'
<!DOCTYPE html>
<html lang="fr">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Overture Maps Explorer</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.jsx"></script>
  </body>
</html>
'@
Set-Content "frontend/index.html" $indexHtml -Encoding UTF8

# main.jsx
$mainJsx = @'
import React from "react"
import ReactDOM from "react-dom/client"
import App from "./App.jsx"
import "./index.css"

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)
'@
Set-Content "frontend/src/main.jsx" $mainJsx -Encoding UTF8

# index.css
$indexCss = @'
body {
  margin: 0;
  background: #0c0e12;
  color: #e8e6e1;
  font-family: sans-serif;
}
'@
Set-Content "frontend/src/index.css" $indexCss -Encoding UTF8

Write-Host "  + fichiers frontend crees" -ForegroundColor Green

# ─── App.jsx ─────────────────────────────────────────────────
Write-Host ""
Write-Host "[4/6] Installation App.jsx..." -ForegroundColor Yellow

if (Test-Path "overture_explorer.jsx") {
    Copy-Item "overture_explorer.jsx" "frontend/src/App.jsx" -Force
    Write-Host "  -> App.jsx installe" -ForegroundColor Green
} else {
    Write-Host "  [ATTENTION] overture_explorer.jsx manquant" -ForegroundColor Red
}

# ─── npm install ─────────────────────────────────────────────
Write-Host ""
Write-Host "[5/6] npm install..." -ForegroundColor Yellow

Push-Location frontend
npm install
Pop-Location

# ─── Fin ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Installation terminee !" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Cyan

Write-Host ""
Write-Host "DEMARRAGE :" -ForegroundColor Yellow

Write-Host ""
Write-Host "Backend :" -ForegroundColor White
Write-Host "  cd backend"
Write-Host "  python backend.py"

Write-Host ""
Write-Host "Frontend :" -ForegroundColor White
Write-Host "  cd frontend"
Write-Host "  npm run dev"