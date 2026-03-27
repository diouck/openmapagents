# ============================================================
# Installer MapLibre GL JS dans le frontend
# Exécuter depuis le dossier frontend/
# ============================================================

Write-Host "Installation de MapLibre GL JS..." -ForegroundColor Cyan
Write-Host ""

npm install react-map-gl maplibre-gl

Write-Host ""
Write-Host "[OK] react-map-gl + maplibre-gl installés" -ForegroundColor Green
Write-Host ""
Write-Host "Styles gratuits disponibles (OpenFreeMap, sans clé API) :" -ForegroundColor Yellow
Write-Host "  Dark   : https://tiles.openfreemap.org/styles/dark" -ForegroundColor DarkGray
Write-Host "  Liberty: https://tiles.openfreemap.org/styles/liberty" -ForegroundColor DarkGray
Write-Host "  Bright : https://tiles.openfreemap.org/styles/bright" -ForegroundColor DarkGray
Write-Host "  Positron: https://tiles.openfreemap.org/styles/positron" -ForegroundColor DarkGray
Write-Host ""
Write-Host "Maintenant remplace frontend\src\App.jsx par le nouveau fichier" -ForegroundColor Yellow
