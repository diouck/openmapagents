#!/bin/sh
# Injecte la config runtime dans /env.js au démarrage du conteneur nginx.
# Le token vient de l'environnement (backend/.env via env_file dans docker-compose).
# Priorité : VITE_MAPBOX_TOKEN, puis MAPBOX_ACCESS_TOKEN.
set -e

TOKEN="${VITE_MAPBOX_TOKEN:-${MAPBOX_ACCESS_TOKEN:-}}"

cat > /usr/share/nginx/html/env.js <<EOF
window.__ENV__ = { MAPBOX_TOKEN: "${TOKEN}" };
EOF

echo "[runtime-env] env.js généré (MAPBOX_TOKEN: ${TOKEN:+présent}${TOKEN:-absent})"
