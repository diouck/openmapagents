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
