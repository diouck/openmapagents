// Configuration runtime injectée au démarrage du conteneur (voir docker-entrypoint.d).
// Valeur par défaut vide : en développement, le token vient de frontend/.env (VITE_MAPBOX_TOKEN).
window.__ENV__ = window.__ENV__ || {};
