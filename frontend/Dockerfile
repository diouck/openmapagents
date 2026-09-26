# syntax=docker/dockerfile:1
# OpenMapAgents — frontend (React/Vite build served by nginx)
# Build context = repo root:  docker build -f frontend/Dockerfile .

# ---- Stage 1: build the static bundle ----
FROM node:20-slim AS build
WORKDIR /app

# Prefer the reproducible lockfile install; fall back to npm install if the
# Windows-authored lockfile drifts from package.json (keeps CI unblocked).
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund \
    || npm install --no-audit --no-fund --legacy-peer-deps

# Public build-time config baked into the Vite bundle (optional; empty by default).
# Vite exposes process.env vars prefixed VITE_ to import.meta.env at build.
ARG VITE_MAPBOX_TOKEN=""
ARG VITE_LITELLM_API_URL="/api/llm"
ARG VITE_CHAT_INPUT="true"
ENV VITE_MAPBOX_TOKEN=$VITE_MAPBOX_TOKEN \
    VITE_LITELLM_API_URL=$VITE_LITELLM_API_URL \
    VITE_CHAT_INPUT=$VITE_CHAT_INPUT

# Build (index.html + app.html + doc.html + robots.txt + sitemap.xml)
COPY frontend/ ./
RUN npm run build

# ---- Stage 2: serve with nginx ----
FROM nginx:1.27-alpine AS runtime

# Backend upstream used by the /api proxy (compose service name by default).
# The official nginx entrypoint runs envsubst on templates at startup;
# NGINX_ENVSUBST_FILTER restricts substitution to this one variable so that
# nginx runtime vars ($uri, $host, ...) are left untouched.
ENV BACKEND_UPSTREAM=http://backend:8000 \
    NGINX_ENVSUBST_FILTER=BACKEND_UPSTREAM

RUN rm -f /etc/nginx/conf.d/default.conf
COPY frontend/nginx.conf.template /etc/nginx/templates/openmapagents.conf.template
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost/robots.txt >/dev/null 2>&1 || exit 1
