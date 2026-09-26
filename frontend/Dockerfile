# syntax=docker/dockerfile:1
# OpenMapAgents — frontend (React/Vite build served by nginx)
# Build context = repo root:  docker build -f frontend/Dockerfile .

# ---- Stage 1: build the static bundle ----
FROM node:20-alpine AS build
WORKDIR /app

# Install deps from lockfile (reproducible)
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

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
