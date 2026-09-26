# syntax=docker/dockerfile:1
# OpenMapAgents — backend (FastAPI / uvicorn)
# Build context = repo root:  docker build -f backend/Dockerfile .
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    BACKEND_PORT=8000

WORKDIR /app

# Runtime libs occasionally needed by geospatial / arrow wheels + curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
      libexpat1 \
      curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps first so this layer is cached across code changes
COPY requirements.txt requirements_agent.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements_agent.txt

# Application code
COPY backend/ ./backend/

# Non-root user + writable runtime dirs
RUN useradd -m -u 10001 appuser \
    && mkdir -p /app/backend/data/cache /app/logs \
    && chown -R appuser:appuser /app
USER appuser

WORKDIR /app/backend
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=45s --retries=3 \
  CMD curl -fsS "http://localhost:${BACKEND_PORT}/openapi.json" || exit 1

# Production: run uvicorn directly (no --reload)
CMD ["sh", "-c", "uvicorn agent:app --host 0.0.0.0 --port ${BACKEND_PORT}"]
