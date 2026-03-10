# Stage 1: Build frontend
FROM node:22-slim AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --ignore-scripts
COPY frontend/ ./
RUN npm run build

# Stage 2: Python application
FROM python:3.12-slim
WORKDIR /app

# Install Python dependencies (API group only — no ML libs needed)
COPY pyproject.toml ./
COPY src/ src/
RUN pip install --no-cache-dir duckdb structlog fastapi uvicorn pydantic

# Use PYTHONPATH so PROJECT_ROOT resolves to /app/ (not site-packages)
ENV PYTHONPATH=/app/src

# Copy deploy database into the standard data/ location
COPY deploy/flowcast.duckdb data/flowcast.duckdb

# Copy built frontend from stage 1
COPY --from=frontend /app/frontend/dist frontend/dist

# Copy server script
COPY scripts/serve.py scripts/serve.py

EXPOSE 8000

# Render sets PORT env var; shell form expands it
CMD sh -c "python scripts/serve.py --port ${PORT:-8000}"
