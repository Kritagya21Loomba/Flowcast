"""FastAPI application factory."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from flowcast.config import FRONTEND_DIST_DIR
from flowcast.api.routers import overview, sites, forecasts, clusters, correlations, models


def create_app() -> FastAPI:
    app = FastAPI(title="Flowcast API", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    app.include_router(overview.router, prefix="/api", tags=["overview"])
    app.include_router(sites.router, prefix="/api", tags=["sites"])
    app.include_router(forecasts.router, prefix="/api", tags=["forecasts"])
    app.include_router(clusters.router, prefix="/api", tags=["clusters"])
    app.include_router(correlations.router, prefix="/api", tags=["correlations"])
    app.include_router(models.router, prefix="/api", tags=["models"])

    # Serve built frontend in production
    if FRONTEND_DIST_DIR.exists():
        app.mount("/", StaticFiles(directory=str(FRONTEND_DIST_DIR), html=True))

    return app


app = create_app()
