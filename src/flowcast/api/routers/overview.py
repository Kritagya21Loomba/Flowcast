"""Overview endpoint — system-wide statistics."""

from fastapi import APIRouter, Depends
import duckdb

from flowcast.api.deps import get_db
from flowcast.api.schemas import OverviewStats

router = APIRouter()


@router.get("/overview", response_model=OverviewStats)
def get_overview(con: duckdb.DuckDBPyConnection = Depends(get_db)) -> OverviewStats:
    total_sites = con.execute("SELECT COUNT(*) FROM signal_sites").fetchone()[0]
    sites_with_coords = con.execute(
        "SELECT COUNT(*) FROM signal_sites WHERE latitude IS NOT NULL"
    ).fetchone()[0]
    clustered_sites = con.execute("SELECT COUNT(*) FROM site_clusters").fetchone()[0]
    total_daily = con.execute("SELECT COUNT(*) FROM traffic_daily").fetchone()[0]

    date_range = con.execute(
        "SELECT MIN(date), MAX(date) FROM traffic_daily"
    ).fetchone()

    total_forecasts = con.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]
    model_count = con.execute("SELECT COUNT(*) FROM model_registry").fetchone()[0]

    best_mape = con.execute(
        "SELECT MIN(test_mape) FROM model_registry"
    ).fetchone()[0]

    corr_pairs = con.execute("SELECT COUNT(*) FROM site_correlations").fetchone()[0]

    return OverviewStats(
        total_sites=total_sites,
        sites_with_coords=sites_with_coords,
        clustered_sites=clustered_sites,
        total_daily_rows=total_daily,
        date_range_start=date_range[0],
        date_range_end=date_range[1],
        total_forecasts=total_forecasts,
        model_count=model_count,
        best_model_mape=best_mape,
        correlation_pairs=corr_pairs,
    )
