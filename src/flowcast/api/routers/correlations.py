"""Correlations endpoint — site pair network."""

from fastapi import APIRouter, Depends, Query
import duckdb

from flowcast.api.deps import get_db
from flowcast.api.schemas import CorrelationPair, CorrelationResponse

router = APIRouter()


@router.get("/correlations", response_model=CorrelationResponse)
def list_correlations(
    min_pearson: float = Query(default=0.8, ge=0, le=1),
    limit: int = Query(default=500, ge=1, le=10000),
    site_id: int | None = None,
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> CorrelationResponse:
    site_filter = ""
    if site_id is not None:
        site_filter = f"AND (sc.site_a = {site_id} OR sc.site_b = {site_id})"

    rows = con.execute(f"""
        SELECT sc.site_a, sc.site_b, sc.pearson_daily, sc.cosine_hourly, sc.lag_minutes,
               sa.latitude, sa.longitude, sb.latitude, sb.longitude
        FROM site_correlations sc
        LEFT JOIN signal_sites sa ON sc.site_a = sa.site_id
        LEFT JOIN signal_sites sb ON sc.site_b = sb.site_id
        WHERE sc.pearson_daily >= {min_pearson}
        {site_filter}
        ORDER BY sc.pearson_daily DESC
        LIMIT {limit}
    """).fetchall()

    pairs = [
        CorrelationPair(
            site_a=r[0], site_b=r[1], pearson_daily=r[2],
            cosine_hourly=r[3], lag_minutes=r[4],
            site_a_lat=r[5], site_a_lon=r[6],
            site_b_lat=r[7], site_b_lon=r[8],
        )
        for r in rows
    ]

    return CorrelationResponse(pairs=pairs, count=len(pairs))
