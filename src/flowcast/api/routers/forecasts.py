"""Forecasts endpoint."""

from fastapi import APIRouter, Depends
import duckdb

from flowcast.api.deps import get_db
from flowcast.api.schemas import ForecastPoint, SiteForecastResponse

router = APIRouter()


@router.get("/sites/{site_id}/forecasts", response_model=SiteForecastResponse)
def get_site_forecasts(
    site_id: int,
    model_id: str | None = None,
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> SiteForecastResponse:
    # Use latest model if not specified
    if model_id is None:
        row = con.execute(
            "SELECT model_id FROM model_registry ORDER BY trained_at DESC LIMIT 1"
        ).fetchone()
        model_id = row[0] if row else None

    if model_id is None:
        return SiteForecastResponse(site_id=site_id, model_id=None, forecasts=[])

    rows = con.execute("""
        SELECT forecast_date, horizon_days, predicted_volume,
               prediction_lower, prediction_upper, actual_volume
        FROM forecasts
        WHERE site_id = ? AND model_id = ?
        ORDER BY horizon_days
    """, [site_id, model_id]).fetchall()

    forecasts = [
        ForecastPoint(
            forecast_date=r[0], horizon_days=r[1], predicted_volume=r[2],
            prediction_lower=r[3], prediction_upper=r[4], actual_volume=r[5],
        )
        for r in rows
    ]

    return SiteForecastResponse(
        site_id=site_id, model_id=model_id, forecasts=forecasts,
    )
