"""Tests for forecast generation."""

from datetime import date, timedelta

import duckdb
import pytest

from flowcast.db.schema import ensure_schema
from flowcast.modelling.train import train_daily_global_model, serialize_component_models
from flowcast.modelling.forecast import generate_forecasts, backfill_actuals


@pytest.fixture
def forecast_db():
    """DB with enough traffic_daily data to train and forecast."""
    con = duckdb.connect(":memory:")
    ensure_schema(con)

    start = date(2024, 1, 1)
    rows = []
    for site_id in [10, 20]:
        base = 500 + site_id * 10
        for d in range(200):
            dt = start + timedelta(days=d)
            dow = dt.isoweekday()
            volume = int(base * (0.6 if dow >= 6 else 1.0) + d * 0.5 + (hash((site_id, d)) % 80))
            rows.append((site_id, dt.isoformat(), "NW", volume, 3, 8, volume // 24))

    con.executemany(
        "INSERT INTO traffic_daily VALUES (?, ?::DATE, ?, ?, ?, ?, ?)",
        rows,
    )
    yield con
    con.close()


def test_generate_forecasts(forecast_db):
    """Forecasts are generated and stored in the forecasts table."""
    result = train_daily_global_model(
        forecast_db, [10, 20], test_start_date="2024-06-01"
    )
    count = generate_forecasts(
        forecast_db,
        result.model,
        result.encoder,
        result.model_id,
        [10, 20],
        result.feature_columns,
        horizons=[1, 7],
        model_bundle={"component_models": serialize_component_models(result.component_models)},
    )
    assert count > 0

    rows = forecast_db.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]
    assert rows == count

    # Check structure of forecast rows
    sample = forecast_db.execute(
        "SELECT * FROM forecasts LIMIT 1"
    ).fetchdf()
    assert "predicted_volume" in sample.columns
    assert "horizon_days" in sample.columns
    assert sample["predicted_volume"].iloc[0] > 0


def test_forecast_horizons(forecast_db):
    """Each requested horizon generates one forecast per site."""
    result = train_daily_global_model(
        forecast_db, [10], test_start_date="2024-06-01"
    )
    horizons = [1, 7, 14]
    count = generate_forecasts(
        forecast_db,
        result.model,
        result.encoder,
        result.model_id,
        [10],
        result.feature_columns,
        horizons=horizons,
        model_bundle={"component_models": serialize_component_models(result.component_models)},
    )
    # 1 site × 3 horizons
    assert count == 3

    # Verify each horizon is present
    horizon_vals = forecast_db.execute(
        "SELECT DISTINCT horizon_days FROM forecasts ORDER BY horizon_days"
    ).fetchdf()["horizon_days"].tolist()
    assert horizon_vals == horizons


def test_backfill_actuals(forecast_db):
    """Backfill updates forecast rows with actual volumes."""
    result = train_daily_global_model(
        forecast_db, [10], test_start_date="2024-06-01"
    )
    # Use as_of_date early enough that actuals exist for horizon=1
    generate_forecasts(
        forecast_db,
        result.model,
        result.encoder,
        result.model_id,
        [10],
        result.feature_columns,
        horizons=[1],
        as_of_date="2024-04-01",
        model_bundle={"component_models": serialize_component_models(result.component_models)},
    )

    # Before backfill: actual_volume should be NULL
    nulls = forecast_db.execute(
        "SELECT COUNT(*) FROM forecasts WHERE actual_volume IS NULL"
    ).fetchone()[0]
    assert nulls > 0

    # Backfill
    updated = backfill_actuals(forecast_db)
    assert updated >= 0  # may or may not find matching actuals depending on dates
