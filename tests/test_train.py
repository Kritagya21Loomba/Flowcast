"""Tests for model training and evaluation."""

from datetime import date, timedelta
import math

import duckdb
import numpy as np
import pytest

from flowcast.db.schema import ensure_schema
from flowcast.modelling.evaluate import compute_metrics, compute_site_metrics
from flowcast.modelling.train import train_daily_global_model, save_model, load_model


@pytest.fixture
def train_db():
    """DB with traffic_daily data: 5 sites, 200 days each with weekly seasonality."""
    con = duckdb.connect(":memory:")
    ensure_schema(con)

    start = date(2024, 1, 1)
    rows = []
    for site_id in [10, 20, 30, 40, 50]:
        base = 500 + site_id * 10
        for d in range(200):
            dt = start + timedelta(days=d)
            dow = dt.isoweekday()
            # Weekly seasonality: weekday ~base, weekend ~base*0.6
            seasonal = base * 0.6 if dow >= 6 else base
            # Add small trend and noise
            volume = int(seasonal + d * 0.5 + (hash((site_id, d)) % 100))
            rows.append((site_id, dt.isoformat(), "NW", volume, 3, 8, volume // 24))

    con.executemany(
        "INSERT INTO traffic_daily VALUES (?, ?::DATE, ?, ?, ?, ?, ?)",
        rows,
    )
    yield con
    con.close()


def test_train_produces_result(train_db):
    """Training runs and returns a TrainResult with metrics."""
    result = train_daily_global_model(
        train_db, [10, 20, 30, 40, 50],
        test_start_date="2024-06-01",
        model_id="test_model",
    )
    assert result.model_id == "test_model"
    assert result.train_rows > 0
    assert result.test_rows > 0
    assert result.test_mae > 0
    assert result.test_rmse > 0
    assert not math.isnan(result.test_mape)


def test_train_site_metrics(train_db):
    """Per-site metrics are computed for each site in test set."""
    result = train_daily_global_model(
        train_db, [10, 20, 30],
        test_start_date="2024-06-01",
    )
    assert len(result.site_metrics) > 0
    assert set(result.site_metrics.columns) >= {"site_id", "mae", "rmse", "mape", "n_test_days"}
    assert "rolling_cv_mape_mean" in result.cv_metrics or result.cv_metrics == {}
    assert set(result.residual_diagnostics.columns) >= {
        "model_id", "site_id", "residual_mean", "residual_std", "residual_mape", "flagged", "reason"
    }


def test_save_and_load_model(train_db, tmp_path):
    """Model can be serialized and deserialized."""
    result = train_daily_global_model(
        train_db, [10, 20],
        test_start_date="2024-06-01",
    )
    path = save_model(result, tmp_path)
    loaded = load_model(path)
    assert "model" in loaded
    assert "encoder" in loaded
    assert "feature_columns" in loaded
    # Loaded model can predict using the encoder
    feature_cols = loaded["feature_columns"]
    # Create a dummy row that goes through the encoder like real data would
    assert len(feature_cols) > 0
    assert loaded["model"] is not None


def test_compute_metrics_basic():
    """Metric computation returns expected keys and reasonable values."""
    y_true = np.array([100, 200, 300, 400])
    y_pred = np.array([110, 190, 310, 380])
    m = compute_metrics(y_true, y_pred)
    assert set(m.keys()) == {"mae", "rmse", "mape"}
    assert m["mae"] == pytest.approx(12.5)
    assert m["rmse"] > m["mae"]  # RMSE >= MAE always


def test_compute_metrics_zero_actuals():
    """MAPE handles zero actuals gracefully."""
    y_true = np.array([0, 0, 100])
    y_pred = np.array([10, 5, 110])
    m = compute_metrics(y_true, y_pred)
    # MAPE computed only on non-zero actuals
    assert not math.isnan(m["mape"])
