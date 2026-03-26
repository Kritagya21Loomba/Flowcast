"""Forecast generation and actuals backfill."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from sklearn.preprocessing import OrdinalEncoder

from flowcast.modelling.features import build_daily_features
from flowcast.modelling.train import CATEGORICAL_FEATURES, _prepare_features
from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def _predict_quantiles(component_models: dict[str, Any], X: np.ndarray, point_pred: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    q10 = component_models.get("quantile_p10")
    q90 = component_models.get("quantile_p90")
    lower = q10["model"].predict(X) if q10 else point_pred * 0.85
    upper = q90["model"].predict(X) if q90 else point_pred * 1.15
    return np.maximum(0, lower), np.maximum(lower, upper)


def generate_forecasts(
    con: duckdb.DuckDBPyConnection,
    model: Any,
    encoder: OrdinalEncoder,
    model_id: str,
    site_ids: list[int],
    feature_columns: list[str],
    horizons: list[int] | None = None,
    as_of_date: str | None = None,
    model_bundle: dict[str, Any] | None = None,
) -> int:
    """Generate forecasts for each site at each horizon using recursive+direct ensemble."""
    if horizons is None:
        horizons = [1, 7, 14, 28]
    max_h = max(horizons)

    if as_of_date is None:
        row = con.execute("SELECT MAX(date) FROM traffic_daily").fetchone()
        as_of_date = str(row[0])
    as_of = pd.Timestamp(as_of_date)

    df = build_daily_features(con, site_ids, end_date=as_of_date)
    if df.empty:
        return 0
    df["date"] = pd.to_datetime(df["date"])

    component_models = (model_bundle or {}).get("component_models", {}) if model_bundle else {}
    direct_component_models = {
        k: v for k, v in component_models.items() if str(k).startswith("direct_h")
    }
    forecast_rows = []
    forecast_id = 0

    for site_id in site_ids:
        site_df = df[df["site_id"] == site_id].sort_values("date").copy()
        if site_df.empty:
            continue
        recent_volumes = site_df["total_volume"].tail(56).tolist()
        template = site_df.iloc[-1].to_dict()

        for day in range(1, max_h + 1):
            forecast_date = as_of + timedelta(days=day)
            row = dict(template)
            row["site_id"] = site_id
            row["date"] = forecast_date
            row["day_of_week"] = forecast_date.isoweekday()
            row["month"] = forecast_date.month
            row["quarter"] = (forecast_date.month - 1) // 3 + 1
            row["is_weekend"] = 1 if forecast_date.isoweekday() >= 6 else 0
            row["volume_lag_1"] = recent_volumes[-1] if recent_volumes else 0
            row["volume_lag_7"] = recent_volumes[-7] if len(recent_volumes) >= 7 else row["volume_lag_1"]
            row["volume_lag_14"] = recent_volumes[-14] if len(recent_volumes) >= 14 else row["volume_lag_7"]
            row["volume_lag_28"] = recent_volumes[-28] if len(recent_volumes) >= 28 else row["volume_lag_14"]
            row["volume_rolling_mean_7"] = float(np.mean(recent_volumes[-7:])) if recent_volumes else 0.0
            row["volume_rolling_mean_28"] = float(np.mean(recent_volumes[-28:])) if recent_volumes else 0.0
            row["volume_rolling_std_7"] = float(np.std(recent_volumes[-7:], ddof=1)) if len(recent_volumes) >= 2 else 0.0
            row["cluster_lag_1"] = row["volume_lag_1"]
            row["cluster_lag_7"] = row["volume_lag_7"]
            row["cluster_lag_14"] = row["volume_lag_14"]
            row["cluster_corridor_avg"] = row["volume_lag_1"]
            row["top_neighbor_lag_1"] = row["volume_lag_1"]

            pred_df = pd.DataFrame([row])
            X, _ = _prepare_features(pred_df, CATEGORICAL_FEATURES, encoder=encoder)
            recursive_pred = np.maximum(0, model.predict(X))

            direct_key = f"direct_h{day}"
            if direct_key in direct_component_models:
                dm = direct_component_models[direct_key]
                Xd, _ = _prepare_features(pred_df, CATEGORICAL_FEATURES, encoder=dm["encoder"])
                direct_pred = np.maximum(0, dm["model"].predict(Xd))
                point_pred = 0.65 * recursive_pred + 0.35 * direct_pred
            else:
                point_pred = recursive_pred

            lower, upper = _predict_quantiles(component_models, X, point_pred)
            pred_val = float(point_pred[0])

            if day in horizons:
                forecast_id += 1
                forecast_rows.append(
                    (
                        forecast_id,
                        model_id,
                        site_id,
                        forecast_date.strftime("%Y-%m-%d"),
                        day,
                        round(pred_val, 1),
                        round(float(lower[0]), 1),
                        round(float(upper[0]), 1),
                        None,
                    )
                )

            recent_volumes.append(pred_val)
            if len(recent_volumes) > 56:
                recent_volumes.pop(0)

    if forecast_rows:
        con.executemany(
            "INSERT INTO forecasts VALUES (?, ?, ?, ?::DATE, ?, ?, ?, ?, ?, current_timestamp)",
            forecast_rows,
        )
    return len(forecast_rows)


def backfill_actuals(con: duckdb.DuckDBPyConnection) -> int:
    """Update forecast rows with actual_volume where actuals now exist."""
    con.execute(
        """
        UPDATE forecasts f
        SET actual_volume = td.total_volume
        FROM traffic_daily td
        WHERE f.site_id = td.site_id
          AND f.forecast_date = td.date
          AND f.actual_volume IS NULL
        """
    )
    count = con.execute("SELECT COUNT(*) FROM forecasts WHERE actual_volume IS NOT NULL").fetchone()[0]
    return int(count)
