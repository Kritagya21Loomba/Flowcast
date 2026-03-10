"""Forecast generation and actuals backfill."""

from __future__ import annotations

from datetime import datetime, timedelta

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import OrdinalEncoder

from flowcast.modelling.features import build_daily_features
from flowcast.modelling.holidays_au import get_victorian_public_holidays, _get_school_holidays
from flowcast.modelling.train import ALL_FEATURES, CATEGORICAL_FEATURES, TARGET, _prepare_features
from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def generate_forecasts(
    con: duckdb.DuckDBPyConnection,
    model: HistGradientBoostingRegressor,
    encoder: OrdinalEncoder,
    model_id: str,
    site_ids: list[int],
    feature_columns: list[str],
    horizons: list[int] | None = None,
    as_of_date: str | None = None,
) -> int:
    """Generate forecasts for each site at each horizon.

    For horizon=1, uses actual lag values from traffic_daily.
    For horizon>1, uses iterative (recursive) forecasting where
    previous predictions serve as lag inputs.

    Returns the number of forecast rows inserted.
    """
    if horizons is None:
        horizons = [1, 7, 14, 28]

    max_horizon = max(horizons)

    # Get the latest date in the data
    if as_of_date is None:
        row = con.execute("SELECT MAX(date) FROM traffic_daily").fetchone()
        as_of_date = str(row[0])

    as_of = pd.Timestamp(as_of_date)
    log.info("generating_forecasts", model_id=model_id, as_of=as_of_date,
             horizons=horizons, sites=len(site_ids))

    # Pre-compute holiday sets for the forecast window
    forecast_end_year = (as_of + timedelta(days=max_horizon)).year
    pub_holidays = get_victorian_public_holidays(as_of.year, forecast_end_year)
    school_holidays = _get_school_holidays()

    # Fetch feature data up to as_of_date for lag computation
    df = build_daily_features(con, site_ids, end_date=as_of_date)
    if df.empty:
        log.warning("no_feature_data_for_forecasts")
        return 0

    df["date"] = pd.to_datetime(df["date"])

    forecast_rows = []
    forecast_id = 0

    for site_id in site_ids:
        site_df = df[df["site_id"] == site_id].sort_values("date").copy()
        if site_df.empty:
            continue

        last_row = site_df.iloc[-1].copy()

        # Build rolling history for iterative prediction
        recent_volumes = site_df["total_volume"].tail(28).tolist()

        for day in range(1, max_horizon + 1):
            forecast_date = as_of + timedelta(days=day)

            # Build feature row for this day
            row_dict = {}
            row_dict["site_id"] = site_id
            row_dict["date"] = forecast_date
            row_dict["region"] = last_row["region"]
            row_dict["detector_count"] = last_row["detector_count"]
            row_dict["peak_hour"] = last_row["peak_hour"]
            row_dict["peak_hour_volume"] = last_row.get("peak_hour_volume", 0)

            # Calendar features
            row_dict["day_of_week"] = forecast_date.isoweekday()
            row_dict["month"] = forecast_date.month
            row_dict["day_of_month"] = forecast_date.day
            row_dict["week_of_year"] = forecast_date.isocalendar()[1]
            row_dict["quarter"] = (forecast_date.month - 1) // 3 + 1
            row_dict["is_weekend"] = 1 if forecast_date.isoweekday() >= 6 else 0

            # Lag features from recent_volumes
            n = len(recent_volumes)
            row_dict["volume_lag_1"] = recent_volumes[-1] if n >= 1 else 0
            row_dict["volume_lag_7"] = recent_volumes[-7] if n >= 7 else 0
            row_dict["volume_lag_14"] = recent_volumes[-14] if n >= 14 else 0
            row_dict["volume_lag_28"] = recent_volumes[-28] if n >= 28 else 0

            # Rolling features
            if n >= 7:
                last_7 = recent_volumes[-7:]
                row_dict["volume_rolling_mean_7"] = np.mean(last_7)
                row_dict["volume_rolling_std_7"] = np.std(last_7, ddof=1) if len(last_7) > 1 else 0
            else:
                row_dict["volume_rolling_mean_7"] = np.mean(recent_volumes) if recent_volumes else 0
                row_dict["volume_rolling_std_7"] = 0

            if n >= 28:
                row_dict["volume_rolling_mean_28"] = np.mean(recent_volumes[-28:])
            else:
                row_dict["volume_rolling_mean_28"] = np.mean(recent_volumes) if recent_volumes else 0

            row_dict["site_avg_volume"] = last_row.get("site_avg_volume", 0)
            row_dict["cluster_id"] = last_row.get("cluster_id", -1)

            # Holiday features — use pre-computed sets
            fd = forecast_date.date() if hasattr(forecast_date, 'date') else forecast_date
            row_dict["is_public_holiday"] = 1 if fd in pub_holidays else 0
            row_dict["is_school_holiday"] = 1 if fd in school_holidays else 0

            # Build DataFrame for prediction
            pred_df = pd.DataFrame([row_dict])

            # Prepare features using the same encoder
            X, _ = _prepare_features(pred_df, CATEGORICAL_FEATURES, encoder=encoder)
            prediction = float(model.predict(X)[0])
            prediction = max(0, prediction)  # volume can't be negative

            # Confidence interval: simple heuristic based on rolling std
            std = row_dict.get("volume_rolling_std_7", 0)
            if std and not np.isnan(std):
                lower = max(0, prediction - 1.96 * std)
                upper = prediction + 1.96 * std
            else:
                lower = prediction * 0.8
                upper = prediction * 1.2

            # Store forecast for requested horizons
            if day in horizons:
                forecast_id += 1
                forecast_rows.append((
                    forecast_id, model_id, site_id,
                    forecast_date.strftime("%Y-%m-%d"), day,
                    round(prediction, 1), round(lower, 1), round(upper, 1),
                    None,  # actual_volume (filled later by backfill)
                ))

            # Update rolling history for next iteration
            recent_volumes.append(prediction)
            if len(recent_volumes) > 28:
                recent_volumes.pop(0)

    # Bulk insert
    if forecast_rows:
        con.executemany(
            "INSERT INTO forecasts VALUES (?, ?, ?, ?::DATE, ?, ?, ?, ?, ?, current_timestamp)",
            forecast_rows,
        )

    log.info("forecasts_generated", count=len(forecast_rows))
    return len(forecast_rows)


def backfill_actuals(con: duckdb.DuckDBPyConnection) -> int:
    """Update forecast rows with actual_volume where actuals now exist."""
    result = con.execute("""
        UPDATE forecasts f
        SET actual_volume = td.total_volume
        FROM traffic_daily td
        WHERE f.site_id = td.site_id
          AND f.forecast_date = td.date
          AND f.actual_volume IS NULL
    """)
    count = result.fetchone()[0] if result.description else 0
    log.info("actuals_backfilled", updated=count)
    return count
