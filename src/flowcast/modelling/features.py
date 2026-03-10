"""SQL-based feature engineering for traffic forecasting."""

from __future__ import annotations

import duckdb
import pandas as pd

from flowcast.modelling.holidays_au import add_holiday_features
from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def build_daily_features(
    con: duckdb.DuckDBPyConnection,
    site_ids: list[int],
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Extract ML-ready feature matrix from traffic_daily.

    Uses DuckDB window functions for lag and rolling features, then adds
    holiday flags in Python via the holidays_au module.

    Returns a DataFrame with calendar, lag, rolling, and holiday features.
    Rows without enough history for lag-28 are dropped.
    """
    if not site_ids:
        return pd.DataFrame()

    site_list = ", ".join(str(s) for s in site_ids)

    date_filter = ""
    if start_date:
        date_filter += f" AND date >= '{start_date}'"
    if end_date:
        date_filter += f" AND date <= '{end_date}'"

    # We query a wider window than needed so that lag features are available
    # for the first rows in the requested range. The outer WHERE trims later.
    sql = f"""
        WITH base AS (
            SELECT
                site_id,
                date,
                total_volume,
                detector_count,
                peak_hour,
                peak_hour_volume,
                region,
                -- Calendar features
                EXTRACT(ISODOW FROM date)::INT AS day_of_week,
                EXTRACT(MONTH FROM date)::INT AS month,
                EXTRACT(DAY FROM date)::INT AS day_of_month,
                EXTRACT(WEEK FROM date)::INT AS week_of_year,
                EXTRACT(QUARTER FROM date)::INT AS quarter,
                CASE WHEN EXTRACT(ISODOW FROM date) IN (6, 7) THEN 1 ELSE 0 END AS is_weekend,
                -- Lag features
                LAG(total_volume, 1) OVER w AS volume_lag_1,
                LAG(total_volume, 7) OVER w AS volume_lag_7,
                LAG(total_volume, 14) OVER w AS volume_lag_14,
                LAG(total_volume, 28) OVER w AS volume_lag_28,
                -- Rolling features (exclude current day)
                AVG(total_volume) OVER (
                    PARTITION BY site_id ORDER BY date
                    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                ) AS volume_rolling_mean_7,
                AVG(total_volume) OVER (
                    PARTITION BY site_id ORDER BY date
                    ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
                ) AS volume_rolling_mean_28,
                STDDEV(total_volume) OVER (
                    PARTITION BY site_id ORDER BY date
                    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                ) AS volume_rolling_std_7
            FROM traffic_daily
            WHERE site_id IN ({site_list})
            WINDOW w AS (PARTITION BY site_id ORDER BY date)
        ),
        with_site_avg AS (
            SELECT
                b.*,
                AVG(b.total_volume) OVER (PARTITION BY b.site_id) AS site_avg_volume,
                sc.cluster_id
            FROM base b
            LEFT JOIN site_clusters sc ON b.site_id = sc.site_id
        )
        SELECT * FROM with_site_avg
        WHERE volume_lag_28 IS NOT NULL {date_filter}
        ORDER BY site_id, date
    """

    df = con.execute(sql).fetchdf()

    if df.empty:
        log.warning("no_features_returned", site_count=len(site_ids))
        return df

    # Add holiday features in Python (uses the holidays library)
    df = add_holiday_features(df)

    log.info("features_built", rows=len(df), sites=df["site_id"].nunique(),
             date_range=f"{df['date'].min()} to {df['date'].max()}")
    return df
