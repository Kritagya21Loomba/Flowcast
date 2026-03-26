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
        WITH corr_pairs AS (
            SELECT site_a AS site_id, site_b AS neighbor_id, pearson_daily
            FROM site_correlations
            UNION ALL
            SELECT site_b AS site_id, site_a AS neighbor_id, pearson_daily
            FROM site_correlations
        ),
        top_neighbors AS (
            SELECT
                site_id,
                neighbor_id,
                ROW_NUMBER() OVER (
                    PARTITION BY site_id
                    ORDER BY pearson_daily DESC NULLS LAST, neighbor_id
                ) AS rn
            FROM corr_pairs
        ),
        top_neighbor_map AS (
            SELECT site_id, neighbor_id
            FROM top_neighbors
            WHERE rn = 1
        ),
        base AS (
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
        cluster_daily AS (
            SELECT
                sc.cluster_id,
                td.date,
                AVG(td.total_volume) AS cluster_mean_volume
            FROM traffic_daily td
            LEFT JOIN site_clusters sc ON td.site_id = sc.site_id
            GROUP BY sc.cluster_id, td.date
        ),
        cluster_daily_lagged AS (
            SELECT
                cluster_id,
                date,
                cluster_mean_volume,
                LAG(cluster_mean_volume, 1) OVER (PARTITION BY cluster_id ORDER BY date) AS cluster_lag_1,
                LAG(cluster_mean_volume, 7) OVER (PARTITION BY cluster_id ORDER BY date) AS cluster_lag_7,
                LAG(cluster_mean_volume, 14) OVER (PARTITION BY cluster_id ORDER BY date) AS cluster_lag_14
            FROM cluster_daily
        ),
        with_site_avg AS (
            SELECT
                b.*,
                AVG(b.total_volume) OVER (PARTITION BY b.site_id) AS site_avg_volume,
                sc.cluster_id,
                COALESCE(cdl.cluster_mean_volume, b.total_volume) AS cluster_mean_volume,
                COALESCE(cdl.cluster_lag_1, b.volume_lag_1) AS cluster_lag_1,
                COALESCE(cdl.cluster_lag_7, b.volume_lag_7) AS cluster_lag_7,
                COALESCE(cdl.cluster_lag_14, b.volume_lag_14) AS cluster_lag_14
            FROM base b
            LEFT JOIN site_clusters sc ON b.site_id = sc.site_id
            LEFT JOIN cluster_daily_lagged cdl
                ON sc.cluster_id = cdl.cluster_id
               AND b.date = cdl.date
        ),
        with_neighbor AS (
            SELECT
                wsa.*,
                tn.neighbor_id AS top_neighbor_site_id,
                tdn.total_volume AS top_neighbor_lag_1,
                cluster_day.cluster_mean_volume AS cluster_corridor_avg
            FROM with_site_avg wsa
            LEFT JOIN top_neighbor_map tn ON wsa.site_id = tn.site_id
            LEFT JOIN traffic_daily tdn
                ON tdn.site_id = tn.neighbor_id
               AND tdn.date = wsa.date - INTERVAL 1 DAY
            LEFT JOIN cluster_daily cluster_day
                ON wsa.cluster_id = cluster_day.cluster_id
               AND cluster_day.date = wsa.date - INTERVAL 1 DAY
        ),
        with_external AS (
            SELECT
                wn.*,
                COALESCE(wd.rainfall_mm, 0) AS rainfall_mm,
                COALESCE(wd.temperature_c, 0) AS temperature_c,
                COALESCE(wd.wind_kmh, 0) AS wind_kmh,
                COALESCE(wd.severe_weather_flag, 0) AS severe_weather_flag,
                COALESCE(ev.afl_games_count, 0) AS afl_games_count,
                COALESCE(ev.concerts_count, 0) AS concerts_count,
                COALESCE(ev.cbd_events_count, 0) AS cbd_events_count,
                COALESCE(ev.roadworks_flag, 0) AS roadworks_flag,
                COALESCE(ev.school_zone_flag, 0) AS school_zone_flag,
                COALESCE(gf.degree, 0) AS graph_degree,
                COALESCE(gf.centrality, 0) AS graph_centrality,
                COALESCE(gf.clustering_coeff, 0) AS graph_clustering_coeff
            FROM with_neighbor wn
            LEFT JOIN site_weather_daily wd
                ON wn.site_id = wd.site_id AND wn.date = wd.date
            LEFT JOIN site_events_daily ev
                ON wn.site_id = ev.site_id AND wn.date = ev.date
            LEFT JOIN site_graph_features gf
                ON wn.site_id = gf.site_id
        )
        SELECT * FROM with_external
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
