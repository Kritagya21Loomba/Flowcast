"""Data quality and imputation utilities for ingestion."""

from __future__ import annotations

import duckdb


def impute_missing_days(con: duckdb.DuckDBPyConnection) -> int:
    """Impute missing site-days using cluster and site rolling means."""
    con.execute(
        """
        INSERT INTO traffic_daily (site_id, date, region, total_volume, detector_count, peak_hour, peak_hour_volume)
        WITH bounds AS (
            SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM traffic_daily
        ),
        calendar AS (
            SELECT * FROM generate_series((SELECT min_date FROM bounds), (SELECT max_date FROM bounds), INTERVAL 1 DAY) AS t(date)
        ),
        grid AS (
            SELECT s.site_id, c.date
            FROM signal_sites s
            CROSS JOIN calendar c
        ),
        missing AS (
            SELECT g.site_id, g.date
            FROM grid g
            LEFT JOIN traffic_daily td ON td.site_id = g.site_id AND td.date = g.date
            WHERE td.site_id IS NULL
        ),
        cluster_avg AS (
            SELECT sc.cluster_id, td.date, AVG(td.total_volume) AS cluster_vol
            FROM traffic_daily td
            LEFT JOIN site_clusters sc ON td.site_id = sc.site_id
            GROUP BY sc.cluster_id, td.date
        ),
        site_avg AS (
            SELECT site_id, AVG(total_volume) AS site_vol, MODE(region) AS region
            FROM traffic_daily
            GROUP BY site_id
        )
        SELECT
            m.site_id,
            m.date,
            COALESCE(sa.region, 'UNK') AS region,
            CAST(COALESCE(ca.cluster_vol, sa.site_vol, 0) AS INTEGER) AS total_volume,
            0::SMALLINT AS detector_count,
            0::TINYINT AS peak_hour,
            CAST(COALESCE(ca.cluster_vol, sa.site_vol, 0) / 24 AS INTEGER) AS peak_hour_volume
        FROM missing m
        LEFT JOIN site_clusters sc ON m.site_id = sc.site_id
        LEFT JOIN cluster_avg ca ON sc.cluster_id = ca.cluster_id AND m.date = ca.date
        LEFT JOIN site_avg sa ON m.site_id = sa.site_id
        """
    )
    return int(con.execute("SELECT COUNT(*) FROM traffic_daily WHERE detector_count = 0").fetchone()[0])


def detect_detector_health(con: duckdb.DuckDBPyConnection) -> int:
    """Detect potentially faulty or stuck detectors and persist daily flags."""
    con.execute("DELETE FROM detector_health_daily")
    con.execute(
        """
        INSERT INTO detector_health_daily (site_id, date, detector_count, zero_interval_pct, stuck_score, health_flag)
        WITH flat AS (
            SELECT
                nb_scats_site AS site_id,
                qt_interval_count AS date,
                nb_detector AS detector_id,
                CAST(volume AS DOUBLE) AS volume
            FROM traffic_volumes
            UNPIVOT (volume FOR interval_code IN (
                V00, V01, V02, V03, V04, V05, V06, V07, V08, V09, V10, V11,
                V12, V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, V23,
                V24, V25, V26, V27, V28, V29, V30, V31, V32, V33, V34, V35,
                V36, V37, V38, V39, V40, V41, V42, V43, V44, V45, V46, V47,
                V48, V49, V50, V51, V52, V53, V54, V55, V56, V57, V58, V59,
                V60, V61, V62, V63, V64, V65, V66, V67, V68, V69, V70, V71,
                V72, V73, V74, V75, V76, V77, V78, V79, V80, V81, V82, V83,
                V84, V85, V86, V87, V88, V89, V90, V91, V92, V93, V94, V95
            ))
        ),
        detector_stats AS (
            SELECT
                site_id,
                date,
                detector_id,
                AVG(CASE WHEN volume = 0 THEN 1.0 ELSE 0.0 END) AS zero_interval_pct_detector,
                STDDEV_POP(volume) AS detector_std
            FROM flat
            GROUP BY site_id, date, detector_id
        ),
        stats AS (
            SELECT
                site_id,
                date,
                COUNT(*)::SMALLINT AS detector_count,
                AVG(zero_interval_pct_detector) AS zero_interval_pct,
                AVG(CASE WHEN COALESCE(detector_std, 0) < 0.5 THEN 1.0 ELSE 0.0 END) AS stuck_score
            FROM detector_stats
            GROUP BY site_id, date
        )
        SELECT
            site_id,
            date,
            detector_count,
            zero_interval_pct,
            stuck_score,
            CASE
                WHEN zero_interval_pct > 0.9 THEN 'mostly_zero'
                WHEN stuck_score > 0.85 THEN 'stuck_pattern'
                ELSE 'ok'
            END AS health_flag
        FROM stats
        """
    )
    return int(con.execute("SELECT COUNT(*) FROM detector_health_daily WHERE health_flag <> 'ok'").fetchone()[0])
