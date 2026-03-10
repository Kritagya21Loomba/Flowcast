"""Materialized aggregate tables for dashboard performance."""

import duckdb

from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def _hour_sum_expr(hour: int) -> str:
    """Generate SQL for summing 4 V-columns for a given hour."""
    start = hour * 4
    cols = [f"COALESCE(V{start + i:02d},0)" for i in range(4)]
    return "+".join(cols)


def build_daily_aggregates(con: duckdb.DuckDBPyConnection) -> None:
    """Build the traffic_daily table from traffic_volumes.

    Uses a single-pass approach: compute 24 hourly sums as columns,
    then aggregate across detectors per site/day without row expansion.
    """
    con.execute("DELETE FROM traffic_daily")

    # Build 24 hour-sum columns inline
    hour_sums = [f"SUM({_hour_sum_expr(h)}) AS h{h:02d}" for h in range(24)]
    hour_cols = ", ".join(hour_sums)

    # Build GREATEST/ARG for peak hour detection
    h_refs = [f"h{h:02d}" for h in range(24)]
    greatest_expr = f"GREATEST({', '.join(h_refs)})"

    # Use CASE to find which hour has the peak volume
    peak_hour_cases = " ".join(
        f"WHEN {h_refs[h]} = peak_vol THEN {h}" for h in range(24)
    )

    con.execute(f"""
        INSERT INTO traffic_daily (site_id, date, region, total_volume, detector_count, peak_hour, peak_hour_volume)
        WITH site_day AS (
            SELECT
                nb_scats_site AS site_id,
                qt_interval_count AS date,
                MODE(nm_region) AS region,
                COUNT(DISTINCT nb_detector)::SMALLINT AS detector_count,
                {hour_cols}
            FROM traffic_volumes
            GROUP BY nb_scats_site, qt_interval_count
        ),
        with_peak AS (
            SELECT
                *,
                ({' + '.join(h_refs)}) AS total_volume,
                {greatest_expr} AS peak_vol
            FROM site_day
        )
        SELECT
            site_id,
            date,
            region,
            total_volume,
            detector_count,
            (CASE {peak_hour_cases} ELSE 0 END)::TINYINT AS peak_hour,
            peak_vol AS peak_hour_volume
        FROM with_peak
    """)
    count = con.execute("SELECT COUNT(*) FROM traffic_daily").fetchone()[0]
    log.info("daily_aggregates_built", rows=count)
