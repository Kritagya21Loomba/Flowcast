"""Site selection based on data quality criteria."""

from __future__ import annotations

import duckdb

from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def select_modelling_sites(
    con: duckdb.DuckDBPyConnection,
    min_history_days: int = 365,
    max_zero_pct: float = 0.10,
    max_sites: int = 500,
) -> list[int]:
    """Select sites suitable for modelling based on data quality.

    Criteria:
        1. At least min_history_days of data
        2. Average daily volume above the 25th percentile of all qualifying sites
        3. Fewer than max_zero_pct of days with zero total volume
        4. Ranked by data_days * avg_volume, capped at max_sites

    Returns list of site_ids sorted by quality score descending.
    """
    # First pass: get all sites meeting history and zero-pct thresholds
    candidates = con.execute(f"""
        WITH site_stats AS (
            SELECT
                site_id,
                COUNT(*) AS data_days,
                AVG(total_volume) AS avg_volume,
                SUM(CASE WHEN total_volume = 0 THEN 1 ELSE 0 END)::FLOAT
                    / COUNT(*) AS zero_pct
            FROM traffic_daily
            GROUP BY site_id
            HAVING data_days >= {min_history_days}
                AND zero_pct < {max_zero_pct}
        )
        SELECT site_id, data_days, avg_volume, zero_pct
        FROM site_stats
        ORDER BY data_days * avg_volume DESC
    """).fetchdf()

    if candidates.empty:
        log.warning("no_qualifying_sites",
                    min_history_days=min_history_days, max_zero_pct=max_zero_pct)
        return []

    # Second pass: filter by 25th percentile of avg_volume
    p25 = candidates["avg_volume"].quantile(0.25)
    filtered = candidates[candidates["avg_volume"] >= p25]

    # Cap at max_sites
    selected = filtered.head(max_sites)
    site_ids = selected["site_id"].tolist()

    log.info("sites_selected",
             total_candidates=len(candidates),
             after_volume_filter=len(filtered),
             selected=len(site_ids),
             avg_volume_p25=round(p25, 1),
             min_days=int(selected["data_days"].min()),
             max_days=int(selected["data_days"].max()))
    return site_ids
