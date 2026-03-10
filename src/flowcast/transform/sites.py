"""Populate the signal_sites dimension table from traffic_volumes."""

import duckdb

from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def populate_signal_sites(con: duckdb.DuckDBPyConnection) -> None:
    """Extract distinct sites from traffic_volumes into signal_sites."""
    con.execute("DELETE FROM signal_sites")
    con.execute("""
        INSERT INTO signal_sites (site_id, region, detector_count, first_seen, last_seen)
        SELECT
            nb_scats_site,
            MODE(nm_region),
            COUNT(DISTINCT nb_detector)::SMALLINT,
            MIN(qt_interval_count),
            MAX(qt_interval_count)
        FROM traffic_volumes
        GROUP BY nb_scats_site
    """)
    count = con.execute("SELECT COUNT(*) FROM signal_sites").fetchone()[0]
    log.info("signal_sites_populated", count=count)
