"""UNPIVOT view creation for normalized traffic readings."""

import duckdb

from flowcast.db.schema import VIEWS
from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def create_readings_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create or replace the traffic_readings UNPIVOT view."""
    con.execute(VIEWS["traffic_readings"])
    log.info("view_created", name="traffic_readings")
