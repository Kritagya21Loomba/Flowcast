"""FastAPI dependency injection for DuckDB connections."""

from __future__ import annotations

from typing import Generator

import duckdb

from flowcast.db.connection import get_readonly_connection


def get_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Yield a read-only DuckDB connection for the request lifecycle."""
    con = get_readonly_connection()
    try:
        yield con
    finally:
        con.close()
