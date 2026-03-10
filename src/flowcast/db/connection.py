"""DuckDB connection factory."""

import tempfile
from contextlib import contextmanager
from pathlib import Path

import duckdb

from flowcast.config import DB_PATH, DATA_DIR, DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS


def get_connection(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Create and configure a DuckDB connection.

    Args:
        db_path: Path to the database file. Defaults to config.DB_PATH.
                 Use ':memory:' string cast to Path for in-memory databases.
    """
    path = db_path or DB_PATH

    if str(path) == ":memory:":
        con = duckdb.connect(":memory:")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(path))

    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    # Set temp directory explicitly to avoid Windows path issues
    tmp = tempfile.gettempdir().replace("\\", "/")
    con.execute(f"SET temp_directory = '{tmp}'")
    return con


def get_readonly_connection(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Create a read-only DuckDB connection for API serving.

    Opens the database in read-only mode for safe concurrent access.
    Note: Does NOT call ensure_views() because CREATE VIEW is not allowed
    in read-only mode. API endpoints use tables directly, not the UNPIVOT view.
    """
    path = db_path or DB_PATH
    con = duckdb.connect(str(path), read_only=True)
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    tmp = tempfile.gettempdir().replace("\\", "/")
    con.execute(f"SET temp_directory = '{tmp}'")
    return con


@contextmanager
def transaction(con: duckdb.DuckDBPyConnection):
    """Context manager for a DuckDB transaction."""
    con.begin()
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
