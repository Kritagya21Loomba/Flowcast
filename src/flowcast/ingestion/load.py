"""DuckDB bulk loading and ingestion manifest management."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from flowcast.ingestion.extract import ExtractedFile
from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def is_already_ingested(con: duckdb.DuckDBPyConnection, csv_filename: str) -> bool:
    """Check if a CSV has already been loaded."""
    result = con.execute(
        "SELECT 1 FROM ingestion_manifest WHERE csv_filename = ?",
        [csv_filename],
    ).fetchone()
    return result is not None


def record_ingestion(
    con: duckdb.DuckDBPyConnection,
    source_zip: str,
    inner_zip: str | None,
    csv_filename: str,
    csv_date: date,
    row_count: int,
) -> None:
    """Record a loaded CSV in the ingestion manifest."""
    con.execute(
        """
        INSERT INTO ingestion_manifest (source_zip, inner_zip, csv_filename, csv_date, row_count)
        VALUES (?, ?, ?, ?, ?)
        """,
        [source_zip, inner_zip, csv_filename, csv_date, row_count],
    )


def load_csv_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    file: ExtractedFile,
    source_zip: str,
    inner_zip: str | None = None,
) -> int:
    """Load a single CSV file into the traffic_volumes table.

    Returns the number of rows inserted, or 0 if already ingested.
    """
    if is_already_ingested(con, file.filename):
        log.debug("skip_already_ingested", csv=file.filename)
        return 0

    csv_path_str = str(file.path).replace("\\", "/")

    con.begin()
    try:
        result = con.execute(
            f"""
            INSERT INTO traffic_volumes
            SELECT
                '{file.filename}' AS source_file,
                *
            FROM read_csv('{csv_path_str}',
                header=true,
                delim=',',
                auto_detect=true)
            """
        )
        row_count = result.fetchone()[0] if result.description else 0

        record_ingestion(con, source_zip, inner_zip, file.filename, file.csv_date, row_count)
        con.commit()
    except Exception:
        con.rollback()
        raise

    log.info("loaded_csv", csv=file.filename, rows=row_count)
    return row_count


def load_csvs_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    files: list[ExtractedFile],
    source_zip: str,
    inner_zip: str | None = None,
) -> int:
    """Load multiple CSV files into DuckDB.

    Returns total rows inserted.
    """
    total = 0
    for i, file in enumerate(files, 1):
        rows = load_csv_to_duckdb(con, file, source_zip, inner_zip)
        total += rows
        if rows > 0:
            log.info(
                "load_progress",
                file=f"{i}/{len(files)}",
                csv=file.filename,
                rows=rows,
                total_so_far=total,
            )
    return total


def get_ingestion_summary(con: duckdb.DuckDBPyConnection) -> dict:
    """Get a summary of what has been ingested."""
    total_csvs = con.execute("SELECT COUNT(*) FROM ingestion_manifest").fetchone()[0]
    total_rows = con.execute("SELECT COALESCE(SUM(row_count), 0) FROM ingestion_manifest").fetchone()[0]
    date_range = con.execute(
        "SELECT MIN(csv_date), MAX(csv_date) FROM ingestion_manifest"
    ).fetchone()
    return {
        "total_csvs": total_csvs,
        "total_rows": total_rows,
        "min_date": date_range[0],
        "max_date": date_range[1],
    }
