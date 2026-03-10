"""Tests for DuckDB loading."""

from datetime import date

import pytest

from flowcast.ingestion.extract import ExtractedFile
from flowcast.ingestion.load import (
    get_ingestion_summary,
    is_already_ingested,
    load_csv_to_duckdb,
    load_csvs_to_duckdb,
)
from flowcast.utils.logging import setup_logging

setup_logging()


def test_load_single_csv(tmp_db, sample_csv_path):
    """Loading a CSV inserts rows and records in manifest."""
    file = ExtractedFile(
        path=sample_csv_path,
        filename="VSDATA_20250115.csv",
        csv_date=date(2025, 1, 15),
    )
    rows = load_csv_to_duckdb(tmp_db, file, "test.zip")
    assert rows == 3

    count = tmp_db.execute("SELECT COUNT(*) FROM traffic_volumes").fetchone()[0]
    assert count == 3

    assert is_already_ingested(tmp_db, "VSDATA_20250115.csv")


def test_idempotent_load(tmp_db, sample_csv_path):
    """Loading the same CSV twice does not duplicate rows."""
    file = ExtractedFile(
        path=sample_csv_path,
        filename="VSDATA_20250115.csv",
        csv_date=date(2025, 1, 15),
    )
    rows1 = load_csv_to_duckdb(tmp_db, file, "test.zip")
    rows2 = load_csv_to_duckdb(tmp_db, file, "test.zip")
    assert rows1 == 3
    assert rows2 == 0  # Skipped

    count = tmp_db.execute("SELECT COUNT(*) FROM traffic_volumes").fetchone()[0]
    assert count == 3


def test_ingestion_summary(tmp_db, sample_csv_path):
    """Summary returns correct counts after loading."""
    file = ExtractedFile(
        path=sample_csv_path,
        filename="VSDATA_20250115.csv",
        csv_date=date(2025, 1, 15),
    )
    load_csv_to_duckdb(tmp_db, file, "test.zip")
    summary = get_ingestion_summary(tmp_db)
    assert summary["total_csvs"] == 1
    assert summary["total_rows"] == 3
    assert summary["min_date"] == date(2025, 1, 15)
