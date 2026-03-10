"""Tests for the UNPIVOT traffic_readings view and transformations."""

from datetime import date, time

import pytest

from flowcast.ingestion.extract import ExtractedFile
from flowcast.ingestion.load import load_csv_to_duckdb
from flowcast.transform.normalize import create_readings_view
from flowcast.transform.sites import populate_signal_sites
from flowcast.utils.logging import setup_logging
from flowcast.utils.temporal import interval_to_time

setup_logging()


def test_interval_to_time():
    """V-column indices map to correct times."""
    assert interval_to_time(0) == time(0, 0)
    assert interval_to_time(1) == time(0, 15)
    assert interval_to_time(4) == time(1, 0)
    assert interval_to_time(47) == time(11, 45)
    assert interval_to_time(48) == time(12, 0)
    assert interval_to_time(95) == time(23, 45)


def test_interval_to_time_invalid():
    with pytest.raises(ValueError):
        interval_to_time(96)
    with pytest.raises(ValueError):
        interval_to_time(-1)


def test_readings_view_row_count(tmp_db, sample_csv_path):
    """UNPIVOT view produces input_rows * 96 rows."""
    file = ExtractedFile(
        path=sample_csv_path,
        filename="VSDATA_20250115.csv",
        csv_date=date(2025, 1, 15),
    )
    load_csv_to_duckdb(tmp_db, file, "test.zip")
    create_readings_view(tmp_db)

    count = tmp_db.execute("SELECT COUNT(*) FROM traffic_readings").fetchone()[0]
    assert count == 3 * 96  # 3 rows × 96 intervals


def test_readings_view_timestamp(tmp_db, sample_csv_path):
    """Reading timestamps are computed correctly."""
    file = ExtractedFile(
        path=sample_csv_path,
        filename="VSDATA_20250115.csv",
        csv_date=date(2025, 1, 15),
    )
    load_csv_to_duckdb(tmp_db, file, "test.zip")
    create_readings_view(tmp_db)

    # V00 should be midnight, V95 should be 23:45
    row = tmp_db.execute("""
        SELECT timestamp FROM traffic_readings
        WHERE site_id = 100 AND detector = 1 AND interval_num = 0
    """).fetchone()
    assert row[0].hour == 0 and row[0].minute == 0

    row = tmp_db.execute("""
        SELECT timestamp FROM traffic_readings
        WHERE site_id = 100 AND detector = 1 AND interval_num = 95
    """).fetchone()
    assert row[0].hour == 23 and row[0].minute == 45


def test_populate_signal_sites(tmp_db, sample_csv_path):
    """signal_sites table is populated with distinct sites."""
    file = ExtractedFile(
        path=sample_csv_path,
        filename="VSDATA_20250115.csv",
        csv_date=date(2025, 1, 15),
    )
    load_csv_to_duckdb(tmp_db, file, "test.zip")
    populate_signal_sites(tmp_db)

    count = tmp_db.execute("SELECT COUNT(*) FROM signal_sites").fetchone()[0]
    assert count == 2  # sites 100 and 200

    site100 = tmp_db.execute(
        "SELECT detector_count FROM signal_sites WHERE site_id = 100"
    ).fetchone()
    assert site100[0] == 2  # detectors 1 and 2
