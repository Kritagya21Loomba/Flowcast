"""Tests for schema discovery."""

import csv
import io
import zipfile

import pytest

from flowcast.config import EXPECTED_COLUMNS
from flowcast.ingestion.discover import discover_sources
from flowcast.utils.logging import setup_logging

setup_logging()


def test_discover_monthly_zip(sample_monthly_zip, tmp_dir):
    """Discover a monthly ZIP and validate its classification."""
    sources = discover_sources(tmp_dir)
    assert len(sources) == 1
    src = sources[0]
    assert src.zip_type == "monthly"
    assert src.header_valid is True
    assert src.csv_count == 2
    assert src.year_month == "2025-01"


def test_discover_yearly_zip(sample_yearly_zip, tmp_dir):
    """Discover a yearly ZIP with nested inner ZIPs."""
    sources = discover_sources(tmp_dir)
    assert len(sources) == 1
    src = sources[0]
    assert src.zip_type == "yearly"
    assert src.header_valid is True
    assert len(src.inner_zips) == 1


def test_discover_invalid_header(tmp_dir):
    """A ZIP with wrong CSV headers should be flagged as invalid."""
    zip_path = tmp_dir / "traffic_signal_volume_data_february_2025.zip"
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["WRONG_COL1", "WRONG_COL2"])
    writer.writerow(["a", "b"])

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("VSDATA_20250201.csv", buf.getvalue())

    sources = discover_sources(tmp_dir)
    assert len(sources) == 1
    assert sources[0].header_valid is False


def test_discover_empty_dir(tmp_dir):
    """An empty directory should return no sources."""
    sources = discover_sources(tmp_dir)
    assert sources == []


def test_discover_compact_month_zip(tmp_dir):
    """A compact YYYYMM zip name should be parsed as monthly."""
    zip_path = tmp_dir / "traffic_signal_volume_data_202203.zip"
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(EXPECTED_COLUMNS)
    writer.writerow(["100", "2022-03-01", "1", *["0"] * 96, "SPR", "96", "0", "0"])
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("VSDATA_20220301.csv", buf.getvalue())
    sources = discover_sources(tmp_dir)
    assert len(sources) == 1
    assert sources[0].year_month == "2022-03"
