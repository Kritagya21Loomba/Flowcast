"""Tests for ZIP extraction."""

from datetime import date

import pytest

from flowcast.ingestion.extract import (
    extract_inner_zip,
    extract_monthly_zip,
    temp_extract_dir,
)
from flowcast.utils.logging import setup_logging

setup_logging()


def test_extract_monthly_zip(sample_monthly_zip, tmp_dir):
    """Extracting a monthly ZIP produces the correct files."""
    dest = tmp_dir / "extract"
    dest.mkdir()
    files = extract_monthly_zip(sample_monthly_zip, dest)
    assert len(files) == 2
    assert all(f.path.exists() for f in files)
    dates = sorted(f.csv_date for f in files)
    assert dates == [date(2025, 1, 15), date(2025, 1, 16)]


def test_extract_inner_zip(sample_yearly_zip, tmp_dir):
    """Extracting an inner ZIP from a yearly archive works."""
    dest = tmp_dir / "extract"
    dest.mkdir()
    files = extract_inner_zip(sample_yearly_zip, "VSDATA_202501.zip", dest)
    assert len(files) == 1
    assert files[0].csv_date == date(2025, 1, 10)
    assert files[0].path.exists()


def test_temp_extract_dir_cleanup():
    """temp_extract_dir cleans up after context exits."""
    with temp_extract_dir() as tmp:
        assert tmp.exists()
        marker = tmp / "test.txt"
        marker.write_text("hello")
    assert not tmp.exists()
