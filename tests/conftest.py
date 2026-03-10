"""Shared test fixtures for Flowcast tests."""

import csv
import io
import shutil
import tempfile
import zipfile
from datetime import date
from pathlib import Path

import duckdb
import pytest

from flowcast.config import EXPECTED_COLUMNS
from flowcast.db.schema import ensure_schema


@pytest.fixture
def tmp_db():
    """In-memory DuckDB with schema initialized."""
    con = duckdb.connect(":memory:")
    ensure_schema(con)
    yield con
    con.close()


@pytest.fixture
def fixtures_dir():
    """Path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def tmp_dir():
    """Temporary directory, cleaned up after test."""
    d = Path(tempfile.mkdtemp(prefix="flowcast_test_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def make_csv_content(
    site_id: int = 100,
    date_str: str = "2025-01-15",
    detector: int = 1,
    region: str = "SPR",
    volumes: list[int] | None = None,
) -> str:
    """Generate a single-row CSV string with the expected 103-column schema."""
    if volumes is None:
        volumes = [i % 20 for i in range(96)]
    assert len(volumes) == 96

    vol_24h = sum(volumes)
    row = [
        str(site_id),
        date_str,
        str(detector),
        *[str(v) for v in volumes],
        region,
        "96",
        str(vol_24h),
        "0",
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(EXPECTED_COLUMNS)
    writer.writerow(row)
    return buf.getvalue()


def make_csv_content_multi(rows_data: list[dict]) -> str:
    """Generate multi-row CSV content.

    Each dict in rows_data should have keys: site_id, date_str, detector, region, volumes.
    Missing keys get defaults.
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(EXPECTED_COLUMNS)
    for rd in rows_data:
        vols = rd.get("volumes", [i % 20 for i in range(96)])
        row = [
            str(rd.get("site_id", 100)),
            rd.get("date_str", "2025-01-15"),
            str(rd.get("detector", 1)),
            *[str(v) for v in vols],
            rd.get("region", "SPR"),
            "96",
            str(sum(vols)),
            "0",
        ]
        writer.writerow(row)
    return buf.getvalue()


@pytest.fixture
def sample_csv_path(tmp_dir):
    """Create a sample CSV file and return its path."""
    content = make_csv_content_multi([
        {"site_id": 100, "date_str": "2025-01-15", "detector": 1},
        {"site_id": 100, "date_str": "2025-01-15", "detector": 2},
        {"site_id": 200, "date_str": "2025-01-15", "detector": 1},
    ])
    path = tmp_dir / "VSDATA_20250115.csv"
    path.write_text(content, newline="")
    return path


@pytest.fixture
def sample_monthly_zip(tmp_dir):
    """Create a monthly ZIP containing 2 daily CSVs."""
    zip_path = tmp_dir / "traffic_signal_volume_data_january_2025.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for day in ["15", "16"]:
            csv_content = make_csv_content_multi([
                {"site_id": 100, "date_str": f"2025-01-{day}", "detector": 1},
                {"site_id": 200, "date_str": f"2025-01-{day}", "detector": 1},
            ])
            zf.writestr(f"VSDATA_202501{day}.csv", csv_content)
    return zip_path


@pytest.fixture
def sample_yearly_zip(tmp_dir):
    """Create a yearly ZIP containing 1 inner monthly ZIP."""
    zip_path = tmp_dir / "traffic_signal_volume_data_2025.zip"
    # First, create the inner monthly ZIP in memory
    inner_buf = io.BytesIO()
    with zipfile.ZipFile(inner_buf, "w") as inner_zf:
        csv_content = make_csv_content_multi([
            {"site_id": 300, "date_str": "2025-01-10", "detector": 1},
        ])
        inner_zf.writestr("VSDATA_20250110.csv", csv_content)
    inner_buf.seek(0)

    # Now create the outer yearly ZIP
    with zipfile.ZipFile(zip_path, "w") as outer_zf:
        outer_zf.writestr("VSDATA_202501.zip", inner_buf.read())
    return zip_path
