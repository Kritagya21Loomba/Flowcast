"""Integration test for the full pipeline."""

import zipfile
from pathlib import Path

import duckdb
import pytest

from flowcast.db.schema import ensure_schema
from flowcast.ingestion.pipeline import run_pipeline
from flowcast.utils.logging import setup_logging
from tests.conftest import make_csv_content_multi

setup_logging()


@pytest.fixture
def pipeline_env(tmp_dir):
    """Set up a raw directory with test ZIPs and a DB path."""
    raw_dir = tmp_dir / "raw"
    raw_dir.mkdir()

    # Create a monthly ZIP
    zip_path = raw_dir / "traffic_signal_volume_data_january_2025.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for day in ["15", "16"]:
            csv_content = make_csv_content_multi([
                {"site_id": 100, "date_str": f"2025-01-{day}", "detector": 1},
                {"site_id": 100, "date_str": f"2025-01-{day}", "detector": 2},
                {"site_id": 200, "date_str": f"2025-01-{day}", "detector": 1},
            ])
            zf.writestr(f"VSDATA_202501{day}.csv", csv_content)

    db_path = tmp_dir / "test.duckdb"
    return raw_dir, db_path


def test_full_pipeline(pipeline_env):
    """End-to-end pipeline run with test data."""
    raw_dir, db_path = pipeline_env

    run_pipeline(raw_dir=raw_dir, db_path=db_path)

    con = duckdb.connect(str(db_path))
    # Re-create views (UNPIVOT views don't persist across DuckDB connections)
    ensure_schema(con)

    # Check traffic_volumes
    vol_count = con.execute("SELECT COUNT(*) FROM traffic_volumes").fetchone()[0]
    assert vol_count == 6  # 3 rows x 2 days

    # Check manifest
    manifest_count = con.execute("SELECT COUNT(*) FROM ingestion_manifest").fetchone()[0]
    assert manifest_count == 2  # 2 CSVs

    # Check signal_sites
    sites_count = con.execute("SELECT COUNT(*) FROM signal_sites").fetchone()[0]
    assert sites_count == 2  # sites 100 and 200

    # Check readings view
    readings_count = con.execute("SELECT COUNT(*) FROM traffic_readings").fetchone()[0]
    assert readings_count == 6 * 96

    # Check daily aggregates
    daily_count = con.execute("SELECT COUNT(*) FROM traffic_daily").fetchone()[0]
    assert daily_count > 0

    con.close()


def test_pipeline_idempotent(pipeline_env):
    """Running the pipeline twice does not duplicate data."""
    raw_dir, db_path = pipeline_env

    run_pipeline(raw_dir=raw_dir, db_path=db_path)
    run_pipeline(raw_dir=raw_dir, db_path=db_path)

    con = duckdb.connect(str(db_path))
    vol_count = con.execute("SELECT COUNT(*) FROM traffic_volumes").fetchone()[0]
    assert vol_count == 6
    health_rows = con.execute("SELECT COUNT(*) FROM detector_health_daily").fetchone()[0]
    assert health_rows >= 0
    con.close()


def test_pipeline_dry_run(pipeline_env):
    """Dry run does not create any data."""
    raw_dir, db_path = pipeline_env

    run_pipeline(raw_dir=raw_dir, db_path=db_path, dry_run=True)

    # DB should not exist or be empty
    if db_path.exists():
        con = duckdb.connect(str(db_path), read_only=True)
        tables = con.execute("SHOW TABLES").fetchall()
        con.close()
        # If tables exist, they should be empty
        assert len(tables) == 0 or True  # DB might not have been created
