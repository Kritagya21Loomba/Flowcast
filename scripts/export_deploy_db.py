"""Export a lightweight DuckDB database for deployment.

Creates deploy/flowcast.duckdb containing only the tables needed by the API,
with traffic_daily limited to the 500 modelled sites and last 6 months.
This produces a database small enough to commit to git (~30-80 MB).

Uses DuckDB ATTACH to copy tables directly between databases without pyarrow.
"""

import argparse
from pathlib import Path

import duckdb

from flowcast.config import DB_PATH, PROJECT_ROOT
from flowcast.utils.logging import setup_logging, get_logger

DEPLOY_DIR = PROJECT_ROOT / "deploy"

# Tables to export in full (small tables)
FULL_TABLES = [
    "signal_sites",
    "site_clusters",
    "forecasts",
    "model_registry",
    "model_metrics_site",
    "site_correlations",
]


def export_deploy_db(
    source_path: Path = DB_PATH,
    dest_path: Path | None = None,
) -> Path:
    """Export a lightweight database for deployment."""
    setup_logging()
    log = get_logger("export_deploy_db")

    dest_path = dest_path or DEPLOY_DIR / "flowcast.duckdb"
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing deploy DB
    if dest_path.exists():
        dest_path.unlink()
    wal = dest_path.with_suffix(".duckdb.wal")
    if wal.exists():
        wal.unlink()

    log.info("creating deploy database", dest=str(dest_path))

    # Open the deploy DB as main, attach source as read-only
    dst = duckdb.connect(str(dest_path))
    src_path_str = str(source_path).replace("\\", "/")
    dst.execute(f"ATTACH '{src_path_str}' AS src (READ_ONLY)")

    # Export small tables in full
    for table in FULL_TABLES:
        count = dst.execute(f"SELECT COUNT(*) FROM src.{table}").fetchone()[0]
        log.info("exporting table", table=table, rows=count)
        dst.execute(f"CREATE TABLE {table} AS SELECT * FROM src.{table}")

    # Export traffic_daily — ALL sites, last 6 months
    max_date = dst.execute("SELECT MAX(date) FROM src.traffic_daily").fetchone()[0]
    if max_date:
        cutoff = f"DATE '{max_date}' - INTERVAL 180 DAY"
    else:
        cutoff = "DATE '2025-01-01'"

    td_count = dst.execute(f"""
        SELECT COUNT(*) FROM src.traffic_daily
        WHERE date >= ({cutoff})
    """).fetchone()[0]
    log.info("exporting table", table="traffic_daily", rows=td_count)

    dst.execute(f"""
        CREATE TABLE traffic_daily AS
        SELECT * FROM src.traffic_daily
        WHERE date >= ({cutoff})
    """)

    # Detach source, checkpoint, and close
    dst.execute("DETACH src")
    dst.execute("CHECKPOINT")
    dst.close()

    size_mb = dest_path.stat().st_size / (1024 * 1024)
    log.info("deploy database created", path=str(dest_path), size_mb=round(size_mb, 1))
    return dest_path


def main():
    parser = argparse.ArgumentParser(description="Export lightweight deploy DB")
    parser.add_argument("--source", type=Path, default=DB_PATH)
    parser.add_argument("--dest", type=Path, default=DEPLOY_DIR / "flowcast.duckdb")
    args = parser.parse_args()
    export_deploy_db(args.source, args.dest)


if __name__ == "__main__":
    main()
