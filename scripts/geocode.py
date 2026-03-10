"""CLI entry point for loading SCATS site coordinates."""

import argparse
from pathlib import Path

from flowcast.config import DB_PATH, DATA_DIR
from flowcast.db.connection import get_connection
from flowcast.db.schema import ensure_schema
from flowcast.geocoding.load_coords import (
    download_signals_csv,
    parse_signals_csv,
    update_site_coordinates,
)
from flowcast.utils.logging import setup_logging, get_logger

log = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Flowcast: Load SCATS site coordinates."
    )
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument(
        "--csv-path", type=Path, default=DATA_DIR / "victorian_traffic_signals.csv",
        help="Path to signals CSV (downloaded if not present)",
    )
    parser.add_argument("--json-logs", action="store_true")
    args = parser.parse_args()

    setup_logging(json_output=args.json_logs)
    log.info("geocoding_start", db_path=str(args.db_path))

    # Download CSV if not present
    if not args.csv_path.exists():
        download_signals_csv(args.csv_path)

    # Parse CSV
    locations = parse_signals_csv(args.csv_path)

    # Load into DB
    con = get_connection(args.db_path)
    ensure_schema(con)

    updated = update_site_coordinates(con, locations)

    # Report results
    total = con.execute("SELECT COUNT(*) FROM signal_sites").fetchone()[0]
    with_coords = con.execute(
        "SELECT COUNT(*) FROM signal_sites WHERE latitude IS NOT NULL"
    ).fetchone()[0]

    log.info("geocoding_complete",
             sites_updated=updated,
             total_sites=total,
             sites_with_coords=with_coords,
             coverage=f"{100 * with_coords / total:.1f}%")

    con.close()


if __name__ == "__main__":
    main()
