"""CLI entry point for running the ingestion pipeline."""

import argparse
import sys
from pathlib import Path

from flowcast.config import RAW_DIR, DB_PATH
from flowcast.ingestion.pipeline import run_pipeline
from flowcast.utils.logging import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Flowcast: Ingest Victorian traffic signal volume data into DuckDB."
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=RAW_DIR,
        help=f"Directory containing ZIP files (default: {RAW_DIR})",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DB_PATH,
        help=f"Path to DuckDB database file (default: {DB_PATH})",
    )
    parser.add_argument(
        "--months",
        nargs="*",
        help="Only process specific year-months (e.g., 2024-06 2024-07)",
    )
    parser.add_argument(
        "--skip-transforms",
        action="store_true",
        help="Skip post-load transformation steps (views, aggregates)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show execution plan without loading data",
    )
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Output logs in JSON format",
    )

    args = parser.parse_args()
    setup_logging(json_output=args.json_logs)

    run_pipeline(
        raw_dir=args.raw_dir,
        db_path=args.db_path,
        months=args.months,
        dry_run=args.dry_run,
        skip_transforms=args.skip_transforms,
    )


if __name__ == "__main__":
    main()
