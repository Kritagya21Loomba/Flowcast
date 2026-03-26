"""Pipeline orchestrator — ties discovery, extraction, and loading together."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from flowcast.config import RAW_DIR, DB_PATH
from flowcast.db.connection import get_connection
from flowcast.db.schema import ensure_schema
from flowcast.ingestion.discover import SourceDescriptor, discover_sources
from flowcast.ingestion.extract import (
    extract_inner_zip,
    extract_monthly_zip,
    temp_extract_dir,
)
from flowcast.ingestion.load import get_ingestion_summary, load_csvs_to_duckdb
from flowcast.transform.normalize import create_readings_view
from flowcast.transform.sites import populate_signal_sites
from flowcast.transform.aggregates import build_daily_aggregates
from flowcast.ingestion.quality import impute_missing_days, detect_detector_health
from flowcast.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class Batch:
    """A single month's worth of data to ingest."""

    source: SourceDescriptor
    inner_zip: str | None  # None for monthly ZIPs
    label: str  # Human-readable label, e.g. "2024-06"


def build_execution_plan(sources: list[SourceDescriptor]) -> list[Batch]:
    """Flatten sources into chronological monthly batches."""
    batches = []
    for src in sources:
        if src.zip_type == "yearly":
            for inner in src.inner_zips:
                batches.append(Batch(source=src, inner_zip=inner, label=f"{src.year_month}/{inner}"))
        else:
            batches.append(Batch(source=src, inner_zip=None, label=src.year_month))
    return batches


def run_pipeline(
    raw_dir: Path = RAW_DIR,
    db_path: Path = DB_PATH,
    months: list[str] | None = None,
    dry_run: bool = False,
    skip_transforms: bool = False,
) -> None:
    """Run the full ingestion pipeline.

    Args:
        raw_dir: Directory containing ZIP files.
        db_path: Path to DuckDB database file.
        months: Optional filter — only process these year-months (e.g. ["2024-06"]).
        dry_run: If True, show execution plan without loading data.
        skip_transforms: If True, skip post-load transformation steps.
    """
    log.info("pipeline_start", raw_dir=str(raw_dir), db_path=str(db_path))

    # Step 1: Discover
    sources = discover_sources(raw_dir)
    if not sources:
        log.error("no_sources_found", raw_dir=str(raw_dir))
        return

    invalid = [s for s in sources if not s.header_valid]
    if invalid:
        log.warning("invalid_schemas", count=len(invalid), files=[s.zip_path.name for s in invalid])

    valid_sources = [s for s in sources if s.header_valid]
    log.info("discovery_complete", total=len(sources), valid=len(valid_sources))

    # Step 2: Build execution plan
    batches = build_execution_plan(valid_sources)

    if months:
        month_set = set(months)
        batches = [b for b in batches if any(m in b.label for m in month_set)]
        log.info("filtered_batches", months=months, batch_count=len(batches))

    if dry_run:
        log.info("dry_run_plan", batches=len(batches))
        for i, batch in enumerate(batches, 1):
            log.info("planned_batch", num=i, label=batch.label, zip=batch.source.zip_path.name)
        return

    # Step 3: Initialize database
    con = get_connection(db_path)
    ensure_schema(con)

    # Step 4: Process each batch
    total_rows = 0
    for i, batch in enumerate(batches, 1):
        log.info("batch_start", num=f"{i}/{len(batches)}", label=batch.label)
        with temp_extract_dir() as tmp:
            if batch.inner_zip:
                files = extract_inner_zip(batch.source.zip_path, batch.inner_zip, tmp)
            else:
                files = extract_monthly_zip(batch.source.zip_path, tmp)

            rows = load_csvs_to_duckdb(
                con, files, batch.source.zip_path.name, batch.inner_zip
            )
            total_rows += rows
            log.info("batch_complete", label=batch.label, rows=rows, total=total_rows)

    # Step 5: Run transforms
    if not skip_transforms:
        log.info("transforms_start")
        create_readings_view(con)
        populate_signal_sites(con)
        build_daily_aggregates(con)
        imputed = impute_missing_days(con)
        issues = detect_detector_health(con)
        log.info("quality_checks_complete", imputed_days=imputed, detector_issues=issues)
        log.info("transforms_complete")

    # Step 6: Checkpoint and summarize
    con.execute("CHECKPOINT")
    summary = get_ingestion_summary(con)
    log.info("pipeline_complete", **summary)
    con.close()
