"""Schema discovery — scan ZIP files and validate CSV structure."""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from flowcast.config import EXPECTED_COLUMNS
from flowcast.utils.logging import get_logger
from flowcast.utils.temporal import zip_to_year_month

log = get_logger(__name__)


@dataclass
class SourceDescriptor:
    """Metadata about a single ZIP source file."""

    zip_path: Path
    zip_type: Literal["yearly", "monthly"]
    year_month: str  # e.g. "2024" for yearly, "2025-01" for monthly
    inner_zips: list[str] = field(default_factory=list)
    csv_count: int = 0
    sample_header: list[str] = field(default_factory=list)
    header_valid: bool = False


def _read_first_csv_header_from_zip(zf: zipfile.ZipFile) -> list[str]:
    """Read the header row of the first CSV in a ZipFile."""
    csv_names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
    if not csv_names:
        return []
    with zf.open(csv_names[0]) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        header = next(reader, [])
    return [col.strip() for col in header]


def discover_sources(raw_dir: Path) -> list[SourceDescriptor]:
    """Scan raw/ and classify each ZIP, validating CSV headers.

    Returns a list of SourceDescriptor sorted chronologically.
    """
    zip_paths = sorted(raw_dir.glob("*.zip"))
    sources: list[SourceDescriptor] = []

    for zp in zip_paths:
        log.info("discovering", zip_file=zp.name)
        try:
            year_month = zip_to_year_month(zp.name)
        except ValueError:
            log.warning("skip_unrecognized_zip", zip_file=zp.name)
            continue

        with zipfile.ZipFile(zp, "r") as zf:
            entries = zf.namelist()
            inner_zips = sorted(n for n in entries if n.lower().endswith(".zip"))
            csv_entries = sorted(n for n in entries if n.lower().endswith(".csv"))

            if inner_zips:
                # Yearly ZIP — contains monthly ZIPs
                # Sample header from the first CSV inside the first inner ZIP
                header: list[str] = []
                total_csvs = 0
                inner_data = zf.read(inner_zips[0])
                with zipfile.ZipFile(io.BytesIO(inner_data)) as inner_zf:
                    header = _read_first_csv_header_from_zip(inner_zf)
                    total_csvs = sum(
                        1 for n in inner_zf.namelist() if n.lower().endswith(".csv")
                    )
                # Estimate total CSVs across all inner ZIPs
                total_csvs *= len(inner_zips)

                src = SourceDescriptor(
                    zip_path=zp,
                    zip_type="yearly",
                    year_month=year_month,
                    inner_zips=inner_zips,
                    csv_count=total_csvs,
                    sample_header=header,
                    header_valid=header == EXPECTED_COLUMNS,
                )
            elif csv_entries:
                # Monthly ZIP — contains CSVs directly
                header = _read_first_csv_header_from_zip(zf)
                src = SourceDescriptor(
                    zip_path=zp,
                    zip_type="monthly",
                    year_month=year_month,
                    csv_count=len(csv_entries),
                    sample_header=header,
                    header_valid=header == EXPECTED_COLUMNS,
                )
            else:
                log.warning("empty_zip", zip_file=zp.name)
                continue

            if not src.header_valid:
                log.warning(
                    "schema_mismatch",
                    zip_file=zp.name,
                    expected_cols=len(EXPECTED_COLUMNS),
                    actual_cols=len(src.sample_header),
                    diff=set(EXPECTED_COLUMNS) ^ set(src.sample_header),
                )

            log.info(
                "discovered",
                zip_file=zp.name,
                type=src.zip_type,
                csvs=src.csv_count,
                valid=src.header_valid,
            )
            sources.append(src)

    sources.sort(key=lambda s: s.year_month)
    return sources
