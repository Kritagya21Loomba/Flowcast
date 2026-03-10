"""ZIP extraction — handle nested yearly and flat monthly ZIPs."""

from __future__ import annotations

import io
import re
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from flowcast.config import TEMP_DIR_PREFIX
from flowcast.utils.logging import get_logger
from flowcast.utils.temporal import csv_filename_to_date

log = get_logger(__name__)


@dataclass
class ExtractedFile:
    """Metadata about an extracted CSV file."""

    path: Path
    filename: str
    csv_date: date


@contextmanager
def temp_extract_dir():
    """Context manager that creates and cleans up a temp directory."""
    tmp = Path(tempfile.mkdtemp(prefix=TEMP_DIR_PREFIX))
    log.debug("temp_dir_created", path=str(tmp))
    try:
        yield tmp
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        log.debug("temp_dir_cleaned", path=str(tmp))


def _list_csv_entries(zf: zipfile.ZipFile) -> list[str]:
    """List CSV entries in a ZipFile, sorted."""
    return sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))


def _parse_csv_date(filename: str) -> date | None:
    """Try to parse date from a CSV filename. Returns None if unparseable."""
    basename = Path(filename).name
    try:
        return csv_filename_to_date(basename)
    except ValueError:
        log.warning("unparseable_csv_date", filename=filename)
        return None


def extract_monthly_zip(zip_path: Path, dest_dir: Path) -> list[ExtractedFile]:
    """Extract all CSVs from a monthly ZIP directly."""
    results = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = _list_csv_entries(zf)
        for name in csv_names:
            zf.extract(name, dest_dir)
            extracted_path = dest_dir / name
            csv_date = _parse_csv_date(name)
            if csv_date is None:
                continue
            results.append(ExtractedFile(
                path=extracted_path,
                filename=Path(name).name,
                csv_date=csv_date,
            ))
    log.info("extracted_monthly", zip_file=zip_path.name, csvs=len(results))
    return results


def extract_inner_zip(
    outer_zip_path: Path,
    inner_zip_name: str,
    dest_dir: Path,
) -> list[ExtractedFile]:
    """Extract CSVs from an inner ZIP within a yearly ZIP.

    Reads the inner ZIP into memory (BytesIO) to avoid nested file handle issues.
    """
    results = []
    with zipfile.ZipFile(outer_zip_path, "r") as outer_zf:
        inner_bytes = outer_zf.read(inner_zip_name)

    with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zf:
        csv_names = _list_csv_entries(inner_zf)
        for name in csv_names:
            inner_zf.extract(name, dest_dir)
            extracted_path = dest_dir / name
            csv_date = _parse_csv_date(name)
            if csv_date is None:
                continue
            results.append(ExtractedFile(
                path=extracted_path,
                filename=Path(name).name,
                csv_date=csv_date,
            ))
    log.info(
        "extracted_inner",
        outer_zip=outer_zip_path.name,
        inner_zip=inner_zip_name,
        csvs=len(results),
    )
    return results
