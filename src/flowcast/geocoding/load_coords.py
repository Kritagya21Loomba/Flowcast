"""Parse and load SCATS site coordinates into signal_sites."""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

from flowcast.utils.logging import get_logger

log = get_logger(__name__)

SIGNALS_CSV_URL = (
    "https://opendata.transport.vic.gov.au/dataset/"
    "923af458-363d-469f-bc5e-84746a80b9a2/resource/"
    "d094415e-7b73-414a-88f5-6a3a6b5a903d/download/"
    "victorian_traffic_signals.csv"
)


def parse_signals_csv(csv_path: Path) -> list[dict]:
    """Parse the Victorian Traffic Signals CSV into a list of dicts.

    Returns list of dicts with keys: site_id, latitude, longitude, intersection_name.
    Handles BOM-encoded CSVs (encoding='utf-8-sig').
    """
    results = []
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            site_no = row.get("SITE_NO", "").strip()
            lat = row.get("LATITUDE", "").strip()
            lon = row.get("LONGITUDE", "").strip()
            name = row.get("SITE_NAME", "").strip()

            if not site_no or not lat or not lon:
                continue

            try:
                results.append({
                    "site_id": int(site_no),
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "intersection_name": name or None,
                })
            except (ValueError, TypeError):
                log.warning("skipping_invalid_row", site_no=site_no)
                continue

    log.info("csv_parsed", rows=len(results))
    return results


def update_site_coordinates(
    con: duckdb.DuckDBPyConnection,
    locations: list[dict],
) -> int:
    """Update latitude, longitude, intersection_name in signal_sites.

    Only updates sites that already exist in signal_sites.
    Returns count of sites updated.
    """
    updated = 0
    for loc in locations:
        result = con.execute("""
            UPDATE signal_sites
            SET latitude = ?,
                longitude = ?,
                intersection_name = COALESCE(?, intersection_name)
            WHERE site_id = ?
              AND (latitude IS NULL OR longitude IS NULL)
        """, [loc["latitude"], loc["longitude"],
              loc.get("intersection_name"), loc["site_id"]])
        affected = result.fetchone()
        if affected is not None:
            updated += 1

    log.info("coordinates_updated", updated=updated, total_locations=len(locations))
    return updated


def download_signals_csv(dest_path: Path) -> Path:
    """Download the Victorian Traffic Signals CSV from data.vic.gov.au."""
    from urllib.request import urlretrieve
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("downloading_signals_csv", url=SIGNALS_CSV_URL, dest=str(dest_path))
    urlretrieve(SIGNALS_CSV_URL, dest_path)
    log.info("download_complete", path=str(dest_path))
    return dest_path
