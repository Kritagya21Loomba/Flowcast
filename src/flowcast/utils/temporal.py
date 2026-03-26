"""Temporal utility functions for interval/timestamp conversions and filename parsing."""

import re
from datetime import date, time


def interval_to_time(n: int) -> time:
    """Convert a V-column index (0–95) to the start time of that 15-minute interval.

    V00 = 00:00, V01 = 00:15, ..., V95 = 23:45
    """
    if not 0 <= n <= 95:
        raise ValueError(f"Interval index must be 0–95, got {n}")
    hours, quarters = divmod(n, 4)
    return time(hour=hours, minute=quarters * 15)


def csv_filename_to_date(name: str) -> date:
    """Parse a VSDATA CSV filename into a date.

    Expected format: VSDATA_YYYYMMDD.csv
    """
    match = re.match(r"VSDATA_(\d{4})(\d{2})(\d{2})\.csv", name, re.IGNORECASE)
    if not match:
        raise ValueError(f"Cannot parse date from filename: {name}")
    return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))


_MONTH_NAMES = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def zip_to_year_month(name: str) -> str:
    """Parse a traffic signal volume ZIP filename into a 'YYYY-MM' string.

    Handles:
      - traffic_signal_volume_data_2023.zip        -> '2023' (yearly)
      - traffic_signal_volume_data_january_2025.zip -> '2025-01'
      - traffic_signal_volume_data_december_2025 (1).zip -> '2025-12'
    """
    stem = name.lower()
    # Try monthly pattern: ..._{month}_{year}...
    for month_name, month_num in _MONTH_NAMES.items():
        if month_name in stem:
            year_match = re.search(r"(\d{4})", stem)
            if year_match:
                return f"{year_match.group(1)}-{month_num}"
    # Try compact monthly pattern: ..._YYYYMM.zip
    ym_match = re.search(r"_(\d{4})(\d{2})", stem)
    if ym_match:
        return f"{ym_match.group(1)}-{ym_match.group(2)}"
    # Try yearly pattern: ..._YYYY.zip
    year_match = re.search(r"_(\d{4})", stem)
    if year_match:
        return year_match.group(1)
    raise ValueError(f"Cannot parse year/month from ZIP filename: {name}")
