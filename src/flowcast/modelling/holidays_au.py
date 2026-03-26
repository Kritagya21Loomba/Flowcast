"""Victorian public and school holiday calendar integration."""

from __future__ import annotations

from datetime import date

import holidays
import pandas as pd

from flowcast.utils.logging import get_logger

log = get_logger(__name__)

# Victorian school term dates (approximate, sourced from Dept of Education).
# Each tuple is (term_end, holiday_start, holiday_end, next_term_start).
# We store the holiday windows between terms + the summer break.
_SCHOOL_HOLIDAYS_VIC: list[tuple[date, date]] = [
    # 2023
    (date(2023, 1, 1), date(2023, 1, 27)),    # Summer holidays
    (date(2023, 4, 7), date(2023, 4, 23)),     # Term 1-2 break
    (date(2023, 6, 24), date(2023, 7, 10)),    # Term 2-3 break
    (date(2023, 9, 16), date(2023, 10, 2)),    # Term 3-4 break
    (date(2023, 12, 21), date(2023, 12, 31)),  # Summer start
    # 2024
    (date(2024, 1, 1), date(2024, 1, 29)),     # Summer holidays
    (date(2024, 3, 29), date(2024, 4, 14)),    # Term 1-2 break
    (date(2024, 6, 29), date(2024, 7, 14)),    # Term 2-3 break
    (date(2024, 9, 21), date(2024, 10, 6)),    # Term 3-4 break
    (date(2024, 12, 20), date(2024, 12, 31)),  # Summer start
    # 2025
    (date(2025, 1, 1), date(2025, 1, 28)),     # Summer holidays
    (date(2025, 4, 5), date(2025, 4, 21)),     # Term 1-2 break
    (date(2025, 7, 5), date(2025, 7, 20)),     # Term 2-3 break
    (date(2025, 9, 20), date(2025, 10, 5)),    # Term 3-4 break
    (date(2025, 12, 20), date(2025, 12, 31)),  # Summer start
    # 2026
    (date(2026, 1, 1), date(2026, 1, 27)),     # Summer holidays
    (date(2026, 4, 3), date(2026, 4, 19)),     # Term 1-2 break
    (date(2026, 7, 4), date(2026, 7, 19)),     # Term 2-3 break
    (date(2026, 9, 19), date(2026, 10, 4)),    # Term 3-4 break
    (date(2026, 12, 19), date(2026, 12, 31)),  # Summer start
]


def get_victorian_public_holidays(start_year: int, end_year: int) -> set[date]:
    """Return all Victorian public holidays for the given year range (inclusive)."""
    au_holidays = holidays.Australia(subdiv="VIC", years=range(start_year, end_year + 1))
    return set(au_holidays.keys())


def _build_school_holiday_set() -> set[date]:
    """Expand school holiday windows into a set of individual dates."""
    result: set[date] = set()
    for start, end in _SCHOOL_HOLIDAYS_VIC:
        current = start
        while current <= end:
            result.add(current)
            current = date.fromordinal(current.toordinal() + 1)
    return result


_SCHOOL_HOLIDAYS_SET: set[date] | None = None


def _get_school_holidays() -> set[date]:
    global _SCHOOL_HOLIDAYS_SET
    if _SCHOOL_HOLIDAYS_SET is None:
        _SCHOOL_HOLIDAYS_SET = _build_school_holiday_set()
    return _SCHOOL_HOLIDAYS_SET


def add_holiday_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add is_public_holiday and is_school_holiday columns to a DataFrame.

    Expects a 'date' column of type datetime.date or pd.Timestamp.
    """
    dates = pd.to_datetime(df["date"]).dt.date
    years = dates.map(lambda d: d.year)
    start_year, end_year = int(years.min()), int(years.max())

    pub_holidays = get_victorian_public_holidays(start_year, end_year)
    school_holidays = _get_school_holidays()

    df = df.copy()
    df["is_public_holiday"] = dates.map(lambda d: 1 if d in pub_holidays else 0).astype("int8")
    df["is_school_holiday"] = dates.map(lambda d: 1 if d in school_holidays else 0).astype("int8")
    df["is_day_before_public_holiday"] = dates.map(
        lambda d: 1 if date.fromordinal(d.toordinal() + 1) in pub_holidays else 0
    ).astype("int8")
    df["is_day_after_public_holiday"] = dates.map(
        lambda d: 1 if date.fromordinal(d.toordinal() - 1) in pub_holidays else 0
    ).astype("int8")
    df["is_bridge_day"] = dates.map(
        lambda d: 1
        if (d.weekday() == 0 and date.fromordinal(d.toordinal() - 3) in pub_holidays)
        or (d.weekday() == 4 and date.fromordinal(d.toordinal() + 3) in pub_holidays)
        else 0
    ).astype("int8")

    term_starts = {date.fromordinal(end.toordinal() + 1) for _, end in _SCHOOL_HOLIDAYS_VIC}
    term_ends = {date.fromordinal(start.toordinal() - 1) for start, _ in _SCHOOL_HOLIDAYS_VIC}
    df["is_term_start_week"] = dates.map(
        lambda d: 1 if any(0 <= (d - ts).days <= 6 for ts in term_starts if ts.year == d.year) else 0
    ).astype("int8")
    df["is_term_end_week"] = dates.map(
        lambda d: 1 if any(0 <= (d - te).days <= 6 for te in term_ends if te.year == d.year) else 0
    ).astype("int8")

    log.debug("holiday_features_added", public_count=int(df["is_public_holiday"].sum()),
              school_count=int(df["is_school_holiday"].sum()), rows=len(df))
    return df
