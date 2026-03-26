"""Tests for the feature engineering module."""

from datetime import date, timedelta

import duckdb
import pytest

from flowcast.db.schema import ensure_schema
from flowcast.modelling.features import build_daily_features


@pytest.fixture
def feature_db():
    """In-memory DuckDB with traffic_daily data for 2 sites across 60 days."""
    con = duckdb.connect(":memory:")
    ensure_schema(con)

    # Generate 60 days of data for 2 sites starting 2024-06-01
    start = date(2024, 6, 1)
    rows = []
    for site_id in [100, 200]:
        for day_offset in range(60):
            d = start + timedelta(days=day_offset)
            dow = d.isoweekday()  # 1=Mon..7=Sun
            # Weekday base ~1000, weekend base ~600, plus some site offset
            base = 600 if dow >= 6 else 1000
            volume = base + site_id + day_offset
            rows.append((site_id, d.isoformat(), "SPR", volume, 4, 8, volume // 24))

    con.executemany(
        "INSERT INTO traffic_daily VALUES (?, ?::DATE, ?, ?, ?, ?, ?)",
        rows,
    )
    yield con
    con.close()


def test_feature_shape(feature_db):
    """Features DataFrame has expected columns and no NaNs in lag columns."""
    df = build_daily_features(feature_db, [100, 200])
    assert len(df) > 0

    expected_cols = {
        "site_id", "date", "total_volume", "detector_count",
        "peak_hour", "peak_hour_volume", "region",
        "day_of_week", "month", "day_of_month", "week_of_year", "quarter",
        "is_weekend",
        "volume_lag_1", "volume_lag_7", "volume_lag_14", "volume_lag_28",
        "volume_rolling_mean_7", "volume_rolling_mean_28", "volume_rolling_std_7",
        "site_avg_volume", "cluster_id",
        "cluster_mean_volume", "cluster_lag_1", "cluster_lag_7", "cluster_lag_14",
        "top_neighbor_lag_1", "cluster_corridor_avg",
        "rainfall_mm", "temperature_c", "wind_kmh", "severe_weather_flag",
        "afl_games_count", "concerts_count", "cbd_events_count", "roadworks_flag", "school_zone_flag",
        "graph_degree", "graph_centrality", "graph_clustering_coeff",
        "is_public_holiday", "is_school_holiday",
        "is_day_before_public_holiday", "is_day_after_public_holiday",
        "is_bridge_day", "is_term_start_week", "is_term_end_week",
    }
    assert expected_cols.issubset(set(df.columns))

    # No NaN in lag_28 (rows without enough history are dropped)
    assert df["volume_lag_28"].isna().sum() == 0


def test_lag_alignment(feature_db):
    """Lag-7 for a given day should equal the volume from 7 days earlier."""
    df = build_daily_features(feature_db, [100])
    df = df.sort_values("date").reset_index(drop=True)

    # Pick a row and verify lag_7
    for idx in range(len(df) - 1):
        row = df.iloc[idx]
        d = row["date"]
        lag_target = d - timedelta(days=7)
        # Find the row 7 days earlier
        earlier = df[df["date"] == lag_target]
        if not earlier.empty:
            assert row["volume_lag_7"] == earlier.iloc[0]["total_volume"]


def test_rolling_mean(feature_db):
    """Rolling mean 7 should be the average of the previous 7 days."""
    df = build_daily_features(feature_db, [100])
    df = df.sort_values("date").reset_index(drop=True)

    # Verify rolling mean for the last row
    last = df.iloc[-1]
    prev_7 = df.iloc[-8:-1]["total_volume"].mean()
    assert abs(last["volume_rolling_mean_7"] - prev_7) < 0.01


def test_calendar_features(feature_db):
    """Calendar features match known dates."""
    df = build_daily_features(feature_db, [100])
    # June 29, 2024 is a Saturday
    sat_rows = df[df["date"] == date(2024, 6, 29)]
    if not sat_rows.empty:
        row = sat_rows.iloc[0]
        assert row["is_weekend"] == 1
        assert row["day_of_week"] == 6  # ISODOW: Saturday=6


def test_holiday_flags(feature_db):
    """Public holiday flags work for known holidays."""
    df = build_daily_features(feature_db, [100])
    # Check that the column exists and is numeric
    assert df["is_public_holiday"].dtype in ("int8", "int64", "int32")
    assert df["is_school_holiday"].dtype in ("int8", "int64", "int32")


def test_empty_site_list(feature_db):
    """Empty site list returns empty DataFrame."""
    df = build_daily_features(feature_db, [])
    assert len(df) == 0


def test_date_filtering(feature_db):
    """Start/end date filtering works correctly."""
    df = build_daily_features(feature_db, [100], start_date="2024-07-15", end_date="2024-07-20")
    if not df.empty:
        dates = df["date"].dt.date
        assert dates.min() >= date(2024, 7, 15)
        assert dates.max() <= date(2024, 7, 20)
