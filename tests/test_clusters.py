"""Tests for site selection and clustering."""

from datetime import date, timedelta

import duckdb
import numpy as np
import pytest

from flowcast.db.schema import ensure_schema
from flowcast.modelling.site_selection import select_modelling_sites
from flowcast.modelling.clusters import (
    cluster_sites,
    compute_site_hourly_profiles,
    store_clusters,
)


@pytest.fixture
def selection_db():
    """DB with traffic_daily data: 3 sites with varying quality."""
    con = duckdb.connect(":memory:")
    ensure_schema(con)

    start = date(2023, 1, 1)
    rows = []

    # Site 100: good quality, 400 days, avg ~1000
    for d in range(400):
        dt = start + timedelta(days=d)
        rows.append((100, dt.isoformat(), "NW", 1000 + d, 4, 8, 50))

    # Site 200: good quality, 400 days, avg ~500
    for d in range(400):
        dt = start + timedelta(days=d)
        rows.append((200, dt.isoformat(), "SE", 500 + d, 2, 12, 30))

    # Site 300: bad quality — only 100 days (below min_history_days)
    for d in range(100):
        dt = start + timedelta(days=d)
        rows.append((300, dt.isoformat(), "SPR", 800 + d, 3, 9, 40))

    # Site 400: bad quality — too many zero days
    for d in range(400):
        dt = start + timedelta(days=d)
        vol = 0 if d % 2 == 0 else 600  # 50% zero days
        rows.append((400, dt.isoformat(), "NW", vol, 2, 10, vol // 24))

    con.executemany(
        "INSERT INTO traffic_daily VALUES (?, ?::DATE, ?, ?, ?, ?, ?)",
        rows,
    )
    yield con
    con.close()


def test_select_filters_short_history(selection_db):
    """Sites with fewer than min_history_days are excluded."""
    sites = select_modelling_sites(selection_db, min_history_days=365, max_sites=10)
    assert 300 not in sites


def test_select_filters_high_zero_pct(selection_db):
    """Sites with too many zero-volume days are excluded."""
    sites = select_modelling_sites(selection_db, min_history_days=200, max_sites=10)
    assert 400 not in sites


def test_select_respects_max_sites(selection_db):
    """Selection respects the max_sites cap."""
    sites = select_modelling_sites(selection_db, min_history_days=200,
                                   max_zero_pct=0.6, max_sites=1)
    assert len(sites) <= 1


def test_select_returns_sorted_by_quality(selection_db):
    """Higher quality sites appear first."""
    sites = select_modelling_sites(selection_db, min_history_days=200,
                                   max_zero_pct=0.6, max_sites=10)
    # Site 100 has higher data_days * avg_volume than site 200
    if 100 in sites and 200 in sites:
        assert sites.index(100) < sites.index(200)


# --- Clustering tests ---

@pytest.fixture
def cluster_db():
    """DB with traffic_volumes for 4 sites with distinct hourly profiles."""
    con = duckdb.connect(":memory:")
    ensure_schema(con)

    # Create 2 days of data for 4 sites with different patterns
    for site_id, pattern in [
        (10, "morning"),   # High volume hours 7-9
        (20, "morning"),   # Same pattern as site 10
        (30, "evening"),   # High volume hours 17-19
        (40, "flat"),      # Uniform volume all day
    ]:
        for day in ["2024-01-01", "2024-01-02"]:
            volumes = [0] * 96
            if pattern == "morning":
                for i in range(28, 40):  # intervals 28-39 = hours 7-9
                    volumes[i] = 100
            elif pattern == "evening":
                for i in range(68, 80):  # intervals 68-79 = hours 17-19
                    volumes[i] = 100
            elif pattern == "flat":
                volumes = [10] * 96

            vol_cols = ", ".join(str(v) for v in volumes)
            con.execute(f"""
                INSERT INTO traffic_volumes
                VALUES ('test.csv', {site_id}, '{day}'::DATE, 1,
                        {vol_cols}, 'NW', 96, {sum(volumes)}, 0)
            """)

    yield con
    con.close()


def test_hourly_profiles_shape(cluster_db):
    """Profiles have shape (n_sites, 24) and rows sum to ~1."""
    profiles, ids = compute_site_hourly_profiles(cluster_db, [10, 20, 30, 40])
    assert profiles.shape == (4, 24)
    row_sums = profiles.sum(axis=1)
    np.testing.assert_allclose(row_sums, 1.0, atol=0.01)


def test_similar_sites_cluster_together(cluster_db):
    """Sites with the same hourly pattern should be in the same cluster."""
    profiles, ids = compute_site_hourly_profiles(cluster_db, [10, 20, 30, 40])
    df = cluster_sites(profiles, ids, n_clusters=3)

    # Sites 10 and 20 (both morning) should share a cluster
    c10 = df[df["site_id"] == 10]["cluster_id"].iloc[0]
    c20 = df[df["site_id"] == 20]["cluster_id"].iloc[0]
    assert c10 == c20


def test_different_sites_separate(cluster_db):
    """Sites with different profiles should be in different clusters."""
    profiles, ids = compute_site_hourly_profiles(cluster_db, [10, 30, 40])
    df = cluster_sites(profiles, ids, n_clusters=3)

    clusters = df.set_index("site_id")["cluster_id"]
    # All 3 should be in different clusters
    assert len(set(clusters.values)) == 3


def test_store_and_retrieve_clusters(cluster_db):
    """Clusters can be stored and retrieved from DuckDB."""
    profiles, ids = compute_site_hourly_profiles(cluster_db, [10, 20, 30])
    df = cluster_sites(profiles, ids, n_clusters=2)
    store_clusters(cluster_db, df, profiles, ids)

    count = cluster_db.execute("SELECT COUNT(*) FROM site_clusters").fetchone()[0]
    assert count == 3
