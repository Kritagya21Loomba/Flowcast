"""Tests for the Flowcast API endpoints."""

from datetime import date, datetime

import duckdb
import pytest
from fastapi.testclient import TestClient

from flowcast.api.app import create_app
from flowcast.api.deps import get_db
from flowcast.db.schema import ensure_schema


@pytest.fixture
def seeded_db():
    """In-memory DuckDB pre-populated with test data for all API endpoints."""
    con = duckdb.connect(":memory:")
    ensure_schema(con)

    # signal_sites
    con.execute("""
        INSERT INTO signal_sites (site_id, region, detector_count, first_seen, last_seen,
                                  latitude, longitude, intersection_name)
        VALUES
            (100, 'NW', 2, '2024-01-01', '2025-12-31', -37.81, 144.96, 'Test Rd / Main St'),
            (200, 'NW', 1, '2024-06-01', '2025-12-31', -37.82, 144.97, 'King St / Queen Rd'),
            (300, 'SE', 1, '2024-06-01', '2025-12-31', NULL, NULL, NULL)
    """)

    # traffic_daily
    con.execute("""
        INSERT INTO traffic_daily (site_id, date, region, total_volume, detector_count,
                                   peak_hour, peak_hour_volume)
        VALUES
            (100, '2025-11-01', 'NW', 50000, 2, 8, 6000),
            (100, '2025-11-02', 'NW', 48000, 2, 8, 5800),
            (100, '2025-11-03', 'NW', 30000, 2, 9, 3500),
            (200, '2025-11-01', 'NW', 20000, 1, 17, 2500),
            (300, '2025-11-01', 'SE', 15000, 1, 8, 2000)
    """)

    # site_clusters
    profile_a = [float(i) / 24 for i in range(24)]
    profile_b = [float(23 - i) / 24 for i in range(24)]
    con.execute("""
        INSERT INTO site_clusters (site_id, cluster_id, cluster_label, profile_vector,
                                   silhouette_score)
        VALUES (?, 0, 'Morning peak', ?, 0.75),
               (?, 1, 'Evening peak', ?, 0.60)
    """, [100, profile_a, 200, profile_b])

    # model_registry
    con.execute("""
        INSERT INTO model_registry (model_id, model_type, scope, target_column,
                                    feature_columns, n_training_rows, test_mae, test_rmse,
                                    test_mape, trained_at)
        VALUES ('model_001', 'HistGBR', 'global', 'total_volume',
                'lag_1,lag_7', 1000, 2500.0, 4000.0, 3.9, '2025-12-01 00:00:00')
    """)

    # forecasts
    con.execute("""
        INSERT INTO forecasts (forecast_id, model_id, site_id, forecast_date,
                               horizon_days, predicted_volume, prediction_lower,
                               prediction_upper, actual_volume)
        VALUES
            (1, 'model_001', 100, '2025-12-02', 1, 49000.0, 45000.0, 53000.0, 50000),
            (2, 'model_001', 100, '2025-12-08', 7, 47000.0, 40000.0, 54000.0, NULL)
    """)

    # model_metrics_site
    con.execute("""
        INSERT INTO model_metrics_site (model_id, site_id, mae, rmse, mape, n_test_days)
        VALUES ('model_001', 100, 2000.0, 3000.0, 3.5, 60),
               ('model_001', 200, 3000.0, 4500.0, 5.2, 60)
    """)

    # site_correlations
    con.execute("""
        INSERT INTO site_correlations (site_a, site_b, pearson_daily, cosine_hourly,
                                       lag_minutes)
        VALUES (100, 200, 0.92, 0.88, 15),
               (100, 300, 0.75, 0.70, 0)
    """)

    yield con
    con.close()


@pytest.fixture
def client(seeded_db):
    """FastAPI TestClient with the DB dependency overridden."""
    app = create_app()

    def override_get_db():
        yield seeded_db

    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)


# --- Overview ---

def test_overview(client):
    resp = client.get("/api/overview")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_sites"] == 3
    assert data["sites_with_coords"] == 2
    assert data["clustered_sites"] == 2
    assert data["total_daily_rows"] == 5
    assert data["total_forecasts"] == 2
    assert data["model_count"] == 1
    assert data["best_model_mape"] == pytest.approx(3.9)
    assert data["correlation_pairs"] == 2


# --- Sites ---

def test_list_sites(client):
    resp = client.get("/api/sites")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 3
    site_ids = [s["site_id"] for s in data["sites"]]
    assert 100 in site_ids
    assert 200 in site_ids
    assert 300 in site_ids


def test_list_sites_filter_region(client):
    resp = client.get("/api/sites?region=SE")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["sites"][0]["site_id"] == 300


def test_list_sites_filter_cluster(client):
    resp = client.get("/api/sites?cluster_id=0")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["sites"][0]["site_id"] == 100


def test_site_detail(client):
    resp = client.get("/api/sites/100?days=10")
    assert resp.status_code == 200
    data = resp.json()
    assert data["site"]["site_id"] == 100
    assert data["site"]["latitude"] == pytest.approx(-37.81)
    assert data["site"]["cluster_id"] == 0
    assert len(data["daily_volumes"]) == 3
    assert data["hourly_profile"] is not None
    assert len(data["hourly_profile"]) == 24


def test_site_detail_not_found(client):
    resp = client.get("/api/sites/9999")
    assert resp.status_code == 404


# --- Forecasts ---

def test_forecasts(client):
    resp = client.get("/api/sites/100/forecasts")
    assert resp.status_code == 200
    data = resp.json()
    assert data["site_id"] == 100
    assert data["model_id"] == "model_001"
    assert len(data["forecasts"]) == 2
    assert data["forecasts"][0]["horizon_days"] == 1
    assert data["forecasts"][0]["actual_volume"] == 50000
    assert data["forecasts"][1]["actual_volume"] is None


def test_forecasts_empty(client):
    resp = client.get("/api/sites/300/forecasts")
    assert resp.status_code == 200
    data = resp.json()
    assert data["forecasts"] == []


# --- Clusters ---

def test_list_clusters(client):
    resp = client.get("/api/clusters")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    cluster_ids = [c["cluster_id"] for c in data]
    assert 0 in cluster_ids
    assert 1 in cluster_ids
    for c in data:
        assert len(c["profile"]) == 24
        assert c["site_count"] == 1


def test_cluster_detail(client):
    resp = client.get("/api/clusters/0")
    assert resp.status_code == 200
    data = resp.json()
    assert data["cluster_id"] == 0
    assert len(data["sites"]) == 1
    assert data["sites"][0]["site_id"] == 100
    assert len(data["profile"]) == 24


def test_cluster_not_found(client):
    resp = client.get("/api/clusters/99")
    assert resp.status_code == 404


# --- Correlations ---

def test_correlations(client):
    resp = client.get("/api/correlations?min_pearson=0.8")
    assert resp.status_code == 200
    data = resp.json()
    # Only the pair with pearson_daily=0.92 should pass the 0.8 filter
    assert data["count"] == 1
    assert data["pairs"][0]["site_a"] == 100
    assert data["pairs"][0]["site_b"] == 200
    assert data["pairs"][0]["site_a_lat"] == pytest.approx(-37.81)


def test_correlations_with_site_filter(client):
    resp = client.get("/api/correlations?min_pearson=0.5&site_id=100")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2


def test_correlations_limit(client):
    resp = client.get("/api/correlations?min_pearson=0.5&limit=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1


# --- Models ---

def test_list_models(client):
    resp = client.get("/api/models")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["model_id"] == "model_001"
    assert data[0]["test_mape"] == pytest.approx(3.9)


def test_model_site_metrics(client):
    resp = client.get("/api/models/model_001/sites")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    # Default sort by MAE ascending
    assert data[0]["site_id"] == 100
    assert data[0]["mae"] == pytest.approx(2000.0)


def test_model_site_metrics_sort_by_mape(client):
    resp = client.get("/api/models/model_001/sites?sort_by=mape")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["site_id"] == 100  # 3.5 < 5.2
