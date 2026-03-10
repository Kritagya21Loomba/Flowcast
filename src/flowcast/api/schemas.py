"""Pydantic response models for the Flowcast API."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel


# --- Overview ---

class OverviewStats(BaseModel):
    total_sites: int
    sites_with_coords: int
    clustered_sites: int
    total_daily_rows: int
    date_range_start: date | None
    date_range_end: date | None
    total_forecasts: int
    model_count: int
    best_model_mape: float | None
    correlation_pairs: int


# --- Sites ---

class SiteSummary(BaseModel):
    site_id: int
    region: str
    latitude: float | None = None
    longitude: float | None = None
    intersection_name: str | None = None
    detector_count: int | None = None
    first_seen: date | None = None
    last_seen: date | None = None
    cluster_id: int | None = None
    avg_daily_volume: float | None = None


class SiteListResponse(BaseModel):
    sites: list[SiteSummary]
    count: int


class DailyVolume(BaseModel):
    date: date
    total_volume: int
    peak_hour: int | None = None
    peak_hour_volume: int | None = None


class SiteDetailResponse(BaseModel):
    site: SiteSummary
    daily_volumes: list[DailyVolume]
    hourly_profile: list[float] | None = None


# --- Forecasts ---

class ForecastPoint(BaseModel):
    forecast_date: date
    horizon_days: int
    predicted_volume: float
    prediction_lower: float | None = None
    prediction_upper: float | None = None
    actual_volume: int | None = None


class SiteForecastResponse(BaseModel):
    site_id: int
    model_id: str | None = None
    forecasts: list[ForecastPoint]


# --- Clusters ---

class ClusterSummary(BaseModel):
    cluster_id: int
    site_count: int
    avg_silhouette: float
    avg_daily_volume: float | None = None
    profile: list[float]


class ClusterDetail(BaseModel):
    cluster_id: int
    sites: list[SiteSummary]
    profile: list[float]
    avg_silhouette: float


# --- Correlations ---

class CorrelationPair(BaseModel):
    site_a: int
    site_b: int
    pearson_daily: float | None = None
    cosine_hourly: float | None = None
    lag_minutes: int | None = None
    site_a_lat: float | None = None
    site_a_lon: float | None = None
    site_b_lat: float | None = None
    site_b_lon: float | None = None


class CorrelationResponse(BaseModel):
    pairs: list[CorrelationPair]
    count: int


# --- Models ---

class ModelSummary(BaseModel):
    model_id: str
    model_type: str
    scope: str
    test_mae: float | None = None
    test_rmse: float | None = None
    test_mape: float | None = None
    trained_at: str | None = None
    n_training_rows: int | None = None


class SiteMetric(BaseModel):
    site_id: int
    mae: float | None = None
    rmse: float | None = None
    mape: float | None = None
    n_test_days: int | None = None
