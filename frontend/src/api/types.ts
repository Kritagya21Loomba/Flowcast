// API response types matching FastAPI Pydantic schemas

export interface OverviewStats {
  total_sites: number
  sites_with_coords: number
  clustered_sites: number
  total_daily_rows: number
  date_range_start: string | null
  date_range_end: string | null
  total_forecasts: number
  model_count: number
  best_model_mape: number | null
  correlation_pairs: number
}

export interface SiteSummary {
  site_id: number
  region: string
  detector_count: number | null
  first_seen: string | null
  last_seen: string | null
  latitude: number | null
  longitude: number | null
  intersection_name: string | null
  cluster_id: number | null
  avg_daily_volume: number | null
}

export interface SiteListResponse {
  count: number
  sites: SiteSummary[]
}

export interface DailyVolume {
  date: string
  total_volume: number
  peak_hour: number | null
  peak_hour_volume: number | null
}

export interface SiteDetailResponse {
  site: SiteSummary
  daily_volumes: DailyVolume[]
  hourly_profile: number[] | null
}

export interface ForecastPoint {
  forecast_date: string
  horizon_days: number
  predicted_volume: number
  prediction_lower: number | null
  prediction_upper: number | null
  actual_volume: number | null
}

export interface SiteForecastResponse {
  site_id: number
  model_id: string | null
  forecasts: ForecastPoint[]
}

export interface ClusterSummary {
  cluster_id: number
  label: string | null
  site_count: number
  avg_silhouette: number
  avg_daily_volume: number | null
  profile: number[]
}

export interface ClusterDetail {
  cluster_id: number
  label: string | null
  profile: number[]
  sites: SiteSummary[]
  avg_silhouette: number
}

export interface CorrelationPair {
  site_a: number
  site_b: number
  pearson_daily: number
  cosine_hourly: number
  lag_minutes: number | null
  site_a_lat: number | null
  site_a_lon: number | null
  site_b_lat: number | null
  site_b_lon: number | null
}

export interface CorrelationResponse {
  count: number
  pairs: CorrelationPair[]
}

export interface ModelSummary {
  model_id: string
  model_type: string
  scope: string
  test_mae: number | null
  test_rmse: number | null
  test_mape: number | null
  trained_at: string | null
  n_training_rows: number | null
}

export interface SiteMetric {
  site_id: number
  mae: number | null
  rmse: number | null
  mape: number | null
  n_test_days: number | null
}
