import { useNavigate } from 'react-router-dom'
import { useOverview, useModels } from '../api/hooks'
import './DashboardPage.css'

export default function DashboardPage() {
  const { data: overview, isLoading } = useOverview()
  const { data: models } = useModels()
  const navigate = useNavigate()

  const bestModel = models?.[0]

  if (isLoading) return <div className="page-loading">Loading...</div>
  if (!overview) return null

  return (
    <div className="dashboard">
      {/* Hero */}
      <div className="dash-hero">
        <h1>Melbourne Traffic Forecasting</h1>
        <p className="dash-hero-sub">
          Predicting congestion across <strong>{overview.total_sites.toLocaleString()} signalised intersections</strong> using
          machine-learning models trained on <strong>{overview.total_daily_rows.toLocaleString()} days</strong> of
          historical SCATS traffic signal data.
        </p>
      </div>

      {/* Primary KPI — Forecast Accuracy */}
      {bestModel && bestModel.test_mape != null && (
        <div className="dash-forecast-hero">
          <div className="forecast-accuracy-ring">
            <svg viewBox="0 0 120 120">
              <circle cx="60" cy="60" r="52" fill="none" stroke="#2a2d35" strokeWidth="8" />
              <circle
                cx="60" cy="60" r="52"
                fill="none"
                stroke="#34d399"
                strokeWidth="8"
                strokeDasharray={`${(1 - bestModel.test_mape / 100) * 327} 327`}
                strokeLinecap="round"
                transform="rotate(-90 60 60)"
              />
            </svg>
            <div className="accuracy-text">
              <span className="accuracy-value">{(100 - bestModel.test_mape).toFixed(1)}%</span>
              <span className="accuracy-label">Accuracy</span>
            </div>
          </div>
          <div className="forecast-hero-details">
            <h2>Forecast Model Performance</h2>
            <p>
              The global <strong>{bestModel.model_type}</strong> model achieves
              a <strong>{bestModel.test_mape.toFixed(1)}% MAPE</strong> on held-out test data,
              meaning daily volume predictions are within ~{bestModel.test_mape.toFixed(0)}%
              of actual traffic counts.
            </p>
            <div className="forecast-hero-metrics">
              <div className="fh-metric">
                <span className="fh-value">{overview.total_forecasts.toLocaleString()}</span>
                <span className="fh-label">Forecasts Generated</span>
              </div>
              <div className="fh-metric">
                <span className="fh-value">{bestModel.test_mae?.toFixed(0)}</span>
                <span className="fh-label">MAE (vehicles/day)</span>
              </div>
              <div className="fh-metric">
                <span className="fh-value">{bestModel.n_training_rows?.toLocaleString()}</span>
                <span className="fh-label">Training Samples</span>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* KPI Grid */}
      <div className="dash-kpi-grid">
        <div className="kpi-card" onClick={() => navigate('/map')}>
          <span className="kpi-value">{overview.sites_with_coords.toLocaleString()}</span>
          <span className="kpi-label">Geocoded Intersections</span>
          <span className="kpi-sub">of {overview.total_sites.toLocaleString()} total SCATS sites mapped across Greater Melbourne</span>
        </div>
        <div className="kpi-card" onClick={() => navigate('/clusters')}>
          <span className="kpi-value">{overview.clustered_sites}</span>
          <span className="kpi-label">Sites Clustered</span>
          <span className="kpi-sub">Grouped into traffic behaviour profiles by 24-hour volume patterns</span>
        </div>
        <div className="kpi-card" onClick={() => navigate('/correlations')}>
          <span className="kpi-value">{overview.correlation_pairs.toLocaleString()}</span>
          <span className="kpi-label">Correlated Site Pairs</span>
          <span className="kpi-sub">Inferred origin-destination links via temporal correlation analysis</span>
        </div>
        <div className="kpi-card">
          <span className="kpi-value">
            {overview.date_range_start && overview.date_range_end
              ? `${overview.date_range_start} — ${overview.date_range_end}`
              : '—'
            }
          </span>
          <span className="kpi-label">Data Coverage</span>
          <span className="kpi-sub">Continuous daily traffic volumes from Victorian Government SCATS feeds</span>
        </div>
      </div>

      {/* How It Works */}
      <div className="dash-pipeline">
        <h2>How Flowcast Works</h2>
        <div className="pipeline-steps">
          <div className="pipeline-step">
            <div className="step-number">1</div>
            <h3>Ingest</h3>
            <p>Raw SCATS traffic signal CSVs are ingested into a DuckDB analytics database with 96 fifteen-minute volume intervals per detector per day.</p>
          </div>
          <div className="pipeline-step">
            <div className="step-number">2</div>
            <h3>Profile & Cluster</h3>
            <p>Each site's 24-hour volume profile is normalised and clustered via KMeans to identify traffic behaviour archetypes (morning peak, evening peak, etc.).</p>
          </div>
          <div className="pipeline-step">
            <div className="step-number">3</div>
            <h3>Forecast</h3>
            <p>A global HistGradientBoosting model uses lag features, calendar signals, and holiday flags to predict daily volumes at 1, 7, 14, and 28-day horizons.</p>
          </div>
          <div className="pipeline-step">
            <div className="step-number">4</div>
            <h3>Correlate</h3>
            <p>Pearson daily correlation and cosine hourly similarity reveal linked sites — potential origin-destination pairs for network-level congestion insight.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
