import { useSiteDetail, useSiteForecasts } from '../api/hooks'
import DailyVolumeChart from './DailyVolumeChart'
import ForecastChart from './ForecastChart'
import HourlyProfileChart from './HourlyProfileChart'
import './SitePanel.css'

interface Props {
  siteId: number
  onClose: () => void
}

export default function SitePanel({ siteId, onClose }: Props) {
  const { data, isLoading } = useSiteDetail(siteId)
  const { data: forecastData } = useSiteForecasts(siteId)

  if (isLoading) {
    return (
      <div className="site-panel">
        <div className="panel-header">
          <span>Loading...</span>
          <button className="panel-close" onClick={onClose}>&times;</button>
        </div>
      </div>
    )
  }

  if (!data) return null

  const { site, daily_volumes, hourly_profile } = data
  const hasForecast = forecastData && forecastData.forecasts.length > 0

  // Count how many forecasts have actuals to show accuracy
  const withActuals = forecastData?.forecasts.filter((f) => f.actual_volume != null) ?? []
  const avgError = withActuals.length > 0
    ? withActuals.reduce((sum, f) => {
        const err = Math.abs(f.predicted_volume - f.actual_volume!) / f.actual_volume!
        return sum + err
      }, 0) / withActuals.length
    : null

  return (
    <div className="site-panel">
      <div className="panel-header">
        <div>
          <h2>Site {site.site_id}</h2>
          {site.intersection_name && (
            <span className="panel-intersection">{site.intersection_name}</span>
          )}
        </div>
        <button className="panel-close" onClick={onClose}>&times;</button>
      </div>

      <div className="panel-meta">
        <span>Region: {site.region}</span>
        <span>Detectors: {site.detector_count ?? '—'}</span>
        <span>Cluster: {site.cluster_id ?? '—'}</span>
      </div>

      {/* FORECAST SECTION — always first */}
      <div className="panel-section forecast-section">
        <div className="section-header">
          <h3>Traffic Forecast</h3>
          {avgError !== null && (
            <span className={`accuracy-badge ${avgError < 0.05 ? 'good' : avgError < 0.10 ? 'ok' : 'warn'}`}>
              {(100 - avgError * 100).toFixed(0)}% accurate
            </span>
          )}
        </div>

        {hasForecast ? (
          <>
            <ForecastChart forecasts={forecastData.forecasts} />
            <div className="forecast-table">
              <table>
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Horizon</th>
                    <th>Predicted</th>
                    <th>Actual</th>
                  </tr>
                </thead>
                <tbody>
                  {forecastData.forecasts.map((f, i) => (
                    <tr key={i}>
                      <td>{f.forecast_date}</td>
                      <td>{f.horizon_days}d</td>
                      <td>{Math.round(f.predicted_volume).toLocaleString()}</td>
                      <td>{f.actual_volume?.toLocaleString() ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : (
          <div className="no-forecast">
            No forecasts generated for this site. Site may not have been selected
            for modelling (requires 365+ days of data with &lt;10% zero days).
          </div>
        )}
      </div>

      {/* HISTORICAL DATA */}
      {daily_volumes.length > 0 && (
        <div className="panel-section">
          <h3>Historical Volume (last 90 days)</h3>
          <DailyVolumeChart data={daily_volumes} />
        </div>
      )}

      {hourly_profile && (
        <div className="panel-section">
          <h3>Typical Hourly Profile</h3>
          <HourlyProfileChart profile={hourly_profile} />
        </div>
      )}
    </div>
  )
}
