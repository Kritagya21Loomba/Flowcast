import { useMemo, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell,
} from 'recharts'
import { useModels, useModelSiteMetrics } from '../api/hooks'
import type { SiteMetric } from '../api/types'
import './ModelsPage.css'

function mapeColor(mape: number): string {
  if (mape <= 3) return '#34d399'
  if (mape <= 5) return '#60a5fa'
  if (mape <= 10) return '#facc15'
  return '#f87171'
}

function mapeLabel(mape: number): string {
  if (mape <= 3) return 'Excellent'
  if (mape <= 5) return 'Good'
  if (mape <= 10) return 'Fair'
  return 'Poor'
}

export default function ModelsPage() {
  const { data: models, isLoading } = useModels()
  const [selectedModelId, setSelectedModelId] = useState<string | null>(null)

  const effectiveModelId = selectedModelId ?? models?.[0]?.model_id ?? null

  if (isLoading) return <div className="page-loading">Loading models...</div>

  return (
    <div className="models-page">
      <h2>Forecast Models</h2>
      <p className="page-desc">
        Global HistGradientBoosting models trained on historical daily volumes, lag features,
        calendar signals, and holiday flags. Each model card shows overall accuracy — expand
        per-site metrics to see which intersections forecast best.
      </p>

      <div className="models-cards">
        {models?.map((m) => {
          const isActive = effectiveModelId === m.model_id
          const accuracy = m.test_mape != null ? 100 - m.test_mape : null
          return (
            <div
              key={m.model_id}
              className={`model-card ${isActive ? 'selected' : ''}`}
              onClick={() => setSelectedModelId(isActive ? null : m.model_id)}
            >
              <div className="model-card-top">
                {accuracy != null && (
                  <div className="model-ring-wrap">
                    <svg viewBox="0 0 80 80" className="model-ring">
                      <circle cx="40" cy="40" r="34" fill="none" stroke="#2a2d35" strokeWidth="6" />
                      <circle
                        cx="40" cy="40" r="34"
                        fill="none"
                        stroke={mapeColor(m.test_mape!)}
                        strokeWidth="6"
                        strokeDasharray={`${(accuracy / 100) * 213.6} 213.6`}
                        strokeLinecap="round"
                        transform="rotate(-90 40 40)"
                      />
                    </svg>
                    <div className="ring-text">
                      <span className="ring-val">{accuracy.toFixed(0)}%</span>
                      <span className="ring-lbl">Accuracy</span>
                    </div>
                  </div>
                )}
                <div className="model-info">
                  <div className="model-card-header">
                    <span className="model-type">{m.model_type}</span>
                    <span className="model-scope">{m.scope}</span>
                  </div>
                  <div className="model-id">{m.model_id}</div>
                  {m.trained_at && (
                    <div className="model-date">Trained {m.trained_at.slice(0, 10)}</div>
                  )}
                </div>
              </div>

              <div className="model-metrics-grid">
                {m.test_mape != null && (
                  <div className="mstat">
                    <span className="mstat-val" style={{ color: mapeColor(m.test_mape) }}>
                      {m.test_mape.toFixed(1)}%
                    </span>
                    <span className="mstat-label">MAPE</span>
                  </div>
                )}
                {m.test_mae != null && (
                  <div className="mstat">
                    <span className="mstat-val">{m.test_mae.toFixed(0)}</span>
                    <span className="mstat-label">MAE</span>
                  </div>
                )}
                {m.test_rmse != null && (
                  <div className="mstat">
                    <span className="mstat-val">{m.test_rmse.toFixed(0)}</span>
                    <span className="mstat-label">RMSE</span>
                  </div>
                )}
                {m.n_training_rows != null && (
                  <div className="mstat">
                    <span className="mstat-val">{(m.n_training_rows / 1000).toFixed(0)}k</span>
                    <span className="mstat-label">Train Rows</span>
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>

      {effectiveModelId && <SiteMetricsView modelId={effectiveModelId} />}
    </div>
  )
}

function SiteMetricsView({ modelId }: { modelId: string }) {
  const [sortBy, setSortBy] = useState<'mae' | 'rmse' | 'mape'>('mape')
  const { data, isLoading } = useModelSiteMetrics(modelId, sortBy, 200)

  // MAPE distribution histogram
  const histData = useMemo(() => {
    if (!data) return []
    const buckets = [
      { range: '0-2%', min: 0, max: 2, count: 0 },
      { range: '2-4%', min: 2, max: 4, count: 0 },
      { range: '4-6%', min: 4, max: 6, count: 0 },
      { range: '6-8%', min: 6, max: 8, count: 0 },
      { range: '8-10%', min: 8, max: 10, count: 0 },
      { range: '10%+', min: 10, max: Infinity, count: 0 },
    ]
    for (const m of data) {
      if (m.mape == null) continue
      const bucket = buckets.find((b) => m.mape! >= b.min && m.mape! < b.max)
      if (bucket) bucket.count++
    }
    return buckets
  }, [data])

  // Summary stats
  const summary = useMemo(() => {
    if (!data || data.length === 0) return null
    const mapes = data.filter((m): m is SiteMetric & { mape: number } => m.mape != null)
      .map((m) => m.mape)
    if (mapes.length === 0) return null
    const sorted = [...mapes].sort((a, b) => a - b)
    const mean = mapes.reduce((s, v) => s + v, 0) / mapes.length
    const median = sorted[Math.floor(sorted.length / 2)]
    const under5 = mapes.filter((v) => v <= 5).length
    return { mean, median, under5, total: mapes.length }
  }, [data])

  if (isLoading) return <div className="page-loading">Loading site metrics...</div>

  const maxMape = data ? Math.max(...data.filter((m) => m.mape != null).map((m) => m.mape!), 1) : 1

  return (
    <div className="site-metrics">
      <div className="site-metrics-header">
        <h3>Per-Site Accuracy Breakdown</h3>
        <div className="sort-controls">
          Sort by:
          {(['mape', 'mae', 'rmse'] as const).map((s) => (
            <button
              key={s}
              className={sortBy === s ? 'active' : ''}
              onClick={() => setSortBy(s)}
            >
              {s.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      {/* Viz row: histogram + summary */}
      <div className="metrics-viz-row">
        <div className="viz-card">
          <h4>MAPE Distribution</h4>
          <p className="viz-sub">How many sites fall in each error range</p>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={histData} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
              <CartesianGrid stroke="#2a2d35" strokeDasharray="3 3" vertical={false} />
              <XAxis
                dataKey="range"
                tick={{ fill: '#888', fontSize: 11 }}
                axisLine={{ stroke: '#2a2d35' }}
              />
              <YAxis
                tick={{ fill: '#888', fontSize: 11 }}
                axisLine={{ stroke: '#2a2d35' }}
                label={{ value: 'Sites', angle: -90, position: 'insideLeft', fill: '#777', fontSize: 11 }}
              />
              <Tooltip
                contentStyle={{ background: '#1a1d23', border: '1px solid #2a2d35', fontSize: '0.8rem' }}
                formatter={(value) => [String(value), 'Sites']}
              />
              <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                {histData.map((d, i) => (
                  <Cell key={i} fill={mapeColor((d.min + Math.min(d.max, 12)) / 2)} fillOpacity={0.85} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {summary && (
          <div className="viz-card metrics-summary">
            <h4>Accuracy Summary</h4>
            <div className="summary-grid">
              <div className="summary-item">
                <span className="summary-val" style={{ color: mapeColor(summary.mean) }}>
                  {summary.mean.toFixed(1)}%
                </span>
                <span className="summary-lbl">Mean MAPE</span>
              </div>
              <div className="summary-item">
                <span className="summary-val" style={{ color: mapeColor(summary.median) }}>
                  {summary.median.toFixed(1)}%
                </span>
                <span className="summary-lbl">Median MAPE</span>
              </div>
              <div className="summary-item">
                <span className="summary-val" style={{ color: '#34d399' }}>
                  {((summary.under5 / summary.total) * 100).toFixed(0)}%
                </span>
                <span className="summary-lbl">Sites &le; 5% MAPE</span>
              </div>
              <div className="summary-item">
                <span className="summary-val">{summary.total}</span>
                <span className="summary-lbl">Sites Evaluated</span>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Per-site table */}
      <div className="metrics-table-wrapper">
        <table className="metrics-table">
          <thead>
            <tr>
              <th>Site ID</th>
              <th>Rating</th>
              <th>MAPE</th>
              <th>MAE</th>
              <th>RMSE</th>
              <th>Test Days</th>
            </tr>
          </thead>
          <tbody>
            {data?.map((m) => (
              <tr key={m.site_id}>
                <td className="td-site">{m.site_id}</td>
                <td>
                  {m.mape != null && (
                    <span
                      className="accuracy-badge"
                      style={{ color: mapeColor(m.mape), borderColor: mapeColor(m.mape) }}
                    >
                      {mapeLabel(m.mape)}
                    </span>
                  )}
                </td>
                <td>
                  {m.mape != null ? (
                    <div className="mape-cell">
                      <div
                        className="mape-bar"
                        style={{
                          width: `${Math.min((m.mape / maxMape) * 100, 100)}%`,
                          background: mapeColor(m.mape),
                        }}
                      />
                      <span className="mape-text">{m.mape.toFixed(1)}%</span>
                    </div>
                  ) : '—'}
                </td>
                <td>{m.mae?.toFixed(0) ?? '—'}</td>
                <td>{m.rmse?.toFixed(0) ?? '—'}</td>
                <td>{m.n_test_days ?? '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
