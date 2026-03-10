import { useMemo, useState } from 'react'
import {
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  ResponsiveContainer, Tooltip,
  ScatterChart, Scatter, XAxis, YAxis, ZAxis, CartesianGrid, Cell,
  Legend,
} from 'recharts'
import { useClusters, useClusterDetail } from '../api/hooks'
import type { ClusterSummary } from '../api/types'
import './ClustersPage.css'

const CLUSTER_COLORS = [
  '#60a5fa', '#f97316', '#a78bfa', '#34d399',
  '#fb7185', '#facc15', '#2dd4bf', '#e879f9',
]

const HOUR_LABELS = Array.from({ length: 24 }, (_, i) =>
  i % 3 === 0 ? `${i.toString().padStart(2, '0')}:00` : '',
)

function deriveFeatures(profile: number[]) {
  const total = profile.reduce((s, v) => s + v, 0) || 1
  const amPeak = profile.slice(6, 10).reduce((s, v) => s + v, 0) / total
  const pmPeak = profile.slice(16, 20).reduce((s, v) => s + v, 0) / total
  const peakHour = profile.indexOf(Math.max(...profile))
  return { amPeak, pmPeak, peakHour }
}

export default function ClustersPage() {
  const { data: clusters, isLoading } = useClusters()
  const [selectedCluster, setSelectedCluster] = useState<number | null>(null)
  const [hoveredCluster, setHoveredCluster] = useState<number | null>(null)

  // Radar data: 24 hours, one series per cluster
  const radarData = useMemo(() => {
    if (!clusters) return []
    return Array.from({ length: 24 }, (_, hour) => {
      const point: Record<string, number | string> = {
        hour: HOUR_LABELS[hour] || `${hour}`,
      }
      for (const c of clusters) {
        point[`c${c.cluster_id}`] = Number((c.profile[hour] * 100).toFixed(1))
      }
      return point
    })
  }, [clusters])

  // Scatter data: AM peak ratio vs PM peak ratio
  const scatterData = useMemo(() => {
    if (!clusters) return []
    return clusters.map((c) => {
      const { amPeak, pmPeak } = deriveFeatures(c.profile)
      return {
        cluster_id: c.cluster_id,
        amPeak: Number((amPeak * 100).toFixed(1)),
        pmPeak: Number((pmPeak * 100).toFixed(1)),
        siteCount: c.site_count,
        avgVolume: c.avg_daily_volume ?? 0,
      }
    })
  }, [clusters])

  if (isLoading) return <div className="page-loading">Loading clusters...</div>
  if (!clusters || clusters.length === 0) return <div className="page-loading">No cluster data.</div>

  return (
    <div className="clusters-page">
      <h2>Traffic Behaviour Profiles</h2>
      <p className="page-desc">
        Sites clustered by their normalised 24-hour volume shape reveal distinct traffic archetypes.
        These clusters feed into the forecasting model to improve prediction accuracy.
      </p>

      {/* Top row: radar overlay + scatter */}
      <div className="cluster-viz-row">
        {/* All-profiles radar overlay */}
        <div className="viz-card">
          <h3>24-Hour Profile Comparison</h3>
          <p className="viz-sub">Normalised hourly volume share (%) — each line is one cluster</p>
          <ResponsiveContainer width="100%" height={360}>
            <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="75%">
              <PolarGrid stroke="#2a2d35" />
              <PolarAngleAxis
                dataKey="hour"
                tick={{ fill: '#888', fontSize: 10 }}
              />
              <PolarRadiusAxis
                angle={90}
                tick={{ fill: '#666', fontSize: 9 }}
                tickFormatter={(v) => `${v}%`}
                domain={[0, 'auto']}
              />
              {clusters.map((c) => (
                <Radar
                  key={c.cluster_id}
                  name={`Cluster ${c.cluster_id}`}
                  dataKey={`c${c.cluster_id}`}
                  stroke={CLUSTER_COLORS[c.cluster_id] ?? '#888'}
                  fill={CLUSTER_COLORS[c.cluster_id] ?? '#888'}
                  fillOpacity={
                    hoveredCluster === null
                      ? 0.06
                      : hoveredCluster === c.cluster_id
                        ? 0.25
                        : 0.02
                  }
                  strokeWidth={
                    hoveredCluster === null ? 1.5 :
                    hoveredCluster === c.cluster_id ? 3 : 0.5
                  }
                  strokeOpacity={
                    hoveredCluster === null ? 0.9 :
                    hoveredCluster === c.cluster_id ? 1 : 0.2
                  }
                />
              ))}
              <Tooltip
                contentStyle={{ background: '#1a1d23', border: '1px solid #2a2d35', fontSize: '0.75rem' }}
                formatter={(value, name) => [`${value}%`, String(name)]}
              />
            </RadarChart>
          </ResponsiveContainer>
        </div>

        {/* Feature-space scatter */}
        <div className="viz-card">
          <h3>Cluster Separation</h3>
          <p className="viz-sub">AM peak share (6-10h) vs PM peak share (16-20h) — bubble size = site count</p>
          <ResponsiveContainer width="100%" height={360}>
            <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
              <CartesianGrid stroke="#2a2d35" strokeDasharray="3 3" />
              <XAxis
                type="number" dataKey="amPeak" name="AM Peak"
                tick={{ fill: '#888', fontSize: 11 }}
                label={{ value: 'AM Peak Share (%)', position: 'bottom', fill: '#777', fontSize: 11, dy: 10 }}
                domain={['auto', 'auto']}
              />
              <YAxis
                type="number" dataKey="pmPeak" name="PM Peak"
                tick={{ fill: '#888', fontSize: 11 }}
                label={{ value: 'PM Peak Share (%)', angle: -90, position: 'insideLeft', fill: '#777', fontSize: 11, dx: -5 }}
                domain={['auto', 'auto']}
              />
              <ZAxis type="number" dataKey="siteCount" range={[200, 2000]} />
              <Tooltip
                contentStyle={{ background: '#1a1d23', border: '1px solid #2a2d35', fontSize: '0.8rem' }}
                formatter={(value, name) => {
                  if (name === 'AM Peak') return [`${value}%`, 'AM Peak (6-10h)']
                  if (name === 'PM Peak') return [`${value}%`, 'PM Peak (16-20h)']
                  return [String(value), String(name)]
                }}
                labelFormatter={(_, payload) => {
                  const d = payload?.[0]?.payload
                  return d ? `Cluster ${d.cluster_id} — ${d.siteCount} sites` : ''
                }}
              />
              <Legend
                formatter={(value) => <span style={{ color: '#ccc', fontSize: '0.75rem' }}>{value}</span>}
              />
              {clusters.map((c) => {
                const pts = scatterData.filter((d) => d.cluster_id === c.cluster_id)
                return (
                  <Scatter
                    key={c.cluster_id}
                    name={`Cluster ${c.cluster_id}`}
                    data={pts}
                    fill={CLUSTER_COLORS[c.cluster_id] ?? '#888'}
                    onMouseEnter={() => setHoveredCluster(c.cluster_id)}
                    onMouseLeave={() => setHoveredCluster(null)}
                    onClick={() => setSelectedCluster(
                      selectedCluster === c.cluster_id ? null : c.cluster_id,
                    )}
                    cursor="pointer"
                  >
                    {pts.map((_, i) => (
                      <Cell
                        key={i}
                        fill={CLUSTER_COLORS[c.cluster_id] ?? '#888'}
                        fillOpacity={hoveredCluster === null ? 0.85 : hoveredCluster === c.cluster_id ? 1 : 0.2}
                        stroke={CLUSTER_COLORS[c.cluster_id] ?? '#888'}
                        strokeWidth={hoveredCluster === c.cluster_id ? 2 : 0}
                      />
                    ))}
                  </Scatter>
                )
              })}
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Cluster cards: individual radars + stats */}
      <h3 className="section-title">Individual Cluster Profiles</h3>
      <div className="clusters-grid">
        {clusters.map((c) => (
          <ClusterCard
            key={c.cluster_id}
            cluster={c}
            isSelected={selectedCluster === c.cluster_id}
            isHovered={hoveredCluster === c.cluster_id}
            radarData={radarData}
            onSelect={() => setSelectedCluster(selectedCluster === c.cluster_id ? null : c.cluster_id)}
            onHover={setHoveredCluster}
          />
        ))}
      </div>

      {selectedCluster !== null && <ClusterDetailView clusterId={selectedCluster} />}
    </div>
  )
}

function ClusterCard({
  cluster,
  isSelected,
  isHovered,
  radarData,
  onSelect,
  onHover,
}: {
  cluster: ClusterSummary
  isSelected: boolean
  isHovered: boolean
  radarData: Record<string, number | string>[]
  onSelect: () => void
  onHover: (id: number | null) => void
}) {
  const color = CLUSTER_COLORS[cluster.cluster_id] ?? '#888'
  const { amPeak, pmPeak, peakHour } = useMemo(
    () => deriveFeatures(cluster.profile),
    [cluster.profile],
  )

  return (
    <div
      className={`cluster-card ${isSelected ? 'selected' : ''} ${isHovered ? 'hovered' : ''}`}
      style={{ borderColor: isSelected || isHovered ? color : undefined }}
      onClick={onSelect}
      onMouseEnter={() => onHover(cluster.cluster_id)}
      onMouseLeave={() => onHover(null)}
    >
      <div className="cluster-card-header">
        <div className="cluster-id-row">
          <span className="cluster-swatch" style={{ background: color }} />
          <span className="cluster-id" style={{ color }}>Cluster {cluster.cluster_id}</span>
        </div>
        <span className="cluster-count">{cluster.site_count} sites</span>
      </div>

      <div className="cluster-stats">
        <div className="cstat">
          <span className="cstat-val">{peakHour}:00</span>
          <span className="cstat-label">Peak Hour</span>
        </div>
        <div className="cstat">
          <span className="cstat-val">{(amPeak * 100).toFixed(0)}%</span>
          <span className="cstat-label">AM Share</span>
        </div>
        <div className="cstat">
          <span className="cstat-val">{(pmPeak * 100).toFixed(0)}%</span>
          <span className="cstat-label">PM Share</span>
        </div>
        <div className="cstat">
          <span className="cstat-val">{cluster.avg_daily_volume ? `${(cluster.avg_daily_volume / 1000).toFixed(0)}k` : '—'}</span>
          <span className="cstat-label">Avg Vol</span>
        </div>
        <div className="cstat">
          <span className="cstat-val">{cluster.avg_silhouette.toFixed(2)}</span>
          <span className="cstat-label">Silhouette</span>
        </div>
      </div>

      <ResponsiveContainer width="100%" height={160}>
        <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="70%">
          <PolarGrid stroke="#2a2d35" />
          <PolarAngleAxis dataKey="hour" tick={{ fill: '#666', fontSize: 8 }} />
          <Radar
            dataKey={`c${cluster.cluster_id}`}
            stroke={color}
            fill={color}
            fillOpacity={0.2}
            strokeWidth={2}
          />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  )
}

function ClusterDetailView({ clusterId }: { clusterId: number }) {
  const { data, isLoading } = useClusterDetail(clusterId)
  const color = CLUSTER_COLORS[clusterId] ?? '#888'

  if (isLoading) return <div className="page-loading">Loading cluster detail...</div>
  if (!data) return null

  return (
    <div className="cluster-detail" style={{ borderColor: color }}>
      <div className="cluster-detail-header">
        <h3 style={{ color }}>Cluster {data.cluster_id}</h3>
        <span className="detail-sub">{data.sites.length} sites — avg silhouette: {data.avg_silhouette.toFixed(3)}</span>
      </div>
      <div className="cluster-sites-table">
        <table>
          <thead>
            <tr>
              <th>Site ID</th>
              <th>Intersection</th>
              <th>Region</th>
              <th>Detectors</th>
              <th>Avg Volume</th>
            </tr>
          </thead>
          <tbody>
            {data.sites.map((s) => (
              <tr key={s.site_id}>
                <td>{s.site_id}</td>
                <td>{s.intersection_name ?? '—'}</td>
                <td>{s.region}</td>
                <td>{s.detector_count ?? '—'}</td>
                <td>{s.avg_daily_volume ? Math.round(s.avg_daily_volume).toLocaleString() : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
