import { useMemo, useState, useCallback } from 'react'
import MapGL, { Source, Layer, NavigationControl, Popup } from 'react-map-gl/maplibre'
import 'maplibre-gl/dist/maplibre-gl.css'
import {
  ScatterChart, Scatter, XAxis, YAxis, Tooltip as RTooltip,
  ResponsiveContainer, CartesianGrid, Cell, ZAxis,
} from 'recharts'
import { useCorrelations } from '../api/hooks'
import type { CorrelationPair } from '../api/types'
import type { MapLayerMouseEvent } from 'maplibre-gl'
import './CorrelationsPage.css'

const CARTO_DARK = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json'
const MELBOURNE = { longitude: 144.96, latitude: -37.81 }

function strengthColor(pearson: number): string {
  if (pearson >= 0.95) return '#34d399'
  if (pearson >= 0.90) return '#60a5fa'
  if (pearson >= 0.85) return '#a78bfa'
  return '#facc15'
}

function strengthLabel(pearson: number): string {
  if (pearson >= 0.95) return 'Very strong'
  if (pearson >= 0.90) return 'Strong'
  if (pearson >= 0.85) return 'Moderate'
  return 'Weak'
}

export default function CorrelationsPage() {
  const [minPearson, setMinPearson] = useState(0.85)
  const [limit, setLimit] = useState(200)
  const { data, isLoading } = useCorrelations(minPearson, limit)
  const [hoveredPair, setHoveredPair] = useState<CorrelationPair | null>(null)

  const pairs = data?.pairs ?? []

  // Summary stats
  const stats = useMemo(() => {
    if (pairs.length === 0) return null
    const avgPearson = pairs.reduce((s, p) => s + p.pearson_daily, 0) / pairs.length
    const avgCosine = pairs.reduce((s, p) => s + p.cosine_hourly, 0) / pairs.length
    const withLag = pairs.filter((p) => p.lag_minutes != null && p.lag_minutes > 0)
    const avgLag = withLag.length > 0
      ? withLag.reduce((s, p) => s + p.lag_minutes!, 0) / withLag.length
      : 0
    const uniqueSites = new Set(pairs.flatMap((p) => [p.site_a, p.site_b])).size
    return { avgPearson, avgCosine, avgLag, uniqueSites }
  }, [pairs])

  // GeoJSON for connection lines
  const linesGeoJson = useMemo(() => {
    const features = pairs
      .filter((p) => p.site_a_lat != null && p.site_b_lat != null &&
                     p.site_a_lon != null && p.site_b_lon != null)
      .map((p, i) => ({
        type: 'Feature' as const,
        properties: {
          idx: i,
          pearson: p.pearson_daily,
          site_a: p.site_a,
          site_b: p.site_b,
        },
        geometry: {
          type: 'LineString' as const,
          coordinates: [
            [p.site_a_lon!, p.site_a_lat!],
            [p.site_b_lon!, p.site_b_lat!],
          ],
        },
      }))
    return { type: 'FeatureCollection' as const, features }
  }, [pairs])

  // GeoJSON for site endpoint dots
  const dotsGeoJson = useMemo(() => {
    const siteMap = new globalThis.Map<number, [number, number]>()
    for (const p of pairs) {
      if (p.site_a_lat != null && p.site_a_lon != null)
        siteMap.set(p.site_a, [p.site_a_lon, p.site_a_lat])
      if (p.site_b_lat != null && p.site_b_lon != null)
        siteMap.set(p.site_b, [p.site_b_lon, p.site_b_lat])
    }
    const features = Array.from(siteMap.entries()).map(([siteId, [lon, lat]]) => ({
      type: 'Feature' as const,
      properties: { site_id: siteId },
      geometry: { type: 'Point' as const, coordinates: [lon, lat] },
    }))
    return { type: 'FeatureCollection' as const, features }
  }, [pairs])

  // Scatter plot data
  const scatterData = useMemo(
    () => pairs.map((p) => ({
      pearson: Number(p.pearson_daily.toFixed(3)),
      cosine: Number(p.cosine_hourly.toFixed(3)),
      lag: p.lag_minutes ?? 0,
      site_a: p.site_a,
      site_b: p.site_b,
    })),
    [pairs],
  )

  const onMapHover = useCallback(
    (e: MapLayerMouseEvent) => {
      const feat = e.features?.[0]
      if (feat?.properties?.idx !== undefined) {
        setHoveredPair(pairs[feat.properties.idx as number] ?? null)
      } else {
        setHoveredPair(null)
      }
    },
    [pairs],
  )

  return (
    <div className="correlations-page">
      <h2>Linked Sites — Origin-Destination Inference</h2>
      <p className="page-desc">
        Sites with highly correlated daily traffic patterns likely share commuter flows.
        Lines on the map connect inferred origin-destination pairs — thicker and greener
        means stronger correlation. The scatter plot reveals how daily and hourly similarity
        relate across all pairs.
      </p>

      {/* Controls + KPIs */}
      <div className="corr-controls">
        <div className="control-group">
          <label className="control-label">Min Correlation</label>
          <div className="slider-row">
            <input
              type="range" min={0.5} max={0.99} step={0.01}
              value={minPearson}
              onChange={(e) => setMinPearson(Number(e.target.value))}
            />
            <span className="slider-value">{minPearson.toFixed(2)}</span>
          </div>
        </div>
        <div className="control-group">
          <label className="control-label">Max Pairs</label>
          <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={200}>200</option>
            <option value={500}>500</option>
          </select>
        </div>
        {stats && (
          <div className="corr-kpis">
            <div className="kpi"><span className="kpi-val">{pairs.length}</span><span className="kpi-lbl">Pairs</span></div>
            <div className="kpi"><span className="kpi-val">{stats.uniqueSites}</span><span className="kpi-lbl">Sites</span></div>
            <div className="kpi"><span className="kpi-val">{stats.avgPearson.toFixed(3)}</span><span className="kpi-lbl">Avg Pearson</span></div>
            <div className="kpi"><span className="kpi-val">{stats.avgCosine.toFixed(3)}</span><span className="kpi-lbl">Avg Cosine</span></div>
            {stats.avgLag > 0 && (
              <div className="kpi"><span className="kpi-val">{stats.avgLag.toFixed(0)}m</span><span className="kpi-lbl">Avg Lag</span></div>
            )}
          </div>
        )}
      </div>

      {isLoading ? (
        <div className="page-loading">Loading correlations...</div>
      ) : (
        <>
          {/* Viz row: map + scatter */}
          <div className="corr-viz-row">
            {/* Connection map */}
            <div className="viz-card corr-map-card">
              <h3>Connection Map</h3>
              <p className="viz-sub">Lines connect correlated sites — colour and thickness indicate strength</p>
              <div className="corr-map-wrap">
                <MapGL
                  initialViewState={{ ...MELBOURNE, zoom: 10 }}
                  style={{ width: '100%', height: '100%' }}
                  mapStyle={CARTO_DARK}
                  interactiveLayerIds={['corr-lines']}
                  onMouseMove={onMapHover}
                  onMouseLeave={() => setHoveredPair(null)}
                >
                  <NavigationControl position="top-right" />

                  <Source id="corr-lines" type="geojson" data={linesGeoJson}>
                    <Layer
                      id="corr-lines"
                      type="line"
                      paint={{
                        'line-color': [
                          'interpolate', ['linear'], ['get', 'pearson'],
                          0.80, '#facc15',
                          0.85, '#a78bfa',
                          0.90, '#60a5fa',
                          0.95, '#34d399',
                        ] as any,
                        'line-width': [
                          'interpolate', ['linear'], ['get', 'pearson'],
                          0.80, 0.5,
                          0.90, 1.5,
                          0.95, 3,
                        ] as any,
                        'line-opacity': 0.6,
                      }}
                    />
                  </Source>

                  <Source id="corr-dots" type="geojson" data={dotsGeoJson}>
                    <Layer
                      id="corr-dots"
                      type="circle"
                      paint={{
                        'circle-radius': 3,
                        'circle-color': '#e0e0e0',
                        'circle-opacity': 0.7,
                        'circle-stroke-width': 0.5,
                        'circle-stroke-color': 'rgba(0,0,0,0.4)',
                      }}
                    />
                  </Source>

                  {hoveredPair && hoveredPair.site_a_lat != null && hoveredPair.site_b_lat != null && (
                    <Popup
                      latitude={(hoveredPair.site_a_lat + hoveredPair.site_b_lat) / 2}
                      longitude={((hoveredPair.site_a_lon ?? 0) + (hoveredPair.site_b_lon ?? 0)) / 2}
                      closeButton={false}
                      anchor="bottom"
                      className="corr-popup"
                    >
                      <div className="corr-popup-content">
                        <strong>{hoveredPair.site_a} ↔ {hoveredPair.site_b}</strong>
                        <div>Pearson: <b>{hoveredPair.pearson_daily.toFixed(3)}</b></div>
                        <div>Cosine: {hoveredPair.cosine_hourly.toFixed(3)}</div>
                        {hoveredPair.lag_minutes != null && hoveredPair.lag_minutes > 0 && (
                          <div>Lag: {hoveredPair.lag_minutes} min</div>
                        )}
                      </div>
                    </Popup>
                  )}

                  <div className="map-strength-legend">
                    <span className="legend-title">Strength</span>
                    <div className="strength-bar" />
                    <div className="strength-labels"><span>0.80</span><span>0.90</span><span>0.95+</span></div>
                  </div>
                </MapGL>
              </div>
            </div>

            {/* Pearson vs Cosine scatter */}
            <div className="viz-card">
              <h3>Pearson vs Cosine Similarity</h3>
              <p className="viz-sub">Each dot is one site pair — high on both axes = strongest link</p>
              <ResponsiveContainer width="100%" height={340}>
                <ScatterChart margin={{ top: 10, right: 10, bottom: 20, left: 10 }}>
                  <CartesianGrid stroke="#2a2d35" strokeDasharray="3 3" />
                  <XAxis
                    type="number" dataKey="pearson" name="Pearson"
                    tick={{ fill: '#888', fontSize: 10 }}
                    domain={[minPearson, 1]}
                    label={{ value: 'Pearson (daily)', position: 'bottom', fill: '#777', fontSize: 11, dy: 10 }}
                  />
                  <YAxis
                    type="number" dataKey="cosine" name="Cosine"
                    tick={{ fill: '#888', fontSize: 10 }}
                    domain={['auto', 1]}
                    label={{ value: 'Cosine (hourly)', angle: -90, position: 'insideLeft', fill: '#777', fontSize: 11, dx: -5 }}
                  />
                  <ZAxis type="number" dataKey="lag" range={[30, 120]} />
                  <RTooltip
                    contentStyle={{ background: '#1a1d23', border: '1px solid #2a2d35', fontSize: '0.78rem' }}
                    labelFormatter={(_, payload) => {
                      const d = payload?.[0]?.payload
                      return d ? `Site ${d.site_a} ↔ ${d.site_b}` : ''
                    }}
                    formatter={(value, name) => {
                      if (name === 'Pearson') return [String(value), 'Pearson (daily)']
                      if (name === 'Cosine') return [String(value), 'Cosine (hourly)']
                      return [String(value), String(name)]
                    }}
                  />
                  <Scatter data={scatterData}>
                    {scatterData.map((d, i) => (
                      <Cell key={i} fill={strengthColor(d.pearson)} fillOpacity={0.7} />
                    ))}
                  </Scatter>
                </ScatterChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Table */}
          <h3 className="section-title">All Linked Pairs</h3>
          <div className="corr-table-wrapper">
            <table className="corr-table">
              <thead>
                <tr>
                  <th>Site A</th>
                  <th>Site B</th>
                  <th>Strength</th>
                  <th>Pearson (daily)</th>
                  <th>Cosine (hourly)</th>
                  <th>Lag</th>
                </tr>
              </thead>
              <tbody>
                {pairs.map((p, i) => (
                  <tr
                    key={i}
                    onMouseEnter={() => setHoveredPair(p)}
                    onMouseLeave={() => setHoveredPair(null)}
                    className={
                      hoveredPair?.site_a === p.site_a && hoveredPair?.site_b === p.site_b
                        ? 'row-hovered' : ''
                    }
                  >
                    <td className="td-site">{p.site_a}</td>
                    <td className="td-site">{p.site_b}</td>
                    <td>
                      <span
                        className="strength-badge"
                        style={{ color: strengthColor(p.pearson_daily), borderColor: strengthColor(p.pearson_daily) }}
                      >
                        {strengthLabel(p.pearson_daily)}
                      </span>
                    </td>
                    <td>
                      <div className="bar-cell">
                        <div
                          className="bar-fill"
                          style={{
                            width: `${((p.pearson_daily - minPearson) / (1 - minPearson)) * 100}%`,
                            background: strengthColor(p.pearson_daily),
                          }}
                        />
                        <span className="bar-text">{p.pearson_daily.toFixed(3)}</span>
                      </div>
                    </td>
                    <td>
                      <div className="bar-cell">
                        <div className="bar-fill" style={{ width: `${p.cosine_hourly * 100}%`, background: '#a78bfa' }} />
                        <span className="bar-text">{p.cosine_hourly.toFixed(3)}</span>
                      </div>
                    </td>
                    <td className="td-lag">
                      {p.lag_minutes != null && p.lag_minutes > 0
                        ? `${p.lag_minutes} min`
                        : <span className="no-lag">Sync</span>
                      }
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}
