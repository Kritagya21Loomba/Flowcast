import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import MapGL, { Source, Layer, NavigationControl, Popup } from 'react-map-gl/maplibre'
import type { MapLayerMouseEvent, MapRef } from 'react-map-gl/maplibre'
import 'maplibre-gl/dist/maplibre-gl.css'
import type { SiteSummary } from '../api/types'
import './SiteMap.css'

const MELBOURNE_CENTER = { longitude: 144.96, latitude: -37.81 }
const CARTO_DARK = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json'

// Cluster color palette
const CLUSTER_PALETTE: [number, string][] = [
  [0, '#60a5fa'], [1, '#f97316'], [2, '#a78bfa'], [3, '#34d399'],
  [4, '#fb7185'], [5, '#facc15'], [6, '#2dd4bf'], [7, '#e879f9'],
]

const CLUSTER_LABELS: Record<number, string> = {
  0: 'Cluster 0', 1: 'Cluster 1', 2: 'Cluster 2', 3: 'Cluster 3',
  4: 'Cluster 4', 5: 'Cluster 5', 6: 'Cluster 6', 7: 'Cluster 7',
}

// Region colors
const REGION_PALETTE: [string, string][] = [
  ['NW', '#60a5fa'], ['NE', '#f97316'], ['SE', '#a78bfa'],
  ['SW', '#34d399'], ['CENT', '#fb7185'], ['INNE', '#facc15'],
]

// Volume color ramp (blue → yellow → red)
const VOLUME_STOPS: [number, string][] = [
  [0, '#1e3a5f'], [10000, '#2563eb'], [30000, '#60a5fa'],
  [60000, '#facc15'], [100000, '#f97316'], [200000, '#ef4444'],
]

export type ColorMode = 'cluster' | 'region' | 'volume'

interface Props {
  sites: SiteSummary[]
  onSiteClick?: (siteId: number) => void
  selectedSiteId?: number | null
  colorMode?: ColorMode
  focusSiteId?: number | null
}

export default function SiteMap({
  sites,
  onSiteClick,
  selectedSiteId,
  colorMode = 'cluster',
  focusSiteId,
}: Props) {
  const [hoveredSite, setHoveredSite] = useState<SiteSummary | null>(null)
  const mapRef = useRef<MapRef>(null)

  // Build GeoJSON FeatureCollection — recomputed only when sites change
  const geojson = useMemo(() => {
    const features = sites
      .filter((s) => s.latitude != null && s.longitude != null)
      .map((s) => ({
        type: 'Feature' as const,
        geometry: {
          type: 'Point' as const,
          coordinates: [s.longitude!, s.latitude!],
        },
        properties: {
          site_id: s.site_id,
          region: s.region,
          cluster_id: s.cluster_id ?? -1,
          intersection_name: s.intersection_name ?? '',
          detector_count: s.detector_count ?? 1,
          avg_daily_volume: s.avg_daily_volume ?? 0,
          selected: s.site_id === selectedSiteId ? 1 : 0,
        },
      }))
    return { type: 'FeatureCollection' as const, features }
  }, [sites, selectedSiteId])

  // Color expression based on mode
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const circleColor = useMemo((): any => {
    if (colorMode === 'region') {
      return [
        'match', ['get', 'region'],
        ...REGION_PALETTE.flatMap(([r, c]) => [r, c]),
        '#555',
      ]
    }
    if (colorMode === 'volume') {
      return [
        'interpolate', ['linear'], ['get', 'avg_daily_volume'],
        ...VOLUME_STOPS.flatMap(([v, c]) => [v, c]),
      ]
    }
    // Default: cluster
    return [
      'match', ['get', 'cluster_id'],
      ...CLUSTER_PALETTE.flatMap(([id, c]) => [id, c]),
      '#555',
    ]
  }, [colorMode])

  // Circle radius: scale by avg_daily_volume
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const circleRadius: any = [
    'interpolate', ['linear'], ['zoom'],
    8, ['interpolate', ['linear'], ['get', 'avg_daily_volume'], 0, 2, 50000, 4, 200000, 7],
    14, ['interpolate', ['linear'], ['get', 'avg_daily_volume'], 0, 4, 50000, 10, 200000, 18],
  ]

  // Lookup map for hover/click
  const siteById = useMemo(() => {
    const lookup = new globalThis.Map<number, SiteSummary>()
    for (const s of sites) lookup.set(s.site_id, s)
    return lookup
  }, [sites])

  // Fly to site when focusSiteId changes
  useEffect(() => {
    if (focusSiteId == null) return
    const site = siteById.get(focusSiteId)
    if (site?.latitude != null && site?.longitude != null) {
      mapRef.current?.flyTo({
        center: [site.longitude!, site.latitude!],
        zoom: 14,
        duration: 1200,
      })
    }
  }, [focusSiteId, siteById])

  const onLayerClick = useCallback(
    (e: MapLayerMouseEvent) => {
      const feature = e.features?.[0]
      if (feature?.properties?.site_id) {
        onSiteClick?.(feature.properties.site_id as number)
      }
    },
    [onSiteClick],
  )

  const onLayerHover = useCallback(
    (e: MapLayerMouseEvent) => {
      const feature = e.features?.[0]
      if (feature?.properties?.site_id) {
        const site = siteById.get(feature.properties.site_id as number)
        setHoveredSite(site ?? null)
      }
    },
    [siteById],
  )

  const onLayerLeave = useCallback(() => setHoveredSite(null), [])

  return (
    <MapGL
      ref={mapRef}
      initialViewState={{ ...MELBOURNE_CENTER, zoom: 10 }}
      style={{ width: '100%', height: '100%' }}
      mapStyle={CARTO_DARK}
      interactiveLayerIds={['sites-circle']}
      onClick={onLayerClick}
      onMouseMove={onLayerHover}
      onMouseLeave={onLayerLeave}
      cursor={hoveredSite ? 'pointer' : ''}
    >
      <NavigationControl position="top-right" />

      <Source id="sites" type="geojson" data={geojson}>
        {/* Outer glow for selected site */}
        <Layer
          id="sites-selected-glow"
          type="circle"
          filter={['==', ['get', 'selected'], 1]}
          paint={{
            'circle-radius': 16,
            'circle-color': '#60a5fa',
            'circle-opacity': 0.25,
            'circle-blur': 0.8,
          }}
        />

        {/* Main circle layer */}
        <Layer
          id="sites-circle"
          type="circle"
          paint={{
            'circle-radius': circleRadius,
            'circle-color': circleColor,
            'circle-opacity': 0.85,
            'circle-stroke-width': [
              'case', ['==', ['get', 'selected'], 1], 2, 0.5,
            ] as any,
            'circle-stroke-color': [
              'case', ['==', ['get', 'selected'], 1], '#ffffff', 'rgba(0,0,0,0.4)',
            ] as any,
          }}
        />
      </Source>

      {hoveredSite && hoveredSite.latitude != null && hoveredSite.longitude != null && (
        <Popup
          latitude={hoveredSite.latitude}
          longitude={hoveredSite.longitude}
          closeButton={false}
          anchor="bottom"
          offset={12}
          className="site-popup"
        >
          <div className="popup-content">
            <strong>Site {hoveredSite.site_id}</strong>
            {hoveredSite.intersection_name && (
              <div className="popup-name">{hoveredSite.intersection_name}</div>
            )}
            <div className="popup-meta">
              <span>{hoveredSite.region}</span>
              <span>Cluster {hoveredSite.cluster_id ?? '—'}</span>
              {hoveredSite.avg_daily_volume != null && (
                <span>{Math.round(hoveredSite.avg_daily_volume).toLocaleString()} veh/day</span>
              )}
            </div>
          </div>
        </Popup>
      )}
    </MapGL>
  )
}

// Legend component — exported for use in MapPage
export function MapLegend({
  colorMode,
  onModeChange,
}: {
  colorMode: ColorMode
  onModeChange: (mode: ColorMode) => void
}) {
  const items = useMemo(() => {
    if (colorMode === 'region') {
      return REGION_PALETTE.map(([label, color]) => ({ label, color }))
    }
    if (colorMode === 'volume') {
      return VOLUME_STOPS.map(([val, color]) => ({
        label: val === 0 ? '0' : `${(val / 1000).toFixed(0)}k`,
        color,
      }))
    }
    return CLUSTER_PALETTE.map(([id, color]) => ({
      label: CLUSTER_LABELS[id] ?? `Cluster ${id}`,
      color,
    }))
  }, [colorMode])

  return (
    <div className="map-legend">
      <div className="legend-header">
        <span className="legend-title">Colour by</span>
        <div className="legend-toggle">
          {(['cluster', 'region', 'volume'] as const).map((m) => (
            <button
              key={m}
              className={colorMode === m ? 'active' : ''}
              onClick={() => onModeChange(m)}
            >
              {m === 'cluster' ? 'Cluster' : m === 'region' ? 'Region' : 'Volume'}
            </button>
          ))}
        </div>
      </div>
      <div className={`legend-items ${colorMode === 'volume' ? 'gradient' : ''}`}>
        {colorMode === 'volume' ? (
          <div className="legend-gradient">
            <div
              className="gradient-bar"
              style={{
                background: `linear-gradient(to right, ${VOLUME_STOPS.map(([, c]) => c).join(', ')})`,
              }}
            />
            <div className="gradient-labels">
              {VOLUME_STOPS.filter((_, i) => i % 2 === 0 || i === VOLUME_STOPS.length - 1).map(([val]) => (
                <span key={val}>{val === 0 ? '0' : `${(val / 1000).toFixed(0)}k`}</span>
              ))}
            </div>
          </div>
        ) : (
          items.map(({ label, color }) => (
            <div key={label} className="legend-item">
              <span className="legend-swatch" style={{ background: color }} />
              <span className="legend-label">{label}</span>
            </div>
          ))
        )}
      </div>
      <div className="legend-size-hint">
        Circle size = average daily volume
      </div>
    </div>
  )
}
