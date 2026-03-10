import { useState, useMemo, useCallback, useRef, useEffect } from 'react'
import { useSites, useOverview } from '../api/hooks'
import SiteMap, { MapLegend, type ColorMode } from '../components/SiteMap'
import SitePanel from '../components/SitePanel'
import type { SiteSummary } from '../api/types'
import './MapPage.css'

export default function MapPage() {
  const { data: overview } = useOverview()
  const { data: sitesData, isLoading } = useSites()
  const [selectedSiteId, setSelectedSiteId] = useState<number | null>(null)
  const [colorMode, setColorMode] = useState<ColorMode>('volume')
  const [focusSiteId, setFocusSiteId] = useState<number | null>(null)

  // Search state
  const [searchQuery, setSearchQuery] = useState('')
  const [showResults, setShowResults] = useState(false)
  const searchRef = useRef<HTMLDivElement>(null)

  // Close search dropdown on outside click
  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setShowResults(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Filter sites based on search query
  const searchResults = useMemo(() => {
    if (!searchQuery.trim() || !sitesData?.sites) return []
    const q = searchQuery.toLowerCase().trim()
    return sitesData.sites
      .filter((s: SiteSummary) => {
        const idMatch = String(s.site_id).includes(q)
        const nameMatch = s.intersection_name?.toLowerCase().includes(q)
        return idMatch || nameMatch
      })
      .slice(0, 12)
  }, [searchQuery, sitesData])

  const handleSelectSite = useCallback((siteId: number) => {
    setSelectedSiteId(siteId)
    setFocusSiteId(siteId)
    setSearchQuery('')
    setShowResults(false)
  }, [])

  return (
    <div className="map-page">
      <div className="map-container">
        {/* Stats bar */}
        {overview && (
          <div className="stats-bar">
            <span><strong>{overview.sites_with_coords.toLocaleString()}</strong> mapped intersections</span>
            <span className="stats-divider" />
            <span><strong>{overview.total_forecasts.toLocaleString()}</strong> forecasts</span>
            {overview.best_model_mape != null && (
              <span className="stats-accent">{(100 - overview.best_model_mape).toFixed(1)}% forecast accuracy</span>
            )}
            <span className="stats-divider" />
            <span className="stats-hint">Click a site to view its forecast</span>
          </div>
        )}

        {/* Search bar */}
        <div className="site-search" ref={searchRef}>
          <div className="search-input-wrap">
            <svg className="search-icon" viewBox="0 0 20 20" fill="currentColor" width="16" height="16">
              <path fillRule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clipRule="evenodd" />
            </svg>
            <input
              type="text"
              placeholder="Search site ID or intersection..."
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value)
                setShowResults(true)
              }}
              onFocus={() => searchQuery && setShowResults(true)}
            />
            {searchQuery && (
              <button className="search-clear" onClick={() => { setSearchQuery(''); setShowResults(false) }}>&times;</button>
            )}
          </div>
          {showResults && searchResults.length > 0 && (
            <div className="search-results">
              {searchResults.map((s: SiteSummary) => (
                <button
                  key={s.site_id}
                  className="search-result-item"
                  onClick={() => handleSelectSite(s.site_id)}
                >
                  <span className="result-id">Site {s.site_id}</span>
                  {s.intersection_name && (
                    <span className="result-name">{s.intersection_name}</span>
                  )}
                  <span className="result-meta">{s.region} {s.cluster_id != null ? `· Cluster ${s.cluster_id}` : ''}</span>
                </button>
              ))}
            </div>
          )}
          {showResults && searchQuery.trim() && searchResults.length === 0 && (
            <div className="search-results">
              <div className="search-no-results">No sites found</div>
            </div>
          )}
        </div>

        {isLoading ? (
          <div className="map-loading">Loading sites...</div>
        ) : (
          <>
            <SiteMap
              sites={sitesData?.sites ?? []}
              onSiteClick={(id) => { setSelectedSiteId(id); setFocusSiteId(id) }}
              selectedSiteId={selectedSiteId}
              colorMode={colorMode}
              focusSiteId={focusSiteId}
            />
            <MapLegend colorMode={colorMode} onModeChange={setColorMode} />
          </>
        )}
      </div>

      {selectedSiteId !== null && (
        <SitePanel
          siteId={selectedSiteId}
          onClose={() => setSelectedSiteId(null)}
        />
      )}
    </div>
  )
}
