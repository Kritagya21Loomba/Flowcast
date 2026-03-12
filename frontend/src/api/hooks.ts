// React Query hooks for all API endpoints

import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'
import fetchJson from './client'
import type {
  OverviewStats,
  SiteListResponse,
  SiteDetailResponse,
  SiteForecastResponse,
  ClusterSummary,
  ClusterDetail,
  CorrelationResponse,
  ModelSummary,
  SiteMetric,
} from './types'

export function useOverview() {
  return useQuery({
    queryKey: ['overview'],
    queryFn: () => fetchJson<OverviewStats>('/overview'),
  })
}

export function useSites(region?: string, clusterId?: number) {
  const params = new URLSearchParams()
  if (region) params.set('region', region)
  if (clusterId !== undefined) params.set('cluster_id', String(clusterId))
  const qs = params.toString()
  return useQuery({
    queryKey: ['sites', region, clusterId],
    queryFn: () => fetchJson<SiteListResponse>(`/sites${qs ? `?${qs}` : ''}`),
  })
}

export function useSiteDetail(siteId: number, days = 90) {
  return useQuery({
    queryKey: ['site', siteId, days],
    queryFn: () => fetchJson<SiteDetailResponse>(`/sites/${siteId}?days=${days}`),
  })
}

export function useSiteForecasts(siteId: number) {
  return useQuery({
    queryKey: ['forecasts', siteId],
    queryFn: () => fetchJson<SiteForecastResponse>(`/sites/${siteId}/forecasts`),
  })
}

export function useClusters() {
  return useQuery({
    queryKey: ['clusters'],
    queryFn: () => fetchJson<ClusterSummary[]>('/clusters'),
  })
}

export function useClusterDetail(clusterId: number) {
  return useQuery({
    queryKey: ['cluster', clusterId],
    queryFn: () => fetchJson<ClusterDetail>(`/clusters/${clusterId}`),
  })
}

export function useCorrelations(minPearson = 0.8, limit = 200, siteId?: number) {
  // In static mode, we fetch all correlations and filter client-side
  const { data: rawData, ...rest } = useQuery({
    queryKey: ['correlations-raw'],
    queryFn: () => fetchJson<CorrelationResponse>('/correlations'),
  })

  const data = useMemo(() => {
    if (!rawData) return undefined
    let pairs = rawData.pairs.filter(p => p.pearson_daily >= minPearson)
    if (siteId !== undefined) {
      pairs = pairs.filter(p => p.site_a === siteId || p.site_b === siteId)
    }
    pairs = pairs.slice(0, limit)
    return { pairs, count: pairs.length }
  }, [rawData, minPearson, limit, siteId])

  return { data, ...rest }
}

export function useModels() {
  return useQuery({
    queryKey: ['models'],
    queryFn: () => fetchJson<ModelSummary[]>('/models'),
  })
}

export function useModelSiteMetrics(modelId: string, sortBy = 'mae', limit = 50) {
  // In static mode, we fetch all metrics and sort/limit client-side
  const { data: rawData, ...rest } = useQuery({
    queryKey: ['model-sites-raw', modelId],
    queryFn: () => fetchJson<SiteMetric[]>(`/models/${modelId}/sites`),
    enabled: !!modelId,
  })

  const data = useMemo(() => {
    if (!rawData) return undefined
    const sorted = [...rawData].sort((a, b) => {
      const av = a[sortBy as keyof SiteMetric] as number ?? 0
      const bv = b[sortBy as keyof SiteMetric] as number ?? 0
      return av - bv
    })
    return sorted.slice(0, limit)
  }, [rawData, sortBy, limit])

  return { data, ...rest }
}
