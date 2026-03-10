// React Query hooks for all API endpoints

import { useQuery } from '@tanstack/react-query'
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
  const params = new URLSearchParams({
    min_pearson: String(minPearson),
    limit: String(limit),
  })
  if (siteId !== undefined) params.set('site_id', String(siteId))
  return useQuery({
    queryKey: ['correlations', minPearson, limit, siteId],
    queryFn: () => fetchJson<CorrelationResponse>(`/correlations?${params}`),
  })
}

export function useModels() {
  return useQuery({
    queryKey: ['models'],
    queryFn: () => fetchJson<ModelSummary[]>('/models'),
  })
}

export function useModelSiteMetrics(modelId: string, sortBy = 'mae', limit = 50) {
  return useQuery({
    queryKey: ['model-sites', modelId, sortBy, limit],
    queryFn: () =>
      fetchJson<SiteMetric[]>(`/models/${modelId}/sites?sort_by=${sortBy}&limit=${limit}`),
    enabled: !!modelId,
  })
}
