"""Clusters endpoints."""

from fastapi import APIRouter, Depends, HTTPException
import duckdb

from flowcast.api.deps import get_db
from flowcast.api.schemas import ClusterSummary, ClusterDetail, SiteSummary

router = APIRouter()


@router.get("/clusters", response_model=list[ClusterSummary])
def list_clusters(
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[ClusterSummary]:
    # Get cluster stats
    stats = con.execute("""
        SELECT sc.cluster_id, COUNT(*) AS site_count,
               ROUND(AVG(sc.silhouette_score), 3) AS avg_sil,
               ROUND(AVG(td.avg_vol), 0) AS avg_vol
        FROM site_clusters sc
        LEFT JOIN (
            SELECT site_id, AVG(total_volume) AS avg_vol
            FROM traffic_daily GROUP BY site_id
        ) td ON sc.site_id = td.site_id
        GROUP BY sc.cluster_id
        ORDER BY sc.cluster_id
    """).fetchall()

    # Compute average profile per cluster in Python
    all_profiles = con.execute("""
        SELECT cluster_id, profile_vector
        FROM site_clusters
        ORDER BY cluster_id
    """).fetchall()

    # Group profiles by cluster
    from collections import defaultdict
    cluster_profiles: dict[int, list[list[float]]] = defaultdict(list)
    for cid, pvec in all_profiles:
        if pvec:
            cluster_profiles[cid].append(list(pvec))

    results = []
    for row in stats:
        cid = row[0]
        profiles = cluster_profiles.get(cid, [])
        if profiles:
            avg_profile = [
                sum(p[i] for p in profiles) / len(profiles)
                for i in range(24)
            ]
        else:
            avg_profile = [0.0] * 24

        results.append(ClusterSummary(
            cluster_id=cid,
            site_count=row[1],
            avg_silhouette=row[2],
            avg_daily_volume=row[3],
            profile=avg_profile,
        ))

    return results


@router.get("/clusters/{cluster_id}", response_model=ClusterDetail)
def get_cluster_detail(
    cluster_id: int,
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> ClusterDetail:
    # Get sites in this cluster
    rows = con.execute("""
        SELECT ss.site_id, ss.region, ss.latitude, ss.longitude,
               ss.intersection_name, ss.detector_count, ss.first_seen, ss.last_seen,
               sc.cluster_id,
               AVG(td.total_volume) AS avg_daily_volume
        FROM site_clusters sc
        JOIN signal_sites ss ON sc.site_id = ss.site_id
        LEFT JOIN traffic_daily td ON ss.site_id = td.site_id
        WHERE sc.cluster_id = ?
        GROUP BY ss.site_id, ss.region, ss.latitude, ss.longitude,
                 ss.intersection_name, ss.detector_count, ss.first_seen,
                 ss.last_seen, sc.cluster_id
        ORDER BY ss.site_id
    """, [cluster_id]).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")

    sites = [
        SiteSummary(
            site_id=r[0], region=r[1], latitude=r[2], longitude=r[3],
            intersection_name=r[4], detector_count=r[5],
            first_seen=r[6], last_seen=r[7], cluster_id=r[8],
            avg_daily_volume=round(r[9], 0) if r[9] else None,
        )
        for r in rows
    ]

    # Avg profile and silhouette
    profile_rows = con.execute("""
        SELECT profile_vector, silhouette_score
        FROM site_clusters WHERE cluster_id = ?
    """, [cluster_id]).fetchall()

    profiles = [list(r[0]) for r in profile_rows if r[0]]
    avg_sil = sum(r[1] for r in profile_rows) / len(profile_rows) if profile_rows else 0

    if profiles:
        avg_profile = [
            sum(p[i] for p in profiles) / len(profiles) for i in range(24)
        ]
    else:
        avg_profile = [0.0] * 24

    return ClusterDetail(
        cluster_id=cluster_id,
        sites=sites,
        profile=avg_profile,
        avg_silhouette=round(avg_sil, 3),
    )
