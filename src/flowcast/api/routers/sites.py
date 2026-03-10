"""Sites endpoints — listing and detail."""

from fastapi import APIRouter, Depends, Query
import duckdb

from flowcast.api.deps import get_db
from flowcast.api.schemas import (
    SiteSummary, SiteListResponse, DailyVolume, SiteDetailResponse,
)

router = APIRouter()


@router.get("/sites", response_model=SiteListResponse)
def list_sites(
    region: str | None = None,
    cluster_id: int | None = None,
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> SiteListResponse:
    where_clauses = []
    if region:
        where_clauses.append(f"ss.region = '{region}'")
    if cluster_id is not None:
        where_clauses.append(f"sc.cluster_id = {cluster_id}")

    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    rows = con.execute(f"""
        SELECT
            ss.site_id, ss.region, ss.latitude, ss.longitude,
            ss.intersection_name, ss.detector_count, ss.first_seen, ss.last_seen,
            sc.cluster_id,
            AVG(td.total_volume) AS avg_daily_volume
        FROM signal_sites ss
        LEFT JOIN site_clusters sc ON ss.site_id = sc.site_id
        LEFT JOIN traffic_daily td ON ss.site_id = td.site_id
        {where}
        GROUP BY ss.site_id, ss.region, ss.latitude, ss.longitude,
                 ss.intersection_name, ss.detector_count, ss.first_seen,
                 ss.last_seen, sc.cluster_id
        ORDER BY ss.site_id
    """).fetchall()

    sites = [
        SiteSummary(
            site_id=r[0], region=r[1], latitude=r[2], longitude=r[3],
            intersection_name=r[4], detector_count=r[5],
            first_seen=r[6], last_seen=r[7], cluster_id=r[8],
            avg_daily_volume=round(r[9], 0) if r[9] else None,
        )
        for r in rows
    ]
    return SiteListResponse(sites=sites, count=len(sites))


@router.get("/sites/{site_id}", response_model=SiteDetailResponse)
def get_site_detail(
    site_id: int,
    days: int = Query(default=90, ge=1, le=365),
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> SiteDetailResponse:
    # Site info
    row = con.execute("""
        SELECT ss.site_id, ss.region, ss.latitude, ss.longitude,
               ss.intersection_name, ss.detector_count, ss.first_seen, ss.last_seen,
               sc.cluster_id,
               AVG(td.total_volume) AS avg_daily_volume
        FROM signal_sites ss
        LEFT JOIN site_clusters sc ON ss.site_id = sc.site_id
        LEFT JOIN traffic_daily td ON ss.site_id = td.site_id
        WHERE ss.site_id = ?
        GROUP BY ss.site_id, ss.region, ss.latitude, ss.longitude,
                 ss.intersection_name, ss.detector_count, ss.first_seen,
                 ss.last_seen, sc.cluster_id
    """, [site_id]).fetchone()

    if not row:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Site {site_id} not found")

    site = SiteSummary(
        site_id=row[0], region=row[1], latitude=row[2], longitude=row[3],
        intersection_name=row[4], detector_count=row[5],
        first_seen=row[6], last_seen=row[7], cluster_id=row[8],
        avg_daily_volume=round(row[9], 0) if row[9] else None,
    )

    # Daily volumes (last N days)
    daily_rows = con.execute("""
        SELECT date, total_volume, peak_hour, peak_hour_volume
        FROM traffic_daily
        WHERE site_id = ?
        ORDER BY date DESC
        LIMIT ?
    """, [site_id, days]).fetchall()

    daily_volumes = [
        DailyVolume(date=r[0], total_volume=r[1], peak_hour=r[2], peak_hour_volume=r[3])
        for r in reversed(daily_rows)
    ]

    # Hourly profile from site_clusters
    profile_row = con.execute(
        "SELECT profile_vector FROM site_clusters WHERE site_id = ?", [site_id]
    ).fetchone()
    hourly_profile = list(profile_row[0]) if profile_row and profile_row[0] else None

    return SiteDetailResponse(
        site=site, daily_volumes=daily_volumes, hourly_profile=hourly_profile,
    )
