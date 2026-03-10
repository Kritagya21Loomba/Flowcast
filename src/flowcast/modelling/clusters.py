"""Temporal profile clustering of traffic sites."""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_samples

from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def _hour_sum_expr(hour: int) -> str:
    """Generate SQL for summing 4 V-columns for a given hour."""
    start = hour * 4
    cols = [f"COALESCE(V{start + i:02d},0)" for i in range(4)]
    return "+".join(cols)


def compute_site_hourly_profiles(
    con: duckdb.DuckDBPyConnection,
    site_ids: list[int],
) -> tuple[np.ndarray, list[int]]:
    """Compute average normalized hourly volume profile for each site.

    Returns:
        profiles: ndarray of shape (n_sites, 24), each row sums to ~1.0
        ordered_site_ids: list of site_ids matching row order
    """
    site_list = ", ".join(str(s) for s in site_ids)

    # Build 24 hour-sum expressions
    hour_sums = [f"SUM({_hour_sum_expr(h)}) AS h{h:02d}" for h in range(24)]
    hour_cols = ", ".join(hour_sums)
    h_refs = [f"h{h:02d}" for h in range(24)]
    total_expr = "+".join(h_refs)

    sql = f"""
        WITH site_day_hourly AS (
            SELECT
                nb_scats_site AS site_id,
                {hour_cols}
            FROM traffic_volumes
            WHERE nb_scats_site IN ({site_list})
            GROUP BY nb_scats_site, qt_interval_count
        ),
        site_avg AS (
            SELECT
                site_id,
                {', '.join(f'AVG({h}) AS {h}' for h in h_refs)}
            FROM site_day_hourly
            GROUP BY site_id
        )
        SELECT
            site_id,
            {', '.join(f'{h} / NULLIF({total_expr}, 0) AS {h}' for h in h_refs)}
        FROM site_avg
        ORDER BY site_id
    """

    df = con.execute(sql).fetchdf()
    ordered_ids = df["site_id"].tolist()
    profiles = df[h_refs].values.astype(np.float64)

    # Replace any NaN (from division by zero) with uniform distribution
    nan_mask = np.isnan(profiles).any(axis=1)
    if nan_mask.any():
        profiles[nan_mask] = 1.0 / 24
        log.warning("nan_profiles_replaced", count=int(nan_mask.sum()))

    log.info("hourly_profiles_computed", sites=len(ordered_ids))
    return profiles, ordered_ids


def cluster_sites(
    profiles: np.ndarray,
    site_ids: list[int],
    n_clusters: int = 8,
) -> pd.DataFrame:
    """Cluster sites by hourly profile similarity using KMeans.

    Returns DataFrame with columns: site_id, cluster_id, silhouette_score
    """
    # Clamp n_clusters to number of sites
    k = min(n_clusters, len(site_ids))

    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = kmeans.fit_predict(profiles)

    # Silhouette requires 2 <= n_labels < n_samples
    if 2 <= k < len(site_ids):
        sil_scores = silhouette_samples(profiles, labels)
    else:
        sil_scores = np.zeros(len(site_ids))

    result = pd.DataFrame({
        "site_id": site_ids,
        "cluster_id": labels.astype(int),
        "silhouette_score": sil_scores.astype(float),
    })

    avg_sil = float(sil_scores.mean())
    log.info("sites_clustered", n_clusters=k, avg_silhouette=round(avg_sil, 3))
    return result


def store_clusters(
    con: duckdb.DuckDBPyConnection,
    clusters_df: pd.DataFrame,
    profiles: np.ndarray,
    site_ids: list[int],
) -> None:
    """Write cluster assignments and profiles to site_clusters table."""
    con.execute("DELETE FROM site_clusters")

    # Build a mapping of site_id -> profile row index
    id_to_idx = {sid: i for i, sid in enumerate(site_ids)}

    rows = []
    for _, row in clusters_df.iterrows():
        sid = int(row["site_id"])
        idx = id_to_idx[sid]
        profile = profiles[idx].tolist()
        rows.append((sid, int(row["cluster_id"]), None, profile,
                      float(row["silhouette_score"])))

    con.executemany(
        "INSERT INTO site_clusters (site_id, cluster_id, cluster_label, "
        "profile_vector, silhouette_score) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    log.info("clusters_stored", count=len(rows))


def describe_clusters(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return summary statistics per cluster."""
    return con.execute("""
        SELECT
            sc.cluster_id,
            COUNT(*) AS site_count,
            ROUND(AVG(sc.silhouette_score), 3) AS avg_silhouette,
            ROUND(AVG(td.avg_vol), 0) AS avg_daily_volume
        FROM site_clusters sc
        LEFT JOIN (
            SELECT site_id, AVG(total_volume) AS avg_vol
            FROM traffic_daily
            GROUP BY site_id
        ) td ON sc.site_id = td.site_id
        GROUP BY sc.cluster_id
        ORDER BY sc.cluster_id
    """).fetchdf()
