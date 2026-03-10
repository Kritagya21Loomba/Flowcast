"""Origin-destination pattern inference via temporal correlation."""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def compute_daily_correlations(
    con: duckdb.DuckDBPyConnection,
    site_ids: list[int],
    min_overlap_days: int = 180,
) -> pd.DataFrame:
    """Compute pairwise Pearson correlation of daily volume time series.

    Pivots traffic_daily into a wide matrix (dates as rows, sites as columns),
    then computes numpy correlation. Filters pairs by minimum overlapping days.

    Returns DataFrame: site_a, site_b, pearson_daily, n_overlap_days
    """
    site_list = ", ".join(str(s) for s in site_ids)

    df = con.execute(f"""
        SELECT site_id, date, total_volume
        FROM traffic_daily
        WHERE site_id IN ({site_list})
        ORDER BY date
    """).fetchdf()

    if df.empty:
        return pd.DataFrame(columns=["site_a", "site_b", "pearson_daily", "n_overlap_days"])

    # Pivot to wide: rows=dates, columns=site_ids
    pivot = df.pivot(index="date", columns="site_id", values="total_volume")

    n_sites = len(pivot.columns)
    site_order = list(pivot.columns)

    log.info("computing_daily_correlations",
             sites=n_sites, dates=len(pivot), pairs=n_sites * (n_sites - 1) // 2)

    # Fill NaN with column mean for correlation computation
    filled = pivot.fillna(pivot.mean())
    values = filled.values  # shape (n_dates, n_sites)

    # Compute correlation matrix
    corr_matrix = np.corrcoef(values.T)  # shape (n_sites, n_sites)

    # Count overlapping non-null days per pair
    valid = ~pivot.isna()
    valid_arr = valid.values  # shape (n_dates, n_sites)

    # Build result pairs
    rows = []
    for i in range(n_sites):
        for j in range(i + 1, n_sites):
            overlap = int(np.sum(valid_arr[:, i] & valid_arr[:, j]))
            if overlap >= min_overlap_days:
                rows.append({
                    "site_a": site_order[i],
                    "site_b": site_order[j],
                    "pearson_daily": float(corr_matrix[i, j]),
                    "n_overlap_days": overlap,
                })

    result = pd.DataFrame(rows)
    log.info("daily_correlations_computed", pairs=len(result))
    return result


def compute_hourly_profile_similarity(
    profiles: np.ndarray,
    site_ids: list[int],
) -> pd.DataFrame:
    """Compute pairwise cosine similarity of hourly profiles.

    profiles: shape (n_sites, 24) from compute_site_hourly_profiles.
    Returns DataFrame: site_a, site_b, cosine_hourly
    """
    sim_matrix = cosine_similarity(profiles)  # shape (n, n)

    rows = []
    for i in range(len(site_ids)):
        for j in range(i + 1, len(site_ids)):
            rows.append({
                "site_a": site_ids[i],
                "site_b": site_ids[j],
                "cosine_hourly": float(sim_matrix[i, j]),
            })

    return pd.DataFrame(rows)


def estimate_lag(
    con: duckdb.DuckDBPyConnection,
    site_a: int,
    site_b: int,
    max_lag_intervals: int = 16,
) -> int:
    """Estimate the time offset between two sites using cross-correlation.

    Uses V-column data from traffic_volumes for 15-minute resolution.
    Computes cross-correlation at lags from -max_lag_intervals to +max_lag_intervals
    (each interval is 15 minutes).

    Returns lag in minutes (positive = site_b follows site_a).
    """
    # Extract volume vectors: sum all detectors per interval
    v_cols = [f"SUM(COALESCE(V{i:02d},0))" for i in range(96)]
    v_list = ", ".join(v_cols)

    def _get_signal(site_id: int) -> np.ndarray:
        rows = con.execute(f"""
            SELECT {v_list}
            FROM traffic_volumes
            WHERE nb_scats_site = {site_id}
            GROUP BY qt_interval_count
            ORDER BY qt_interval_count
        """).fetchall()
        if not rows:
            return np.array([])
        # Flatten: each row has 96 values, concatenate all days
        return np.concatenate([np.array(r) for r in rows])

    sig_a = _get_signal(site_a)
    sig_b = _get_signal(site_b)

    if len(sig_a) == 0 or len(sig_b) == 0:
        return 0

    # Align lengths
    min_len = min(len(sig_a), len(sig_b))
    sig_a = sig_a[:min_len].astype(float)
    sig_b = sig_b[:min_len].astype(float)

    # Normalize
    sig_a = (sig_a - sig_a.mean()) / (sig_a.std() + 1e-10)
    sig_b = (sig_b - sig_b.mean()) / (sig_b.std() + 1e-10)

    best_lag = 0
    best_corr = -np.inf

    for lag in range(-max_lag_intervals, max_lag_intervals + 1):
        if lag >= 0:
            c = np.dot(sig_a[lag:], sig_b[:min_len - lag]) / (min_len - abs(lag))
        else:
            c = np.dot(sig_a[:min_len + lag], sig_b[-lag:]) / (min_len - abs(lag))
        if c > best_corr:
            best_corr = c
            best_lag = lag

    return best_lag * 15  # Convert intervals to minutes


def build_correlation_network(
    daily_corr: pd.DataFrame,
    hourly_sim: pd.DataFrame,
    min_pearson: float = 0.7,
    min_cosine: float = 0.85,
) -> pd.DataFrame:
    """Merge correlation and similarity data, filter by thresholds.

    Returns DataFrame of significant site pairs with columns:
    site_a, site_b, pearson_daily, cosine_hourly
    """
    if daily_corr.empty or hourly_sim.empty:
        return pd.DataFrame(columns=["site_a", "site_b", "pearson_daily", "cosine_hourly"])

    merged = daily_corr.merge(hourly_sim, on=["site_a", "site_b"], how="inner")
    filtered = merged[
        (merged["pearson_daily"] >= min_pearson) &
        (merged["cosine_hourly"] >= min_cosine)
    ].copy()

    log.info("correlation_network_built",
             total_pairs=len(merged),
             significant_pairs=len(filtered))
    return filtered


def store_correlations(
    con: duckdb.DuckDBPyConnection,
    pairs: pd.DataFrame,
) -> None:
    """Write correlated pairs to site_correlations table."""
    con.execute("DELETE FROM site_correlations")

    if pairs.empty:
        log.info("no_correlations_to_store")
        return

    rows = []
    for _, row in pairs.iterrows():
        rows.append((
            int(row["site_a"]),
            int(row["site_b"]),
            float(row.get("pearson_daily", 0)),
            float(row.get("cosine_hourly", 0)),
            int(row.get("lag_minutes", 0)) if "lag_minutes" in row.index else 0,
        ))

    con.executemany(
        "INSERT INTO site_correlations VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    log.info("correlations_stored", count=len(rows))
