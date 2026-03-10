"""Evaluation metrics and reporting for traffic forecasting models."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from flowcast.utils.logging import get_logger

log = get_logger(__name__)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    """Compute MAE, RMSE, MAPE for predictions vs actuals."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    # MAPE: only where actuals > 0 to avoid division by zero
    mask = y_true > 0
    if mask.any():
        mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)
    else:
        mape = float("nan")

    return {"mae": mae, "rmse": rmse, "mape": mape}


def compute_site_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-site MAE, RMSE, MAPE.

    Expects DataFrame with columns: site_id, actual, predicted.
    Returns DataFrame with columns: site_id, mae, rmse, mape, n_test_days.
    """
    results = []
    for site_id, group in df.groupby("site_id"):
        m = compute_metrics(group["actual"].values, group["predicted"].values)
        m["site_id"] = site_id
        m["n_test_days"] = len(group)
        results.append(m)
    return pd.DataFrame(results)


def plot_predictions(
    df: pd.DataFrame,
    site_id: int,
    save_path: Path | None = None,
) -> None:
    """Plot actual vs predicted for a single site."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    site_df = df[df["site_id"] == site_id].sort_values("date")
    if site_df.empty:
        log.warning("no_data_for_plot", site_id=site_id)
        return

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(site_df["date"], site_df["actual"], label="Actual", alpha=0.8)
    ax.plot(site_df["date"], site_df["predicted"], label="Predicted", alpha=0.8)
    ax.set_title(f"Site {site_id} — Actual vs Predicted Daily Volume")
    ax.set_xlabel("Date")
    ax.set_ylabel("Volume")
    ax.legend()
    fig.tight_layout()

    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=100)
        log.info("plot_saved", path=str(save_path))
    plt.close(fig)


def plot_feature_importance(
    importances: np.ndarray,
    feature_names: list[str],
    save_path: Path | None = None,
) -> None:
    """Bar chart of feature importances."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    sorted_idx = np.argsort(importances)[::-1][:20]  # top 20
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(
        [feature_names[i] for i in sorted_idx[::-1]],
        importances[sorted_idx[::-1]],
    )
    ax.set_title("Top 20 Feature Importances")
    ax.set_xlabel("Importance")
    fig.tight_layout()

    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=100)
        log.info("plot_saved", path=str(save_path))
    plt.close(fig)
