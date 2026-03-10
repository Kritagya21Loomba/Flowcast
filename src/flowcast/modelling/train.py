"""Global traffic volume forecasting model training."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import OrdinalEncoder

from flowcast.modelling.evaluate import compute_metrics, compute_site_metrics
from flowcast.modelling.features import build_daily_features
from flowcast.utils.logging import get_logger

log = get_logger(__name__)

TARGET = "total_volume"

CATEGORICAL_FEATURES = ["region", "day_of_week", "month", "quarter"]

NUMERIC_FEATURES = [
    "detector_count", "peak_hour",
    "volume_lag_1", "volume_lag_7", "volume_lag_14", "volume_lag_28",
    "volume_rolling_mean_7", "volume_rolling_mean_28", "volume_rolling_std_7",
    "is_weekend", "is_public_holiday", "is_school_holiday",
    "site_avg_volume", "cluster_id",
]

ALL_FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES


@dataclass
class TrainResult:
    """Result of a model training run."""
    model_id: str
    model: HistGradientBoostingRegressor
    encoder: OrdinalEncoder
    feature_columns: list[str]
    train_rows: int
    test_rows: int
    test_mae: float
    test_rmse: float
    test_mape: float
    site_metrics: pd.DataFrame = field(default_factory=pd.DataFrame)


def _prepare_features(
    df: pd.DataFrame,
    categorical_features: list[str],
    encoder: OrdinalEncoder | None = None,
    fit: bool = False,
) -> tuple[np.ndarray, OrdinalEncoder]:
    """Convert DataFrame to feature matrix with encoded categoricals."""
    df = df.copy()

    # Fill NaN in cluster_id with -1 (no cluster assigned)
    if "cluster_id" in df.columns:
        df["cluster_id"] = df["cluster_id"].fillna(-1)

    if fit:
        encoder = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
        df[categorical_features] = encoder.fit_transform(df[categorical_features].astype(str))
    else:
        df[categorical_features] = encoder.transform(df[categorical_features].astype(str))

    return df[ALL_FEATURES].values, encoder


def train_daily_global_model(
    con: duckdb.DuckDBPyConnection,
    site_ids: list[int],
    test_start_date: str = "2025-10-01",
    model_id: str | None = None,
) -> TrainResult:
    """Train a global HistGradientBoostingRegressor on daily volume data.

    Uses time-based split: everything before test_start_date for training,
    the rest for evaluation.
    """
    if model_id is None:
        model_id = f"daily_global_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    log.info("building_features", sites=len(site_ids))
    df = build_daily_features(con, site_ids)

    if df.empty:
        raise ValueError("No feature data available for training")

    # Time-based split
    df["date"] = pd.to_datetime(df["date"])
    train_mask = df["date"] < pd.Timestamp(test_start_date)
    train_df = df[train_mask]
    test_df = df[~train_mask]

    if train_df.empty:
        raise ValueError(f"No training data before {test_start_date}")
    if test_df.empty:
        raise ValueError(f"No test data from {test_start_date} onwards")

    log.info("data_split", train_rows=len(train_df), test_rows=len(test_df),
             train_end=str(train_df["date"].max().date()),
             test_start=str(test_df["date"].min().date()))

    # Prepare features
    X_train, encoder = _prepare_features(train_df, CATEGORICAL_FEATURES, fit=True)
    X_test, _ = _prepare_features(test_df, CATEGORICAL_FEATURES, encoder=encoder)
    y_train = train_df[TARGET].values
    y_test = test_df[TARGET].values

    # Build categorical feature mask for HistGBR
    cat_mask = [i < len(CATEGORICAL_FEATURES) for i in range(len(ALL_FEATURES))]

    model = HistGradientBoostingRegressor(
        max_iter=500,
        max_depth=8,
        learning_rate=0.05,
        min_samples_leaf=20,
        l2_regularization=0.1,
        categorical_features=cat_mask,
        random_state=42,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=20,
    )

    log.info("training_model", model_type="HistGradientBoostingRegressor")
    model.fit(X_train, y_train)
    log.info("training_complete", n_iter=model.n_iter_)

    # Evaluate
    y_pred = model.predict(X_test)
    metrics = compute_metrics(y_test, y_pred)

    # Per-site metrics
    eval_df = test_df[["site_id", "date"]].copy()
    eval_df["actual"] = y_test
    eval_df["predicted"] = y_pred
    site_metrics = compute_site_metrics(eval_df)

    log.info("evaluation_complete",
             test_mae=round(metrics["mae"], 1),
             test_rmse=round(metrics["rmse"], 1),
             test_mape=round(metrics["mape"], 2))

    return TrainResult(
        model_id=model_id,
        model=model,
        encoder=encoder,
        feature_columns=ALL_FEATURES,
        train_rows=len(train_df),
        test_rows=len(test_df),
        test_mae=metrics["mae"],
        test_rmse=metrics["rmse"],
        test_mape=metrics["mape"],
        site_metrics=site_metrics,
    )


def save_model(result: TrainResult, models_dir: Path) -> Path:
    """Serialize model and encoder to disk using joblib."""
    models_dir.mkdir(parents=True, exist_ok=True)
    path = models_dir / f"{result.model_id}.joblib"
    joblib.dump({"model": result.model, "encoder": result.encoder,
                 "feature_columns": result.feature_columns}, path)
    log.info("model_saved", path=str(path))
    return path


def load_model(path: Path) -> dict:
    """Load a serialized model from disk."""
    return joblib.load(path)


def register_model(con: duckdb.DuckDBPyConnection, result: TrainResult, artifact_path: str) -> None:
    """Write model metadata to model_registry table."""
    con.execute(
        "INSERT INTO model_registry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            result.model_id,
            "hist_gbr",
            "global",
            TARGET,
            json.dumps(result.feature_columns),
            result.train_rows,
            None,  # train_start_date (could be extracted but not critical)
            None,  # train_end_date
            result.test_mae,
            result.test_rmse,
            result.test_mape,
            artifact_path,
            datetime.now(),
        ],
    )
    log.info("model_registered", model_id=result.model_id)


def store_site_metrics(con: duckdb.DuckDBPyConnection, result: TrainResult) -> None:
    """Write per-site evaluation metrics to model_metrics_site table."""
    con.execute(
        "DELETE FROM model_metrics_site WHERE model_id = ?",
        [result.model_id],
    )
    for _, row in result.site_metrics.iterrows():
        con.execute(
            "INSERT INTO model_metrics_site VALUES (?, ?, ?, ?, ?, ?)",
            [result.model_id, int(row["site_id"]),
             float(row["mae"]), float(row["rmse"]), float(row["mape"]),
             int(row["n_test_days"])],
        )
    log.info("site_metrics_stored", count=len(result.site_metrics))
