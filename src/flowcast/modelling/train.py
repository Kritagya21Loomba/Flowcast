"""Global traffic volume forecasting model training."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import OrdinalEncoder

from flowcast.modelling.evaluate import compute_metrics, compute_site_metrics
from flowcast.modelling.features import build_daily_features
from flowcast.utils.logging import get_logger

log = get_logger(__name__)

TARGET = "total_volume"

CATEGORICAL_FEATURES = ["region", "day_of_week", "month", "quarter"]

NUMERIC_FEATURES = [
    "detector_count",
    "peak_hour",
    "volume_lag_1",
    "volume_lag_7",
    "volume_lag_14",
    "volume_lag_28",
    "volume_rolling_mean_7",
    "volume_rolling_mean_28",
    "volume_rolling_std_7",
    "is_weekend",
    "is_public_holiday",
    "is_school_holiday",
    "is_day_before_public_holiday",
    "is_day_after_public_holiday",
    "is_bridge_day",
    "is_term_start_week",
    "is_term_end_week",
    "site_avg_volume",
    "cluster_id",
    "cluster_mean_volume",
    "cluster_lag_1",
    "cluster_lag_7",
    "cluster_lag_14",
    "top_neighbor_lag_1",
    "cluster_corridor_avg",
    "rainfall_mm",
    "temperature_c",
    "wind_kmh",
    "severe_weather_flag",
    "afl_games_count",
    "concerts_count",
    "cbd_events_count",
    "roadworks_flag",
    "school_zone_flag",
    "graph_degree",
    "graph_centrality",
    "graph_clustering_coeff",
]

ALL_FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES


@dataclass
class TrainedModelBundle:
    """Model + metadata bundle for one model variant."""

    key: str
    model: Any
    encoder: OrdinalEncoder
    feature_columns: list[str]
    model_type: str
    scope: str


@dataclass
class TrainResult:
    """Result of a model training run."""

    model_id: str
    model: Any
    encoder: OrdinalEncoder
    feature_columns: list[str]
    train_rows: int
    test_rows: int
    test_mae: float
    test_rmse: float
    test_mape: float
    site_metrics: pd.DataFrame = field(default_factory=pd.DataFrame)
    cv_metrics: dict[str, float] = field(default_factory=dict)
    residual_diagnostics: pd.DataFrame = field(default_factory=pd.DataFrame)
    component_models: dict[str, TrainedModelBundle] = field(default_factory=dict)


def _prepare_features(
    df: pd.DataFrame,
    categorical_features: list[str],
    encoder: OrdinalEncoder | None = None,
    fit: bool = False,
) -> tuple[np.ndarray, OrdinalEncoder]:
    """Convert DataFrame to feature matrix with encoded categoricals."""
    df = df.copy()
    for col in ALL_FEATURES:
        if col not in df.columns:
            df[col] = 0

    if "cluster_id" in df.columns:
        df["cluster_id"] = df["cluster_id"].fillna(-1)

    df[categorical_features] = df[categorical_features].fillna("missing").astype(str)
    if fit:
        encoder = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
        df[categorical_features] = encoder.fit_transform(df[categorical_features])
    else:
        if encoder is None:
            raise ValueError("encoder is required when fit=False")
        df[categorical_features] = encoder.transform(df[categorical_features])

    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    X = df[ALL_FEATURES].to_numpy(dtype=np.float64, copy=False)
    return X, encoder


def _build_hist_model(loss: str = "squared_error", quantile: float | None = None) -> HistGradientBoostingRegressor:
    kwargs: dict[str, Any] = {
        "max_iter": 500,
        "max_depth": 8,
        "learning_rate": 0.05,
        "min_samples_leaf": 20,
        "l2_regularization": 0.1,
        "random_state": 42,
        "early_stopping": True,
        "validation_fraction": 0.1,
        "n_iter_no_change": 20,
        "loss": loss,
    }
    if quantile is not None:
        kwargs["quantile"] = quantile
    return HistGradientBoostingRegressor(**kwargs)


def _fit_optional_model(model_name: str, X: np.ndarray, y: np.ndarray) -> Any | None:
    try:
        if model_name == "lightgbm":
            from lightgbm import LGBMRegressor

            model = LGBMRegressor(
                n_estimators=400,
                learning_rate=0.05,
                max_depth=8,
                min_child_samples=20,
                random_state=42,
            )
            model.fit(X, y)
            return model
        if model_name == "catboost":
            from catboost import CatBoostRegressor

            model = CatBoostRegressor(
                iterations=400,
                learning_rate=0.05,
                depth=8,
                loss_function="RMSE",
                random_seed=42,
                verbose=False,
            )
            model.fit(X, y)
            return model
    except ImportError:
        log.warning("optional_model_unavailable", model_name=model_name)
    return None


def _seasonal_naive_predictions(test_df: pd.DataFrame, train_df: pd.DataFrame) -> np.ndarray:
    lookup = (
        train_df.sort_values("date")
        .set_index(["site_id", "date"])["total_volume"]
        .to_dict()
    )
    preds: list[float] = []
    for row in test_df.itertuples(index=False):
        back_7 = pd.Timestamp(row.date) - pd.Timedelta(days=7)
        v = lookup.get((row.site_id, back_7), np.nan)
        if np.isnan(v):
            v = float(train_df[train_df["site_id"] == row.site_id]["total_volume"].mean())
        preds.append(float(v))
    return np.array(preds, dtype=float)


def run_rolling_cv(df: pd.DataFrame, n_splits: int = 4) -> dict[str, float]:
    """Rolling-window CV using time-ordered splits."""
    ordered = df.sort_values("date").reset_index(drop=True)
    if len(ordered) < 120:
        return {}
    splitter = TimeSeriesSplit(n_splits=n_splits)
    fold_mapes: list[float] = []
    for train_idx, test_idx in splitter.split(ordered):
        train_fold = ordered.iloc[train_idx]
        test_fold = ordered.iloc[test_idx]
        X_train, enc = _prepare_features(train_fold, CATEGORICAL_FEATURES, fit=True)
        X_test, _ = _prepare_features(test_fold, CATEGORICAL_FEATURES, encoder=enc)
        model = _build_hist_model()
        model.fit(X_train, train_fold[TARGET].values)
        pred = model.predict(X_test)
        fold_mapes.append(compute_metrics(test_fold[TARGET].values, pred)["mape"])
    return {"rolling_cv_mape_mean": float(np.mean(fold_mapes)), "rolling_cv_mape_std": float(np.std(fold_mapes))}


def run_hyperparameter_optimization(X_train: np.ndarray, y_train: np.ndarray, X_val: np.ndarray, y_val: np.ndarray) -> dict[str, Any]:
    """Optional Optuna optimization; returns best params or empty dict."""
    try:
        import optuna
    except ImportError:
        return {}

    def objective(trial: Any) -> float:
        model = HistGradientBoostingRegressor(
            max_iter=trial.suggest_int("max_iter", 200, 600),
            max_depth=trial.suggest_int("max_depth", 4, 10),
            learning_rate=trial.suggest_float("learning_rate", 0.02, 0.15),
            min_samples_leaf=trial.suggest_int("min_samples_leaf", 10, 60),
            l2_regularization=trial.suggest_float("l2_regularization", 0.0, 0.4),
            random_state=42,
        )
        model.fit(X_train, y_train)
        pred = model.predict(X_val)
        return compute_metrics(y_val, pred)["mape"]

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=15)
    return dict(study.best_params)


def _train_direct_models(train_df: pd.DataFrame) -> dict[int, tuple[Any, OrdinalEncoder]]:
    direct_models: dict[int, tuple[Any, OrdinalEncoder]] = {}
    for horizon in (1, 7, 14, 28):
        h_df = train_df.copy().sort_values(["site_id", "date"])
        h_df["target_h"] = h_df.groupby("site_id")[TARGET].shift(-horizon)
        h_df = h_df.dropna(subset=["target_h"])
        if h_df.empty:
            continue
        X, enc = _prepare_features(h_df, CATEGORICAL_FEATURES, fit=True)
        y = h_df["target_h"].values
        model = _build_hist_model()
        model.fit(X, y)
        direct_models[horizon] = (model, enc)
    return direct_models


def _cluster_specific_predictions(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
) -> np.ndarray:
    pred = np.full(len(test_df), np.nan, dtype=float)
    for cluster_id, c_train in train_df.groupby("cluster_id"):
        c_test_idx = test_df.index[test_df["cluster_id"] == cluster_id]
        if len(c_test_idx) == 0:
            continue
        c_test = test_df.loc[c_test_idx]
        X_train, enc = _prepare_features(c_train, CATEGORICAL_FEATURES, fit=True)
        X_test, _ = _prepare_features(c_test, CATEGORICAL_FEATURES, encoder=enc)
        model = _build_hist_model()
        model.fit(X_train, c_train[TARGET].values)
        pred[test_df.index.get_indexer(c_test_idx)] = model.predict(X_test)
    return pred


def _compute_residual_diagnostics(model_id: str, eval_df: pd.DataFrame, mape_threshold: float = 8.0) -> pd.DataFrame:
    rows = []
    for site_id, group in eval_df.groupby("site_id"):
        residuals = group["actual"] - group["predicted"]
        metrics = compute_metrics(group["actual"].values, group["predicted"].values)
        flagged = int(metrics["mape"] >= mape_threshold)
        reason = "high_mape" if flagged else None
        rows.append(
            {
                "model_id": model_id,
                "site_id": int(site_id),
                "residual_mean": float(residuals.mean()),
                "residual_std": float(residuals.std(ddof=0)),
                "residual_mape": float(metrics["mape"]),
                "flagged": flagged,
                "reason": reason,
            }
        )
    return pd.DataFrame(rows)


def _predict(model: Any, X: np.ndarray) -> np.ndarray:
    return model.predict(X)


def train_daily_global_model(
    con: duckdb.DuckDBPyConnection,
    site_ids: list[int],
    test_start_date: str = "2025-10-01",
    model_id: str | None = None,
) -> TrainResult:
    """Train blended global model stack on daily volume data."""
    if model_id is None:
        model_id = f"daily_global_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    df = build_daily_features(con, site_ids)
    if df.empty:
        raise ValueError("No feature data available for training")

    df["date"] = pd.to_datetime(df["date"])
    train_df = df[df["date"] < pd.Timestamp(test_start_date)].copy()
    test_df = df[df["date"] >= pd.Timestamp(test_start_date)].copy()
    if train_df.empty or test_df.empty:
        raise ValueError("Training/test split has empty partition")

    X_train, encoder = _prepare_features(train_df, CATEGORICAL_FEATURES, fit=True)
    X_test, _ = _prepare_features(test_df, CATEGORICAL_FEATURES, encoder=encoder)
    y_train = train_df[TARGET].values
    y_test = test_df[TARGET].values

    global_model = _build_hist_model()
    global_model.fit(X_train, y_train)
    pred_global = _predict(global_model, X_test)

    pred_cluster = _cluster_specific_predictions(train_df, test_df)
    pred_cluster = np.where(np.isnan(pred_cluster), pred_global, pred_cluster)

    direct_models = _train_direct_models(train_df)
    pred_direct = pred_global.copy()
    if direct_models:
        direct_vals = []
        for horizon, (dm, denc) in direct_models.items():
            h_df = test_df.copy().sort_values(["site_id", "date"])
            h_df["horizon"] = (h_df["date"] - pd.Timestamp(test_start_date)).dt.days + 1
            h_df = h_df[h_df["horizon"] == horizon]
            if h_df.empty:
                continue
            X_h, _ = _prepare_features(h_df, CATEGORICAL_FEATURES, encoder=denc)
            p = dm.predict(X_h)
            idx = test_df.index.get_indexer(h_df.index)
            pred_direct[idx] = p
            direct_vals.append(p)

    pred_naive = _seasonal_naive_predictions(test_df, train_df)
    pred_ensemble = 0.45 * pred_global + 0.30 * pred_cluster + 0.15 * pred_direct + 0.10 * pred_naive

    q10_model = _build_hist_model(loss="quantile", quantile=0.1)
    q90_model = _build_hist_model(loss="quantile", quantile=0.9)
    q10_model.fit(X_train, y_train)
    q90_model.fit(X_train, y_train)

    lightgbm_model = _fit_optional_model("lightgbm", X_train, y_train)
    catboost_model = _fit_optional_model("catboost", X_train, y_train)

    metrics = compute_metrics(y_test, pred_ensemble)
    eval_df = test_df[["site_id", "date"]].copy()
    eval_df["actual"] = y_test
    eval_df["predicted"] = pred_ensemble
    site_metrics = compute_site_metrics(eval_df)
    residual_diag = _compute_residual_diagnostics(model_id, eval_df)
    cv_metrics = run_rolling_cv(train_df)

    optuna_params: dict[str, Any] = {}
    if len(X_train) > 120 and len(X_test) > 30:
        optuna_params = run_hyperparameter_optimization(
            X_train,
            y_train,
            X_test[: min(len(X_test), 100)],
            y_test[: min(len(y_test), 100)],
        )
    if optuna_params:
        cv_metrics["optuna_best_params"] = optuna_params

    component_models: dict[str, TrainedModelBundle] = {
        "global": TrainedModelBundle("global", global_model, encoder, ALL_FEATURES, "hist_gbr", "global"),
        "quantile_p10": TrainedModelBundle("quantile_p10", q10_model, encoder, ALL_FEATURES, "hist_gbr_quantile", "global"),
        "quantile_p90": TrainedModelBundle("quantile_p90", q90_model, encoder, ALL_FEATURES, "hist_gbr_quantile", "global"),
    }
    if lightgbm_model is not None:
        component_models["lightgbm"] = TrainedModelBundle("lightgbm", lightgbm_model, encoder, ALL_FEATURES, "lightgbm", "global")
    if catboost_model is not None:
        component_models["catboost"] = TrainedModelBundle("catboost", catboost_model, encoder, ALL_FEATURES, "catboost", "global")
    for horizon, (model, enc) in direct_models.items():
        key = f"direct_h{horizon}"
        component_models[key] = TrainedModelBundle(key, model, enc, ALL_FEATURES, "hist_gbr_direct", f"horizon_{horizon}")

    return TrainResult(
        model_id=model_id,
        model=global_model,
        encoder=encoder,
        feature_columns=ALL_FEATURES,
        train_rows=len(train_df),
        test_rows=len(test_df),
        test_mae=metrics["mae"],
        test_rmse=metrics["rmse"],
        test_mape=metrics["mape"],
        site_metrics=site_metrics,
        cv_metrics=cv_metrics,
        residual_diagnostics=residual_diag,
        component_models=component_models,
    )


def save_model(result: TrainResult, models_dir: Path) -> Path:
    """Serialize model stack to disk using joblib."""
    models_dir.mkdir(parents=True, exist_ok=True)
    path = models_dir / f"{result.model_id}.joblib"
    payload = {
        "model": result.model,
        "encoder": result.encoder,
        "feature_columns": result.feature_columns,
        "component_models": serialize_component_models(result.component_models),
        "cv_metrics": result.cv_metrics,
    }
    joblib.dump(payload, path)
    log.info("model_saved", path=str(path))
    return path


def load_model(path: Path) -> dict:
    """Load a serialized model from disk."""
    return joblib.load(path)


def serialize_component_models(component_models: dict[str, TrainedModelBundle]) -> dict[str, dict[str, Any]]:
    """Convert typed component model bundles into dict payloads."""
    return {
        k: {
            "model": v.model,
            "encoder": v.encoder,
            "feature_columns": v.feature_columns,
            "model_type": v.model_type,
            "scope": v.scope,
        }
        for k, v in component_models.items()
    }


def register_model(con: duckdb.DuckDBPyConnection, result: TrainResult, artifact_path: str) -> None:
    """Write model metadata to model_registry table."""
    con.execute(
        "INSERT INTO model_registry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            result.model_id,
            "ensemble",
            "global_cluster_direct_naive",
            TARGET,
            json.dumps(result.feature_columns),
            result.train_rows,
            None,
            None,
            result.test_mae,
            result.test_rmse,
            result.test_mape,
            artifact_path,
            datetime.now(),
        ],
    )
    for bundle in result.component_models.values():
        if bundle.key == "global":
            continue
        con.execute(
            "INSERT INTO model_registry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                f"{result.model_id}__{bundle.key}",
                bundle.model_type,
                bundle.scope,
                TARGET,
                json.dumps(bundle.feature_columns),
                result.train_rows,
                None,
                None,
                None,
                None,
                None,
                artifact_path,
                datetime.now(),
            ],
        )
    log.info("model_registered", model_id=result.model_id, components=len(result.component_models))


def store_site_metrics(con: duckdb.DuckDBPyConnection, result: TrainResult) -> None:
    """Write per-site evaluation metrics and diagnostics."""
    con.execute("DELETE FROM model_metrics_site WHERE model_id = ?", [result.model_id])
    for _, row in result.site_metrics.iterrows():
        con.execute(
            "INSERT INTO model_metrics_site VALUES (?, ?, ?, ?, ?, ?)",
            [
                result.model_id,
                int(row["site_id"]),
                float(row["mae"]),
                float(row["rmse"]),
                float(row["mape"]),
                int(row["n_test_days"]),
            ],
        )

    con.execute("DELETE FROM site_diagnostics WHERE model_id = ?", [result.model_id])
    if not result.residual_diagnostics.empty:
        con.executemany(
            "INSERT INTO site_diagnostics VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    str(r["model_id"]),
                    int(r["site_id"]),
                    float(r["residual_mean"]),
                    float(r["residual_std"]),
                    float(r["residual_mape"]),
                    int(r["flagged"]),
                    r["reason"],
                )
                for _, r in result.residual_diagnostics.iterrows()
            ],
        )
    log.info("site_metrics_stored", count=len(result.site_metrics))
