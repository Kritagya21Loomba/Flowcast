"""CLI entry point for the Flowcast modelling pipeline."""

import argparse
import sys
from pathlib import Path

from flowcast.config import (
    DB_PATH, MODELS_DIR, REPORTS_DIR,
    DEFAULT_MAX_SITES, DEFAULT_N_CLUSTERS, DEFAULT_TEST_START_DATE,
)
from flowcast.db.connection import get_connection
from flowcast.db.schema import ensure_schema
from flowcast.utils.logging import setup_logging, get_logger

log = get_logger(__name__)

ALL_STEPS = ["select", "cluster", "train", "forecast", "od", "evaluate"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Flowcast: Train traffic forecasting models."
    )
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--max-sites", type=int, default=DEFAULT_MAX_SITES)
    parser.add_argument("--n-clusters", type=int, default=DEFAULT_N_CLUSTERS)
    parser.add_argument("--test-start-date", default=DEFAULT_TEST_START_DATE)
    parser.add_argument(
        "--steps",
        nargs="*",
        default=ALL_STEPS,
        choices=ALL_STEPS,
        help="Which pipeline steps to run (default: all)",
    )
    parser.add_argument("--json-logs", action="store_true")
    args = parser.parse_args()

    setup_logging(json_output=args.json_logs)
    log.info("modelling_pipeline_start", db_path=str(args.db_path), steps=args.steps)

    con = get_connection(args.db_path)
    ensure_schema(con)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # State shared between steps
    site_ids: list[int] = []
    profiles = None
    train_result = None

    try:
        # --- Step 1: Site Selection ---
        if "select" in args.steps:
            from flowcast.modelling.site_selection import select_modelling_sites
            log.info("step_start", step="select")
            site_ids = select_modelling_sites(con, max_sites=args.max_sites)
            if not site_ids:
                log.error("no_sites_selected")
                sys.exit(1)
            log.info("step_complete", step="select", sites=len(site_ids))

        # --- Step 2: Clustering ---
        if "cluster" in args.steps:
            from flowcast.modelling.clusters import (
                compute_site_hourly_profiles, cluster_sites,
                store_clusters, describe_clusters,
            )
            if not site_ids:
                log.error("cluster_requires_select", hint="Run with --steps select cluster")
                sys.exit(1)

            log.info("step_start", step="cluster")
            profiles, ordered_ids = compute_site_hourly_profiles(con, site_ids)
            clusters_df = cluster_sites(profiles, ordered_ids, n_clusters=args.n_clusters)
            store_clusters(con, clusters_df, profiles, ordered_ids)

            summary = describe_clusters(con)
            for _, row in summary.iterrows():
                log.info("cluster_summary",
                         cluster=int(row["cluster_id"]),
                         sites=int(row["site_count"]),
                         avg_silhouette=row["avg_silhouette"],
                         avg_volume=row["avg_daily_volume"])
            log.info("step_complete", step="cluster")

        # --- Step 3: Training ---
        if "train" in args.steps:
            from flowcast.modelling.train import (
                train_daily_global_model, save_model,
                register_model, store_site_metrics, serialize_component_models,
            )
            if not site_ids:
                log.error("train_requires_select")
                sys.exit(1)

            log.info("step_start", step="train")
            train_result = train_daily_global_model(
                con, site_ids, test_start_date=args.test_start_date,
            )
            artifact_path = save_model(train_result, MODELS_DIR)
            register_model(con, train_result, str(artifact_path))
            store_site_metrics(con, train_result)
            log.info("step_complete", step="train",
                     mae=round(train_result.test_mae, 1),
                     rmse=round(train_result.test_rmse, 1),
                     mape=round(train_result.test_mape, 2))

        # --- Step 4: Forecast ---
        if "forecast" in args.steps:
            from flowcast.modelling.forecast import generate_forecasts
            if train_result is None:
                log.error("forecast_requires_train")
                sys.exit(1)

            log.info("step_start", step="forecast")
            count = generate_forecasts(
                con,
                train_result.model,
                train_result.encoder,
                train_result.model_id,
                site_ids,
                train_result.feature_columns,
                model_bundle={"component_models": serialize_component_models(train_result.component_models)},
            )
            log.info("step_complete", step="forecast", rows=count)

        # --- Step 5: OD Inference ---
        if "od" in args.steps:
            from flowcast.modelling.od_inference import (
                compute_daily_correlations, compute_hourly_profile_similarity,
                build_correlation_network, store_correlations,
                compute_graph_features, store_graph_features,
            )
            from flowcast.modelling.clusters import compute_site_hourly_profiles as _get_profiles
            if not site_ids:
                log.error("od_requires_select")
                sys.exit(1)

            log.info("step_start", step="od")
            daily_corr = compute_daily_correlations(con, site_ids)

            if profiles is None:
                profiles, ordered_ids = _get_profiles(con, site_ids)
            else:
                ordered_ids = site_ids

            hourly_sim = compute_hourly_profile_similarity(profiles, ordered_ids)
            network = build_correlation_network(daily_corr, hourly_sim)
            store_correlations(con, network)
            graph_df = compute_graph_features(network)
            store_graph_features(con, graph_df)
            log.info("step_complete", step="od", correlated_pairs=len(network))

        # --- Step 6: Evaluation Report ---
        if "evaluate" in args.steps:
            from flowcast.modelling.evaluate import (
                plot_predictions, plot_feature_importance,
            )
            if train_result is None:
                log.warning("evaluate_requires_train")
            else:
                log.info("step_start", step="evaluate")

                # Re-fetch test predictions for plotting and importance
                from flowcast.modelling.features import build_daily_features
                from flowcast.modelling.train import (
                    _prepare_features, CATEGORICAL_FEATURES,
                )
                from sklearn.inspection import permutation_importance
                import pandas as pd

                # Get top 5 best-predicted sites for plots
                top_sites = (
                    train_result.site_metrics
                    .nsmallest(5, "mae")["site_id"]
                    .tolist()
                )

                df = build_daily_features(con, top_sites)
                df["date"] = pd.to_datetime(df["date"])
                test_df = df[df["date"] >= pd.Timestamp(args.test_start_date)]

                if not test_df.empty:
                    X_test, _ = _prepare_features(
                        test_df, CATEGORICAL_FEATURES, encoder=train_result.encoder,
                    )
                    y_test = test_df["total_volume"].values
                    preds = train_result.model.predict(X_test)

                    # Permutation importance (more reliable than tree-based importance)
                    perm_result = permutation_importance(
                        train_result.model, X_test, y_test,
                        n_repeats=5, random_state=42,
                    )
                    plot_feature_importance(
                        perm_result.importances_mean,
                        train_result.feature_columns,
                        save_path=REPORTS_DIR / f"{train_result.model_id}_importance.png",
                    )

                    # Prediction plots per site
                    plot_df = test_df[["site_id", "date", "total_volume"]].copy()
                    plot_df["actual"] = test_df["total_volume"].values
                    plot_df["predicted"] = preds

                    for site_id in top_sites:
                        plot_predictions(
                            plot_df, site_id,
                            save_path=REPORTS_DIR / f"{train_result.model_id}_site_{site_id}.png",
                        )

                log.info("step_complete", step="evaluate",
                         reports_dir=str(REPORTS_DIR))

    finally:
        con.close()

    log.info("modelling_pipeline_complete")


if __name__ == "__main__":
    main()
