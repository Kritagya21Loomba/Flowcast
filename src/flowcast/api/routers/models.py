"""Model registry and per-site metrics endpoints."""

from fastapi import APIRouter, Depends, Query
import duckdb

from flowcast.api.deps import get_db
from flowcast.api.schemas import ModelSummary, SiteMetric

router = APIRouter()


@router.get("/models", response_model=list[ModelSummary])
def list_models(
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[ModelSummary]:
    rows = con.execute("""
        SELECT model_id, model_type, scope, test_mae, test_rmse, test_mape,
               trained_at, n_training_rows
        FROM model_registry
        ORDER BY trained_at DESC
    """).fetchall()

    return [
        ModelSummary(
            model_id=r[0], model_type=r[1], scope=r[2],
            test_mae=r[3], test_rmse=r[4], test_mape=r[5],
            trained_at=str(r[6]) if r[6] else None,
            n_training_rows=r[7],
        )
        for r in rows
    ]


@router.get("/models/{model_id}/sites", response_model=list[SiteMetric])
def get_model_site_metrics(
    model_id: str,
    sort_by: str = Query(default="mae", pattern="^(mae|rmse|mape)$"),
    limit: int = Query(default=50, ge=1, le=500),
    con: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[SiteMetric]:
    rows = con.execute(f"""
        SELECT site_id, mae, rmse, mape, n_test_days
        FROM model_metrics_site
        WHERE model_id = ?
        ORDER BY {sort_by} ASC
        LIMIT {limit}
    """, [model_id]).fetchall()

    return [
        SiteMetric(
            site_id=r[0], mae=r[1], rmse=r[2], mape=r[3], n_test_days=r[4],
        )
        for r in rows
    ]
