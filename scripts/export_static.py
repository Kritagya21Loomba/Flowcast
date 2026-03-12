"""Export all API responses as static JSON files for GitHub Pages deployment.

Generates /data/*.json files that mirror the /api/* endpoint structure,
allowing the frontend to work without a backend server.
"""

import json
from pathlib import Path

from fastapi.testclient import TestClient

from flowcast.api.app import app
from flowcast.utils.logging import setup_logging, get_logger

STATIC_DIR = Path(__file__).resolve().parent.parent / "frontend" / "public" / "data"


def export_static():
    setup_logging()
    log = get_logger("export_static")

    client = TestClient(app)
    out = STATIC_DIR
    out.mkdir(parents=True, exist_ok=True)

    def save(path: str, data):
        fp = out / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(json.dumps(data, default=str), encoding="utf-8")
        log.info("wrote", path=str(fp.relative_to(out)), size_kb=round(fp.stat().st_size / 1024, 1))

    # 1. Overview
    r = client.get("/api/overview")
    save("overview.json", r.json())

    # 2. All sites
    r = client.get("/api/sites")
    sites_data = r.json()
    save("sites.json", sites_data)

    # 3. All clusters summary
    r = client.get("/api/clusters")
    save("clusters.json", r.json())

    # 4. Per-cluster detail
    for cid in range(8):
        r = client.get(f"/api/clusters/{cid}")
        if r.status_code == 200:
            save(f"clusters/{cid}.json", r.json())

    # 5. Correlations — export all at low threshold for client-side filtering
    r = client.get("/api/correlations?min_pearson=0.5&limit=10000")
    save("correlations.json", r.json())

    # 6. Models
    r = client.get("/api/models")
    models = r.json()
    save("models.json", models)

    # 7. Per-model site metrics — all sites, all sort orders are same data
    for m in models:
        mid = m["model_id"]
        r = client.get(f"/api/models/{mid}/sites?sort_by=mape&limit=500")
        if r.status_code == 200:
            save(f"models/{mid}/sites.json", r.json())

    # 8. Per-site detail + forecasts for ALL sites with coordinates
    #    Skip files that already exist to allow resuming after crashes.
    site_ids_with_coords = []
    for s in sites_data.get("sites", []):
        if s.get("latitude") is not None:
            site_ids_with_coords.append(s["site_id"])

    total = len(site_ids_with_coords)
    skipped = 0
    for i, site_id in enumerate(sorted(site_ids_with_coords), 1):
        if i % 500 == 0 or i == total:
            log.info("sites progress", done=i, total=total, skipped=skipped)

        detail_path = out / f"sites/{site_id}.json"
        if not detail_path.exists():
            r = client.get(f"/api/sites/{site_id}?days=90")
            if r.status_code == 200:
                save(f"sites/{site_id}.json", r.json())
        else:
            skipped += 1

        forecast_path = out / f"sites/{site_id}/forecasts.json"
        if not forecast_path.exists():
            r = client.get(f"/api/sites/{site_id}/forecasts")
            if r.status_code == 200:
                save(f"sites/{site_id}/forecasts.json", r.json())

    log.info("static export complete", total_sites=total)


if __name__ == "__main__":
    export_static()
