"""Post-ingestion validation checks."""

import sys
from pathlib import Path

from flowcast.config import DB_PATH
from flowcast.db.connection import get_connection
from flowcast.db.schema import ensure_schema
from flowcast.utils.logging import setup_logging, get_logger

log = get_logger(__name__)


def run_checks(db_path: Path = DB_PATH) -> bool:
    setup_logging()
    con = get_connection(db_path)
    ensure_schema(con)
    passed = 0
    failed = 0

    checks = [
        (
            "traffic_volumes has rows",
            "SELECT COUNT(*) FROM traffic_volumes",
            lambda v: v > 0,
        ),
        (
            "ingestion_manifest has entries",
            "SELECT COUNT(*) FROM ingestion_manifest",
            lambda v: v > 0,
        ),
        (
            "signal_sites populated",
            "SELECT COUNT(*) FROM signal_sites",
            lambda v: v > 0,
        ),
        (
            "no duplicate CSVs in manifest",
            "SELECT COUNT(*) FROM (SELECT csv_filename, COUNT(*) c FROM ingestion_manifest GROUP BY 1 HAVING c > 1)",
            lambda v: v == 0,
        ),
        (
            "traffic_readings view works",
            "SELECT COUNT(*) FROM traffic_readings LIMIT 1",
            lambda v: v >= 0,
        ),
        (
            "date range spans multiple years",
            "SELECT EXTRACT(YEAR FROM MAX(csv_date)) - EXTRACT(YEAR FROM MIN(csv_date)) FROM ingestion_manifest",
            lambda v: v >= 1,
        ),
        (
            "traffic_daily has rows",
            "SELECT COUNT(*) FROM traffic_daily",
            lambda v: v > 0,
        ),
    ]

    for name, query, check_fn in checks:
        try:
            result = con.execute(query).fetchone()[0]
            ok = check_fn(result)
            status = "PASS" if ok else "FAIL"
            if ok:
                passed += 1
            else:
                failed += 1
            log.info("check", name=name, status=status, value=result)
        except Exception as e:
            failed += 1
            log.error("check_error", name=name, error=str(e))

    con.close()
    total = passed + failed
    log.info("validation_complete", passed=passed, failed=failed, total=total)
    return failed == 0


if __name__ == "__main__":
    setup_logging()
    ok = run_checks()
    sys.exit(0 if ok else 1)
