"""Central configuration for the Flowcast platform."""

import os
from pathlib import Path

# Project root is the directory containing pyproject.toml
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "raw"
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = Path(os.environ.get("FLOWCAST_DB_PATH", str(DATA_DIR / "flowcast.duckdb")))

TEMP_DIR_PREFIX = "flowcast_"

# Modelling directories
MODELS_DIR = DATA_DIR / "models"
REPORTS_DIR = DATA_DIR / "reports"

# API settings
API_HOST = "0.0.0.0"
API_PORT = int(os.environ.get("PORT", "8000"))
FRONTEND_DIST_DIR = PROJECT_ROOT / "frontend" / "dist"

# Modelling defaults
DEFAULT_MAX_SITES = 500
DEFAULT_N_CLUSTERS = 8
DEFAULT_TEST_START_DATE = "2025-10-01"
DEFAULT_MIN_HISTORY_DAYS = 365

# DuckDB settings
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "256MB")
DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS", "2"))

# Expected CSV schema — the 103 columns found in VSDATA files
V_COLUMNS = [f"V{i:02d}" for i in range(96)]

EXPECTED_COLUMNS = [
    "NB_SCATS_SITE",
    "QT_INTERVAL_COUNT",
    "NB_DETECTOR",
    *V_COLUMNS,
    "NM_REGION",
    "CT_RECORDS",
    "QT_VOLUME_24HOUR",
    "CT_ALARM_24HOUR",
]
