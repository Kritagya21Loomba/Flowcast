"""Database schema definitions and initialization."""

import duckdb

from flowcast.config import V_COLUMNS

# Generate V-column DDL: V00 SMALLINT, V01 SMALLINT, ..., V95 SMALLINT
_V_COLUMN_DDL = ",\n    ".join(f"{col} SMALLINT" for col in V_COLUMNS)

# All V-column names for UNPIVOT
_V_COLUMN_LIST = ", ".join(V_COLUMNS)

TABLES = {
    "ingestion_manifest": f"""
        CREATE TABLE IF NOT EXISTS ingestion_manifest (
            source_zip       VARCHAR NOT NULL,
            inner_zip        VARCHAR,
            csv_filename     VARCHAR NOT NULL,
            csv_date         DATE NOT NULL,
            row_count        INTEGER NOT NULL,
            ingested_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (csv_filename)
        )
    """,
    "traffic_volumes": f"""
        CREATE TABLE IF NOT EXISTS traffic_volumes (
            source_file      VARCHAR NOT NULL,
            nb_scats_site    INTEGER NOT NULL,
            qt_interval_count DATE NOT NULL,
            nb_detector      SMALLINT NOT NULL,
            {_V_COLUMN_DDL},
            nm_region        VARCHAR(8) NOT NULL,
            ct_records       SMALLINT,
            qt_volume_24hour INTEGER,
            ct_alarm_24hour  SMALLINT
        )
    """,
    "signal_sites": """
        CREATE TABLE IF NOT EXISTS signal_sites (
            site_id          INTEGER PRIMARY KEY,
            region           VARCHAR(8) NOT NULL,
            detector_count   SMALLINT,
            first_seen       DATE,
            last_seen        DATE,
            latitude         DOUBLE,
            longitude        DOUBLE,
            intersection_name VARCHAR
        )
    """,
    "traffic_hourly": """
        CREATE TABLE IF NOT EXISTS traffic_hourly (
            site_id          INTEGER NOT NULL,
            date             DATE NOT NULL,
            hour             TINYINT NOT NULL,
            detector         SMALLINT NOT NULL,
            region           VARCHAR(8) NOT NULL,
            volume_sum       INTEGER NOT NULL,
            volume_avg       FLOAT,
            volume_max       SMALLINT,
            volume_min       SMALLINT
        )
    """,
    "traffic_daily": """
        CREATE TABLE IF NOT EXISTS traffic_daily (
            site_id          INTEGER NOT NULL,
            date             DATE NOT NULL,
            region           VARCHAR(8) NOT NULL,
            total_volume     INTEGER NOT NULL,
            detector_count   SMALLINT NOT NULL,
            peak_hour        TINYINT,
            peak_hour_volume INTEGER
        )
    """,
    "road_geometry": """
        CREATE TABLE IF NOT EXISTS road_geometry (
            road_id          INTEGER PRIMARY KEY,
            road_name        VARCHAR,
            geometry_wkt     VARCHAR,
            road_class       VARCHAR
        )
    """,
    "gtfs_routes": """
        CREATE TABLE IF NOT EXISTS gtfs_routes (
            route_id         VARCHAR PRIMARY KEY,
            route_short_name VARCHAR,
            route_long_name  VARCHAR,
            route_type       INTEGER
        )
    """,
    "gtfs_stops": """
        CREATE TABLE IF NOT EXISTS gtfs_stops (
            stop_id          VARCHAR PRIMARY KEY,
            stop_name        VARCHAR,
            stop_lat         DOUBLE,
            stop_lon         DOUBLE
        )
    """,
    "site_clusters": """
        CREATE TABLE IF NOT EXISTS site_clusters (
            site_id          INTEGER PRIMARY KEY,
            cluster_id       INTEGER NOT NULL,
            cluster_label    VARCHAR,
            profile_vector   DOUBLE[24],
            silhouette_score FLOAT,
            assigned_at      TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """,
    "model_registry": """
        CREATE TABLE IF NOT EXISTS model_registry (
            model_id         VARCHAR PRIMARY KEY,
            model_type       VARCHAR NOT NULL,
            scope            VARCHAR NOT NULL,
            target_column    VARCHAR NOT NULL,
            feature_columns  VARCHAR NOT NULL,
            n_training_rows  INTEGER,
            train_start_date DATE,
            train_end_date   DATE,
            test_mae         FLOAT,
            test_rmse        FLOAT,
            test_mape        FLOAT,
            artifact_path    VARCHAR,
            trained_at       TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """,
    "forecasts": """
        CREATE TABLE IF NOT EXISTS forecasts (
            forecast_id      INTEGER,
            model_id         VARCHAR NOT NULL,
            site_id          INTEGER NOT NULL,
            forecast_date    DATE NOT NULL,
            horizon_days     INTEGER NOT NULL,
            predicted_volume FLOAT NOT NULL,
            prediction_lower FLOAT,
            prediction_upper FLOAT,
            actual_volume    INTEGER,
            created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """,
    "model_metrics_site": """
        CREATE TABLE IF NOT EXISTS model_metrics_site (
            model_id         VARCHAR NOT NULL,
            site_id          INTEGER NOT NULL,
            mae              FLOAT,
            rmse             FLOAT,
            mape             FLOAT,
            n_test_days      INTEGER,
            PRIMARY KEY (model_id, site_id)
        )
    """,
    "site_correlations": """
        CREATE TABLE IF NOT EXISTS site_correlations (
            site_a           INTEGER NOT NULL,
            site_b           INTEGER NOT NULL,
            pearson_daily    FLOAT,
            cosine_hourly    FLOAT,
            lag_minutes      INTEGER,
            PRIMARY KEY (site_a, site_b)
        )
    """,
    "site_weather_daily": """
        CREATE TABLE IF NOT EXISTS site_weather_daily (
            site_id            INTEGER NOT NULL,
            date               DATE NOT NULL,
            rainfall_mm        FLOAT,
            temperature_c      FLOAT,
            wind_kmh           FLOAT,
            severe_weather_flag TINYINT DEFAULT 0,
            PRIMARY KEY (site_id, date)
        )
    """,
    "site_events_daily": """
        CREATE TABLE IF NOT EXISTS site_events_daily (
            site_id            INTEGER NOT NULL,
            date               DATE NOT NULL,
            afl_games_count    INTEGER DEFAULT 0,
            concerts_count     INTEGER DEFAULT 0,
            cbd_events_count   INTEGER DEFAULT 0,
            roadworks_flag     TINYINT DEFAULT 0,
            school_zone_flag   TINYINT DEFAULT 0,
            PRIMARY KEY (site_id, date)
        )
    """,
    "site_graph_features": """
        CREATE TABLE IF NOT EXISTS site_graph_features (
            site_id            INTEGER PRIMARY KEY,
            degree             INTEGER DEFAULT 0,
            weighted_degree    FLOAT DEFAULT 0,
            centrality         FLOAT DEFAULT 0,
            clustering_coeff   FLOAT DEFAULT 0,
            updated_at         TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """,
    "detector_health_daily": """
        CREATE TABLE IF NOT EXISTS detector_health_daily (
            site_id            INTEGER NOT NULL,
            date               DATE NOT NULL,
            detector_count     SMALLINT,
            zero_interval_pct  FLOAT,
            stuck_score        FLOAT,
            health_flag        VARCHAR,
            PRIMARY KEY (site_id, date)
        )
    """,
    "site_diagnostics": """
        CREATE TABLE IF NOT EXISTS site_diagnostics (
            model_id           VARCHAR NOT NULL,
            site_id            INTEGER NOT NULL,
            residual_mean      FLOAT,
            residual_std       FLOAT,
            residual_mape      FLOAT,
            flagged            TINYINT DEFAULT 0,
            reason             VARCHAR,
            PRIMARY KEY (model_id, site_id)
        )
    """,
}

VIEWS = {
    "traffic_readings": f"""
        CREATE OR REPLACE VIEW traffic_readings AS
        SELECT
            nb_scats_site    AS site_id,
            qt_interval_count AS date,
            nb_detector      AS detector,
            nm_region        AS region,
            CAST(REPLACE(interval_code, 'V', '') AS INTEGER) AS interval_num,
            qt_interval_count + INTERVAL (CAST(REPLACE(interval_code, 'V', '') AS INTEGER) * 15) MINUTE
                             AS timestamp,
            volume,
            source_file
        FROM traffic_volumes
        UNPIVOT (
            volume FOR interval_code IN ({_V_COLUMN_LIST})
        )
    """,
}


def ensure_views(con: duckdb.DuckDBPyConnection) -> None:
    """Recreate views only. Use this for API connections where tables already exist."""
    for name, ddl in VIEWS.items():
        con.execute(ddl)


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create all tables and views if they don't exist."""
    for name, ddl in TABLES.items():
        con.execute(ddl)
    ensure_views(con)
