# Flowcast — Melbourne Traffic Intelligence & Forecasting Platform

Flowcast ingests three years of Victorian SCATS traffic signal data (131 million rows across 4,741 intersections), trains a gradient-boosted forecasting model that predicts daily traffic volumes at 3.9% error, clusters sites into behavioural profiles, infers origin-destination relationships, and presents everything through an interactive React dashboard.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Using the Dashboard](#using-the-dashboard)
   - [Dashboard (Home)](#dashboard-home)
   - [Site Map](#site-map)
   - [Traffic Profiles (Clusters)](#traffic-profiles-clusters)
   - [Linked Sites (Correlations)](#linked-sites-correlations)
   - [Forecast Models](#forecast-models)
3. [How Predictions Work](#how-predictions-work)
4. [Project Architecture](#project-architecture)
5. [Data Ingestion Pipeline](#data-ingestion-pipeline)
6. [Database Schema](#database-schema)
7. [Geocoding](#geocoding)
8. [Modelling Pipeline](#modelling-pipeline)
   - [Site Selection](#site-selection)
   - [Clustering](#clustering)
   - [Feature Engineering](#feature-engineering)
   - [Model Training](#model-training)
   - [Forecast Generation](#forecast-generation)
   - [Origin-Destination Inference](#origin-destination-inference)
9. [REST API](#rest-api)
10. [Frontend](#frontend)
11. [Testing](#testing)
12. [Project Structure](#project-structure)
13. [Configuration Reference](#configuration-reference)
14. [Dependencies](#dependencies)

---

## Quick Start

### Prerequisites

- Python 3.12+
- Node.js 22+ and npm 11+
- [uv](https://docs.astral.sh/uv/) package manager
- ~5.3 GB of raw SCATS ZIP files in `raw/` (not included in the repository)

### 1. Install Python dependencies

```bash
uv sync --all-extras
```

### 2. Run the data ingestion pipeline

```bash
.venv/Scripts/python scripts/ingest.py
```

This discovers all ZIP files in `raw/`, extracts CSVs, loads them into DuckDB, and builds aggregate tables. The pipeline is idempotent — re-running it skips already-ingested files.

### 3. Geocode intersection sites

```bash
.venv/Scripts/python scripts/geocode.py
```

Downloads the Victorian Traffic Signals CSV from the Victorian Government open data portal and updates each site with its latitude, longitude, and intersection name. Achieves 93.8% geocoding coverage (4,449 of 4,741 sites).

### 4. Run the modelling pipeline

```bash
.venv/Scripts/python scripts/model.py
```

Selects the top 500 sites by data quality, clusters them into 8 behavioural groups, trains a HistGradientBoosting forecasting model, generates multi-horizon forecasts, and computes site-to-site correlations. Takes roughly 5–10 minutes.

### 5. Start the API server

```bash
.venv/Scripts/python scripts/serve.py
```

Starts the FastAPI server on `http://localhost:8000`. The API serves data from the DuckDB database via read-only connections.

### 6. Build and run the frontend

```bash
cd frontend
npm install
npm run dev
```

Opens the React dashboard at `http://localhost:5173` with hot-reload. The Vite dev server proxies `/api` requests to the FastAPI backend.

For production, build the frontend (`npm run build`) and the FastAPI server will serve the static files from `frontend/dist/` automatically.

### 7. Run the test suite

```bash
.venv/Scripts/python -m pytest tests/ -v
```

Runs all 58 tests across ingestion, modelling, and API layers.

---

## Using the Dashboard

The dashboard has five pages, accessible from the sidebar navigation.

### Dashboard (Home)

The landing page provides a platform overview and explains what Flowcast does:

- **Forecast Accuracy Ring** — An SVG donut chart showing the best model's accuracy (100% minus MAPE). A 96.1% accuracy means the model's average daily volume prediction is within 3.9% of the actual value.
- **KPI Cards** — Geocoded intersections, clustered sites, correlated site pairs, and data coverage date range. Each card links to its corresponding detail page.
- **How Flowcast Works** — A four-step pipeline summary: Ingest → Profile & Cluster → Forecast → Correlate.

### Site Map

An interactive WebGL-rendered map of all 4,449 geocoded Melbourne intersections.

- **Circle markers** represent sites. Circle size encodes average daily traffic volume. Colour encodes one of three modes toggled via the legend:
  - **Volume** (default): Blue-to-yellow-to-red ramp (0 to 200k vehicles/day).
  - **Cluster**: 8-colour palette showing which behavioural cluster the site belongs to.
  - **Region**: Colour by SCATS administrative region (NW, NE, SE, SW, CENT, INNE).
- **Search bar** (top-right): Search by site ID or intersection name. Selecting a result flies the map to that site and opens its detail panel.
- **Click a site** to open the **Site Panel** on the right:
  - **Traffic Forecast** section (highlighted): Shows predicted volumes for horizons of 1, 7, 14, and 28 days with a confidence band chart. If actuals are available, an accuracy badge shows the model's performance for this specific site.
  - **Historical Volume**: Area chart of the last 90 days of daily traffic.
  - **Hourly Profile**: Bar chart showing the typical 24-hour traffic shape from the site's cluster assignment.
  - **Forecast Table**: Date, horizon, predicted volume, and actual volume (if available).

**What the forecast tells you**: For any given site, the predicted volume is the model's best estimate of how many vehicles will pass through that intersection on a future date. The confidence band shows the plausible range (approximately 95% of the time, the actual value should fall within this band). Comparing predicted vs. actual volumes tells you how reliable the forecast is for this intersection.

### Traffic Profiles (Clusters)

Shows how Melbourne's intersections group into 8 distinct behavioural patterns based on their 24-hour traffic shapes.

- **Radar Chart Overlay**: All 8 cluster profiles superimposed on a single polar chart. Each spoke represents one hour of the day (0–23). Values show what fraction of total daily traffic occurs in each hour. This reveals at a glance which clusters are morning-peaking, evening-peaking, or flat.
- **AM vs PM Scatter Plot**: A bubble chart separating clusters by their AM peak share (06:00–10:00 traffic proportion) vs. PM peak share (16:00–20:00 traffic proportion). Bubble size indicates how many sites belong to that cluster. Clusters that are more spread apart have more distinct traffic behaviours.
- **Cluster Cards**: One card per cluster showing its individual radar chart, peak hour, AM/PM share percentages, average volume, and silhouette score (a measure of how well-separated this cluster is from others — values closer to 1.0 mean tighter, more distinct clusters).
- **Cross-highlighting**: Hovering on a scatter bubble highlights the corresponding radar line and cluster card, and vice versa.

**What the clusters tell you**: Intersections in the same cluster behave similarly throughout the day. A cluster with a sharp 8am spike is likely a commuter corridor. A flat cluster might be a commercial/retail area. This grouping is used as a feature in the forecasting model — the model learns that sites with the same temporal pattern tend to respond similarly to weekends, holidays, and weather.

### Linked Sites (Correlations)

Shows which pairs of intersections have correlated daily traffic patterns, suggesting they may be part of the same travel corridor or origin-destination route.

- **Controls**: Adjust the minimum Pearson correlation threshold (default 0.85) and maximum pair count with a slider and dropdown.
- **Connection Map**: MapLibre lines connecting correlated site pairs. Line colour indicates correlation strength (yellow=0.80 → green=0.95+). Line width also scales with strength.
- **Pearson vs Cosine Scatter**: Each dot is a site pair. X-axis is Pearson correlation (daily volume patterns), Y-axis is cosine similarity (hourly profile shapes). Dot size encodes the estimated time lag between the two sites.
- **Pairs Table**: Lists every pair with strength badge (Weak/Moderate/Strong/Very Strong), visual Pearson and cosine bars, and lag in minutes.

**What the correlations tell you**: Two sites with high Pearson correlation see similar daily volume rises and falls — when one is busy, the other tends to be busy too. High cosine similarity means their hourly shapes are alike (e.g., both peak at 8am). A positive lag (e.g., +30 minutes) means traffic at site B tends to follow site A by 30 minutes, suggesting vehicles travel from A to B. Together, these metrics help identify travel corridors and potential origin-destination pairs without requiring individual vehicle tracking.

### Forecast Models

Detailed performance metrics for each trained model.

- **Model Cards**: One per model showing an accuracy ring, model type (HistGradientBoosting), scope (global), and key metrics (MAPE, MAE, RMSE, training sample count).
- **MAPE Distribution Histogram**: How many sites fall into each error bracket (0–2%, 2–4%, 4–6%, etc.). Most sites should cluster in the low-error buckets.
- **Accuracy Summary**: Mean MAPE, median MAPE, percentage of sites with ≤5% error, and total sites evaluated.
- **Per-Site Table**: Every modelled site ranked by accuracy with visual MAPE bars and rating badges (Excellent ≤3%, Good ≤5%, Fair ≤10%, Poor >10%).

**What the model metrics tell you**: MAPE (Mean Absolute Percentage Error) is the primary accuracy measure — a 3.9% MAPE means the model's predictions are, on average, within 3.9% of the true daily volume. MAE gives the error in absolute vehicle counts. RMSE penalises large errors more heavily. Sites with higher MAPE may have more erratic traffic or insufficient historical data.

---

## How Predictions Work

Flowcast predicts the **total daily vehicle count** at each intersection for 1, 7, 14, and 28 days into the future. Here is how, at a high level:

1. **Historical patterns**: The model looks at each site's recent traffic — what happened yesterday, last week, two weeks ago, and four weeks ago (lag features). It also considers the rolling weekly and monthly averages.

2. **Calendar context**: The model knows what day of the week it is (weekdays are typically busier than weekends), the month (seasonal patterns), whether it is a Victorian public holiday, and whether schools are on holiday.

3. **Site identity**: Each site has a long-term average volume and belongs to one of 8 behavioural clusters. The model learns that, for example, "morning commuter corridor" sites respond differently to weekends than "commercial district" sites.

4. **Gradient boosting**: A HistGradientBoostingRegressor (500 decision trees, each building on the errors of the previous one) combines all these signals into a single prediction. The model was trained on data before October 2025 and tested on data from October 2025 onward.

5. **Recursive forecasting**: For multi-day forecasts, the model predicts day 1 first, then uses that prediction as a "lag" input to predict day 2, and so on through day 28. This means uncertainty compounds over longer horizons.

6. **Confidence intervals**: The model estimates a plausible range using the recent variability (rolling 7-day standard deviation). If traffic has been volatile recently, the confidence band widens.

---

## Project Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Raw SCATS Data (17 ZIPs, ~5.3 GB compressed, ~31.7 GB uncompressed)  │
└────────────────────────────┬────────────────────────────────────────┘
                             │ scripts/ingest.py
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Ingestion Pipeline                                                   │
│  discover → extract → load (idempotent) → transform → checkpoint      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DuckDB (data/flowcast.duckdb)                                        │
│  traffic_volumes (131M rows) · signal_sites (4,741) · traffic_daily   │
│  site_clusters · model_registry · forecasts · site_correlations       │
└────────────────────────────┬────────────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
     scripts/geocode.py  scripts/model.py  scripts/serve.py
     (coordinates)       (ML pipeline)     (FastAPI server)
                                            │
                                            ▼
                                   ┌──────────────────┐
                                   │  React Dashboard  │
                                   │  (Vite + MapLibre │
                                   │   + Recharts)     │
                                   └──────────────────┘
```

---

## Data Ingestion Pipeline

### Source Data

The raw data consists of Victorian SCATS (Sydney Coordinated Adaptive Traffic System) traffic signal volume readings. Each CSV file covers one day and contains one row per site-detector combination with 96 columns of 15-minute interval volume counts (V00 through V95), spanning midnight to 23:45.

- **17 ZIP files** in `raw/` (~5.3 GB compressed, ~31.7 GB uncompressed)
- **Yearly ZIPs** (e.g., `traffic_signal_volume_data_2023.zip`) contain nested monthly ZIPs
- **Monthly ZIPs** (e.g., `traffic_signal_volume_data_january_2025.zip`) contain daily CSVs directly
- **CSV schema**: 103 columns — `NB_SCATS_SITE`, `QT_INTERVAL_COUNT` (date), `NB_DETECTOR`, `V00`–`V95`, `NM_REGION`, `CT_RECORDS`, `QT_VOLUME_24HOUR`, `CT_ALARM_24HOUR`
- **~1,160 daily CSVs** covering January 2023 through March 2026

### Pipeline Steps

The pipeline (`src/flowcast/ingestion/pipeline.py`) runs in six stages:

**1. Discover** (`ingestion/discover.py`): Scans `raw/` for ZIP files, classifies each as yearly or monthly, parses year/month from filenames, opens a sample CSV to validate the 103-column header against the expected schema.

**2. Plan**: Flattens all sources into a chronological list of batches. Yearly ZIPs produce one batch per inner monthly ZIP. Optional month filtering narrows the scope.

**3. Initialize**: Opens a DuckDB connection and creates all tables and views with `CREATE TABLE IF NOT EXISTS`.

**4. Process Batches**: For each batch:
   - Creates a temporary directory (cleaned up after each batch to keep disk usage low).
   - Extracts CSVs from the ZIP (or nested ZIP-within-ZIP for yearly archives).
   - For each CSV:
     - Checks the `ingestion_manifest` table — if this CSV has already been loaded, it is skipped entirely (idempotency).
     - Inserts rows into `traffic_volumes` via DuckDB's `read_csv()` function within an explicit transaction.
     - Records the CSV in `ingestion_manifest` with provenance metadata (source ZIP, inner ZIP, filename, date, row count).
     - Transaction commits on success, rolls back on failure.

**5. Transform**:
   - Creates the `traffic_readings` UNPIVOT view (wide-to-long transformation of 96 V-columns into individual rows).
   - Populates `signal_sites` dimension table (aggregates site metadata from `traffic_volumes` — region via `MODE()`, detector count via `COUNT(DISTINCT)`, date range).
   - Builds `traffic_daily` aggregate table using inline SQL that sums groups of 4 V-columns per hour (24 hourly sums) without row expansion, then finds the peak hour via `GREATEST()`.

**6. Checkpoint**: Flushes the DuckDB Write-Ahead Log to disk and reports summary statistics.

### Key Design Decisions

- **Wide-format storage**: Storing data in the original 96-column format avoids materialising 12.6 billion long-format rows (131M × 96). The UNPIVOT view handles long-format access when needed.
- **Manifest-based idempotency**: The `ingestion_manifest` table with `csv_filename` as primary key ensures no CSV is loaded twice, making the pipeline safe to interrupt and resume.
- **Per-batch temp cleanup**: With ~31.7 GB uncompressed, extracting everything at once would be impractical. Each batch extracts to a fresh temp directory that is deleted immediately after loading.
- **Transaction-per-CSV**: Each CSV load is wrapped in an explicit `BEGIN`/`COMMIT`/`ROLLBACK` block, providing fine-grained crash recovery.

---

## Database Schema

DuckDB database at `data/flowcast.duckdb`. All tables created with `IF NOT EXISTS` for idempotency.

### Core Tables

| Table | Purpose | Key Columns |
|---|---|---|
| `traffic_volumes` | Raw SCATS data (wide format) | `nb_scats_site`, `qt_interval_count` (date), `nb_detector`, `V00`–`V95` (96 × 15-min volumes), `nm_region` |
| `ingestion_manifest` | Tracks loaded CSVs | `csv_filename` (PK), `source_zip`, `inner_zip`, `csv_date`, `row_count`, `ingested_at` |
| `signal_sites` | Site dimension table | `site_id` (PK), `region`, `detector_count`, `first_seen`, `last_seen`, `latitude`, `longitude`, `intersection_name` |
| `traffic_daily` | Materialised daily aggregates | `site_id`, `date`, `region`, `total_volume`, `detector_count`, `peak_hour`, `peak_hour_volume` |
| `traffic_hourly` | Hourly aggregates | `site_id`, `date`, `hour`, `detector`, `region`, `volume_sum`, `volume_avg`, `volume_max`, `volume_min` |

### Modelling Tables

| Table | Purpose | Key Columns |
|---|---|---|
| `site_clusters` | Cluster assignments | `site_id` (PK), `cluster_id`, `profile_vector` (DOUBLE[24] — normalised hourly profile), `silhouette_score` |
| `model_registry` | Model metadata | `model_id` (PK), `model_type`, `scope`, `target_column`, `feature_columns` (JSON), `n_training_rows`, `test_mae`, `test_rmse`, `test_mape`, `artifact_path` |
| `model_metrics_site` | Per-site evaluation | `model_id` + `site_id` (composite PK), `mae`, `rmse`, `mape`, `n_test_days` |
| `forecasts` | Generated predictions | `forecast_id`, `model_id`, `site_id`, `forecast_date`, `horizon_days`, `predicted_volume`, `prediction_lower`, `prediction_upper`, `actual_volume` |
| `site_correlations` | Correlated site pairs | `site_a` + `site_b` (composite PK), `pearson_daily`, `cosine_hourly`, `lag_minutes` |

### Views

| View | Purpose |
|---|---|
| `traffic_readings` | UNPIVOT of `traffic_volumes` — transforms each wide row into 96 long-format rows with computed `timestamp` and `volume` columns |

**Note**: Due to a DuckDB 1.5 bug, UNPIVOT views do not persist across connections. The `ensure_views()` function re-creates them on each new read-write connection. Read-only API connections skip this entirely and query materialised tables directly.

---

## Geocoding

Sites are geocoded using the Victorian Government open data portal's **Victorian Traffic Signals CSV**, which maps SCATS site numbers to latitude, longitude, and intersection names.

**Script**: `scripts/geocode.py`

1. Downloads the CSV from `https://opendata.transport.vic.gov.au/...`
2. Parses site coordinates with BOM-safe UTF-8 reading.
3. Updates `signal_sites` rows where `latitude IS NULL OR longitude IS NULL` (idempotent — won't overwrite existing coordinates).
4. Result: 4,449 of 4,741 sites geocoded (93.8% coverage).

---

## Modelling Pipeline

The modelling pipeline (`scripts/model.py`) runs six sequential steps: select → cluster → train → forecast → od → evaluate.

### Site Selection

**Module**: `src/flowcast/modelling/site_selection.py`

Not all 4,741 sites are suitable for modelling. Sites are filtered through four criteria:

| Criterion | Threshold | Reason |
|---|---|---|
| Minimum history | ≥365 days of data | Need sufficient history for lag features and seasonality |
| Maximum zero-volume % | <10% zero-volume days | High zero rates indicate faulty detectors or inactive sites |
| Volume floor | Above 25th percentile of avg volume | Exclude very low-volume sites where prediction is less meaningful |
| Cap | Top 500 by quality score | Quality score = `data_days × avg_volume` (balances data quantity and importance) |

**Result**: 500 sites selected for modelling.

### Clustering

**Module**: `src/flowcast/modelling/clusters.py`

Sites are grouped by their temporal traffic shape — how traffic distributes across the 24 hours of a day.

**Process**:
1. For each site, compute the average volume for each hour (0–23) by summing groups of 4 V-columns from `traffic_volumes`.
2. Normalise each site's 24-hour profile to sum to 1.0 (converting absolute volumes to proportional shares).
3. Run **KMeans clustering** (`n_clusters=8`, `n_init=10`, `random_state=42`) on the 24-dimensional normalised profiles.
4. Compute per-site **silhouette scores** to measure cluster separation quality.

**Result**: 8 clusters capturing patterns like morning commuter corridors, evening peaks, flat commercial traffic, and dual AM/PM peaks. Stored in `site_clusters` with the full 24-element profile vector.

### Feature Engineering

**Module**: `src/flowcast/modelling/features.py`

The model predicts `total_volume` (daily vehicle count) from 18 features computed via a single SQL query with DuckDB window functions, plus Python-side holiday lookups.

| Category | Features | Details |
|---|---|---|
| **Lag features** (4) | `volume_lag_1`, `volume_lag_7`, `volume_lag_14`, `volume_lag_28` | Previous day's volume, same-day-last-week, two-weeks-ago, four-weeks-ago |
| **Rolling features** (3) | `volume_rolling_mean_7`, `volume_rolling_mean_28`, `volume_rolling_std_7` | Trailing averages and volatility (window excludes current day) |
| **Calendar features** (6) | `day_of_week`, `month`, `day_of_month`, `week_of_year`, `quarter`, `is_weekend` | Temporal position features |
| **Holiday features** (2) | `is_public_holiday`, `is_school_holiday` | Victorian public holidays (via `holidays` library) and school term dates (hardcoded 2023–2026 windows) |
| **Site features** (3) | `site_avg_volume`, `cluster_id`, `detector_count` | Long-term average, behavioural cluster, and detector count |

The first 28 days of each site's history are dropped because `volume_lag_28` is not yet available.

### Model Training

**Module**: `src/flowcast/modelling/train.py`

**Algorithm**: `sklearn.ensemble.HistGradientBoostingRegressor` — chosen for its native categorical feature support, built-in missing value handling, histogram-based splitting efficiency on large datasets, and integrated early stopping.

**Hyperparameters**:

| Parameter | Value |
|---|---|
| `max_iter` | 500 |
| `max_depth` | 8 |
| `learning_rate` | 0.05 |
| `min_samples_leaf` | 20 |
| `l2_regularization` | 0.1 |
| `early_stopping` | True |
| `validation_fraction` | 0.1 (10% for early stopping) |
| `n_iter_no_change` | 20 |
| `random_state` | 42 |

**Train/test split**: Temporal split at October 1, 2025. All data before this date is training; all data on or after is test. This prevents data leakage from future-to-past.

**Categorical features**: `region`, `day_of_week`, `month`, `quarter` — encoded via `OrdinalEncoder` with unknown handling (encoded as -1).

**Evaluation metrics**:
- **MAE** (Mean Absolute Error): Average absolute difference in vehicles.
- **RMSE** (Root Mean Squared Error): Penalises large errors more heavily.
- **MAPE** (Mean Absolute Percentage Error): Average percentage error (only where actuals > 0).

**Result**: ~3.9% MAPE across all 500 test sites. Model artifact saved as `.joblib` file in `data/models/`.

### Forecast Generation

**Module**: `src/flowcast/modelling/forecast.py`

Forecasts are generated using **recursive (iterative) multi-step forecasting**:

1. For each site, fetch historical features up to the as-of date.
2. Extract the last 28 days of actual volumes as a sliding window.
3. For each day from 1 to 28:
   - Build a synthetic feature row using calendar features for the target date, lag features from the sliding window, and rolling statistics.
   - Predict using the trained model.
   - Clamp prediction to ≥0 (volume cannot be negative).
   - **Append the prediction to the sliding window** — future predictions use prior predictions as inputs. This is what makes it "recursive".
4. Only horizon days 1, 7, 14, and 28 are stored in the `forecasts` table, but all intermediate days are predicted to maintain the recursive lag chain.

**Confidence intervals**: Heuristic, not probabilistic.
- If recent 7-day rolling standard deviation is available: `prediction ± 1.96 × std` (approximating a 95% interval).
- Fallback: `prediction × 0.8` to `prediction × 1.2` (±20% band).

**Backfill**: The `backfill_actuals()` function later updates `forecasts.actual_volume` from `traffic_daily` when the actual data becomes available, enabling forecast accuracy evaluation.

### Origin-Destination Inference

**Module**: `src/flowcast/modelling/od_inference.py`

Identifies pairs of sites with correlated traffic patterns, suggesting they may be on the same travel corridor.

**Three metrics are computed for each pair**:

1. **Pearson correlation** (`compute_daily_correlations`): Correlation of daily total volumes between two sites. Requires ≥180 overlapping days. Values near 1.0 mean the sites consistently get busy and quiet on the same days.

2. **Cosine similarity** (`compute_hourly_profile_similarity`): Similarity of normalised 24-hour profiles (same profiles used for clustering). Values near 1.0 mean the sites have the same temporal shape.

3. **Lag estimation** (`estimate_lag`): Cross-correlation at 15-minute resolution over ±16 intervals (±240 minutes). The lag at maximum cross-correlation estimates how much later traffic arrives at site B after passing site A. Positive lag = B follows A.

**Network filtering**: Only pairs meeting **both** `pearson ≥ 0.7` **and** `cosine ≥ 0.85` are stored in `site_correlations`.

---

## REST API

**Framework**: FastAPI + uvicorn, mounted at `/api`.

All endpoints are **read-only** (GET only). CORS allows the Vite dev server (`http://localhost:5173`). Each request gets a fresh read-only DuckDB connection that is closed after the response.

### Endpoints

| Method | Path | Description | Key Parameters |
|---|---|---|---|
| GET | `/api/overview` | Platform statistics | — |
| GET | `/api/sites` | List all sites | `region?`, `cluster_id?` |
| GET | `/api/sites/{site_id}` | Site detail + recent volumes + hourly profile | `days?` (1–365, default 90) |
| GET | `/api/sites/{site_id}/forecasts` | Forecast predictions for a site | `model_id?` (defaults to latest model) |
| GET | `/api/clusters` | All cluster summaries with profiles | — |
| GET | `/api/clusters/{cluster_id}` | Cluster detail with member sites | — |
| GET | `/api/correlations` | Correlated site pairs | `min_pearson?` (0–1, default 0.8), `limit?` (1–10000, default 500), `site_id?` |
| GET | `/api/models` | All trained models | — |
| GET | `/api/models/{model_id}/sites` | Per-site metrics for a model | `sort_by?` (mae/rmse/mape), `limit?` (1–500, default 50) |

**Production serving**: When `frontend/dist/` exists, FastAPI mounts it as a static file server with `html=True` for SPA routing fallback, meaning the entire app can be served from a single process.

**Connection management**: Each request receives a fresh DuckDB read-only connection via FastAPI's dependency injection (`Depends(get_db)`). The connection is closed in a `finally` block after the response. Read-only mode (`read_only=True`) allows safe concurrent reads without locking. The read-only connection does not create views (which would require write access), so all API queries use materialised tables directly.

---

## Frontend

**Stack**: Vite 7 + React 19 + TypeScript 5.9 + MapLibre GL JS 5 + Recharts 3 + React Query 5 + React Router 7.

### Architecture

- **Routing**: `BrowserRouter` with 5 routes nested in a shared `Layout` (sidebar + content area).
- **Data fetching**: React Query hooks with 5-minute stale time and 1 retry. All data comes from the `/api` endpoints.
- **Styling**: Component-scoped CSS files with a global dark theme (`#111318` background, `#e0e0e0` text, `#60a5fa` accent blue).
- **Maps**: MapLibre GL JS with Carto Dark Matter basemap tiles (no API key needed). Sites rendered as WebGL GeoJSON circle layers (not DOM markers) for GPU-accelerated rendering.
- **Charts**: Recharts library for all statistical charts (area, bar, scatter, radar, composed). Custom SVG for accuracy ring donut charts.

### Visualisation Techniques Used

| Technique | Pages |
|---|---|
| WebGL GeoJSON circle layer (data-driven radius + colour) | Site Map |
| GeoJSON line layer (data-driven colour + width) | Linked Sites |
| MapLibre popup (hover tooltip) | Site Map, Linked Sites |
| Fly-to camera animation | Site Map (search, click) |
| Area chart (gradient fill) | Site Panel (daily volume) |
| Bar chart | Site Panel (hourly profile), Models (MAPE histogram) |
| Composed chart (area + line + scatter) | Site Panel (forecast with confidence band) |
| Radar/spider chart (multi-series overlay) | Traffic Profiles |
| Radar chart (individual) | Traffic Profiles (per-cluster cards) |
| Bubble scatter plot | Traffic Profiles (AM vs PM peak) |
| Scatter plot (coloured dots) | Linked Sites (Pearson vs Cosine) |
| SVG donut/accuracy ring | Dashboard, Models |
| Inline bar cells in tables | Linked Sites, Models |
| Colour-coded badges | Site Panel, Linked Sites, Models |
| Gradient bar legend | Site Map, Linked Sites |

### Vite Configuration

- Dev server proxy: `/api` → `http://localhost:8000` (FastAPI backend).
- Standard `@vitejs/plugin-react` for React Fast Refresh.
- Production build outputs to `frontend/dist/`.

---

## Testing

**Framework**: pytest 8+ with test paths at `tests/` and source path `src/`.

**58 tests** across 10 test files, all using in-memory DuckDB databases for full isolation and zero filesystem side effects.

### Test Breakdown

| Test File | Module Tested | Tests | Approach |
|---|---|---|---|
| `test_discover.py` | `ingestion.discover` | 4 | Creates ZIP fixtures, validates discovery and header checking |
| `test_extract.py` | `ingestion.extract` | 3 | Extracts monthly and nested yearly ZIPs, verifies temp cleanup |
| `test_load.py` | `ingestion.load` | 3 | Loads CSVs, tests idempotency (no double-loads), verifies manifest |
| `test_normalize.py` | `transform.normalize`, `utils.temporal` | 5 | UNPIVOT view row count/timestamps, site population, interval conversion |
| `test_pipeline.py` | `ingestion.pipeline` | 3 | End-to-end pipeline integration, idempotency, dry-run mode |
| `test_features.py` | `modelling.features` | 7 | Feature shape, lag alignment, rolling means, calendar, holidays |
| `test_clusters.py` | `modelling.site_selection`, `modelling.clusters` | 8 | Selection filters, profile shapes, cluster assignments, storage |
| `test_train.py` | `modelling.train`, `modelling.evaluate` | 5 | Training result, metrics, model save/load, edge cases |
| `test_forecast.py` | `modelling.forecast` | 3 | Forecast generation, horizon correctness, actual backfill |
| `test_api.py` | `api` (all 6 routers) | 17 | Every endpoint tested with seeded data and dependency injection override |

### Test Data Strategy

- **Unit/integration tests**: In-memory DuckDB with `ensure_schema()` plus targeted `INSERT` statements.
- **CSV generation**: `conftest.py` provides `make_csv_content()` and `make_csv_content_multi()` helpers that produce synthetic CSVs matching the exact 103-column SCATS schema.
- **ZIP fixtures**: Test ZIPs mirror real-world structure (monthly flat ZIPs, yearly nested ZIPs).
- **API tests**: `seeded_db` fixture manually inserts rows into all 7 API-relevant tables, and FastAPI's `dependency_overrides` replaces the production DuckDB dependency with the test database.

---

## Project Structure

```
Flowcast/
├── raw/                          # Raw SCATS ZIP files (~5.3 GB, gitignored)
├── data/
│   ├── flowcast.duckdb           # DuckDB database (gitignored)
│   ├── models/                   # Trained model .joblib files (gitignored)
│   ├── reports/                  # Generated plots (gitignored)
│   └── victorian_traffic_signals.csv  # Geocoding source (gitignored)
├── src/flowcast/
│   ├── config.py                 # Central configuration (paths, constants, defaults)
│   ├── utils/
│   │   ├── logging.py            # structlog setup (console/JSON output)
│   │   └── temporal.py           # Date/time parsing (interval→time, filename→date, ZIP→year-month)
│   ├── db/
│   │   ├── schema.py             # All table/view DDL (12 tables, 1 UNPIVOT view)
│   │   └── connection.py         # Connection factory (read-write, read-only, transaction context manager)
│   ├── ingestion/
│   │   ├── discover.py           # ZIP discovery and header validation
│   │   ├── extract.py            # ZIP extraction (flat and nested) with temp dir cleanup
│   │   ├── load.py               # DuckDB bulk loading with manifest-based idempotency
│   │   └── pipeline.py           # Pipeline orchestrator (discover→extract→load→transform→checkpoint)
│   ├── transform/
│   │   ├── normalize.py          # UNPIVOT view creation
│   │   ├── sites.py              # signal_sites population from traffic_volumes
│   │   └── aggregates.py         # Daily aggregate materialisation (single-pass from wide format)
│   ├── geocoding/
│   │   └── load_coords.py        # Victorian Traffic Signals CSV download, parsing, coordinate update
│   ├── modelling/
│   │   ├── site_selection.py     # Data quality filtering (min days, max zeros, volume floor, cap)
│   │   ├── clusters.py           # 24-hour profile extraction + KMeans clustering + silhouette scoring
│   │   ├── features.py           # SQL-based feature engineering (lags, rolling, calendar) + holiday features
│   │   ├── holidays_au.py        # Victorian public holidays + school term dates (2023–2026)
│   │   ├── train.py              # HistGBR training, OrdinalEncoder, model save/load/register
│   │   ├── evaluate.py           # MAE/RMSE/MAPE computation + matplotlib plots
│   │   ├── forecast.py           # Recursive multi-step forecast generation + confidence intervals + backfill
│   │   └── od_inference.py       # Pearson correlation, cosine similarity, cross-correlation lag, network filtering
│   └── api/
│       ├── app.py                # FastAPI app factory (CORS, router registration, static file serving)
│       ├── deps.py               # DuckDB read-only dependency injection
│       ├── schemas.py            # All Pydantic response models (11 schemas)
│       └── routers/
│           ├── overview.py       # GET /api/overview
│           ├── sites.py          # GET /api/sites, GET /api/sites/{id}
│           ├── forecasts.py      # GET /api/sites/{id}/forecasts
│           ├── clusters.py       # GET /api/clusters, GET /api/clusters/{id}
│           ├── correlations.py   # GET /api/correlations
│           └── models.py         # GET /api/models, GET /api/models/{id}/sites
├── scripts/
│   ├── ingest.py                 # CLI: data ingestion pipeline
│   ├── geocode.py                # CLI: download + apply site coordinates
│   ├── model.py                  # CLI: full modelling pipeline (select/cluster/train/forecast/od/evaluate)
│   ├── serve.py                  # CLI: start FastAPI/uvicorn server
│   └── validate.py               # CLI: post-ingestion integrity checks (7 validation queries)
├── frontend/
│   ├── index.html                # HTML shell
│   ├── package.json              # Node dependencies (React 19, MapLibre, Recharts, React Query, etc.)
│   ├── vite.config.ts            # Dev proxy to FastAPI, React plugin
│   ├── tsconfig.json             # TypeScript config
│   └── src/
│       ├── main.tsx              # React entry point
│       ├── App.tsx               # Router setup (5 routes)
│       ├── index.css             # Global dark theme styles
│       ├── api/
│       │   ├── types.ts          # TypeScript interfaces (mirrors Pydantic schemas)
│       │   ├── client.ts         # Fetch wrapper (base URL /api)
│       │   └── hooks.ts          # 9 React Query hooks
│       ├── components/
│       │   ├── Layout.tsx/css    # Sidebar navigation + content outlet
│       │   ├── SiteMap.tsx/css   # MapLibre GL map with GeoJSON layers, search fly-to
│       │   ├── SitePanel.tsx/css # Site detail panel (forecast, history, hourly profile)
│       │   ├── DailyVolumeChart.tsx   # Recharts area chart
│       │   ├── HourlyProfileChart.tsx # Recharts bar chart
│       │   └── ForecastChart.tsx      # Recharts composed chart (area + line + scatter)
│       └── pages/
│           ├── DashboardPage.tsx/css   # Hero, accuracy ring, KPIs, pipeline overview
│           ├── MapPage.tsx/css         # Full map with search, legend, site panel
│           ├── ClustersPage.tsx/css    # Radar overlay, scatter plot, cluster cards
│           ├── CorrelationsPage.tsx/css # Connection map, scatter, pairs table
│           └── ModelsPage.tsx/css      # Model cards, MAPE histogram, per-site table
├── tests/
│   ├── conftest.py               # Shared fixtures and CSV generators
│   ├── test_discover.py          # 4 tests
│   ├── test_extract.py           # 3 tests
│   ├── test_load.py              # 3 tests
│   ├── test_normalize.py         # 5 tests
│   ├── test_pipeline.py          # 3 tests
│   ├── test_features.py          # 7 tests
│   ├── test_clusters.py          # 8 tests
│   ├── test_train.py             # 5 tests
│   ├── test_forecast.py          # 3 tests
│   └── test_api.py               # 17 tests
├── pyproject.toml                # Project metadata, dependencies, pytest config
├── uv.lock                       # Locked dependency versions
└── .gitignore                    # Excludes raw/, data/*, .venv/, frontend/node_modules/, frontend/dist/
```

---

## Configuration Reference

All configuration lives in `src/flowcast/config.py`.

| Constant | Value | Purpose |
|---|---|---|
| `RAW_DIR` | `<project_root>/raw` | Source ZIP directory |
| `DATA_DIR` | `<project_root>/data` | Database and artifacts |
| `DB_PATH` | `data/flowcast.duckdb` | Default database path |
| `MODELS_DIR` | `data/models` | Trained model `.joblib` files |
| `REPORTS_DIR` | `data/reports` | Generated PNG plots |
| `FRONTEND_DIST_DIR` | `frontend/dist` | Built frontend static assets |
| `API_HOST` | `0.0.0.0` | FastAPI bind host |
| `API_PORT` | `8000` | FastAPI bind port |
| `DEFAULT_MAX_SITES` | `500` | Maximum sites for modelling |
| `DEFAULT_N_CLUSTERS` | `8` | KMeans cluster count |
| `DEFAULT_TEST_START_DATE` | `2025-10-01` | Train/test temporal split |
| `DEFAULT_MIN_HISTORY_DAYS` | `365` | Minimum site history for modelling |
| `DUCKDB_MEMORY_LIMIT` | `4GB` | Per-connection memory cap |
| `DUCKDB_THREADS` | `4` | DuckDB thread count |
| `V_COLUMNS` | `[V00, V01, ..., V95]` | 96 volume column names |
| `EXPECTED_COLUMNS` | 103-element list | Full CSV header schema |

---

## Dependencies

### Python (pyproject.toml)

| Package | Version | Group | Purpose |
|---|---|---|---|
| `duckdb` | ≥1.0.0 | core | Analytics database |
| `structlog` | ≥24.0.0 | core | Structured logging |
| `fastapi` | ≥0.110 | api | REST API framework |
| `uvicorn` | ≥0.27 | api | ASGI server |
| `pydantic` | ≥2.0 | api | Data validation/serialisation |
| `scikit-learn` | ≥1.4 | model | ML algorithms (HistGBR, KMeans, silhouette) |
| `pandas` | ≥2.2 | model | DataFrames for feature engineering |
| `numpy` | ≥1.26 | model | Numerical operations |
| `holidays` | ≥0.40 | model | Australian public holiday calendar |
| `matplotlib` | ≥3.8 | model | Evaluation plots |
| `joblib` | ≥1.3 | model | Model serialisation |
| `pytest` | ≥8.0 | dev | Test framework |
| `pytest-cov` | ≥5.0 | dev | Code coverage |
| `httpx` | ≥0.27 | dev | HTTP client for API tests |

### Frontend (package.json)

| Package | Version | Purpose |
|---|---|---|
| `react` | ^19.2.0 | UI framework |
| `react-dom` | ^19.2.0 | DOM rendering |
| `react-router-dom` | ^7.13.1 | Client-side routing |
| `@tanstack/react-query` | ^5.90.21 | Data fetching and caching |
| `maplibre-gl` | ^5.19.0 | WebGL map rendering |
| `react-map-gl` | ^8.1.0 | React bindings for MapLibre |
| `recharts` | ^3.8.0 | Statistical charts |
| `vite` | ^7.3.1 | Build tool and dev server |
| `typescript` | ~5.9.3 | Type checking |

---

*Built with DuckDB, scikit-learn, FastAPI, React, MapLibre GL, and Recharts.*
