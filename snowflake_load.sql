-- ============================================================
-- TfL GCS → Snowflake Loading Script
-- ============================================================
-- Run this when you're ready to analyse your captured data.
-- At that point you'll have months of JSON files in GCS.
-- These commands load all of them into Snowflake in one go.
--
-- Run these commands in order, once only.
-- ============================================================


-- ── STEP 1: Create a database and schema for TfL data ────────

CREATE DATABASE IF NOT EXISTS TFL;
USE DATABASE TFL;
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;


-- ── STEP 2: Create a GCS integration ─────────────────────────
-- This tells Snowflake how to talk to your GCS bucket.
-- You only do this once.

CREATE STORAGE INTEGRATION gcs_tfl_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = GCS
    ENABLED                   = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://YOUR_BUCKET_NAME/tfl/');
-- Replace YOUR_BUCKET_NAME with your actual GCS bucket name

-- After running this, run:
DESC INTEGRATION gcs_tfl_integration;
-- Copy the STORAGE_GCP_SERVICE_ACCOUNT value — you'll need to give
-- this service account read access to your GCS bucket in the GCP console.
-- (IAM → add member → Storage Object Viewer role)


-- ── STEP 3: Create an external stage pointing at GCS ─────────
-- A "stage" is Snowflake's name for an external file location.

CREATE STAGE tfl_gcs_stage
    URL                = 'gcs://YOUR_BUCKET_NAME/tfl/'
    STORAGE_INTEGRATION = gcs_tfl_integration
    FILE_FORMAT        = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE);


-- Verify Snowflake can see your files:
LIST @tfl_gcs_stage;


-- ── STEP 4: Create raw tables ─────────────────────────────────
-- Each table has a RAW_JSON column (VARIANT type) to hold the
-- full JSON payload, plus key columns extracted for easy querying.
-- Loading everything into VARIANT first is the safest pattern —
-- you never lose data even if the schema evolves.


-- Line Status
CREATE TABLE IF NOT EXISTS raw_line_status (
    captured_at         TIMESTAMP_TZ,
    line_id             VARCHAR,
    line_name           VARCHAR,
    mode_name           VARCHAR,
    status_severity     NUMBER,
    status_description  VARCHAR,
    reason              VARCHAR,
    disruption_category VARCHAR,
    disruption_type     VARCHAR,
    affected_routes     VARCHAR,
    affected_stops      VARCHAR,
    valid_from          TIMESTAMP_TZ,
    valid_to            TIMESTAMP_TZ,
    _loaded_at          TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- BikePoints
CREATE TABLE IF NOT EXISTS raw_bikepoints (
    captured_at         TIMESTAMP_TZ,
    station_id          VARCHAR,
    station_name        VARCHAR,
    lat                 FLOAT,
    lng                 FLOAT,
    bikes_available     NUMBER,
    e_bikes_available   NUMBER,
    docks_available     NUMBER,
    total_docks         NUMBER,
    occupancy_pct       FLOAT,
    is_installed        VARCHAR,
    is_locked           VARCHAR,
    is_temporary        VARCHAR,
    _loaded_at          TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- Road Disruptions
CREATE TABLE IF NOT EXISTS raw_road_disruptions (
    captured_at         TIMESTAMP_TZ,
    disruption_id       VARCHAR,
    category            VARCHAR,
    sub_category        VARCHAR,
    description         VARCHAR,
    location            VARCHAR,
    lat                 FLOAT,
    lng                 FLOAT,
    severity            VARCHAR,
    is_blocking         BOOLEAN,
    streets_affected    VARCHAR,
    start_date          TIMESTAMP_TZ,
    end_date            TIMESTAMP_TZ,
    last_modified       TIMESTAMP_TZ,
    _loaded_at          TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- Air Quality
CREATE TABLE IF NOT EXISTS raw_air_quality (
    captured_at         TIMESTAMP_TZ,
    forecast_type       VARCHAR,
    forecast_summary    VARCHAR,
    forecast_text       VARCHAR,
    no2_band            VARCHAR,
    o3_band             VARCHAR,
    pm10_band           VARCHAR,
    pm25_band           VARCHAR,
    so2_band            VARCHAR,
    _loaded_at          TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);


-- ── STEP 5: Create a file format for reading JSON from GCS ───

CREATE OR REPLACE FILE FORMAT tfl_json_format
    TYPE            = JSON
    STRIP_OUTER_ARRAY = FALSE   -- our files have a top-level object, not array
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = TRUE;


-- ── STEP 6: Load each endpoint ───────────────────────────────
-- The JSON structure in each file is:
-- {
--   "endpoint": "line_status",
--   "captured_at": "2025-03-15T14:32:00Z",
--   "row_count": 18,
--   "rows": [ {...}, {...}, ... ]
-- }
-- We use FLATTEN to explode the "rows" array into individual rows.


-- Load Line Status
COPY INTO raw_line_status (
    captured_at, line_id, line_name, mode_name,
    status_severity, status_description, reason,
    disruption_category, disruption_type,
    affected_routes, affected_stops,
    valid_from, valid_to
)
FROM (
    SELECT
        TO_TIMESTAMP_TZ(f.value:captured_at::VARCHAR,  'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        f.value:line_id::VARCHAR,
        f.value:line_name::VARCHAR,
        f.value:mode_name::VARCHAR,
        f.value:status_severity::NUMBER,
        f.value:status_description::VARCHAR,
        f.value:reason::VARCHAR,
        f.value:disruption_category::VARCHAR,
        f.value:disruption_type::VARCHAR,
        f.value:affected_routes::VARCHAR,
        f.value:affected_stops::VARCHAR,
        NULLIF(f.value:valid_from::VARCHAR,  '')::TIMESTAMP_TZ,
        NULLIF(f.value:valid_to::VARCHAR,    '')::TIMESTAMP_TZ
    FROM @tfl_gcs_stage/line_status/ s,
    LATERAL FLATTEN(input => s.$1:rows) f
)
FILE_FORMAT = tfl_json_format
ON_ERROR    = CONTINUE;  -- skip malformed files rather than failing entire load


-- Load BikePoints
COPY INTO raw_bikepoints (
    captured_at, station_id, station_name, lat, lng,
    bikes_available, e_bikes_available, docks_available,
    total_docks, occupancy_pct,
    is_installed, is_locked, is_temporary
)
FROM (
    SELECT
        TO_TIMESTAMP_TZ(f.value:captured_at::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        f.value:station_id::VARCHAR,
        f.value:station_name::VARCHAR,
        f.value:lat::FLOAT,
        f.value:lng::FLOAT,
        f.value:bikes_available::NUMBER,
        f.value:e_bikes_available::NUMBER,
        f.value:docks_available::NUMBER,
        f.value:total_docks::NUMBER,
        f.value:occupancy_pct::FLOAT,
        f.value:is_installed::VARCHAR,
        f.value:is_locked::VARCHAR,
        f.value:is_temporary::VARCHAR
    FROM @tfl_gcs_stage/bikepoints/ s,
    LATERAL FLATTEN(input => s.$1:rows) f
)
FILE_FORMAT = tfl_json_format
ON_ERROR    = CONTINUE;


-- Load Road Disruptions
COPY INTO raw_road_disruptions (
    captured_at, disruption_id, category, sub_category,
    description, location, lat, lng,
    severity, is_blocking, streets_affected,
    start_date, end_date, last_modified
)
FROM (
    SELECT
        TO_TIMESTAMP_TZ(f.value:captured_at::VARCHAR,  'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        f.value:disruption_id::VARCHAR,
        f.value:category::VARCHAR,
        f.value:sub_category::VARCHAR,
        f.value:description::VARCHAR,
        f.value:location::VARCHAR,
        f.value:lat::FLOAT,
        f.value:lng::FLOAT,
        f.value:severity::VARCHAR,
        f.value:is_blocking::BOOLEAN,
        f.value:streets_affected::VARCHAR,
        NULLIF(f.value:start_date::VARCHAR,    '')::TIMESTAMP_TZ,
        NULLIF(f.value:end_date::VARCHAR,      '')::TIMESTAMP_TZ,
        NULLIF(f.value:last_modified::VARCHAR, '')::TIMESTAMP_TZ
    FROM @tfl_gcs_stage/road_disruptions/ s,
    LATERAL FLATTEN(input => s.$1:rows) f
)
FILE_FORMAT = tfl_json_format
ON_ERROR    = CONTINUE;


-- Load Air Quality
COPY INTO raw_air_quality (
    captured_at, forecast_type, forecast_summary,
    forecast_text, no2_band, o3_band,
    pm10_band, pm25_band, so2_band
)
FROM (
    SELECT
        TO_TIMESTAMP_TZ(f.value:captured_at::VARCHAR, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        f.value:forecast_type::VARCHAR,
        f.value:forecast_summary::VARCHAR,
        f.value:forecast_text::VARCHAR,
        f.value:no2_band::VARCHAR,
        f.value:o3_band::VARCHAR,
        f.value:pm10_band::VARCHAR,
        f.value:pm25_band::VARCHAR,
        f.value:so2_band::VARCHAR
    FROM @tfl_gcs_stage/air_quality/ s,
    LATERAL FLATTEN(input => s.$1:rows) f
)
FILE_FORMAT = tfl_json_format
ON_ERROR    = CONTINUE;


-- ── STEP 7: Verify the load ───────────────────────────────────

SELECT
    'line_status'      AS table_name,
    COUNT(*)           AS total_rows,
    COUNT(DISTINCT DATE(captured_at)) AS days_of_data,
    MIN(captured_at)   AS earliest,
    MAX(captured_at)   AS latest
FROM raw_line_status

UNION ALL SELECT 'bikepoints',       COUNT(*), COUNT(DISTINCT DATE(captured_at)), MIN(captured_at), MAX(captured_at) FROM raw_bikepoints
UNION ALL SELECT 'road_disruptions', COUNT(*), COUNT(DISTINCT DATE(captured_at)), MIN(captured_at), MAX(captured_at) FROM raw_road_disruptions
UNION ALL SELECT 'air_quality',      COUNT(*), COUNT(DISTINCT DATE(captured_at)), MIN(captured_at), MAX(captured_at) FROM raw_air_quality

ORDER BY table_name;
