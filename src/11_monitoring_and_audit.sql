-- =========================================================
-- File: 11_monitoring_and_audit.sql
-- Purpose: Create monitoring and audit structures for pipeline execution
-- =========================================================

-- Step 1: Set working context
USE DATABASE EV_DEMO;
USE WAREHOUSE EV_WH;

-- Step 2: Set Monitoring schema
CREATE SCHEMA IF NOT EXISTS MONITORING;

USE SCHEMA MONITORING;

-- Step 3: Create pipeline run audit table
CREATE OR REPLACE TABLE PIPELINE_RUN_AUDIT (
    run_id               STRING,
    pipeline_name        STRING,
    run_timestamp        TIMESTAMP_NTZ,
    run_status           STRING,
    run_trigger_type     STRING,
    source_name          STRING,
    run_notes            STRING
);

-- Step 4: Create layer row counts audit table
CREATE OR REPLACE TABLE LAYER_ROW_COUNTS_AUDIT (
    run_id               STRING,
    layer_name           STRING,
    object_name          STRING,
    row_count            NUMBER,
    audit_timestamp      TIMESTAMP_NTZ,
    audit_notes          STRING
);

-- Step 5: Create a run identifier for the current execution
SET run_id = (SELECT UUID_STRING());

-- Step 6: Insert pipeline run record
INSERT INTO PIPELINE_RUN_AUDIT (
    run_id,
    pipeline_name,
    run_timestamp,
    run_status,
    run_trigger_type,
    source_name,
    run_notes
)
SELECT
    $run_id,
    'EV Population Snowflake Demo Pipeline',
    CURRENT_TIMESTAMP(),
    'SUCCESS',
    'MANUAL',
    'ElectricVehiclePopulationData JSON file',
    'Manual end-to-end pipeline execution including Bronze, Silver, Gold, Sharing, Semantic, and Monitoring layers';

-- Step 7: Insert row count audit for Bronze source document
INSERT INTO LAYER_ROW_COUNTS_AUDIT (
    run_id,
    layer_name,
    object_name,
    row_count,
    audit_timestamp,
    audit_notes
)
SELECT
    $run_id,
    'BRONZE',
    'BRONZE_EV_SOURCE_DOCUMENT',
    COUNT(*),
    CURRENT_TIMESTAMP(),
    'Source JSON document count'
FROM EV_DEMO.BRONZE.BRONZE_EV_SOURCE_DOCUMENT;

-- Step 8: Insert row count audit for Bronze raw rows
INSERT INTO LAYER_ROW_COUNTS_AUDIT (
    run_id,
    layer_name,
    object_name,
    row_count,
    audit_timestamp,
    audit_notes
)
SELECT
    $run_id,
    'BRONZE',
    'BRONZE_EV_RAW_ROWS',
    COUNT(*),
    CURRENT_TIMESTAMP(),
    'Exploded raw records from the data array'
FROM EV_DEMO.BRONZE.BRONZE_EV_RAW_ROWS;

-- Step 9: Insert row count audit for Silver clean table
INSERT INTO LAYER_ROW_COUNTS_AUDIT (
    run_id,
    layer_name,
    object_name,
    row_count,
    audit_timestamp,
    audit_notes
)
SELECT
    $run_id,
    'SILVER',
    'SILVER_EV_CLEAN',
    COUNT(*),
    CURRENT_TIMESTAMP(),
    'Cleaned and deduplicated Silver records'
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN;

-- Step 10: Insert row count audit for Silver rejects
INSERT INTO LAYER_ROW_COUNTS_AUDIT (
    run_id,
    layer_name,
    object_name,
    row_count,
    audit_timestamp,
    audit_notes
)
SELECT
    $run_id,
    'SILVER',
    'SILVER_EV_REJECTS',
    COUNT(*),
    CURRENT_TIMESTAMP(),
    'Rejected Silver records due to data quality rules'
FROM EV_DEMO.MONITORING.SILVER_EV_REJECTS;

-- Step 11: Insert row count audit for Gold fact table
INSERT INTO LAYER_ROW_COUNTS_AUDIT (
    run_id,
    layer_name,
    object_name,
    row_count,
    audit_timestamp,
    audit_notes
)
SELECT
    $run_id,
    'GOLD',
    'FACT_EV_REGISTRATIONS',
    COUNT(*),
    CURRENT_TIMESTAMP(),
    'Analytics-ready fact table for EV registrations'
FROM EV_DEMO.GOLD.FACT_EV_REGISTRATIONS;

-- Step 12: Insert row count audit for Gold aggregate tables
INSERT INTO LAYER_ROW_COUNTS_AUDIT (
    run_id,
    layer_name,
    object_name,
    row_count,
    audit_timestamp,
    audit_notes
)
SELECT
    $run_id,
    'GOLD',
    'AGG_EV_BY_STATE_YEAR',
    COUNT(*),
    CURRENT_TIMESTAMP(),
    'Gold aggregate by state and model year'
FROM EV_DEMO.GOLD.AGG_EV_BY_STATE_YEAR;

INSERT INTO LAYER_ROW_COUNTS_AUDIT (
    run_id,
    layer_name,
    object_name,
    row_count,
    audit_timestamp,
    audit_notes
)
SELECT
    $run_id,
    'GOLD',
    'AGG_EV_BY_MAKE',
    COUNT(*),
    CURRENT_TIMESTAMP(),
    'Gold aggregate by make'
FROM EV_DEMO.GOLD.AGG_EV_BY_MAKE;

INSERT INTO LAYER_ROW_COUNTS_AUDIT (
    run_id,
    layer_name,
    object_name,
    row_count,
    audit_timestamp,
    audit_notes
)
SELECT
    $run_id,
    'GOLD',
    'AGG_EV_TYPE_SUMMARY',
    COUNT(*),
    CURRENT_TIMESTAMP(),
    'Gold aggregate by EV type and CAFV eligibility'
FROM EV_DEMO.GOLD.AGG_EV_TYPE_SUMMARY;

-- Step 13: Reconciliation summary query
SELECT
    run_id,
    layer_name,
    object_name,
    row_count,
    audit_timestamp,
    audit_notes
FROM LAYER_ROW_COUNTS_AUDIT
WHERE run_id = $run_id
ORDER BY
    CASE layer_name
        WHEN 'BRONZE' THEN 1
        WHEN 'SILVER' THEN 2
        WHEN 'GOLD' THEN 3
        ELSE 4
    END,
    object_name;

-- Step 14: Show pipeline run audit
SELECT *
FROM PIPELINE_RUN_AUDIT
WHERE run_id = $run_id;

-- Step 15: Reconciliation helper query
SELECT
    MAX(CASE WHEN object_name = 'BRONZE_EV_RAW_ROWS' THEN row_count END) AS bronze_raw_rows,
    MAX(CASE WHEN object_name = 'SILVER_EV_CLEAN' THEN row_count END) AS silver_clean_rows,
    MAX(CASE WHEN object_name = 'SILVER_EV_REJECTS' THEN row_count END) AS silver_reject_rows,
    MAX(CASE WHEN object_name = 'FACT_EV_REGISTRATIONS' THEN row_count END) AS gold_fact_rows
FROM LAYER_ROW_COUNTS_AUDIT
WHERE run_id = $run_id;
