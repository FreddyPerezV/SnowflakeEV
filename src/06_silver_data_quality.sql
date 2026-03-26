-- =========================================================
-- File: 06_silver_data_quality.sql
-- Purpose: Apply data quality checks to the Silver layer
-- =========================================================

-- Step 1: Set working context
USE DATABASE EV_DEMO;
USE WAREHOUSE EV_WH;

-- Step 2: Create Monitoring schema if it does not exist
CREATE SCHEMA IF NOT EXISTS MONITORING;

-- Step 3: Create a table to store DQ check results
CREATE OR REPLACE TABLE MONITORING.DQ_RESULTS (
    check_timestamp      TIMESTAMP_NTZ,
    layer_name           STRING,
    table_name           STRING,
    check_name           STRING,
    check_category       STRING,
    check_result         STRING,
    affected_row_count   NUMBER,
    total_row_count      NUMBER,
    check_details        STRING
);

-- Step 4: Create a table to store rejected Silver records
CREATE OR REPLACE TABLE MONITORING.SILVER_EV_REJECTS AS
SELECT
    *,
    CASE
        WHEN dol_vehicle_id IS NULL THEN 'Missing DOL Vehicle ID'
        WHEN make IS NULL THEN 'Missing make'
        WHEN model IS NULL THEN 'Missing model'
        WHEN model_year IS NULL THEN 'Missing model year'
        WHEN state IS NULL THEN 'Missing state'
        WHEN electric_range < 0 THEN 'Invalid electric range'
        WHEN base_msrp < 0 THEN 'Invalid base MSRP'
        ELSE 'Other data quality issue'
    END AS reject_reason,
    CURRENT_TIMESTAMP() AS reject_timestamp
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN
WHERE
    dol_vehicle_id IS NULL
    OR make IS NULL
    OR model IS NULL
    OR model_year IS NULL
    OR state IS NULL
    OR electric_range < 0
    OR base_msrp < 0;

-- Step 5: Insert completeness checks into DQ results
INSERT INTO MONITORING.DQ_RESULTS
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SILVER' AS layer_name,
    'SILVER_EV_CLEAN' AS table_name,
    'Null DOL Vehicle ID check' AS check_name,
    'COMPLETENESS' AS check_category,
    CASE WHEN COUNT_IF(dol_vehicle_id IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
    COUNT_IF(dol_vehicle_id IS NULL) AS affected_row_count,
    COUNT(*) AS total_row_count,
    'DOL Vehicle ID should not be null in Silver layer' AS check_details
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN;

INSERT INTO MONITORING.DQ_RESULTS
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SILVER' AS layer_name,
    'SILVER_EV_CLEAN' AS table_name,
    'Null make check' AS check_name,
    'COMPLETENESS' AS check_category,
    CASE WHEN COUNT_IF(make IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
    COUNT_IF(make IS NULL) AS affected_row_count,
    COUNT(*) AS total_row_count,
    'Make should not be null in Silver layer' AS check_details
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN;

INSERT INTO MONITORING.DQ_RESULTS
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SILVER' AS layer_name,
    'SILVER_EV_CLEAN' AS table_name,
    'Null model check' AS check_name,
    'COMPLETENESS' AS check_category,
    CASE WHEN COUNT_IF(model IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
    COUNT_IF(model IS NULL) AS affected_row_count,
    COUNT(*) AS total_row_count,
    'Model should not be null in Silver layer' AS check_details
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN;

INSERT INTO MONITORING.DQ_RESULTS
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SILVER' AS layer_name,
    'SILVER_EV_CLEAN' AS table_name,
    'Null model year check' AS check_name,
    'COMPLETENESS' AS check_category,
    CASE WHEN COUNT_IF(model_year IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
    COUNT_IF(model_year IS NULL) AS affected_row_count,
    COUNT(*) AS total_row_count,
    'Model year should not be null in Silver layer' AS check_details
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN;

INSERT INTO MONITORING.DQ_RESULTS
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SILVER' AS layer_name,
    'SILVER_EV_CLEAN' AS table_name,
    'Null state check' AS check_name,
    'COMPLETENESS' AS check_category,
    CASE WHEN COUNT_IF(state IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
    COUNT_IF(state IS NULL) AS affected_row_count,
    COUNT(*) AS total_row_count,
    'State should not be null in Silver layer' AS check_details
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN;

-- Step 6: Insert validity checks into DQ results
INSERT INTO MONITORING.DQ_RESULTS
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SILVER' AS layer_name,
    'SILVER_EV_CLEAN' AS table_name,
    'Negative electric range check' AS check_name,
    'VALIDITY' AS check_category,
    CASE WHEN COUNT_IF(electric_range < 0) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
    COUNT_IF(electric_range < 0) AS affected_row_count,
    COUNT(*) AS total_row_count,
    'Electric range should not be negative' AS check_details
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN;

INSERT INTO MONITORING.DQ_RESULTS
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SILVER' AS layer_name,
    'SILVER_EV_CLEAN' AS table_name,
    'Negative base MSRP check' AS check_name,
    'VALIDITY' AS check_category,
    CASE WHEN COUNT_IF(base_msrp < 0) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
    COUNT_IF(base_msrp < 0) AS affected_row_count,
    COUNT(*) AS total_row_count,
    'Base MSRP should not be negative' AS check_details
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN;

INSERT INTO MONITORING.DQ_RESULTS
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SILVER' AS layer_name,
    'SILVER_EV_CLEAN' AS table_name,
    'Model year range check' AS check_name,
    'VALIDITY' AS check_category,
    CASE
        WHEN COUNT_IF(model_year < 1990 OR model_year > YEAR(CURRENT_DATE()) + 1) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS check_result,
    COUNT_IF(model_year < 1990 OR model_year > YEAR(CURRENT_DATE()) + 1) AS affected_row_count,
    COUNT(*) AS total_row_count,
    'Model year should be within a realistic business range' AS check_details
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN;

-- Step 7: Insert uniqueness check into DQ results
INSERT INTO MONITORING.DQ_RESULTS
SELECT
    CURRENT_TIMESTAMP() AS check_timestamp,
    'SILVER' AS layer_name,
    'SILVER_EV_CLEAN' AS table_name,
    'DOL Vehicle ID uniqueness check' AS check_name,
    'UNIQUENESS' AS check_category,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
    COUNT(*) AS affected_row_count,
    (SELECT COUNT(*) FROM EV_DEMO.SILVER.SILVER_EV_CLEAN) AS total_row_count,
    'DOL Vehicle ID should be unique after Silver deduplication' AS check_details
FROM (
    SELECT dol_vehicle_id
    FROM EV_DEMO.SILVER.SILVER_EV_CLEAN
    WHERE dol_vehicle_id IS NOT NULL
    GROUP BY dol_vehicle_id
    HAVING COUNT(*) > 1
) duplicates;

-- Step 8: Validation queries
SELECT *
FROM MONITORING.DQ_RESULTS
ORDER BY check_timestamp DESC, check_category, check_name;

SELECT
    reject_reason,
    COUNT(*) AS rejected_rows
FROM MONITORING.SILVER_EV_REJECTS
GROUP BY reject_reason
ORDER BY rejected_rows DESC;

SELECT COUNT(*) AS total_rejected_rows
FROM MONITORING.SILVER_EV_REJECTS;
