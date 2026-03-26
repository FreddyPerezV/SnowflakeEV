-- =========================================================
-- File: 04_bronze_profiling.sql
-- Purpose: Perform exploratory data profiling on Bronze raw rows
-- =========================================================

-- Step 1: Set working context
USE DATABASE EV_DEMO;
USE SCHEMA BRONZE;
USE WAREHOUSE EV_WH;

-- Step 2: Count total raw rows ingested
SELECT 
    COUNT(*) AS total_raw_rows
FROM BRONZE_EV_RAW_ROWS;

------------------------------------------------------------
-- Step 3: Inspect sample raw records
------------------------------------------------------------

SELECT
    raw_row
FROM BRONZE_EV_RAW_ROWS
LIMIT 10;

------------------------------------------------------------
-- Step 4: Map array positions into readable columns
-- This query helps us understand the dataset structure
------------------------------------------------------------

SELECT
    raw_row[8]::STRING  AS vin_1_10,
    raw_row[9]::STRING  AS county,
    raw_row[10]::STRING AS city,
    raw_row[11]::STRING AS state,
    raw_row[12]::STRING AS postal_code,
    raw_row[13]::NUMBER AS model_year,
    raw_row[14]::STRING AS make,
    raw_row[15]::STRING AS model,
    raw_row[16]::STRING AS ev_type,
    raw_row[17]::STRING AS cafv_eligibility,
    raw_row[18]::NUMBER AS electric_range,
    raw_row[19]::NUMBER AS base_msrp,
    raw_row[21]::STRING AS dol_vehicle_id
FROM BRONZE_EV_RAW_ROWS
LIMIT 20;

------------------------------------------------------------
-- Step 5: Null value analysis for critical fields
------------------------------------------------------------

SELECT
    COUNT(*) AS total_rows,

    COUNT_IF(raw_row[14] IS NULL) AS null_make,
    COUNT_IF(raw_row[15] IS NULL) AS null_model,
    COUNT_IF(raw_row[13] IS NULL) AS null_model_year,
    COUNT_IF(raw_row[11] IS NULL) AS null_state,
    COUNT_IF(raw_row[10] IS NULL) AS null_city,

    COUNT_IF(raw_row[18] IS NULL) AS null_electric_range,
    COUNT_IF(raw_row[19] IS NULL) AS null_base_msrp

FROM BRONZE_EV_RAW_ROWS;

------------------------------------------------------------
-- Step 6: Distribution of vehicle makes
------------------------------------------------------------

SELECT
    raw_row[14]::STRING AS make,
    COUNT(*) AS vehicle_count
FROM BRONZE_EV_RAW_ROWS
GROUP BY make
ORDER BY vehicle_count DESC
LIMIT 20;

------------------------------------------------------------
-- Step 7: Distribution of model years
------------------------------------------------------------

SELECT
    raw_row[13]::NUMBER AS model_year,
    COUNT(*) AS vehicles
FROM BRONZE_EV_RAW_ROWS
GROUP BY model_year
ORDER BY model_year DESC;

------------------------------------------------------------
-- Step 8: Electric vehicle type distribution
------------------------------------------------------------

SELECT
    raw_row[16]::STRING AS ev_type,
    COUNT(*) AS vehicles
FROM BRONZE_EV_RAW_ROWS
GROUP BY ev_type
ORDER BY vehicles DESC;

------------------------------------------------------------
-- Step 9: Electric range statistics
------------------------------------------------------------

SELECT
    MIN(raw_row[18]::NUMBER) AS min_range,
    MAX(raw_row[18]::NUMBER) AS max_range,
    AVG(raw_row[18]::NUMBER) AS avg_range
FROM BRONZE_EV_RAW_ROWS;

------------------------------------------------------------
-- Step 10: Identify potential duplicates using DOL Vehicle ID
------------------------------------------------------------

SELECT
    raw_row[21]::STRING AS dol_vehicle_id,
    COUNT(*) AS duplicates
FROM BRONZE_EV_RAW_ROWS
GROUP BY dol_vehicle_id
HAVING COUNT(*) > 1
ORDER BY duplicates DESC;

------------------------------------------------------------
-- Step 11: Geographic distribution
------------------------------------------------------------

SELECT
    raw_row[11]::STRING AS state,
    COUNT(*) AS vehicles
FROM BRONZE_EV_RAW_ROWS
GROUP BY state
ORDER BY vehicles DESC;

------------------------------------------------------------
-- Step 12: Top cities by EV registrations
------------------------------------------------------------

SELECT
    raw_row[10]::STRING AS city,
    COUNT(*) AS ev_count
FROM BRONZE_EV_RAW_ROWS
GROUP BY city
ORDER BY ev_count DESC
LIMIT 20;
