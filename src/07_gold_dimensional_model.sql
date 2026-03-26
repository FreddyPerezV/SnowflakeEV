-- =========================================================
-- File: 07_gold_dimensional_model.sql
-- Purpose: Create the Gold dimensional model for EV analytics
-- =========================================================

-- Step 1: Set working context
USE DATABASE EV_DEMO;
USE WAREHOUSE EV_WH;

-- Step 2: Create Gold schema if it does not exist
CREATE SCHEMA IF NOT EXISTS GOLD;

USE SCHEMA GOLD;

-- Step 3: Create dimension for location
CREATE OR REPLACE TABLE DIM_LOCATION AS
SELECT DISTINCT
    MD5(
        COALESCE(state, 'UNKNOWN') || '|' ||
        COALESCE(county, 'UNKNOWN') || '|' ||
        COALESCE(city, 'UNKNOWN') || '|' ||
        COALESCE(postal_code, 'UNKNOWN')
    ) AS location_key,
    state,
    county,
    city,
    postal_code,
    CURRENT_TIMESTAMP() AS gold_created_at
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN
WHERE state IS NOT NULL;

-- Step 4: Create dimension for make and model
CREATE OR REPLACE TABLE DIM_MAKE_MODEL AS
SELECT DISTINCT
    MD5(
        COALESCE(make, 'UNKNOWN') || '|' ||
        COALESCE(model, 'UNKNOWN') || '|' ||
        COALESCE(TO_VARCHAR(model_year), 'UNKNOWN')
    ) AS make_model_key,
    make,
    model,
    model_year,
    CURRENT_TIMESTAMP() AS gold_created_at
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN
WHERE make IS NOT NULL
  AND model IS NOT NULL
  AND model_year IS NOT NULL;

-- Step 5: Create dimension for EV type
CREATE OR REPLACE TABLE DIM_EV_TYPE AS
SELECT DISTINCT
    MD5(
        COALESCE(electric_vehicle_type, 'UNKNOWN') || '|' ||
        COALESCE(cafv_eligibility, 'UNKNOWN')
    ) AS ev_type_key,
    electric_vehicle_type,
    cafv_eligibility,
    CURRENT_TIMESTAMP() AS gold_created_at
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN
WHERE electric_vehicle_type IS NOT NULL;

-- Step 6: Create fact table for EV registrations
CREATE OR REPLACE TABLE FACT_EV_REGISTRATIONS AS
SELECT
    s.dol_vehicle_id AS vehicle_registration_id,

    MD5(
        COALESCE(s.state, 'UNKNOWN') || '|' ||
        COALESCE(s.county, 'UNKNOWN') || '|' ||
        COALESCE(s.city, 'UNKNOWN') || '|' ||
        COALESCE(s.postal_code, 'UNKNOWN')
    ) AS location_key,

    MD5(
        COALESCE(s.make, 'UNKNOWN') || '|' ||
        COALESCE(s.model, 'UNKNOWN') || '|' ||
        COALESCE(TO_VARCHAR(s.model_year), 'UNKNOWN')
    ) AS make_model_key,

    MD5(
        COALESCE(s.electric_vehicle_type, 'UNKNOWN') || '|' ||
        COALESCE(s.cafv_eligibility, 'UNKNOWN')
    ) AS ev_type_key,

    s.vin_1_10,
    s.legislative_district,
    s.electric_range,
    s.base_msrp,
    s.electric_utility,
    s.census_tract_2020,

    1 AS vehicle_count,

    s.source_file_name,
    s.source_row_index,
    s.ingestion_timestamp,
    s.load_id,
    CURRENT_TIMESTAMP() AS gold_created_at
FROM EV_DEMO.SILVER.SILVER_EV_CLEAN s
WHERE s.dol_vehicle_id IS NOT NULL
  AND s.make IS NOT NULL
  AND s.model IS NOT NULL
  AND s.model_year IS NOT NULL
  AND s.state IS NOT NULL;

-- Step 7: Create aggregate table by state and model year
CREATE OR REPLACE TABLE AGG_EV_BY_STATE_YEAR AS
SELECT
    l.state,
    m.model_year,
    COUNT(*) AS total_ev_registrations,
    AVG(f.electric_range) AS avg_electric_range,
    AVG(f.base_msrp) AS avg_base_msrp,
    CURRENT_TIMESTAMP() AS gold_created_at
FROM FACT_EV_REGISTRATIONS f
JOIN DIM_LOCATION l
    ON f.location_key = l.location_key
JOIN DIM_MAKE_MODEL m
    ON f.make_model_key = m.make_model_key
GROUP BY
    l.state,
    m.model_year;

-- Step 8: Create aggregate table by make
CREATE OR REPLACE TABLE AGG_EV_BY_MAKE AS
SELECT
    m.make,
    COUNT(*) AS total_ev_registrations,
    AVG(f.electric_range) AS avg_electric_range,
    AVG(f.base_msrp) AS avg_base_msrp,
    CURRENT_TIMESTAMP() AS gold_created_at
FROM FACT_EV_REGISTRATIONS f
JOIN DIM_MAKE_MODEL m
    ON f.make_model_key = m.make_model_key
GROUP BY
    m.make;

-- Step 9: Create aggregate table by EV type
CREATE OR REPLACE TABLE AGG_EV_TYPE_SUMMARY AS
SELECT
    e.electric_vehicle_type,
    e.cafv_eligibility,
    COUNT(*) AS total_ev_registrations,
    AVG(f.electric_range) AS avg_electric_range,
    AVG(f.base_msrp) AS avg_base_msrp,
    CURRENT_TIMESTAMP() AS gold_created_at
FROM FACT_EV_REGISTRATIONS f
JOIN DIM_EV_TYPE e
    ON f.ev_type_key = e.ev_type_key
GROUP BY
    e.electric_vehicle_type,
    e.cafv_eligibility;

-- Step 10: Validation queries
SELECT COUNT(*) AS total_dim_location_rows
FROM DIM_LOCATION;

SELECT COUNT(*) AS total_dim_make_model_rows
FROM DIM_MAKE_MODEL;

SELECT COUNT(*) AS total_dim_ev_type_rows
FROM DIM_EV_TYPE;

SELECT COUNT(*) AS total_fact_rows
FROM FACT_EV_REGISTRATIONS;

SELECT *
FROM AGG_EV_BY_STATE_YEAR
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM AGG_EV_BY_MAKE
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM AGG_EV_TYPE_SUMMARY
ORDER BY total_ev_registrations DESC
LIMIT 20;
