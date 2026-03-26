-- =========================================================
-- File: 05_silver_clean_layer.sql
-- Purpose: Create the Silver clean layer from Bronze raw rows
-- =========================================================



-- Step 1: Set working context
USE DATABASE EV_DEMO;
USE WAREHOUSE EV_WH;

-- Step 2: Create Silver schema if it does not exist
CREATE SCHEMA IF NOT EXISTS SILVER;

USE SCHEMA SILVER;

-- Step 3: Create the Silver clean table
CREATE OR REPLACE TABLE SILVER_EV_CLEAN AS
WITH source_data AS (
    SELECT
        raw_row,
        row_index,
        source_file_name,
        ingestion_timestamp,
        load_id
    FROM EV_DEMO.BRONZE.BRONZE_EV_RAW_ROWS
),

typed_data AS (
    SELECT
        NULLIF(TRIM(raw_row[8]::STRING), '')  AS vin_1_10,
        NULLIF(TRIM(raw_row[9]::STRING), '')  AS county,
        NULLIF(TRIM(raw_row[10]::STRING), '') AS city,
        NULLIF(UPPER(TRIM(raw_row[11]::STRING)), '') AS state,
        NULLIF(TRIM(raw_row[12]::STRING), '') AS postal_code,

        TRY_TO_NUMBER(raw_row[13]::STRING) AS model_year,
        NULLIF(UPPER(TRIM(raw_row[14]::STRING)), '') AS make,
        NULLIF(UPPER(TRIM(raw_row[15]::STRING)), '') AS model,
        NULLIF(TRIM(raw_row[16]::STRING), '') AS electric_vehicle_type,
        NULLIF(TRIM(raw_row[17]::STRING), '') AS cafv_eligibility,

        TRY_TO_NUMBER(raw_row[18]::STRING) AS electric_range,
        TRY_TO_NUMBER(raw_row[19]::STRING) AS base_msrp,
        TRY_TO_NUMBER(raw_row[20]::STRING) AS legislative_district,

        NULLIF(TRIM(raw_row[21]::STRING), '') AS dol_vehicle_id,
        NULLIF(TRIM(raw_row[22]::STRING), '') AS vehicle_location,
        NULLIF(TRIM(raw_row[23]::STRING), '') AS electric_utility,
        NULLIF(TRIM(raw_row[24]::STRING), '') AS census_tract_2020,

        row_index AS source_row_index,
        source_file_name,
        ingestion_timestamp,
        load_id
    FROM source_data
),

normalized_data AS (
    SELECT
        vin_1_10,
        county,
        city,
        state,
        postal_code,
        model_year,
        make,
        model,
        electric_vehicle_type,
        cafv_eligibility,

        CASE
            WHEN electric_range < 0 THEN NULL
            ELSE electric_range
        END AS electric_range,

        CASE
            WHEN base_msrp < 0 THEN NULL
            ELSE base_msrp
        END AS base_msrp,

        legislative_district,
        dol_vehicle_id,
        vehicle_location,
        electric_utility,
        census_tract_2020,

        source_row_index,
        source_file_name,
        ingestion_timestamp,
        load_id,

        CURRENT_TIMESTAMP() AS silver_created_at
    FROM typed_data
),

deduplicated_data AS (
    SELECT *
    FROM normalized_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
            dol_vehicle_id,
            vin_1_10 || '-' || COALESCE(TO_VARCHAR(model_year), 'UNKNOWN') || '-' || COALESCE(make, 'UNKNOWN') || '-' || COALESCE(model, 'UNKNOWN')
        )
        ORDER BY ingestion_timestamp DESC, source_row_index DESC
    ) = 1
)

SELECT
    vin_1_10,
    county,
    city,
    state,
    postal_code,
    model_year,
    make,
    model,
    electric_vehicle_type,
    cafv_eligibility,
    electric_range,
    base_msrp,
    legislative_district,
    dol_vehicle_id,
    vehicle_location,
    electric_utility,
    census_tract_2020,
    source_row_index,
    source_file_name,
    ingestion_timestamp,
    load_id,
    silver_created_at
FROM deduplicated_data;

-- Step 4: Validate total rows in Silver
SELECT COUNT(*) AS total_silver_rows
FROM SILVER_EV_CLEAN;

-- Step 5: Preview Silver records
SELECT *
FROM SILVER_EV_CLEAN
LIMIT 20;

-- Step 6: Validate duplicates after deduplication
SELECT
    dol_vehicle_id,
    COUNT(*) AS record_count
FROM SILVER_EV_CLEAN
GROUP BY dol_vehicle_id
HAVING COUNT(*) > 1
ORDER BY record_count DESC;

-- Step 7: Validate nulls in key business fields
SELECT
    COUNT(*) AS total_rows,
    COUNT_IF(dol_vehicle_id IS NULL) AS null_dol_vehicle_id,
    COUNT_IF(make IS NULL) AS null_make,
    COUNT_IF(model IS NULL) AS null_model,
    COUNT_IF(model_year IS NULL) AS null_model_year,
    COUNT_IF(state IS NULL) AS null_state
FROM SILVER_EV_CLEAN;