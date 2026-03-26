-- =========================================================
-- File: 02_file_format_and_stage_validation.sql
-- Purpose: Create the JSON file format and validate the staged EV dataset
-- =========================================================

-- Step 1: Set the working context
USE DATABASE EV_DEMO;
USE SCHEMA BRONZE;
USE WAREHOUSE EV_WH;

-- Step 2: Create a reusable JSON file format
CREATE OR REPLACE FILE FORMAT FF_JSON_EV_RAW
    TYPE = 'JSON'
    COMPRESSION = AUTO;

-- Step 3: Validate that the file exists in the internal stage
LIST @EV_RAW_STAGE;

-- Step 4: Preview the root JSON document from the stage
SELECT
    $1 AS raw_document
FROM @EV_RAW_STAGE (FILE_FORMAT => 'FF_JSON_EV_RAW')
LIMIT 1;

-- Step 5: Inspect the top-level keys in the JSON document
SELECT
    $1:meta AS meta_section,
    $1:data AS data_section
FROM @EV_RAW_STAGE (FILE_FORMAT => 'FF_JSON_EV_RAW')
LIMIT 1;

-- Step 6: Preview records from the data array
SELECT
    f.index AS row_index,
    f.value AS raw_row
FROM @EV_RAW_STAGE (FILE_FORMAT => 'FF_JSON_EV_RAW') s,
     LATERAL FLATTEN(INPUT => s.$1:data) f
LIMIT 10;

-- Step 7: Map array positions to business columns for validation
SELECT
    f.value[8]::STRING  AS vin_1_10,
    f.value[9]::STRING  AS county,
    f.value[10]::STRING AS city,
    f.value[11]::STRING AS state,
    f.value[12]::STRING AS postal_code,
    f.value[13]::STRING AS model_year,
    f.value[14]::STRING AS make,
    f.value[15]::STRING AS model,
    f.value[16]::STRING AS electric_vehicle_type,
    f.value[17]::STRING AS cafv_eligibility,
    f.value[18]::NUMBER AS electric_range,
    f.value[19]::NUMBER AS base_msrp,
    f.value[20]::NUMBER AS legislative_district,
    f.value[21]::STRING AS dol_vehicle_id,
    f.value[22]::STRING AS vehicle_location,
    f.value[23]::STRING AS electric_utility,
    f.value[24]::STRING AS census_tract_2020
FROM @EV_RAW_STAGE (FILE_FORMAT => 'FF_JSON_EV_RAW') s,
     LATERAL FLATTEN(INPUT => s.$1:data) f
LIMIT 20;
