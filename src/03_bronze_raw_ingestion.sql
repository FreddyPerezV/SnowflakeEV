-- =========================================================
-- File: 03_bronze_raw_ingestion.sql
-- Purpose: Load the source JSON document and explode raw rows into Bronze
-- =========================================================

-- Step 1: Set the working context
USE DATABASE EV_DEMO;
USE SCHEMA BRONZE;
USE WAREHOUSE EV_WH;

-- Step 2: Create a table to store the original source document
CREATE OR REPLACE TABLE BRONZE_EV_SOURCE_DOCUMENT (
    source_document      VARIANT,
    source_file_name     STRING,
    ingestion_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    load_id              STRING
);

-- Step 3: Create a table to store exploded raw rows from the data array
CREATE OR REPLACE TABLE BRONZE_EV_RAW_ROWS (
    raw_row              VARIANT,
    row_index            NUMBER,
    source_file_name     STRING,
    ingestion_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    load_id              STRING
);
-- Step 4: Load the full source document
INSERT INTO BRONZE_EV_SOURCE_DOCUMENT (
    source_document,
    source_file_name,
    ingestion_timestamp,
    load_id
)
SELECT
    $1 AS source_document,
    METADATA$FILENAME AS source_file_name,
    CURRENT_TIMESTAMP() AS ingestion_timestamp,
    UUID_STRING() AS load_id
FROM @EV_RAW_STAGE (FILE_FORMAT => 'FF_JSON_EV_RAW');

-- Step 5: Explode the data array into row-level raw records
INSERT INTO BRONZE_EV_RAW_ROWS (
    raw_row,
    row_index,
    source_file_name,
    ingestion_timestamp,
    load_id
)
SELECT
    f.value AS raw_row,
    f.index AS row_index,
    METADATA$FILENAME AS source_file_name,
    CURRENT_TIMESTAMP() AS ingestion_timestamp,
    UUID_STRING() AS load_id
FROM @EV_RAW_STAGE (FILE_FORMAT => 'FF_JSON_EV_RAW') s,
     LATERAL FLATTEN(INPUT => s.$1:data) f;

-- Step 6: Validate load counts
SELECT COUNT(*) AS total_source_documents
FROM BRONZE_EV_SOURCE_DOCUMENT;

SELECT COUNT(*) AS total_raw_rows
FROM BRONZE_EV_RAW_ROWS;

-- Step 7: Preview exploded Bronze rows
SELECT *
FROM BRONZE_EV_RAW_ROWS
LIMIT 10;
