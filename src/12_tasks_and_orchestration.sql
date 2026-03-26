-- =========================================================
-- File: 12_tasks_and_orchestration.sql
-- Purpose: Create basic task-based orchestration and scheduled audit monitoring
-- =========================================================

-- Step 1: Set working context
USE DATABASE EV_DEMO;
USE WAREHOUSE EV_WH;

-- Step 2: Set Monitoring schema
USE SCHEMA MONITORING;

-- Step 3: Create or replace reconciliation view
CREATE OR REPLACE VIEW VW_PIPELINE_RECONCILIATION AS
SELECT
    run_id,
    MAX(CASE WHEN object_name = 'BRONZE_EV_SOURCE_DOCUMENT' THEN row_count END) AS bronze_source_document_rows,
    MAX(CASE WHEN object_name = 'BRONZE_EV_RAW_ROWS' THEN row_count END) AS bronze_raw_rows,
    MAX(CASE WHEN object_name = 'SILVER_EV_CLEAN' THEN row_count END) AS silver_clean_rows,
    MAX(CASE WHEN object_name = 'SILVER_EV_REJECTS' THEN row_count END) AS silver_reject_rows,
    MAX(CASE WHEN object_name = 'FACT_EV_REGISTRATIONS' THEN row_count END) AS gold_fact_rows,
    MAX(CASE WHEN object_name = 'AGG_EV_BY_STATE_YEAR' THEN row_count END) AS gold_agg_state_year_rows,
    MAX(CASE WHEN object_name = 'AGG_EV_BY_MAKE' THEN row_count END) AS gold_agg_make_rows,
    MAX(CASE WHEN object_name = 'AGG_EV_TYPE_SUMMARY' THEN row_count END) AS gold_agg_type_rows
FROM LAYER_ROW_COUNTS_AUDIT
GROUP BY run_id;

-- Step 4: Create a table to capture scheduled task heartbeat runs
CREATE OR REPLACE TABLE TASK_HEARTBEAT_AUDIT (
    task_run_id          STRING,
    task_name            STRING,
    task_timestamp       TIMESTAMP_NTZ,
    task_status          STRING,
    task_notes           STRING
);

-- Step 5: Create a scheduled task for heartbeat monitoring
CREATE OR REPLACE TASK TASK_PIPELINE_HEARTBEAT
    WAREHOUSE = EV_WH
    SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
INSERT INTO TASK_HEARTBEAT_AUDIT (
    task_run_id,
    task_name,
    task_timestamp,
    task_status,
    task_notes
)
SELECT
    UUID_STRING(),
    'TASK_PIPELINE_HEARTBEAT',
    CURRENT_TIMESTAMP(),
    'SUCCESS',
    'Scheduled monitoring heartbeat for EV demo pipeline';

-- Step 6: Enable the task
ALTER TASK TASK_PIPELINE_HEARTBEAT RESUME;

-- Step 7: Validation query for reconciliation view
SELECT *
FROM VW_PIPELINE_RECONCILIATION
ORDER BY run_id DESC;

-- Step 8: Validation query for heartbeat audit table
SELECT *
FROM TASK_HEARTBEAT_AUDIT
ORDER BY task_timestamp DESC;

-- Step 9: Show task definition
SHOW TASKS LIKE 'TASK_PIPELINE_HEARTBEAT' IN SCHEMA EV_DEMO.MONITORING;

-- Step 10: Manual trigger for validation
EXECUTE TASK TASK_PIPELINE_HEARTBEAT;
