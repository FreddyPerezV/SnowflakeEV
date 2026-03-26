-- =========================================================
-- File: 13_bonus_extensions.sql
-- Purpose: Add minimal, interview-friendly bonus capabilities
-- Notes:
--   1) Sections A-C are functional with small environment-specific edits.
--   2) Section D (Iceberg) requires an external volume and cloud storage setup.
--   3) Openflow itself is configured in Snowsight, so Section E is a target model + checklist.
-- =========================================================

USE DATABASE EV_DEMO;
USE WAREHOUSE EV_WH;

-- =========================================================
-- A. GAP CLOSURE: SHARING LAYER USED BY THE SEMANTIC MODEL
-- =========================================================
CREATE SCHEMA IF NOT EXISTS SHARING;
USE SCHEMA SHARING;

CREATE OR REPLACE SECURE VIEW SVW_EV_ADOPTION_SUMMARY AS
SELECT *
FROM EV_DEMO.GOLD.VW_EV_ADOPTION_SUMMARY;

CREATE OR REPLACE SECURE VIEW SVW_EV_TOP_MAKES AS
SELECT *
FROM EV_DEMO.GOLD.VW_EV_TOP_MAKES;

CREATE OR REPLACE SECURE VIEW SVW_EV_STATE_PERFORMANCE AS
SELECT *
FROM EV_DEMO.GOLD.VW_EV_STATE_PERFORMANCE;

CREATE OR REPLACE SECURE VIEW SVW_EV_TYPE_INSIGHTS AS
SELECT *
FROM EV_DEMO.GOLD.VW_EV_TYPE_INSIGHTS;

-- Validate the secure views referenced by 10_semantic_model_prep.sql
SELECT * FROM EV_DEMO.SHARING.SVW_EV_TOP_MAKES ORDER BY total_ev_registrations DESC LIMIT 10;


-- =========================================================
-- B. MINIMAL AUTOMATION BONUS: DYNAMIC TABLE + ALERT
-- =========================================================
CREATE SCHEMA IF NOT EXISTS OPERATIONS;
USE SCHEMA OPERATIONS;

CREATE OR REPLACE DYNAMIC TABLE DT_EV_MAKE_PERFORMANCE
  TARGET_LAG = '15 minutes'
  WAREHOUSE = EV_WH
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
AS
SELECT
    m.make,
    COUNT(*) AS total_ev_registrations,
    AVG(f.electric_range) AS avg_electric_range,
    AVG(f.base_msrp) AS avg_base_msrp,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS market_share_pct
FROM EV_DEMO.GOLD.FACT_EV_REGISTRATIONS f
JOIN EV_DEMO.GOLD.DIM_MAKE_MODEL m
  ON f.make_model_key = m.make_model_key
GROUP BY m.make;

CREATE OR REPLACE TABLE DQ_ALERT_AUDIT (
    alert_timestamp      TIMESTAMP_NTZ,
    alert_name           STRING,
    fail_count           NUMBER,
    alert_message        STRING
);

CREATE OR REPLACE ALERT ALERT_DQ_FAILURES
  WAREHOUSE = EV_WH
  SCHEDULE = '15 MINUTES'
  IF (EXISTS (
        SELECT 1
        FROM EV_DEMO.MONITORING.DQ_RESULTS
        WHERE check_result = 'FAIL'
          AND check_timestamp >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
  ))
  THEN
    INSERT INTO DQ_ALERT_AUDIT
    SELECT
        CURRENT_TIMESTAMP(),
        'ALERT_DQ_FAILURES',
        COUNT(*),
        'New failing DQ checks detected in the last 15 minutes'
    FROM EV_DEMO.MONITORING.DQ_RESULTS
    WHERE check_result = 'FAIL'
      AND check_timestamp >= DATEADD('minute', -15, CURRENT_TIMESTAMP());

ALTER ALERT ALERT_DQ_FAILURES RESUME;

-- Optional email integration (requires verified recipients and account-level privileges)
-- CREATE NOTIFICATION INTEGRATION EV_EMAIL_INT
--   TYPE = EMAIL
--   ENABLED = TRUE
--   ALLOWED_RECIPIENTS = ('you@example.com');
--
-- Then replace the INSERT action above with SYSTEM$SEND_EMAIL or SYSTEM$SEND_SNOWFLAKE_NOTIFICATION.

SELECT * FROM OPERATIONS.DT_EV_MAKE_PERFORMANCE ORDER BY total_ev_registrations DESC LIMIT 10;
SELECT * FROM OPERATIONS.DQ_ALERT_AUDIT ORDER BY alert_timestamp DESC;


-- =========================================================
-- C. DATA SHARING BONUS: DIRECT SHARE OVER SECURE VIEWS
-- =========================================================
USE SCHEMA SHARING;

CREATE OR REPLACE SHARE EV_DEMO_SHARE;
GRANT USAGE ON DATABASE EV_DEMO TO SHARE EV_DEMO_SHARE;
GRANT USAGE ON SCHEMA EV_DEMO.SHARING TO SHARE EV_DEMO_SHARE;
GRANT SELECT ON VIEW EV_DEMO.SHARING.SVW_EV_ADOPTION_SUMMARY TO SHARE EV_DEMO_SHARE;
GRANT SELECT ON VIEW EV_DEMO.SHARING.SVW_EV_TOP_MAKES TO SHARE EV_DEMO_SHARE;
GRANT SELECT ON VIEW EV_DEMO.SHARING.SVW_EV_STATE_PERFORMANCE TO SHARE EV_DEMO_SHARE;
GRANT SELECT ON VIEW EV_DEMO.SHARING.SVW_EV_TYPE_INSIGHTS TO SHARE EV_DEMO_SHARE;

-- Add the consumer account only when ready.
-- ALTER SHARE EV_DEMO_SHARE ADD ACCOUNTS = ('<ORGNAME>.<ACCOUNT_NAME>');
SHOW SHARES LIKE 'EV_DEMO_SHARE';


-- =========================================================
-- D. ICEBERG BONUS: INTEROP TEMPLATE (ENVIRONMENT-SPECIFIC)
--    NOTE: Requires a real Azure subscription with storage.
--    Skipped on trial accounts — no external cloud storage available.
-- =========================================================
CREATE SCHEMA IF NOT EXISTS INTEROP;
USE SCHEMA INTEROP;

-- Gold curated source (works on trial)
CREATE OR REPLACE VIEW VW_EV_TOP_MAKES_INTEROP_SOURCE AS
SELECT *
FROM EV_DEMO.SHARING.SVW_EV_TOP_MAKES;

-- STEP 1: External volume (requires paid Azure subscription + storage account)
-- CREATE OR REPLACE EXTERNAL VOLUME EV_ICEBERG_EXT_VOL
--   STORAGE_LOCATIONS = (
--     (
--       NAME = 'ev_iceberg_loc'
--       STORAGE_PROVIDER = 'AZURE'
--       STORAGE_BASE_URL = 'azure://<your_storage_account>.blob.core.windows.net/<container>/'
--       AZURE_TENANT_ID = '<your-azure-tenant-guid>'
--     )
--   );

-- STEP 2: Dynamic Iceberg table (requires external volume above)
-- CREATE OR REPLACE DYNAMIC ICEBERG TABLE DIT_EV_TOP_MAKES
--   TARGET_LAG = '1 hour'
--   WAREHOUSE = EV_WH
--   EXTERNAL_VOLUME = 'EV_ICEBERG_EXT_VOL'
--   CATALOG = 'SNOWFLAKE'
--   BASE_LOCATION = 'ev_top_makes'
-- AS
-- SELECT *
-- FROM VW_EV_TOP_MAKES_INTEROP_SOURCE;