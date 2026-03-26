-- =========================================================
-- File: 09_sharing_secure_views.sql
-- Purpose: Create secure views for governed data sharing
-- =========================================================

-- Step 1: Set working context
USE DATABASE EV_DEMO;
USE WAREHOUSE EV_WH;

-- Step 2: Create Sharing schema if it does not exist
CREATE SCHEMA IF NOT EXISTS SHARING;

USE SCHEMA SHARING;

-- Step 3: Create secure view for EV adoption summary
CREATE OR REPLACE SECURE VIEW SVW_EV_ADOPTION_SUMMARY AS
SELECT
    state,
    model_year,
    total_ev_registrations,
    avg_electric_range,
    avg_base_msrp
FROM EV_DEMO.GOLD.VW_EV_ADOPTION_SUMMARY;

-- Step 4: Create secure view for top EV makes
CREATE OR REPLACE SECURE VIEW SVW_EV_TOP_MAKES AS
SELECT
    make,
    total_ev_registrations,
    avg_electric_range,
    avg_base_msrp,
    market_share_pct
FROM EV_DEMO.GOLD.VW_EV_TOP_MAKES;

-- Step 5: Create secure view for state performance
CREATE OR REPLACE SECURE VIEW SVW_EV_STATE_PERFORMANCE AS
SELECT
    state,
    total_ev_registrations,
    distinct_cities,
    distinct_counties,
    avg_electric_range,
    avg_base_msrp
FROM EV_DEMO.GOLD.VW_EV_STATE_PERFORMANCE;

-- Step 6: Create secure view for EV type insights
CREATE OR REPLACE SECURE VIEW SVW_EV_TYPE_INSIGHTS AS
SELECT
    electric_vehicle_type,
    cafv_eligibility,
    total_ev_registrations,
    avg_electric_range,
    avg_base_msrp
FROM EV_DEMO.GOLD.VW_EV_TYPE_INSIGHTS;

-- Step 7: Validation queries
SELECT *
FROM SVW_EV_ADOPTION_SUMMARY
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM SVW_EV_TOP_MAKES
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM SVW_EV_STATE_PERFORMANCE
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM SVW_EV_TYPE_INSIGHTS
ORDER BY total_ev_registrations DESC
LIMIT 20;

COMMENT ON SCHEMA SHARING IS 'Governed schema used to expose secure business-ready datasets for controlled sharing';
COMMENT ON VIEW SVW_EV_ADOPTION_SUMMARY IS 'Secure business view for EV adoption by state and model year';
COMMENT ON VIEW SVW_EV_TOP_MAKES IS 'Secure business view for EV market share by make';
COMMENT ON VIEW SVW_EV_STATE_PERFORMANCE IS 'Secure business view for state-level EV performance metrics';
COMMENT ON VIEW SVW_EV_TYPE_INSIGHTS IS 'Secure business view for EV type and CAFV eligibility insights';
