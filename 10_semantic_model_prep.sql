-- =========================================================
-- File: 10_semantic_model_prep.sql
-- Purpose: Prepare semantic-friendly views for AI and business consumption
-- =========================================================

-- Step 1: Set working context
USE DATABASE EV_DEMO;
USE WAREHOUSE EV_WH;

-- Step 2: Create Semantic schema if it does not exist
CREATE SCHEMA IF NOT EXISTS SEMANTIC;

USE SCHEMA SEMANTIC;

-- Step 3: Create semantic view for EV adoption trends
CREATE OR REPLACE VIEW SEM_EV_ADOPTION AS
SELECT
    state,
    model_year,
    total_ev_registrations,
    ROUND(avg_electric_range, 2) AS average_electric_range_miles,
    ROUND(avg_base_msrp, 2) AS average_base_msrp_usd
FROM EV_DEMO.SHARING.SVW_EV_ADOPTION_SUMMARY;

-- Step 4: Create semantic view for EV make performance
CREATE OR REPLACE VIEW SEM_EV_MAKE_PERFORMANCE AS
SELECT
    make,
    total_ev_registrations,
    ROUND(avg_electric_range, 2) AS average_electric_range_miles,
    ROUND(avg_base_msrp, 2) AS average_base_msrp_usd,
    ROUND(market_share_pct, 2) AS market_share_percentage
FROM EV_DEMO.SHARING.SVW_EV_TOP_MAKES;

-- Step 5: Create semantic view for state-level EV performance
CREATE OR REPLACE VIEW SEM_EV_STATE_PERFORMANCE AS
SELECT
    state,
    total_ev_registrations,
    distinct_cities AS total_cities_with_ev_registrations,
    distinct_counties AS total_counties_with_ev_registrations,
    ROUND(avg_electric_range, 2) AS average_electric_range_miles,
    ROUND(avg_base_msrp, 2) AS average_base_msrp_usd
FROM EV_DEMO.SHARING.SVW_EV_STATE_PERFORMANCE;

-- Step 6: Create semantic view for EV type and CAFV insights
CREATE OR REPLACE VIEW SEM_EV_TYPE_PERFORMANCE AS
SELECT
    electric_vehicle_type,
    cafv_eligibility,
    total_ev_registrations,
    ROUND(avg_electric_range, 2) AS average_electric_range_miles,
    ROUND(avg_base_msrp, 2) AS average_base_msrp_usd
FROM EV_DEMO.SHARING.SVW_EV_TYPE_INSIGHTS;

-- Step 7: Add object comments for semantic clarity
COMMENT ON SCHEMA SEMANTIC IS 'Semantic-ready layer for AI, business users, and natural language analytics';

COMMENT ON VIEW SEM_EV_ADOPTION IS 'Semantic view for EV adoption by state and model year';
COMMENT ON VIEW SEM_EV_MAKE_PERFORMANCE IS 'Semantic view for EV make-level performance and market share';
COMMENT ON VIEW SEM_EV_STATE_PERFORMANCE IS 'Semantic view for state-level EV performance';
COMMENT ON VIEW SEM_EV_TYPE_PERFORMANCE IS 'Semantic view for EV type and CAFV eligibility analysis';

-- Step 8: Validation queries
SELECT *
FROM SEM_EV_ADOPTION
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM SEM_EV_MAKE_PERFORMANCE
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM SEM_EV_STATE_PERFORMANCE
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM SEM_EV_TYPE_PERFORMANCE
ORDER BY total_ev_registrations DESC
LIMIT 20;

---

SELECT *
FROM EV_DEMO.SEMANTIC.SEM_EV_MAKE_PERFORMANCE
WHERE market_share_percentage >= 1
ORDER BY market_share_percentage DESC;

SELECT *
FROM EV_DEMO.SEMANTIC.SEM_EV_ADOPTION
WHERE model_year >= 2020
ORDER BY total_ev_registrations DESC;

SELECT *
FROM EV_DEMO.SEMANTIC.SEM_EV_TYPE_PERFORMANCE
ORDER BY total_ev_registrations DESC;