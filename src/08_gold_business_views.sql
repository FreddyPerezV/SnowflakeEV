-- =========================================================
-- File: 08_gold_business_views.sql
-- Purpose: Create business-ready views on top of the Gold layer
-- =========================================================

-- Step 1: Set working context
USE DATABASE EV_DEMO;
USE WAREHOUSE EV_WH;

-- Step 2: Set Gold schema
USE SCHEMA GOLD;

-- Step 3: Create executive summary view by state and model year
CREATE OR REPLACE VIEW VW_EV_ADOPTION_SUMMARY AS
SELECT
    state,
    model_year,
    total_ev_registrations,
    avg_electric_range,
    avg_base_msrp
FROM AGG_EV_BY_STATE_YEAR;

-- Step 4: Create top makes business view
CREATE OR REPLACE VIEW VW_EV_TOP_MAKES AS
SELECT
    make,
    total_ev_registrations,
    avg_electric_range,
    avg_base_msrp,
    ROUND(
        total_ev_registrations * 100.0
        / SUM(total_ev_registrations) OVER (),
        2
    ) AS market_share_pct
FROM AGG_EV_BY_MAKE;

-- Step 5: Create state performance business view
CREATE OR REPLACE VIEW VW_EV_STATE_PERFORMANCE AS
SELECT
    l.state,
    COUNT(*) AS total_ev_registrations,
    COUNT(DISTINCT l.city) AS distinct_cities,
    COUNT(DISTINCT l.county) AS distinct_counties,
    AVG(f.electric_range) AS avg_electric_range,
    AVG(f.base_msrp) AS avg_base_msrp
FROM FACT_EV_REGISTRATIONS f
JOIN DIM_LOCATION l
    ON f.location_key = l.location_key
GROUP BY l.state;

-- Step 6: Create EV type insights view
CREATE OR REPLACE VIEW VW_EV_TYPE_INSIGHTS AS
SELECT
    electric_vehicle_type,
    cafv_eligibility,
    total_ev_registrations,
    avg_electric_range,
    avg_base_msrp
FROM AGG_EV_TYPE_SUMMARY;

-- Step 7: Validation queries
SELECT *
FROM VW_EV_ADOPTION_SUMMARY
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM VW_EV_TOP_MAKES
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM VW_EV_STATE_PERFORMANCE
ORDER BY total_ev_registrations DESC
LIMIT 20;

SELECT *
FROM VW_EV_TYPE_INSIGHTS
ORDER BY total_ev_registrations DESC
LIMIT 20;


SELECT *
FROM GOLD.VW_EV_TOP_MAKES
WHERE market_share_pct > 1
ORDER BY market_share_pct DESC;
