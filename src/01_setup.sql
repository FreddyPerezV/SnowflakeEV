-- 1. Create project DB and schema (or use existing)
CREATE DATABASE IF NOT EXISTS EV_DEMO;
USE DATABASE EV_DEMO;

CREATE SCHEMA IF NOT EXISTS BRONZE;

-- 2. Small warehouse for demo (change to your trial warehouse if preferred)
CREATE WAREHOUSE IF NOT EXISTS EV_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse for EV pipeline demo';

USE WAREHOUSE EV_WH;


-- 3. Internal stage for JSON file (private, within BRONZE schema)
CREATE STAGE IF NOT EXISTS BRONZE.EV_RAW_STAGE
  FILE_FORMAT = (TYPE = 'JSON')
  COMMENT = 'Stage for EV Population JSON dataset';

LIST @BRONZE.EV_RAW_STAGE;
