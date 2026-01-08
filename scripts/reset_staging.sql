-- ============================================================================
-- Reset Script - Recreate Staging & Restart from Day 1
-- ============================================================================
-- This script:
--   1. Truncates all staging tables
--   2. Optionally drops and recreates Raw Vault, Business Vault, and Mart tables
--   3. Reloads Day 1 data from TPCH sample
-- ============================================================================

USE ROLE DEV_DATA_ENGINEER;
USE DATABASE DEV_EDW;
USE WAREHOUSE DBT_COMPUTE_WH;

-- ============================================================================
-- CONFIGURATION
-- ============================================================================
SET initial_load_date = '1992-01-08';
SET full_reset = TRUE;  -- TRUE = also reset RDV/BDV/MART tables, FALSE = staging only

-- ============================================================================
-- STEP 1: Truncate Staging Tables
-- ============================================================================
USE SCHEMA STAGING;

TRUNCATE TABLE REGION;
TRUNCATE TABLE NATION;
TRUNCATE TABLE CUSTOMER;
TRUNCATE TABLE SUPPLIER;
TRUNCATE TABLE PART;
TRUNCATE TABLE PARTSUPP;
TRUNCATE TABLE ORDERS;
TRUNCATE TABLE LINEITEM;

SELECT '✅ Staging tables truncated' AS STATUS;

-- ============================================================================
-- STEP 2: Reset Data Vault Tables (if full_reset = TRUE)
-- ============================================================================
-- Run these manually or via dbt run --full-refresh

-- To fully reset all dbt models, run:
--   dbt run --full-refresh
--
-- Or reset specific layers:
--   dbt run --select raw_vault --full-refresh
--   dbt run --select business_vault --full-refresh
--   dbt run --select mart --full-refresh

-- ============================================================================
-- STEP 3: Load Reference Data (Day 1)
-- ============================================================================
INSERT INTO REGION (R_REGIONKEY, R_NAME, R_COMMENT, LOAD_DATE)
SELECT R_REGIONKEY, R_NAME, R_COMMENT, $initial_load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.REGION;

INSERT INTO NATION (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT, LOAD_DATE)
SELECT N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT, $initial_load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION;

SELECT '✅ Reference data loaded (REGION, NATION)' AS STATUS;

-- ============================================================================
-- STEP 4: Load Master Data (Day 1)
-- ============================================================================
INSERT INTO CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT, LOAD_DATE)
SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT, $initial_load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER;

INSERT INTO SUPPLIER (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT, LOAD_DATE)
SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT, $initial_load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.SUPPLIER;

INSERT INTO PART (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT, LOAD_DATE)
SELECT P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT, $initial_load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PART;

INSERT INTO PARTSUPP (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT, LOAD_DATE)
SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT, $initial_load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PARTSUPP;

SELECT '✅ Master data loaded (CUSTOMER, SUPPLIER, PART, PARTSUPP)' AS STATUS;

-- ============================================================================
-- STEP 5: Load Transactional Data (Day 1)
-- ============================================================================
INSERT INTO ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LOAD_DATE)
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, $initial_load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS
WHERE O_ORDERDATE = $initial_load_date::DATE;

INSERT INTO LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, LOAD_DATE)
SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, $initial_load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM
WHERE L_ORDERKEY IN (SELECT O_ORDERKEY FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS WHERE O_ORDERDATE = $initial_load_date::DATE);

SELECT '✅ Transactional data loaded (ORDERS, LINEITEM)' AS STATUS;

-- ============================================================================
-- STEP 6: Verify Load
-- ============================================================================
SELECT '📊 STAGING TABLE COUNTS' AS REPORT;

SELECT 'REGION' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM REGION
UNION ALL SELECT 'NATION', COUNT(*) FROM NATION
UNION ALL SELECT 'CUSTOMER', COUNT(*) FROM CUSTOMER
UNION ALL SELECT 'SUPPLIER', COUNT(*) FROM SUPPLIER
UNION ALL SELECT 'PART', COUNT(*) FROM PART
UNION ALL SELECT 'PARTSUPP', COUNT(*) FROM PARTSUPP
UNION ALL SELECT 'ORDERS', COUNT(*) FROM ORDERS
UNION ALL SELECT 'LINEITEM', COUNT(*) FROM LINEITEM
ORDER BY TABLE_NAME;

-- ============================================================================
-- NEXT STEPS
-- ============================================================================
-- Run the following to rebuild all Data Vault layers:
--
--   dbt run --full-refresh    # Full rebuild of all models
--   dbt test                  # Validate data integrity
--
-- Or incrementally (if tables already exist):
--
--   dbt run
--   dbt test
-- ============================================================================

SELECT '🚀 Reset complete! Run: dbt run --full-refresh && dbt test' AS NEXT_STEPS;
