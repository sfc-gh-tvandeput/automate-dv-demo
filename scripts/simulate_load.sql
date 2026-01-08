-- ============================================================================
-- Simulate Data Load Script
-- ============================================================================
-- Usage: Update the variables below and run the script
-- After running: Execute `dbt run && dbt test` to process changes
-- ============================================================================

USE ROLE DEV_DATA_ENGINEER;
USE DATABASE DEV_EDW;
USE SCHEMA STAGING;
USE WAREHOUSE DBT_COMPUTE_WH;

-- ============================================================================
-- CONFIGURATION - Modify these variables
-- ============================================================================
SET load_date = '1992-01-12';           -- New load date (use next available date)
SET num_customers_to_change = 5;         -- How many customers to modify
SET num_orders_to_load = 100;            -- How many new orders to load (0 = skip)

-- ============================================================================
-- STEP 1: Simulate Customer Attribute Changes
-- ============================================================================
-- This creates new versions of existing customers with modified attributes
-- triggering SCD2 changes in satellites and dim_customer

INSERT INTO CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT, LOAD_DATE)
WITH latest_customers AS (
    SELECT c.*,
           ROW_NUMBER() OVER (PARTITION BY C_CUSTKEY ORDER BY LOAD_DATE DESC) as rn
    FROM CUSTOMER c
),
customers_to_change AS (
    SELECT * FROM latest_customers 
    WHERE rn = 1
    ORDER BY RANDOM()
    LIMIT $num_customers_to_change
)
SELECT 
    C_CUSTKEY,
    C_NAME,
    C_ADDRESS || ' - UPDATED',                              -- Modified address
    C_NATIONKEY,
    C_PHONE,
    ROUND(C_ACCTBAL * (1 + (RANDOM() / 10)), 2),           -- Random balance change
    CASE MOD(ABS(HASH(C_CUSTKEY)), 5)                       -- Rotate market segment
        WHEN 0 THEN 'AUTOMOBILE'
        WHEN 1 THEN 'BUILDING'
        WHEN 2 THEN 'FURNITURE'
        WHEN 3 THEN 'HOUSEHOLD'
        ELSE 'MACHINERY'
    END,
    'Modified on ' || $load_date,
    $load_date::DATE
FROM customers_to_change;

-- ============================================================================
-- STEP 2: Load New Orders (Optional)
-- ============================================================================
-- Loads orders from TPCH sample data for the specified date

INSERT INTO ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LOAD_DATE)
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, $load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS
WHERE O_ORDERDATE = $load_date::DATE
LIMIT $num_orders_to_load;

-- Load corresponding line items
INSERT INTO LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, LOAD_DATE)
SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, $load_date::DATE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM
WHERE L_ORDERKEY IN (
    SELECT O_ORDERKEY FROM ORDERS WHERE LOAD_DATE = $load_date::DATE
);

-- ============================================================================
-- STEP 3: Verify Load Results
-- ============================================================================
SELECT '📊 LOAD SUMMARY FOR ' || $load_date AS REPORT;

SELECT 
    'Customers modified' AS METRIC, 
    COUNT(*) AS COUNT 
FROM CUSTOMER WHERE LOAD_DATE = $load_date::DATE
UNION ALL
SELECT 
    'Orders loaded', 
    COUNT(*) 
FROM ORDERS WHERE LOAD_DATE = $load_date::DATE
UNION ALL
SELECT 
    'Line items loaded', 
    COUNT(*) 
FROM LINEITEM WHERE LOAD_DATE = $load_date::DATE;

-- Show which customers were changed
SELECT 'Modified customers:' AS INFO;
SELECT C_CUSTKEY, C_NAME, C_MKTSEGMENT, C_ACCTBAL 
FROM CUSTOMER 
WHERE LOAD_DATE = $load_date::DATE
ORDER BY C_CUSTKEY;

-- ============================================================================
-- NEXT STEPS
-- ============================================================================
-- Run the following commands to process the changes:
--
--   dbt run              # Process all incremental models
--   dbt test             # Validate data integrity
--
-- Or to run specific layers:
--
--   dbt run --select staging      # Refresh staging views
--   dbt run --select raw_vault    # Load hubs, links, satellites
--   dbt run --select business_vault  # Update PIT tables
--   dbt run --select mart         # Update dimensions and facts
-- ============================================================================
