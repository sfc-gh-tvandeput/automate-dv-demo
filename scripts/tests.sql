select top 100 * from dev_edw.mart.dim_customer;

select * from dev_edw.mart.dim_customer
where customer_name like 'Customer#000171034';

select * from DEV_EDW.STAGING.CUSTOMER
where C_NAME like 'Customer#000171034';

delete from DEV_EDW.STAGING.CUSTOMER
where C_NAME like 'Customer#000171034';

-- Insert a segment change for Customer#000171034 (from BUILDING to AUTOMOBILE)
INSERT INTO DEV_EDW.STAGING.CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT, LOAD_DATE)
SELECT 
    C_CUSTKEY,
    C_NAME,
    C_ADDRESS,
    C_NATIONKEY,
    C_PHONE,
    C_ACCTBAL,
    'BUILDING' AS C_MKTSEGMENT,  -- Changed from BUILDING
    C_COMMENT,
    '1992-01-09'::Date AS LOAD_DATE  -- Must be > max(VALID_FROM) in dim_customer
FROM DEV_EDW.STAGING.CUSTOMER
WHERE C_NAME = 'Customer#000171034'
QUALIFY ROW_NUMBER() OVER (PARTITION BY C_CUSTKEY ORDER BY LOAD_DATE DESC) = 1;

-- Insert a dummy order to trigger the customer change pickup
INSERT INTO DEV_EDW.STAGING.ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LOAD_DATE)
SELECT 
    (SELECT MAX(O_ORDERKEY) + 1 FROM DEV_EDW.STAGING.ORDERS),
    171034,
    'O',
    0.00,
    '1992-01-09'::DATE,
    '5-LOW',
    'Clerk#000000001',
    0,
    'dummy order for customer change',
    '1992-01-09'::DATE;