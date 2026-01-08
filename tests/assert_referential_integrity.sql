SELECT
    f.CUSTOMER_SK,
    f.ORDER_SK
FROM {{ ref('fct_orders') }} f
LEFT JOIN {{ ref('dim_customer') }} d
    ON f.CUSTOMER_SK = d.CUSTOMER_SK
WHERE d.CUSTOMER_SK IS NULL
  AND f.CUSTOMER_SK != 'UNKNOWN'