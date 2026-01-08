WITH fct_revenue AS (
    SELECT SUM(TOTAL_PRICE) AS total_revenue
    FROM {{ ref('fct_orders') }}
),

sat_revenue AS (
    SELECT SUM(TOTALPRICE) AS total_revenue
    FROM {{ ref('sat_order_order_details') }}
)

SELECT
    f.total_revenue AS fct_total,
    s.total_revenue AS sat_total,
    ABS(f.total_revenue - s.total_revenue) AS difference
FROM fct_revenue f
CROSS JOIN sat_revenue s
WHERE ABS(f.total_revenue - s.total_revenue) > 0.01