-- name: revenue_last_30d_by_country
WITH recent_orders AS (
    SELECT *
    FROM mart.fact_orders
    WHERE order_ts >= DATE '2023-07-20' AND order_ts < DATE '2023-09-01'
)
SELECT
    c.country,
    SUM(o.gross_revenue) AS revenue_30d,
    AVG(o.gross_revenue) AS avg_ticket,
    COUNT(DISTINCT o.order_id) AS orders
FROM recent_orders o
JOIN mart.dim_customers c USING (customer_id)
GROUP BY 1
ORDER BY revenue_30d DESC;

-- name: customer_retention_segments
WITH activity AS (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(gross_revenue) AS revenue,
        MAX(order_ts) AS last_order_ts,
        DATE_DIFF('day', MAX(order_ts), DATE '2023-09-01') AS days_since_last
    FROM mart.fact_orders
    GROUP BY 1
)
SELECT
    a.customer_id,
    CASE
        WHEN days_since_last <= 7 THEN 'active'
        WHEN days_since_last <= 21 THEN 'warm'
        ELSE 'churn-risk'
    END AS segment,
    order_count,
    revenue,
    days_since_last
FROM activity a
JOIN mart.dim_customers c ON c.customer_id = a.customer_id
ORDER BY revenue DESC
LIMIT 20;

-- name: rolling_revenue_14d
WITH daily AS (
    SELECT
        CAST(order_ts AS DATE) AS order_date,
        SUM(gross_revenue) AS revenue
    FROM mart.fact_orders
    GROUP BY 1
)
SELECT
    order_date,
    revenue,
    SUM(revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS revenue_14d
FROM daily
ORDER BY order_date;

-- name: product_mix_and_rank
SELECT
    p.category,
    p.name AS product,
    SUM(f.gross_revenue) AS revenue,
    SUM(f.quantity) AS units,
    ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(f.gross_revenue) DESC) AS revenue_rank
FROM mart.fact_orders f
JOIN mart.dim_products p USING (product_id)
GROUP BY 1, 2
QUALIFY revenue_rank <= 3
ORDER BY category, revenue_rank;
