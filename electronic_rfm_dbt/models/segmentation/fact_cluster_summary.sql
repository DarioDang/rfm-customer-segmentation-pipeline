WITH base AS (
    SELECT
        customer_id,
        purchase_date,
        total_price
    FROM {{ ref('stg_sale_data') }}
    WHERE LOWER(order_status) = 'completed'
),

dataset_max AS (
    SELECT MAX(purchase_date) AS cutoff_date FROM base
),

params AS (
    SELECT
        cutoff_date,
        cutoff_date - INTERVAL '90 days' AS start_date
    FROM dataset_max
),

recent_revenue AS (
    SELECT
        b.customer_id,
        SUM(b.total_price) AS recent_revenue
    FROM base b
    CROSS JOIN params p
    WHERE b.purchase_date >= p.start_date
      AND b.purchase_date <= p.cutoff_date
    GROUP BY b.customer_id
),

customer_enriched AS (
    SELECT
        c.customer_id,
        c.cluster,
        c.customer_segment,
        c.recency_days,
        c.frequency,
        c.monetary,
        c.basket_size,
        c.tenure_days,
        COALESCE(r.recent_revenue, 0) AS recent_revenue,
        c.total_revenue
    FROM {{ source('analytics', 'customer_clustered_information') }} c
    LEFT JOIN recent_revenue r
        ON c.customer_id = r.customer_id
)

SELECT
    cluster,
    MIN(customer_segment) AS customer_segment,
    COUNT(DISTINCT customer_id) AS customer_count,
    ROUND(AVG(recency_days)::NUMERIC, 2) AS avg_recency_days,
    ROUND(AVG(frequency)::NUMERIC, 2) AS avg_frequency,
    ROUND(AVG(monetary)::NUMERIC, 2) AS avg_monetary,
    ROUND(AVG(basket_size)::NUMERIC, 2) AS avg_basket_size,
    ROUND(AVG(tenure_days)::NUMERIC, 2) AS avg_tenure_days,
    ROUND(SUM(total_revenue)::NUMERIC, 2) AS lifetime_revenue,
    ROUND(SUM(recent_revenue)::NUMERIC, 0) AS recent_revenue_90days
FROM customer_enriched
GROUP BY cluster
ORDER BY cluster
