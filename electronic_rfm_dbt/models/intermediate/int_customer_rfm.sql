-- models/intermediate/int_customer_rfm.sql

WITH base AS (
    SELECT
        customer_id,
        purchase_date,
        total_price,
        add_on_total
    FROM {{ ref('stg_sale_data') }}
    WHERE LOWER(order_status) = 'completed'
),

dataset_max AS (
    -- Get the last purchase date in the dataset
    SELECT MAX(purchase_date) AS max_purchase_date
    FROM base
),

agg_rfm AS (
    SELECT
        b.customer_id,
        MAX(b.purchase_date) AS last_purchase_date,
        MIN(b.purchase_date) AS first_purchase_date,
        ROUND(AVG(b.total_price), 2) AS avg_order_value,
        COUNT(*) AS frequency,
        ROUND(SUM(b.total_price), 2) AS monetary,
        ROUND(SUM(b.total_price + COALESCE(b.add_on_total,0)), 2) AS total_revenue,
        (d.max_purchase_date - MAX(b.purchase_date))::int AS recency_days,
        (MAX(b.purchase_date) - MIN(b.purchase_date))::int AS lifecycle_days
    FROM base b
    CROSS JOIN dataset_max d
    GROUP BY b.customer_id, d.max_purchase_date
),

final AS (
    SELECT * FROM agg_rfm
)

SELECT * FROM final
