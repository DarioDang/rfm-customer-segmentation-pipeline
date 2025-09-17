-- models/segmentation/fact_customer_snapshot.sql

--  Base transactions
WITH base AS (
    SELECT
        customer_id,
        purchase_date,
        total_price,
        COALESCE(add_on_total, 0) AS add_on_total
    FROM {{ ref('stg_sale_data') }}
    WHERE LOWER(order_status) = 'completed'
),

-- Snapshot date = latest transaction date in dataset
dataset_max AS (
    SELECT MAX(purchase_date)::date AS snapshot_date FROM base
),

-- Lifetime revenue per customer
lifetime_revenue AS (
    SELECT
        customer_id,
        SUM(total_price + add_on_total) AS lifetime_revenue
    FROM base
    GROUP BY customer_id
),

-- Recent 90-day revenue per customer
recent_revenue AS (
    SELECT
        b.customer_id,
        SUM(b.total_price + b.add_on_total) AS recent_revenue_90days
    FROM base b
    CROSS JOIN dataset_max d
    WHERE b.purchase_date >= d.snapshot_date - INTERVAL '90 days'
      AND b.purchase_date <= d.snapshot_date
    GROUP BY b.customer_id
),

-- Enriched snapshot
snapshot AS (
    SELECT
        c.customer_id,
        c.cluster,
        c.customer_segment,
        c.segment_description,
        c.recency_score,
        c.frequency_score,
        c.monetary_score,
        c.rfm_total_score,
        lr.lifetime_revenue,
        rr.recent_revenue_90days,
        d.snapshot_date
    FROM {{ source('analytics', 'customer_clustered_information') }} c
    LEFT JOIN lifetime_revenue lr ON c.customer_id = lr.customer_id
    LEFT JOIN recent_revenue rr ON c.customer_id = rr.customer_id
    CROSS JOIN dataset_max d
)

-- Final select
SELECT * 
FROM snapshot

{% if is_incremental() %}
-- Only insert new snapshots
WHERE snapshot_date > (
    SELECT COALESCE(MAX(snapshot_date), DATE '1900-01-01') FROM {{ this }}
)
{% endif %}
