-- models/marts/dim_customer_clustered.sql

SELECT
    customer_id,
    cluster,
    customer_segment,
    segment_description,

    -- Raw metrics
    recency_days,
    frequency,
    monetary,
    basket_size,
    tenure_days,
    product_diversity,
    ROUND(avg_rating::numeric, 2) AS avg_rating,

    -- Scores
    recency_score,
    frequency_score,
    monetary_score,
    rfm_total_score,

    -- Buckets & flags
    recency_bucket,
    freq_bucket,
    loyalty_member,
    is_churned

FROM {{ source('analytics', 'customer_clustered_information') }}
