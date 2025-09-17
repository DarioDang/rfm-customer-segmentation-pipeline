WITH rfm AS (
    SELECT *
    FROM {{ ref('mart_rfm_features') }}  
),

final AS (
    SELECT
        r.customer_id,
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.product_diversity,
        r.avg_rating,
        r.basket_size,
        r.tenure_days
    FROM rfm r
)

SELECT * FROM final