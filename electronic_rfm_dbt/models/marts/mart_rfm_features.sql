WITH product_diversity AS (
    SELECT
        customer_id,
        COUNT(DISTINCT product_type) AS product_diversity
    FROM {{ ref('stg_sale_data') }}
    GROUP BY customer_id
),

avg_rating AS (
    SELECT
        customer_id,
        ROUND(AVG(rating),2) AS avg_rating
    FROM {{ ref('stg_sale_data') }}
    GROUP BY customer_id
),

tenure AS (
    SELECT
        customer_id,
        DATE(MAX(purchase_date)) - DATE(MIN(purchase_date)) AS tenure_days
    FROM {{ ref('stg_sale_data') }}
    GROUP BY customer_id
),

final AS (
    SELECT
        r.*,

        -- Recency bucket (same as Python pd.cut)
        CASE 
            WHEN r.recency_days BETWEEN 0 AND 90  THEN '0–3 mo'
            WHEN r.recency_days BETWEEN 91 AND 180 THEN '3–6 mo'
            WHEN r.recency_days BETWEEN 181 AND 270 THEN '6–9 mo'
            WHEN r.recency_days BETWEEN 271 AND 365 THEN '9–12 mo'
        END AS recency_bucket,

        -- Frequency bucket
        CASE
            WHEN r.frequency = 1 THEN 'One-time'
            WHEN r.frequency = 2 THEN 'Repeat'
            WHEN r.frequency >= 3 THEN 'Frequent'
        END AS freq_bucket,

        -- Churn flag tied to bucket 
        CASE 
            WHEN r.recency_days BETWEEN 271 AND 365 THEN TRUE
            ELSE FALSE
        END AS is_churned,

        COALESCE(pd.product_diversity, 0) AS product_diversity,
        COALESCE(ar.avg_rating, 0)       AS avg_rating,
        CASE WHEN r.frequency = 0 THEN 0 
             ELSE r.monetary::float / r.frequency END AS basket_size,
        COALESCE(t.tenure_days, 0)       AS tenure_days

    FROM {{ ref('mart_rfm') }} r
    LEFT JOIN product_diversity pd ON r.customer_id = pd.customer_id
    LEFT JOIN avg_rating ar        ON r.customer_id = ar.customer_id
    LEFT JOIN tenure t             ON r.customer_id = t.customer_id
)

SELECT * FROM final