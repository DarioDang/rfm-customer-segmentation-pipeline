WITH rfm AS (
    SELECT * FROM {{ ref('int_customer_rfm') }}
),

loyalty AS (
    SELECT
        customer_id,
        MAX(is_loyal_customer::int) AS is_loyal_int  -- Convert boolean to int for aggregation
    FROM {{ ref('stg_sale_data') }}
    GROUP BY customer_id
),

rfm_scored AS (
    SELECT
        r.customer_id,
        recency_days,
        frequency,
        monetary,
        r.total_revenue,
        l.is_loyal_int::boolean AS loyalty_member,

        -- Quantile-based scoring
        NTILE(5) OVER (ORDER BY recency_days ASC) AS recency_score,   -- Recent = high score
        NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,  -- Frequent = high score
        NTILE(5) OVER (ORDER BY monetary DESC) AS monetary_score     -- High spenders = high score
    FROM rfm r
    LEFT JOIN loyalty l ON r.customer_id = l.customer_id
),

final AS (
  SELECT *,
         recency_score + frequency_score + monetary_score AS rfm_total_score
  FROM rfm_scored
)

SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    loyalty_member,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_total_score,
    total_revenue
FROM final