WITH model_clusters AS (
    SELECT customer_id, cluster
    FROM {{ source('model', 'customer_clusters') }}
),

rfm AS (
    SELECT *
    FROM {{ source('mart', 'mart_rfm_features') }}
),

joined AS (
    SELECT
        rfm.customer_id,
        mc.cluster,
        -- segment info will be joined later
        rfm.recency_days,
        rfm.frequency,
        rfm.monetary,
        rfm.total_revenue,
        rfm.basket_size,
        rfm.tenure_days,
        rfm.product_diversity,
        rfm.avg_rating,
        rfm.recency_score,
        rfm.frequency_score,
        rfm.monetary_score,
        rfm.rfm_total_score,
        rfm.recency_bucket,
        rfm.freq_bucket,
        rfm.loyalty_member,
        rfm.is_churned
    FROM rfm
    INNER JOIN model_clusters mc
        ON rfm.customer_id = mc.customer_id
),

segment_map AS (
    SELECT * FROM (
        VALUES
            (0, 'At-Risk Low Spenders',
                'Customers who have been around for a long time but show low frequency and monetary value. They may have moderate basket sizes but are overall at risk of churning due to weak engagement.'),
            (1, 'High Value Loyalists',
                'New but very active customers who purchase often and spend a lot overall. Even though their tenure is short, their high recency, frequency, and spend indicate strong early loyalty'),
            (2, 'Big-Basket Shoppers',
                'Customers who shop less frequently than Cluster 1 but purchase large baskets when they do. They are newer, tend to spend heavily per transaction, and could grow into valuable long-term customers')
    ) AS t(cluster, customer_segment, segment_description)
)

SELECT
    j.customer_id,
    j.cluster,
    s.customer_segment,
    s.segment_description,
    j.recency_days,
    j.frequency,
    j.monetary,
    j.total_revenue,
    j.basket_size,
    j.tenure_days,
    j.product_diversity,
    j.avg_rating,
    j.recency_score,
    j.frequency_score,
    j.monetary_score,
    j.rfm_total_score,
    j.recency_bucket,
    j.freq_bucket,
    j.loyalty_member,
    j.is_churned
FROM joined j
LEFT JOIN segment_map s
    ON j.cluster = s.cluster
