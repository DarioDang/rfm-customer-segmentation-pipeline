SELECT *
FROM {{ ref('customer_clustered_information') }}
WHERE rfm_total_score < 3 OR rfm_total_score > 15