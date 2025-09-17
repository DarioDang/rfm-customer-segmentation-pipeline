SELECT *
FROM {{ ref('customer_clustered_information') }}
WHERE avg_rating < 1 OR avg_rating > 5