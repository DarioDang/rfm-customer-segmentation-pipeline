WITH cleaned AS (
  SELECT
    CAST(customer_id AS INT) AS customer_id,
    CAST(age AS INT) AS age,

    -- Normalize and map gender values
    CASE 
      WHEN LOWER(TRIM(gender)) IN ('m', 'male') THEN 'male'
      WHEN LOWER(TRIM(gender)) IN ('f', 'female') THEN 'female'
      ELSE 'other'
    END AS gender,

    -- Normalize loyalty and create boolean flag
    LOWER(TRIM(loyalty_member)) AS loyalty_member,
    CASE 
      WHEN LOWER(TRIM(loyalty_member)) = 'yes' THEN TRUE
      ELSE FALSE
    END AS is_loyal_customer,

    -- Clean product info
    LOWER(TRIM(product_type)) AS product_type,
    
    -- Normalize SKU: remove non-alphanumerics and uppercase
    UPPER(REGEXP_REPLACE(TRIM(sku), '[^a-zA-Z0-9]', '', 'g')) AS sku,

    -- Rating and status
    CAST(rating AS INT) AS rating,
    LOWER(TRIM(order_status)) AS order_status,

    -- Payment and shipping
    LOWER(TRIM(payment_method)) AS payment_method,
    LOWER(TRIM(shipping_type)) AS shipping_type,
    LOWER(TRIM(add_ons_purchased)) AS add_ons_purchased,

    -- Pricing and dates
    CAST(total_price AS NUMERIC) AS total_price,
    CAST(unit_price AS NUMERIC) AS unit_price,
    CAST(quantity AS INT) AS quantity,
    CAST(purchase_date AS DATE) AS purchase_date,
    CAST(add_on_total AS NUMERIC) AS add_on_total

  FROM {{ source('raw', 'sales_2023_2024') }}
)

SELECT *
FROM cleaned
WHERE customer_id IS NOT NULL
  AND purchase_date IS NOT NULL
  AND total_price > 0
  AND quantity > 0
  AND purchase_date <= CURRENT_DATE
