SELECT *
FROM {{ ref('fct_seller_performance') }}
WHERE cnt_orders <= 0
