SELECT *
FROM {{ ref('fct_seller_performance') }}
WHERE cnt_orders_dist <= 0
