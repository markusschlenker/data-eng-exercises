SELECT *
FROM {{ ref('fct_orders_geography') }}
WHERE cnt_distinct_order <= 0
