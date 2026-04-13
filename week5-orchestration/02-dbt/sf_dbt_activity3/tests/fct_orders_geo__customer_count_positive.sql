SELECT *
FROM {{ ref('fct_orders_geography') }}
WHERE cnt_distinct_customer <= 0
