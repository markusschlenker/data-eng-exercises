SELECT *
FROM {{ ref('fct_orders') }}
WHERE cnt_distinct_order <= 0
