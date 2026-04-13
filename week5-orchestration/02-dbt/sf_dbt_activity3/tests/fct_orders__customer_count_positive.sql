SELECT *
FROM {{ ref('fct_orders') }}
WHERE cnt_distinct_customer <= 0
