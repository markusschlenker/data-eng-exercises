SELECT *
FROM {{ ref('fct_seller_performance') }}
WHERE total_sales_dollar <= 0
