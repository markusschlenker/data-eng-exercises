{{ config(
    materialized='table'
) }}


SELECT
  seller_id,
  SUM(price) AS total_sales_value,
  COUNT(DISTINCT order_id) AS order_count
FROM  {{ref('int_order_item_sales')}}
GROUP BY seller_id
