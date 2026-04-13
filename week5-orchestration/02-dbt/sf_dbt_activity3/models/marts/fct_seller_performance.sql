{{ config(
        materialized='table'
) }}

with sales as (
    select * from {{ ref('int_order_item_sales') }}
)
select
    seller_id,
    sum(price) as total_sales_dollar,
    count(order_id) as cnt_orders,
    count(DISTINCT order_id) as cnt_orders_dist
from sales
group by 1
