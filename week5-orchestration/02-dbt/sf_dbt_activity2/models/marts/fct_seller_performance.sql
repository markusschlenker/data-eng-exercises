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
    count(DISTINCT order_id) as cnt_orders_dist  -- in case multiple items from one order_id - not the case here
from sales
group by seller_id
