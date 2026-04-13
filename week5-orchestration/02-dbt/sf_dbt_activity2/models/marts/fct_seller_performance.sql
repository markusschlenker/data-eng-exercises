{{ config(
        materialized='table'
) }}

with sales as (
    select * from {{ ref('int_order_item_sales') }}
)
select
    seller_id,
    count(order_id) as cnt_orders,
    sum(price) as total_sales_dollar
from sales
group by seller_id