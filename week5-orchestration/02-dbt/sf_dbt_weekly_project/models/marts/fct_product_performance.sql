{{ config(
    materialized='table',
) }}

with items as (
    select *
    from {{ ref('int_orders_to_items_rel') }}
),
products as (
    select *
    from {{ ref('stg_products') }}
)
select
    p.product_category_name,
    sum(i.price) + sum(i.freight_value) as total_sales,
    sum(i.price + i.freight_value) as total_sales2,
    count(DISTINCT i.product_id) as cnt_distinct_products_sold,
    count(DISTINCT i.order_id) as cnt_distinct_orders,
    avg(i.delivery_days) as avg_delivery_days,
    avg(p.product_weight_g) as avg_product_weight_g,
    avg(p.product_photos_qty) as avg_product_photos_qty
from items as i
join products as p
on i.product_id = p.product_id
group by p.product_category_name
