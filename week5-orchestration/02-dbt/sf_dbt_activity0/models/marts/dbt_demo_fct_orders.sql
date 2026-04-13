{{ config(
    materialized='table',
    schema='dbt_demo_activity0__BI_MARTS',
) }}

select
    substr(order_purchase_timestamp,1,4) as order_purchase_year,
    order_status,
    count(distinct order_id) cnt_distinct_order,
    count(distinct customer_id) cnt_distinct_customer
from {{ ref('dbt_demo_int_orders_cleaned') }}
group by 1,2
