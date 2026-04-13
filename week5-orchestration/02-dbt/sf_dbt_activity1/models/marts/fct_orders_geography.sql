{{ config(
    materialized='table'
) }}

-- aggregate order and customer id count based on 
-- * order_purchase_year
-- * order_status, customer_city and customer_state.
select
    substr(order_purchase_timestamp,1,4) as order_purchase_year,
    order_status,
    customer_city,
    customer_state,
    count(distinct order_id) cnt_distinct_order,
    count(distinct customer_id) cnt_distinct_customer
from {{ ref('int_order_customer_rel_cleaned') }}
group by 1,2,3,4
