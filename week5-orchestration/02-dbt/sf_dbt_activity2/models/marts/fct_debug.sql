with perf as (
    select * from {{ ref('fct_seller_performance') }}
)
select *, cnt_orders - cnt_orders_dist as cnt_order_dev
from perf
where cnt_orders_dist != cnt_orders
order by cnt_order_dev DESC
