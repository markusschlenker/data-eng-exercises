with perf as (
    select * from {{ ref('fct_seller_performance') }}
)
select * 
from perf
where cnt_orders_dist != cnt_orders_dist
