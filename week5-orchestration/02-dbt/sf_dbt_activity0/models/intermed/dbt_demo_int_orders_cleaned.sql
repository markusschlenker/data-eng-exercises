with orders as (
    select * from {{ ref('dbt_demo_stg_orders') }}
    where ORDER_STATUS in ('shipped','approved','delivered')
)
select * from orders
