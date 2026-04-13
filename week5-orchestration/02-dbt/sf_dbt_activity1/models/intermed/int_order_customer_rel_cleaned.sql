
with customers as (
    select 
        so.order_id, 
        so.order_status, 
        so.order_purchase_timestamp, 
        sc.customer_id, 
        sc.customer_city, 
        sc.customer_state
    from {{ ref('stg_orders'   ) }} as so
    join {{ ref('stg_customers') }} as sc
    on so.customer_id = sc.customer_id
    where ORDER_STATUS in ('shipped','approved','delivered')
)
select * from customers
