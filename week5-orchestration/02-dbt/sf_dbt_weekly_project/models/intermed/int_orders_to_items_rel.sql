
with orders as (
    select 
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp, 
        o.order_delivered_customer_date,
        DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date) as delivery_days, 
        
        i.product_id,
        i.seller_id,
        i.shipping_limit_date,
        i.price,
        i.freight_value

    from {{ ref('stg_orders'   ) }} as o
    join {{ ref('stg_order_items') }} as i
    on o.order_id = i.order_id
    --where ORDER_STATUS in ('shipped','approved','delivered')
)
select * from orders
