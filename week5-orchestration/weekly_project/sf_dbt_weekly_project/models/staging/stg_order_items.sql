with order_items as (
    select
        order_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_date,
        cast(price as decimal(10,2)) as price,
        freight_value
    from {{ source('olist', 'OLIST_ORDER_ITEMS_DATASET') }}
)

select * from order_items
