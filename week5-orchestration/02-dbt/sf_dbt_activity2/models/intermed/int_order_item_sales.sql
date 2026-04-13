with items as (
    select * from {{ ref('stg_order_items') }}
    where price IS NOT NULL
)
select
    order_id,
    seller_id,
    price
from items
