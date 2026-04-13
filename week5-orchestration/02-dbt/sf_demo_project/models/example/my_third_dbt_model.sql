with sample_data as (

    select 100 as amount_cents
    union all
    select 250 as amount_cents
    union all
    select 999 as amount_cents

)

select
    amount_cents,
    {{ cents_to_dollars('amount_cents') }} as amount_dollars
from sample_data