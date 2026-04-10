
-- Use the `ref` function to select from other models

select *
from {{ ref('my_first_dbt_model') }}  -- will resolve to correct database.schema.table
where id = 1
