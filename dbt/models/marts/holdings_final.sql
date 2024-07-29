with holdings as (
    select * from {{ ref('staging_holdings') }}
)
select * from holdings;