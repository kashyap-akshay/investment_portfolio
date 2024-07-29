with trades as (
    select * from {{ ref('staging_trades') }}
)
select * from trades;