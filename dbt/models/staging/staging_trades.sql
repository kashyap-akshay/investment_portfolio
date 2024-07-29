with raw_trades as (
    select
        business_date,
        portfolio_id,
        security_id,
        cast(replace(quantity, '(', '-') as float) as quantity,
        cast(replace(price, ',', '') as float) as price,
        currency,
        exchange
    from {{ source('staging', 'trades_stage') }}
)
select * from raw_trades;