with raw_holdings as (
    select
        business_date,
        portfolio_id,
        security_id,
        exchange,
        cast(replace(quantity, ',', '') as float) as quantity,
        cast(replace(market_value, ',', '') as float) as market_value,
        currency
    from {{ source('staging', 'holdings_stage') }}
)
select * from raw_holdings;