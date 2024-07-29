with aggregated as (
    select
        portfolio_id,
        business_date,
        sum(quantity * price) as total_value,
        sum(quantity) as total_quantity
    from {{ ref('trades_final') }}
    group by portfolio_id, business_date
)
select * from aggregated;