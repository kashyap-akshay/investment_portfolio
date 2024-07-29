with raw_portfolio_stats as (
    select
        business_date,
        portfolio_id,
        cast(replace(nav, ',', '') as float) as nav,
        cast(replace(daily_pnl, ',', '') as float) as daily_pnl,
        cast(replace(ytd_return, '%', '') as float) / 100 as ytd_return,
        sharpe_ratio,
        volatility,
        var_95
    from {{ source('staging', 'portfolio_stats_stage') }}
)
select * from raw_portfolio_stats;