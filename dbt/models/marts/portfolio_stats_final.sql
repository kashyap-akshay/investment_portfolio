with portfolio_stats as (
    select * from {{ ref('staging_portfolio_stats') }}
)
select * from portfolio_stats;