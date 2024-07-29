with combined as (
    select
        h.business_date,
        h.portfolio_id,
        h.security_id,
        h.quantity,
        h.market_value,
        ps.nav,
        ps.daily_pnl,
        ps.ytd_return,
        ps.sharpe_ratio,
        ps.volatility,
        ps.var_95
    from {{ ref('holdings_final') }} h
    left join {{ ref('portfolio_stats_final') }} ps
    on h.portfolio_id = ps.portfolio_id
    and h.business_date = ps.business_date
)
select * from combined;