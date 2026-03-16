-- Intermediate model: enrich holdings with account context and weight calculations
-- Materialization: view

with holdings as (
    select * from {{ ref('stg_holdings') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

portfolio_total as (
    select sum(market_value) as total_portfolio_value
    from holdings
),

enriched as (
    select
        h.holding_id,
        h.account_id,
        a.account_name,
        a.account_type,
        h.ticker,
        h.shares,
        h.cost_basis_per_share,
        h.current_price,
        h.asset_class,
        h.snapshot_date,
        h.total_cost_basis,
        h.market_value,
        h.unrealized_gain_loss,

        -- Portfolio weight
        cast(h.market_value / nullif(pt.total_portfolio_value, 0) * 100
            as decimal(6,2)) as portfolio_weight_pct,

        -- Gain/loss percentage
        cast(h.unrealized_gain_loss / nullif(h.total_cost_basis, 0) * 100
            as decimal(8,2)) as gain_loss_pct,

        -- Concentration flag
        case
            when h.market_value / nullif(pt.total_portfolio_value, 0) > 0.20
                then 'high'
            when h.market_value / nullif(pt.total_portfolio_value, 0) > 0.10
                then 'moderate'
            else 'normal'
        end as concentration_level

    from holdings h
    left join accounts a on h.account_id = a.account_id
    cross join portfolio_total pt
)

select * from enriched
