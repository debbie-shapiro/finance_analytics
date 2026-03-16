-- Mart model: portfolio performance summary by account and asset class
-- Provides account-level and asset-class-level aggregation for reporting
-- Materialization: table (final reporting layer)

with enriched as (
    select * from {{ ref('int_portfolio_enriched') }}
),

-- Account-level summary
by_account as (
    select
        account_id,
        account_name,
        account_type,
        count(*) as position_count,
        sum(total_cost_basis) as total_cost_basis,
        sum(market_value) as total_market_value,
        sum(unrealized_gain_loss) as total_unrealized_gain_loss,
        cast(sum(unrealized_gain_loss) / nullif(sum(total_cost_basis), 0) * 100
            as decimal(8,2)) as account_return_pct
    from enriched
    group by account_id, account_name, account_type
),

-- Asset class breakdown
by_asset_class as (
    select
        asset_class,
        count(*) as position_count,
        sum(total_cost_basis) as total_cost_basis,
        sum(market_value) as total_market_value,
        sum(unrealized_gain_loss) as total_unrealized_gain_loss,
        sum(portfolio_weight_pct) as total_weight_pct
    from enriched
    group by asset_class
),

-- Final combined output with a type indicator
account_rows as (
    select
        'account' as summary_type,
        account_id as group_key,
        account_name as group_label,
        account_type as sub_label,
        position_count,
        total_cost_basis,
        total_market_value,
        total_unrealized_gain_loss,
        account_return_pct as return_pct,
        null as weight_pct
    from by_account
),

asset_rows as (
    select
        'asset_class' as summary_type,
        asset_class as group_key,
        asset_class as group_label,
        null as sub_label,
        position_count,
        total_cost_basis,
        total_market_value,
        total_unrealized_gain_loss,
        cast(total_unrealized_gain_loss / nullif(total_cost_basis, 0) * 100
            as decimal(8,2)) as return_pct,
        total_weight_pct as weight_pct
    from by_asset_class
)

select * from account_rows
union all
select * from asset_rows
