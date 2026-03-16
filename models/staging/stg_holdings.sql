-- Staging model: clean and type-cast raw portfolio holdings
-- Source: Schwab positions export
-- Materialization: view

with source as (
    select * from {{ source('schwab', 'raw_holdings') }}
),

cleaned as (
    select
        holding_id,
        account_id,
        upper(trim(ticker)) as ticker,
        cast(shares as decimal(12,4)) as shares,
        cast(cost_basis_per_share as decimal(12,4)) as cost_basis_per_share,
        cast(current_price as decimal(12,4)) as current_price,
        trim(lower(asset_class)) as asset_class,
        cast(snapshot_date as date) as snapshot_date,

        -- Derived fields
        cast(shares * cost_basis_per_share as decimal(14,2)) as total_cost_basis,
        cast(shares * current_price as decimal(14,2)) as market_value,
        cast((shares * current_price) - (shares * cost_basis_per_share) as decimal(14,2)) as unrealized_gain_loss
    from source
    where holding_id is not null
)

select * from cleaned
