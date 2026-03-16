-- Staging model: clean and type-cast raw transactions
-- Source: YNAB budget exports
-- Materialization: view (always reflects latest source data)

with source as (
    select * from {{ source('ynab', 'raw_transactions') }}
),

cleaned as (
    select
        transaction_id,
        account_id,
        cast(transaction_date as date) as transaction_date,
        trim(payee) as payee,
        trim(category_group) as category_group,
        trim(category) as category,
        cast(amount as decimal(12,2)) as amount,
        trim(memo) as memo,
        trim(source_system) as source_system,

        -- Derived fields
        case when amount > 0 then 'inflow' else 'outflow' end as flow_direction,
        extract(year from cast(transaction_date as date)) as transaction_year,
        extract(month from cast(transaction_date as date)) as transaction_month
    from source
    where transaction_id is not null
)

select * from cleaned
