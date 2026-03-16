-- Staging model: clean and type-cast raw accounts
-- Materialization: view

with source as (
    select * from {{ source('ynab', 'raw_accounts') }}
),

cleaned as (
    select
        account_id,
        trim(account_name) as account_name,
        trim(lower(account_type)) as account_type,
        trim(institution) as institution,
        cast(is_active as boolean) as is_active
    from source
    where account_id is not null
)

select * from cleaned
