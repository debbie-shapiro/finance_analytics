-- Incremental model: running ledger of all transactions
-- Only processes new rows on subsequent runs
-- Full refresh rebuilds from scratch

{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

with new_transactions as (
    select
        transaction_id,
        account_id,
        transaction_date,
        payee,
        category_group,
        category,
        amount,
        memo,
        flow_direction,
        transaction_year,
        transaction_month,
        current_timestamp as loaded_at
    from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
        -- On incremental runs, only grab rows newer than what we already have
        where transaction_date > (select max(transaction_date) from {{ this }})
    {% endif %}
)

select * from new_transactions