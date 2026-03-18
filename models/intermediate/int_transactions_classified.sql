-- Intermediate model: classify transactions for tax and budget reporting
-- Joins transactions with accounts to enrich with account context
-- Adds tax_category for Schedule C/E mapping
-- Materialization: view

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

classified as (
    select
        t.transaction_id,
        t.account_id,
        a.account_name,
        a.account_type,
        a.institution,
        t.transaction_date,
        t.payee,
        t.category_group,
        t.category,
        t.amount,
        t.memo,
        t.source_system,
        t.flow_direction,
        t.transaction_year,
        t.transaction_month,

        -- Tax classification: map categories to IRS schedules
        {{ classify_tax_category('t.category', 't.memo') }} as tax_category,

        -- Property attribution for rental transactions
        {{ assign_property('t.category', 't.memo') }} as property_id

    from transactions t
    left join accounts a
        on t.account_id = a.account_id
)

select * from classified
