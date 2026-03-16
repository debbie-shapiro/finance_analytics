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
        case
            when t.category = 'Consulting'       then 'schedule_c_income'
            when t.category = 'Rental Income'     then 'schedule_e_income'
            when t.category = 'Mortgage'
                 and t.memo like '%Centaur%'      then 'schedule_e_expense_centaur'
            when t.category = 'Mortgage'
                 and t.memo like '%Minotaur%'     then 'schedule_e_expense_minotaur'
            when t.category = 'Repairs'
                 and t.memo like '%Centaur%'      then 'schedule_e_expense_centaur'
            when t.category = 'Property Management' then 'schedule_e_expense_centaur'
            when t.category = 'Insurance'
                 and t.memo like '%Umbrella%'     then 'schedule_e_expense_shared'
            when t.category = 'Salary'            then 'w2_income'
            else 'personal'
        end as tax_category,

        -- Property attribution for rental transactions
        case
            when t.memo like '%Centaur%'          then 'PROP_CENTAUR'
            when t.memo like '%Minotaur%'         then 'PROP_MINOTAUR'
            when t.category = 'Property Management' then 'PROP_CENTAUR'
            when t.category = 'Insurance'
                 and t.memo like '%Umbrella%'     then 'SHARED'
            else null
        end as property_id

    from transactions t
    left join accounts a
        on t.account_id = a.account_id
)

select * from classified
