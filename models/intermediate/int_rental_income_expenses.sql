-- Intermediate model: isolate rental transactions and link to properties
-- Filters to rental-related transactions only and joins property details
-- Materialization: view

with classified as (
    select * from {{ ref('int_transactions_classified') }}
    where tax_category like 'schedule_e%'
),

properties as (
    select * from {{ ref('stg_properties') }}
),

-- Handle shared expenses by splitting across properties
shared_expenses as (
    select
        c.transaction_id,
        p.property_id,
        c.transaction_date,
        c.payee,
        c.category,
        -- Split shared expenses proportionally by unit count
        cast(c.amount * (cast(p.units as decimal) /
            (select sum(cast(units as integer)) from {{ ref('stg_properties') }}))
            as decimal(12,2)) as amount,
        c.memo,
        c.transaction_year,
        c.transaction_month,
        'allocated' as allocation_method
    from classified c
    cross join properties p
    where c.property_id = 'SHARED'
),

-- Direct property expenses (not shared)
direct_expenses as (
    select
        c.transaction_id,
        c.property_id,
        c.transaction_date,
        c.payee,
        c.category,
        c.amount,
        c.memo,
        c.transaction_year,
        c.transaction_month,
        'direct' as allocation_method
    from classified c
    where c.property_id != 'SHARED'
      and c.property_id is not null
),

combined as (
    select * from direct_expenses
    union all
    select * from shared_expenses
),

enriched as (
    select
        c.transaction_id,
        c.property_id,
        p.property_name,
        p.llc_name,
        p.address,
        p.units,
        c.transaction_date,
        c.payee,
        c.category,
        c.amount,
        c.memo,
        c.transaction_year,
        c.transaction_month,
        c.allocation_method,
        case when c.amount > 0 then 'income' else 'expense' end as line_type
    from combined c
    left join properties p
        on c.property_id = p.property_id
)

select * from enriched
