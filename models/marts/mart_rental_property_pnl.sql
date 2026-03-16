-- Mart model: rental property P&L by property and month
-- Maps directly to Schedule E reporting structure
-- Materialization: table (final reporting layer)

with rental as (
    select * from {{ ref('int_rental_income_expenses') }}
),

monthly_pnl as (
    select
        property_id,
        property_name,
        llc_name,
        address,
        units,
        transaction_year,
        transaction_month,

        -- Income lines (Schedule E Lines 3-4)
        sum(case when line_type = 'income' then amount else 0 end) as rental_income,

        -- Expense lines (Schedule E Lines 5-19)
        sum(case when line_type = 'expense' and category = 'Mortgage'
            then abs(amount) else 0 end) as mortgage_interest,
        sum(case when line_type = 'expense' and category = 'Repairs'
            then abs(amount) else 0 end) as repairs_maintenance,
        sum(case when line_type = 'expense' and category = 'Property Management'
            then abs(amount) else 0 end) as management_fees,
        sum(case when line_type = 'expense' and category = 'Insurance'
            then abs(amount) else 0 end) as insurance,
        sum(case when line_type = 'expense'
            then abs(amount) else 0 end) as total_expenses,

        -- Net (Schedule E Line 21)
        sum(amount) as net_income_loss,

        count(*) as transaction_count
    from rental
    group by
        property_id,
        property_name,
        llc_name,
        address,
        units,
        transaction_year,
        transaction_month
),

with_ytd as (
    select
        *,
        sum(rental_income) over (
            partition by property_id
            order by transaction_year, transaction_month
        ) as ytd_income,
        sum(total_expenses) over (
            partition by property_id
            order by transaction_year, transaction_month
        ) as ytd_expenses,
        sum(net_income_loss) over (
            partition by property_id
            order by transaction_year, transaction_month
        ) as ytd_net
    from monthly_pnl
)

select * from with_ytd
order by property_id, transaction_year, transaction_month
