-- Your first dbt model from scratch!

with classified as (
    select * from {{ ref('int_transactions_classified') }}
    where flow_direction = 'inflow'
),

monthly_income as (
    select
        transaction_year,
        transaction_month,
        tax_category,
        category,
        count(*) as payment_count,
        sum(amount) as total_income,
    from classified
    group by
        transaction_year,
        transaction_month,
        tax_category,
        category
)
,

with_pct as (
    select
        *,
        round(total_income / sum(total_income) over (
            partition by transaction_year, transaction_month
        ) * 100, 1) as percent_of_monthly_total
    from monthly_income
)

select * from with_pct
order by transaction_year, transaction_month, total_income desc