-- Pivot monthly income into columns using a Jinja loop

with classified as (
    select * from {{ ref('int_transactions_classified') }}
    where flow_direction = 'inflow'
),

pivoted as (
    select
        transaction_year,
        category,
        tax_category,
        sum(amount) as annual_total,
        {{ sum_by_months('amount', range(1, 13)) }}
    from classified
    group by transaction_year, category, tax_category
)

select * from pivoted
order by transaction_year, annual_total desc