-- Mart model: monthly budget summary by category
-- Aggregates transactions into monthly spending/income by category group
-- Materialization: table (final reporting layer)

with classified as (
    select * from {{ ref('int_transactions_classified') }}
),

monthly_summary as (
    select
        transaction_year,
        transaction_month,
        category_group,
        category,
        tax_category,
        count(*) as transaction_count,
        sum(case when flow_direction = 'inflow' then amount else 0 end) as total_inflows,
        sum(case when flow_direction = 'outflow' then amount else 0 end) as total_outflows,
        sum(amount) as net_amount
    from classified
    group by
        transaction_year,
        transaction_month,
        category_group,
        category,
        tax_category
),

with_running as (
    select
        *,
        sum(net_amount) over (
            partition by category_group, category
            order by transaction_year, transaction_month
        ) as ytd_net_amount
    from monthly_summary
)

select * from with_running
order by transaction_year, transaction_month, category_group, category
