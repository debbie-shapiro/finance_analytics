-- Staging model: clean and type-cast raw rental properties
-- Materialization: view

with source as (
    select * from {{ source('rentals', 'raw_properties') }}
),

cleaned as (
    select
        property_id,
        trim(property_name) as property_name,
        trim(llc_name) as llc_name,
        trim(address) as address,
        trim(city) as city,
        trim(state) as state,
        cast(units as integer) as units,
        cast(acquisition_date as date) as acquisition_date,
        cast(purchase_price as decimal(12,2)) as purchase_price
    from source
    where property_id is not null
)

select * from cleaned
