-- Rental income should always be positive
select *
from {{ ref('int_rental_income_expenses') }}
where line_type = 'income'
  and amount <= 0