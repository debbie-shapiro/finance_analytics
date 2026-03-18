{% macro classify_tax_category(category, memo) %}
    case
        when {{ category }} = 'Consulting'       then 'schedule_c_income'
        when {{ category }} = 'Rental Income'     then 'schedule_e_income'
        when {{ category }} = 'Mortgage'
             and {{ memo }} like '%Centaur%'      then 'schedule_e_expense_centaur'
        when {{ category }} = 'Mortgage'
             and {{ memo }} like '%Minotaur%'     then 'schedule_e_expense_minotaur'
        when {{ category }} = 'Repairs'
             and {{ memo }} like '%Centaur%'      then 'schedule_e_expense_centaur'
        when {{ category }} = 'Property Management' then 'schedule_e_expense_centaur'
        when {{ category }} = 'Insurance'
             and {{ memo }} like '%Umbrella%'     then 'schedule_e_expense_shared'
        when {{ category }} = 'Salary'            then 'w2_income'
        else 'personal'
    end
{% endmacro %}