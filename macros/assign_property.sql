{% macro assign_property(category, memo) %}
    case
        when {{ memo }} like '%Centaur%'          then 'PROP_CENTAUR'
        when {{ memo }} like '%Minotaur%'         then 'PROP_MINOTAUR'
        when {{ category }} = 'Property Management' then 'PROP_CENTAUR'
        when {{ category }} = 'Insurance'
             and {{ memo }} like '%Umbrella%'     then 'SHARED'
        else null
    end
{% endmacro %}