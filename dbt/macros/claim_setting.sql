{#- Care setting from the CMS place-of-service code. -#}
{% macro claim_setting(place_of_service) -%}
    case {{ place_of_service }}
        when '21' then 'inpatient'
        when '23' then 'emergency'
        when '11' then 'office'
        else 'other'
    end
{%- endmacro %}
