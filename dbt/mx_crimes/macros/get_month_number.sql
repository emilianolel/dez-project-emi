{#
    This macro returns the number of the given month name
#}

{% macro get_month_number(month_name) -%}

    case {{ month_name }}
        when 'january' then 1
        when 'february' then 2
        when 'march' then 3
        when 'april' then 4
        when 'may' then 5
        when 'june' then 6
        when 'july' then 7
        when 'august' then 8
        when 'september' then 9
        when 'october' then 10
        when 'november' then 11
        when 'december' then 12
    end

{%- endmacro %}