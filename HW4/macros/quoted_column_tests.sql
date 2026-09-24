{# Quote exact-case camelCase column names when dbt emits Snowflake test SQL. #}
{% test unique_quoted(model, column_name) %}
    select {{ adapter.quote(column_name) }} as duplicate_value
    from {{ model }}
    group by {{ adapter.quote(column_name) }}
    having count(*) > 1
{% endtest %}

{% test not_null_quoted(model, column_name) %}
    select {{ adapter.quote(column_name) }}
    from {{ model }}
    where {{ adapter.quote(column_name) }} is null
{% endtest %}
