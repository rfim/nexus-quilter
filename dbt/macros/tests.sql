{# A satellite must never hold two consecutive rows with the same hashdiff for one key. #}
{% test no_repeated_hashdiff(model, keys) %}
select * from (
  select {{ keys | join(', ') }}, hashdiff, lag(hashdiff) over (partition by {{ keys | join(', ') }} order by load_ts) as prev
  from {{ model }}) where prev = hashdiff
{% endtest %}

{# Aggregates published from sensitive data must not contain small cells. #}
{% test dbt_utils_free_min_cell(model, column, minimum) %}
select * from {{ model }} where {{ column }} < {{ minimum }}
{% endtest %}
