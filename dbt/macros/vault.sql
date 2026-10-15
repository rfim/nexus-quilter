{#- Small Data Vault macros, in the style of AutomateDV, written for Spark SQL so the same
    models run on local Spark and on Databricks. -#}

{# Hash key from one or more business-key columns: trimmed, upper-cased, delimited. #}
{% macro hash_key(cols) -%}
md5(concat_ws('||', {% for c in cols %}upper(trim(cast({{ c }} as string))){% if not loop.last %}, {% endif %}{% endfor %}))
{%- endmacro %}

{# Hashdiff over the descriptive columns of a satellite. #}
{% macro hashdiff(cols) -%}
md5(concat_ws('||', {% for c in cols %}coalesce(cast({{ c }} as string), '^^'){% if not loop.last %}, {% endif %}{% endfor %}))
{%- endmacro %}

{# The load timestamp for this run: day 1 or day 2 of the walkthrough. #}
{% macro load_ts() -%}
timestamp '2026-10-0{{ var("load_day") }} 05:30:00'
{%- endmacro %}

{# Hub: one row per business key, first sighting only. `sources` is a list of (relation, key column, record source). #}
{% macro hub(pk, nk, sources) %}
with keys as (
  {% for rel, col, rs in sources %}
  select {{ hash_key([col]) }} as {{ pk }}, upper(trim(cast({{ col }} as string))) as {{ nk }}, '{{ rs }}' as record_source, {{ loop.index }} as _prio
  from {{ rel }} where {{ col }} is not null
  {% if not loop.last %}union all{% endif %}
  {% endfor %}
),
firsts as (
  select {{ pk }}, {{ nk }}, record_source from (
    select *, row_number() over (partition by {{ pk }} order by _prio) as _rn from keys) where _rn = 1
)
select {{ pk }}, {{ nk }}, {{ load_ts() }} as load_ts, record_source from firsts
{% if is_incremental() %} where {{ pk }} not in (select {{ pk }} from {{ this }}) {% endif %}
{% endmacro %}

{# Link: one row per relationship between hubs. `fks` maps hash-key name -> business-key column. #}
{% macro link(pk, fks, rel, record_source) %}
with rels as (
  select distinct {{ hash_key(fks.values() | list) }} as {{ pk }},
         {% for name, col in fks.items() %}{{ hash_key([col]) }} as {{ name }}{% if not loop.last %}, {% endif %}{% endfor %}
  from {{ rel }}
)
select *, {{ load_ts() }} as load_ts, '{{ record_source }}' as record_source from rels
{% if is_incremental() %} where {{ pk }} not in (select {{ pk }} from {{ this }}) {% endif %}
{% endmacro %}

{# Satellite: insert-only; a row lands only when the hashdiff differs from the latest one for its key.
   `keys` are the columns that identify one description (the hash key, plus e.g. month for multi-active). #}
{% macro sat(keys, payload, rel, record_source) %}
with hashed as (
  select {{ keys | join(', ') }}, {{ payload | join(', ') }}, {{ hashdiff(payload) }} as hashdiff from {{ rel }}
),
src as (  -- one description per key per load (two source records can share a key, e.g. one person, two accounts)
  select * except (_rn) from (
    select *, row_number() over (partition by {{ keys | join(', ') }} order by hashdiff) as _rn from hashed) where _rn = 1
)
select src.*, {{ load_ts() }} as load_ts, '{{ record_source }}' as record_source
from src
{% if is_incremental() %}
left join (
  select {{ keys | join(', ') }}, hashdiff as _latest from (
    select *, row_number() over (partition by {{ keys | join(', ') }} order by load_ts desc) as _rn from {{ this }}) where _rn = 1
) cur on {% for k in keys %}cur.{{ k }} = src.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
where cur._latest is null or cur._latest <> src.hashdiff
{% endif %}
{% endmacro %}
