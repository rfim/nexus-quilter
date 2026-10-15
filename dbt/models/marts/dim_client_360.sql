-- Kimball, in gold: one row per client across the three businesses, latest description from each source
with latest as (
  {% for s in ['sat_client_platform', 'sat_client_advice', 'sat_client_cheviot'] %}
  select '{{ s }}' as sat, * except (_rn) from (
    select hk_client, load_ts, to_json(named_struct(
      {% if s == 'sat_client_platform' %}'legal_entity', legal_entity, 'channel', channel{% elif s == 'sat_client_advice' %}'risk_profile', risk_profile, 'adviser_id', adviser_id{% else %}'mandate', mandate, 'portfolio_manager', portfolio_manager{% endif %})) as attrs,
      row_number() over (partition by hk_client order by load_ts desc) as _rn
    from {{ ref(s) }}) where _rn = 1
  {% if not loop.last %}union all{% endif %}
  {% endfor %}
)
select h.hk_client,
       get_json_object(max(case when sat = 'sat_client_platform' then attrs end), '$.legal_entity') as legal_entity,
       get_json_object(max(case when sat = 'sat_client_platform' then attrs end), '$.channel') as channel,
       get_json_object(max(case when sat = 'sat_client_advice' then attrs end), '$.risk_profile') as risk_profile,
       get_json_object(max(case when sat = 'sat_client_advice' then attrs end), '$.adviser_id') as adviser_id,
       get_json_object(max(case when sat = 'sat_client_cheviot' then attrs end), '$.mandate') as cheviot_mandate,
       count(distinct sat) as businesses
from {{ ref('hub_client') }} h join latest l on l.hk_client = h.hk_client
group by h.hk_client
