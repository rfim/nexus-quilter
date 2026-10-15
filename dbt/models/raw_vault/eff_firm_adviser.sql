-- Effectivity satellite: when each firm-adviser relationship is in force (the adviser is the driving key)
with now_src as (
  select {{ hash_key(['firm_ref', 'adviser_id']) }} as hk_firm_adviser, {{ hash_key(['adviser_id']) }} as hk_adviser
  from {{ ref('stg_firm_adviser') }}
)
{% if is_incremental() %}
, latest as (
  select * from (select *, row_number() over (partition by hk_firm_adviser order by load_ts desc) as _rn from {{ this }}) where _rn = 1
)
select n.hk_firm_adviser, n.hk_adviser, true as is_current, {{ load_ts() }} as load_ts, 'qfp_crm' as record_source
from now_src n left join latest l on l.hk_firm_adviser = n.hk_firm_adviser
where l.hk_firm_adviser is null or not l.is_current
union all
select l.hk_firm_adviser, l.hk_adviser, false, {{ load_ts() }}, 'qfp_crm'
from latest l where l.is_current and l.hk_firm_adviser not in (select hk_firm_adviser from now_src)
{% else %}
select hk_firm_adviser, hk_adviser, true as is_current, {{ load_ts() }} as load_ts, 'qfp_crm' as record_source from now_src
{% endif %}
