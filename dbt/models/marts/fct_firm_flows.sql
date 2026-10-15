-- Net flows per firm and quarter, through each adviser's current firm and the firm's current tier
with eff as (
  select * from (select *, row_number() over (partition by hk_firm_adviser order by load_ts desc) as _rn
                 from {{ ref('eff_firm_adviser') }}) where _rn = 1 and is_current
),
tier as (
  select * from (select *, row_number() over (partition by hk_firm order by load_ts desc) as _rn
                 from {{ ref('sat_firm') }}) where _rn = 1
),
flows as (
  select * from (select *, row_number() over (partition by hk_client, month order by load_ts desc) as _rn
                 from {{ ref('sat_client_flows') }}) where _rn = 1
)
select t.firm_name, t.tier, count(distinct eff.hk_adviser) as advisers,
       round(sum(case when f.month >= date '2026-04-01' and f.month < date '2026-07-01' then f.net_flows_gbp end), 0) as net_flows_q2,
       round(sum(case when f.month >= date '2026-07-01' then f.net_flows_gbp end), 0) as net_flows_q3
from eff
join {{ ref('link_firm_adviser') }} lfa on lfa.hk_firm_adviser = eff.hk_firm_adviser
join tier t on t.hk_firm = lfa.hk_firm
join {{ ref('link_client_adviser') }} lca on lca.hk_adviser = eff.hk_adviser
join flows f on f.hk_client = lca.hk_client
group by t.firm_name, t.tier
