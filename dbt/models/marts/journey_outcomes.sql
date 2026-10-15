-- Consumer Duty, aggregates only: groups under 10 journeys are dropped, so nobody downstream sees who is vulnerable
with st as (
  select * from (select *, row_number() over (partition by hk_journey, status order by load_ts desc) as _rn
                 from {{ ref('sat_journey_status') }}) where _rn = 1
),
j as (
  select r.hk_journey, r.journey_type, datediff(c.event_date, r.event_date) as days
  from st r join st c on c.hk_journey = r.hk_journey and c.status = 'completed'
  where r.status = 'requested'
)
select j.journey_type, case when v.vulnerable then 'vulnerable' else 'not vulnerable' end as client_group,
       count(*) as journeys, percentile_disc(0.5) within group (order by j.days) as median_days,
       percentile_disc(0.9) within group (order by j.days) as p90_days
from j
join {{ ref('link_client_journey') }} l on l.hk_journey = j.hk_journey
join (select * from (select *, row_number() over (partition by hk_client order by load_ts desc) as _rn
                     from {{ ref('sat_client_vulnerability') }}) where _rn = 1) v on v.hk_client = l.hk_client
group by all
having count(*) >= 10
