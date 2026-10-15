-- Platform journey status events; on day 2 the platform corrects 25 transfer-out completions (3 days later)
with j as (
  select s.journey_id, p.ni_number, s.journey_type, cast(s.created_on as date) as created_on, cast(s.completed_on as date) as completed_on
  from {{ source('silver', 'service_journey') }} s join {{ ref('stg_platform_client') }} p on p.client_id = s.client_id
),
events as (
  select journey_id, ni_number, journey_type, 'requested' as status, created_on as event_date from j
  union all
  select journey_id, ni_number, journey_type, 'in_progress', date_add(created_on, 1) from j
  where completed_on is not null and completed_on > date_add(created_on, 1)
  union all
  select journey_id, ni_number, journey_type, 'completed',
         case when {{ var('load_day') }} >= 2 and journey_type = 'transfer_out' and pmod(hash(journey_id), 20) = 0
              then date_add(completed_on, 3) else completed_on end
  from j where completed_on is not null
)
select * from events
