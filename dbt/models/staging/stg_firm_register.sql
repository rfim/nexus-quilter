-- The firm register (a seed); on day 2 Harbour and Vale moves up to Gold
select firm_ref, firm_name,
       case when {{ var('load_day') }} >= 2 and firm_ref = 'FRM-03' then 'Gold' else tier end as tier, region
from {{ ref('firm_register') }}
