-- Which firm each adviser works for; on day 2 ADV-007 moves from Larkfield Wealth to Millbrook Partners
select adviser_id,
       case when {{ var('load_day') }} >= 2 and adviser_id = 'ADV-007' then 'FRM-05'
            else concat('FRM-0', cast(pmod(cast(substr(adviser_id, 5) as int), 8) + 1 as string)) end as firm_ref
from (select distinct adviser_id from {{ ref('stg_advice_crm') }})
