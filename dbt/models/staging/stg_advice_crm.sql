-- Quilter Financial Planning adviser CRM: NI numbers typed by hand; annual reviews change risk profiles on day 2
with base as (
  select concat('QFP-', lpad(cast(pmod(hash(client_id), 900000) as string), 6, '0')) as crm_id,
         concat(lower(substr(national_insurance_no, 1, 2)), ' ', substr(national_insurance_no, 3)) as ni_number,
         concat('ADV-', lpad(cast(pmod(hash(client_id), 40) + 1 as string), 3, '0')) as adviser_id,
         element_at(array('Defensive', 'Cautious', 'Balanced', 'Growth', 'Adventurous'), pmod(hash(client_id), 5) + 1) as risk_profile
  from {{ source('silver', 'dim_client') }} where channel = 'Quilter adviser'
)
select crm_id, ni_number, adviser_id,
       case when {{ var('load_day') }} >= 2 and pmod(hash(crm_id), 8) = 0
            then element_at(array('Defensive', 'Cautious', 'Balanced', 'Growth', 'Adventurous'),
                            cast(pmod(array_position(array('Defensive', 'Cautious', 'Balanced', 'Growth', 'Adventurous'), risk_profile), 5) + 1 as int))
            else risk_profile end as risk_profile
from base
