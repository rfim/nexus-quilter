-- FNZ platform: account-centric, keyed by PLT- reference
select concat('PLT-', substr(c.client_id, 2)) as platform_ref, c.client_id, c.national_insurance_no as ni_number,
       c.legal_entity, c.channel, count(a.account_id) as accounts
from {{ source('silver', 'dim_client') }} c
left join {{ source('silver', 'account') }} a on a.client_id = c.client_id
group by all
