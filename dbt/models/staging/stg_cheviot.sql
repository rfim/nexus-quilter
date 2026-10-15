-- Quilter Cheviot: discretionary portfolios for high-net-worth clients, keyed by CHV- reference
select concat('CHV-', lpad(cast(pmod(hash(concat(client_id, 'c')), 90000) as string), 5, '0')) as cheviot_ref,
       national_insurance_no as ni_number,
       concat('PM ', cast(pmod(hash(client_id), 12) + 1 as string)) as portfolio_manager,
       case when pmod(hash(client_id), 3) = 0 then 'Advisory' else 'Discretionary' end as mandate
from {{ source('silver', 'dim_client') }} where legal_entity = 'hnw'
