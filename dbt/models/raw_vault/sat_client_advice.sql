{{ sat(['hk_client'], ['risk_profile', 'adviser_id'],
       '(select ' ~ hash_key(["replace(ni_number, ' ', '')"]) ~ ' as hk_client, * from ' ~ ref('stg_advice_crm') ~ ')', 'qfp_crm') }}
