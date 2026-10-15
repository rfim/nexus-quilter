{{ sat(['hk_firm'], ['firm_name', 'tier', 'region'],
       '(select ' ~ hash_key(['firm_ref']) ~ ' as hk_firm, * from ' ~ ref('stg_firm_register') ~ ')', 'firm_register') }}
