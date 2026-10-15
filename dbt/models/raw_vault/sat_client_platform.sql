{{ sat(['hk_client'], ['legal_entity', 'channel', 'accounts'],
       '(select ' ~ hash_key(['ni_number']) ~ ' as hk_client, * from ' ~ ref('stg_platform_client') ~ ')', 'fnz_platform') }}
