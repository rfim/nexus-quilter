-- Multi-active: one description per client per month
{{ sat(['hk_client', 'month'], ['net_flows_gbp'],
       '(select ' ~ hash_key(['ni_number']) ~ ' as hk_client, * from ' ~ ref('stg_client_flows') ~ ')', 'fnz_platform') }}
