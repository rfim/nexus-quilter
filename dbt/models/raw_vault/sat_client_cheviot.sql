{{ sat(['hk_client'], ['mandate', 'portfolio_manager'],
       '(select ' ~ hash_key(['ni_number']) ~ ' as hk_client, * from ' ~ ref('stg_cheviot') ~ ')', 'cheviot_dims') }}
