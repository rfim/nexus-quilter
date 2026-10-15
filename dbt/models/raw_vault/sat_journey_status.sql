-- Status history, insert-only: a correction lands beside the original, never over it
{{ sat(['hk_journey', 'status'], ['event_date', 'journey_type'],
       '(select ' ~ hash_key(['journey_id']) ~ ' as hk_journey, * from ' ~ ref('stg_journey_events') ~ ')', 'fnz_platform') }}
