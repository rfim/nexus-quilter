-- One client across the platform, the adviser CRM and Quilter Cheviot, on the standardised NI number
{{ hub('hk_client', 'ni_number_bk', [
     (ref('stg_platform_client'), "replace(ni_number, ' ', '')", 'fnz_platform'),
     (ref('stg_advice_crm'), "replace(ni_number, ' ', '')", 'qfp_crm'),
     (ref('stg_cheviot'), "replace(ni_number, ' ', '')", 'cheviot_dims')]) }}
