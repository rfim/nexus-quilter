-- A move to another firm is a new link row; the old one stays as history
{{ link('hk_firm_adviser', {'hk_firm': 'firm_ref', 'hk_adviser': 'adviser_id'}, ref('stg_firm_adviser'), 'qfp_crm') }}
