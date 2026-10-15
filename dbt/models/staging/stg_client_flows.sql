-- Monthly net flows per client, from the certified fact
select p.ni_number, date_trunc('MONTH', t.trade_date) as month, round(sum(t.amount_gbp), 2) as net_flows_gbp
from {{ source('gold', 'fct_platform_transaction') }} t
join {{ source('silver', 'account') }} a on a.account_id = t.account_id
join {{ ref('stg_platform_client') }} p on p.client_id = a.client_id
where t.transaction_type in ('contribution', 'withdrawal', 'fee')
group by all
