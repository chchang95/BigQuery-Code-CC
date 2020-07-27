select state
-- , carrier
-- , product
-- , case when reinsurance_treaty = 'Spkr19_GAP' and date_report_period_start <= '2019-12-31' then 'Spkr19_GAP'
-- when reinsurance_treaty = 'Spkr19_GAP' and date_report_period_start >= '2020-01-01' then 'Spkr20_Classic'
-- else reinsurance_treaty end as accounting_treaty
-- ,case when reinsurance_treaty = 'Spkr20_Classic' then 'Spkr19_GAP' else reinsurance_treaty end as originating_treaty
-- , substr(policy_number,length(policy_number)-1,2) as term
, greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start) as date_accounting
,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
from dw_prod_extracts.ext_policy_update_monthly_premiums epud
left join (select policy_id, policy_number, reinsurance_treaty from dw_prod.dim_policies) dp on epud.policy_id = dp.policy_id
where date_knowledge = '2020-06-30'
and carrier <> 'Canopius'
and product <> 'HO5'
group by 1,2
order by 1,2