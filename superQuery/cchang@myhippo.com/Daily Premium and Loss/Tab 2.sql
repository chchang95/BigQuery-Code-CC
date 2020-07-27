select state
,date_calendar_month_accounting_basis as accounting_month
,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
from dw_prod_extracts.ext_today_knowledge_policy_monthly_premiums mon
where date_knowledge = '2020-07-27'
and carrier <> 'Canopius'
and product <> 'HO5'
group by 1,2
order by 1,2