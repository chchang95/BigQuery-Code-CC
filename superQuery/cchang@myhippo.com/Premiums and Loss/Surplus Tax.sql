select 
        carrier
        , case when update_action in ('terminate', 'revert_terminate', 'reinstate') then 'return' else 'new' end as trans_type
        , greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start) as date_accounting_start
        ,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
from dw_prod_extracts.ext_policy_update_monthly_premiums epud
where carrier = 'canopius'
and date_knowledge = '2020-09-30'
group by 1,2,3
order by 1,2,3