select 
        carrier
        , greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start) as date_accounting_start
        ,sum(written_total) as written_prem_x_ebsl
from dw_prod_extracts.ext_policy_update_monthly_premiums epud
where carrier = 'Canopius'
and date_knowledge = '2020-06-30'
group by 1,2
order by 1,2