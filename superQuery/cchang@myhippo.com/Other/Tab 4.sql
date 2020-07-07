    select 
        epud.policy_id
        ,lower(state) as state
        , lower(carrier) as carrier
        , lower(product) as product
        , extract(year from greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start)) as calendar_year
        , greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start) as date_accounting_start
        ,sum(written_total) as written_total
from dw_prod_extracts.ext_policy_update_monthly_premiums epud
        where date_knowledge = '2020-06-30'
        and carrier <> 'Canopius'
        -- and product <> 'HO5'
group by 1,2,3,4,5,6