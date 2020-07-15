    select 
        -- epud.policy_id
        -- ,lower(state) as state
        carrier
        -- , lower(product) as product
        -- , extract(year from greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start)) as calendar_year
        , greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start) as date_accounting_start
        -- , date_sub(date_add(greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start), INTERVAL 1 MONTH), INTERVAL 1 DAY) as date_accounting_end
        ,sum(written_total) as written_prem_x_ebsl
from dw_prod_extracts.ext_policy_update_monthly_premiums epud
where carrier = 'Canopius'
group by 1,2