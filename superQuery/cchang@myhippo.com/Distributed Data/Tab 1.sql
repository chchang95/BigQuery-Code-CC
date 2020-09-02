select state, sum(written)
from dw_staging_extracts.ext_actuarial_monthly_loss_ratios_premium
where date_bordereau = '2020-08-31'
and carrier <> 'Canopius'
and state = 'CA'

-- select state, sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
-- from dw_prod_extracts.ext_policy_monthly_premiums epud
-- where date_knowledge = '2020-08-31'
-- and carrier <> 'Canopius'