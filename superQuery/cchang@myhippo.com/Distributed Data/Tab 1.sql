select sum(written), sum(earned)
from dw_staging_extracts.ext_actuarial_monthly_loss_ratios_premium
where date_knowledge = '2020-08-31'
and reinsurance_treaty ='Topa'
-- and state = 'CA'
-- written = 

-- select sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
-- ,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
-- from dw_prod_extracts.ext_policy_monthly_premiums epud
-- where date_knowledge = '2020-08-31'
-- and carrier <> 'Canopius'
-- and state = 'CA'