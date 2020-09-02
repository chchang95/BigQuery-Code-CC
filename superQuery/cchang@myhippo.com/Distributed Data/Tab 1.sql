-- select state, sum(written), sum(earned)
-- from dw_staging_extracts.ext_actuarial_monthly_loss_ratios_premium
-- left join dw_prod.dim_policies using(policy_id)
-- where date_knowledge = '2020-08-31'
-- -- and reinsurance_treaty = 'Topa'
-- and carrier <> 'Canopius'
-- group by 1


-- select sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
-- ,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
-- from dw_prod_extracts.ext_policy_monthly_premiums epud
-- where date_knowledge = '2020-08-31'
-- and reinsurance_treaty_accounting = 'Topa'


select * from dw_prod_extracts.ext_actuarial_monthly_loss_ratios_loss
where policy_id = 236923
-- and date_bordereau = '2020-08-31'