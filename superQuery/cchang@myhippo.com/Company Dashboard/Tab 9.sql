select carrier, count(distinct policy_id) as PIF_count
,sum(written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line) as PIF
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, policy_number from dw_prod.dim_policies) dp USING(policy_id)
left join dw_prod.fct_premium_updates fpu on eps.latest_policy_update_id = fpu.policy_update_id
where date_snapshot = '2020-09-30'
and status = 'active'
-- and carrier = 'spinnaker'
group by 1