select state, sum(written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line)/ COUNT(*)
from dw_prod_extracts.ext_policy_snapshots
where status = 'active'
and date_snapshot = '2020-08-31'
and product = 'HO5'
group by 1
order by 1