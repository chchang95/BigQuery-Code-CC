select 
eps.state,
date_snapshot,
date_trunc(date_policy_effective, MONTH) as eff_month
-- case when eps.renewal_number = 0 then 'new' else 'renewal' end as tenure,
-- channel, 
,sum(written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line) as total_WP
,sum(written_exposure) as total_WE
,count(*) as active_count
from dw_prod_extracts.ext_policy_snapshots eps
left join dw_prod.dim_policies using(policy_id)
where status = 'active'
and date_snapshot between '2018-01-31' and '2020-09-30'
-- and date_snapshot = '2020-08-31'
and extract(day from DATE_ADD(date_snapshot, interval 1 day)) = 1
-- and date_policy_effective >= '2020-07-31'
-- and eps.product <> 'HO5'
-- and eps.renewal_number > 0
-- and channel
-- and eps.product = 'ho3'
and eps.carrier = 'topa'
-- and eps.state = 'ca'
group by 1,2,3
order by 1,2