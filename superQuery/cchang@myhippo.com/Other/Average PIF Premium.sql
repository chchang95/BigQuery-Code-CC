select eps.state, case when eps.renewal_number = 0 then 'new' else 'renewal' end as tenure,
-- channel, 
sum(written_base + written_total_optionals)/ sum(written_exposure)
from dw_prod_extracts.ext_policy_snapshots eps
left join dw_prod.dim_policies using(policy_id)
where status = 'active'
and date_snapshot = '2020-08-31'
and date_policy_effective >= '2020-07-31'
-- and eps.product <> 'HO5'
-- and eps.renewal_number > 0
-- and channel
and eps.product = 'HO5'
group by 1,2
order by 1