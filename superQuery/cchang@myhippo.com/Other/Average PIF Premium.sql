select 
-- eps.state,
date_snapshot
-- case when eps.renewal_number = 0 then 'new' else 'renewal' end as tenure,
-- channel, 
,sum(written_base + written_total_optionals)/ sum(written_exposure) as avg_WP
,count(*) as count
from dw_prod_extracts.ext_policy_snapshots eps
left join dw_prod.dim_policies using(policy_id)
where status = 'active'
and date_snapshot between '2019-05-31' and '2020-05-31'
and extract(day from DATEADD(day, 1, date_snapshot)) = 1
-- and date_policy_effective >= '2020-07-31'
-- and eps.product <> 'HO5'
-- and eps.renewal_number > 0
-- and channel
-- and eps.product = 'HO3'
and eps.carrier = 'canopius'
and eps.state = 'ca'
group by 1
order by 1