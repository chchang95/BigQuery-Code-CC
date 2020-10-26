select 
eps.state,
eps.product,
date_snapshot,
date_trunc(date_policy_effective, MONTH) as eff_month,
case when eps.renewal_number = 0 then 'new' else 'renewal' end as tenure,
channel, 
cast(eps.coverage_a as numeric) - mod(cast(eps.coverage_a as numeric),10000) as coverage_a_banded,
cast(eps.calculated_fields_age_of_home as numeric) - mod(cast(eps.calculated_fields_age_of_home as numeric),3) as age_of_home_banded
-- cast(eps.insurance_score as numeric) - mod(cast(eps.insurance_score as numeric),5)
,count(*) as active_count
from dw_prod_extracts.ext_policy_snapshots eps
left join dw_prod.dim_policies using(policy_id)
where status = 'active'
and date_snapshot between '2019-01-31' and '2020-09-30'
and extract(day from DATE_ADD(date_snapshot, interval 1 day)) = 1
group by 1,2,3,4,5,6,7,8
order by 1,2,3