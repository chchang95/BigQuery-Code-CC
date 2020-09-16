select state, 
sum(written_base + written_total_optionals)/ sum(written_exposure)
from dw_prod_extracts.ext_policy_snapshots
where status = 'active'
and date_snapshot = '2020-08-31'
and date_policy_effective >= '2020-07-31'
and product <> 'HO5'
-- and product = 'HO5'
group by 1
order by 1