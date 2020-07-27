select state, carrier,
sum(case when renewal_number > 0 then 1 else 0 end) as renewal_count,
sum(case when renewal_number = 0 then 1 else 0 end) as new_business_count,
count(policy_id) as total_PIF_count
from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-06-30'
and carrier <> 'Canopius'
and status = 'active'
group by 1,2
order by 2,1