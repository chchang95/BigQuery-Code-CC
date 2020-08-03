select state, carrier, date_snapshot,
sum(case when renewal_number > 0 then 1 else 0 end) as renewal_count,
sum(case when renewal_number = 0 then 1 else 0 end) as new_business_count,
count(policy_id) as total_PIF_count,
sum(coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0)) as total_TIV
from dw_prod_extracts.ext_policy_snapshots eps
left join (select date, last_day_of_month from dw_prod.utils_dates where date = date(last_day_of_month)) ud on eps.date_snapshot = date(ud.last_day_of_month)
where 1=1
and date_snapshot > '2019-09-01'
and ud.date is not null
and carrier <> 'Canopius'
and product <> 'HO5'
and status = 'active'
group by 1,2,3
order by 3,1,2