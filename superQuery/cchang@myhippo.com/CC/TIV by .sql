select
-- , carrier,
state,
-- property_data_address_zip,
date_snapshot,
date_trunc(date_policy_effective,MONTH) as policy_eff_month,
channel,
case when renewal_number > 0 then 'renewal' else 'new' end as tenure,
-- , org_id, organization_name, root_organization_name,
-- sum(case when renewal_number > 0 then 1 else 0 end) as renewal_count,
-- sum(case when renewal_number = 0 then 1 else 0 end) as new_business_count,
-- sum(written_base + written_total_optionals) as total_WP,
count(eps.policy_id) as total_PIF_count,
sum(coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0)) as total_TIV
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, policy_number, channel, attributed_organization_id from dw_prod.dim_policies) dp on eps.policy_id = dp.policy_id
-- left join (select organization_id, organization_name, root_organization_name, from dw_prod.dim_organization_mappings) org_table on dp.org_id = org_table.organization_id
-- left join (select date, last_day_of_month from dw_prod.utils_dates where date = date(last_day_of_month)) ud on eps.date_snapshot = date(ud.last_day_of_month)
where 1=1
and date_snapshot >= '2018-01-01'
and extract(day from DATE_ADD(date_snapshot, interval 1 day)) = 1
-- and ud.date is not null
-- and carrier = 'spinnaker'
-- and product <> 'ho5'
and status = 'active'
group by 1,2,3,4
order by 1,2,3,4