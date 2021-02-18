select state,
-- , carrier, date_snapshot
-- , org_id, organization_name, root_organization_name,
-- sum(case when renewal_number > 0 then 1 else 0 end) as renewal_count,
-- sum(case when renewal_number = 0 then 1 else 0 end) as new_business_count,
count(eps.policy_id) as total_PIF_count,
sum(coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0)) as total_TIV
from dw_prod_extracts.ext_policy_snapshots eps
-- left join (select policy_id, policy_number, channel, attributed_organization_id
    -- , case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on eps.policy_id = dp.policy_id
-- left join (select organization_id, organization_name, root_organization_name, from dw_prod.dim_organization_mappings) org_table on dp.org_id = org_table.organization_id
-- left join (select date, last_day_of_month from dw_prod.utils_dates where date = date(last_day_of_month)) ud on eps.date_snapshot = date(ud.last_day_of_month)
where 1=1
and date_snapshot = '2020-12-31'
-- and ud.date is not null
-- and carrier = 'spinnaker'
-- and product <> 'ho5'
and status = 'active'
and state in ('CT','MD','WA')
group by 1
order by 3,1,2