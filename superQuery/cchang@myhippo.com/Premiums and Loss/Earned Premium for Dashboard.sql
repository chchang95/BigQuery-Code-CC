select lower(state) as state
,lower(carrier) as carrier
,lower(product) as product
,extract(year from date_report_period_start) as accident_year
,date_report_period_start as accident_month_begin
,date_report_period_end as accident_month_end
,reinsurance_treaty_accounting as accounting_treaty
,org_id as organization_id
,case when renewal_number = 0 then "New" else "Renewal" end as tenure
,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
,sum(written_exposure) as written_exposure
,sum(earned_exposure) as earned_exposure
,sum(written_exposure * TIV) as written_TIV
,sum(earned_exposure * TIV) as earned_TIV
from dw_prod_extracts.ext_policy_monthly_premiums mon
left join (select policy_id, date_snapshot, coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0) as TIV
          from dw_prod_extracts.ext_policy_snapshots) eps on mon.policy_id = eps.policy_id and mon.date_report_period_end = eps.date_snapshot
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
where date_knowledge = '2020-05-31'
and carrier <> 'Canopius'
-- and product <> 'HO5'
group by 1,2,3,4,5,6,7,8,9
limit 1000