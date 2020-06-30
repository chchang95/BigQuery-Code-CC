with premium as (
select 
state
-- ,carrier
-- ,product
-- ,extract(year from date_report_period_start) as accident_year
-- ,date_report_period_start as accident_month_begin
-- ,reinsurance_treaty_accounting as accounting_treaty
-- ,org_id as organization_id
-- ,case when renewal_number = 0 then "New" else "Renewal" end as tenure
,date_knowledge
,sum(written_total) as written_total
,sum(earned_total) as earned_total
,sum(earned_exposure) as earned_exposure
from dw_prod_extracts.ext_policy_monthly_premiums mon
left join (select policy_id, date_snapshot, coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0) as TIV
          from dw_prod_extracts.ext_policy_snapshots) eps on mon.policy_id = eps.policy_id and mon.date_report_period_end = eps.date_snapshot
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
where date_knowledge <= @as_of
and carrier <> 'Canopius'
and product <> 'HO5'
group by 1,2
)
select 
state
,date_knowledge
,case
    when lag(written_total, 1) over (partition by state order by date_knowledge) is null
    then written_total
    else written_total -
    lag(written_total, 1) over (partition by state order by date_knowledge)
end as incremental_written_total
,case
    when lag(earned_total, 1) over (partition by state order by date_knowledge) is null
    then earned_total
    else earned_total -
    lag(earned_total, 1) over (partition by state order by date_knowledge)
end as incremental_earned_total
,case
    when lag(earned_exposure, 1) over (partition by state order by date_knowledge) is null
    then earned_exposure
    else earned_exposure -
    lag(earned_exposure, 1) over (partition by state order by date_knowledge)
end as incremental_earned_exposure
from premium
order by 1,2