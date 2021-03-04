with policies as (
select eps.policy_id
, eps.policy_number
, eps.product, carrier
, case when state = 'ca' or state = 'tx' then state else 'other' end as state_grouped
, state
, date_trunc(date_policy_effective, MONTH) as term_policy_effective_month
, date_trunc(date_first_effective, MONTH) as orig_policy_effective_month
, case when date_policy_effective >= '2020-08-01' then 'Post August' else 'Pre August' end as policy_cohort
-- , round(Pure_premium_adjusted,0) as noncat_uw_score
, property_data_address_zip as zip
, case when property_data_year_built is null then 'Missing'
        when cast(property_data_year_built as numeric) >= 2000 then 'Post 2000' 
        when cast(property_data_year_built as numeric) >= 1980 then 'Pre 2000' 
        else 'Pre 1980' end as year_built
, channel
, org_id
,do.name as org_name
, case when renewal_number = 0 then "New" else "Renewal" end as tenure
, case when zips.Status = 'Shut Off' then 'shut_zip' else 'open' end as zip_status
, UW_Model_Score as uw_model_score
from dw_prod_extracts.ext_policy_snapshots eps
-- left join dbt_cchin.new_model_policies_scored_20201130 using(policy_id)
left join (select policy_id, case when channel is null then 'Online' else channel end as channel, coalesce(organization_id,0) as org_id from dw_prod.dim_policies) using(policy_id)
left join dbt_cchin.ca_moratorium_zips_august_2020 zips on safe_cast(eps.property_data_address_zip as numeric) = cast(zips.Zips_to_Shut_Off as numeric) and eps.product = lower(zips.Product)
left join (select policy_id, date_first_effective from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) using (policy_id)
left join dw_prod.dim_organizations do on org_id = do.organization_id
left join dbt_actuaries.uw_model_v2_scores_20210131 using (policy_id)
where date_snapshot = '2021-01-31'
)
,claims_supp as (
select mon.*
, case when cc.cat_ind is true then 'Y'
    when cc.cat_ind is false then 'N'
    when peril = 'wind' or peril = 'hail' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
    , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
    , case when cc.recoded_loss_event is null then 'NA' else cc.recoded_loss_event end as recoded_loss_event
    , string_field_1 as new_peril_group
    , last_day(date_trunc(date_effective,MONTH),MONTH) as term_policy_effective_month
    , last_day(date_trunc(date_first_effective, MONTH),MONTH) as policy_effective_month
    , case when renewal_number > 0 then 'renewal' else 'new' end as tenure
,coalesce(recoverable_depreciation,0) as total_recoverable_depreciation
,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) - coalesce(recoveries,0) as loss_incurred_calc
,coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) as expense_incurred_calc
,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) + coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) - coalesce(recoveries,0) as total_incurred_calc
from dbt_actuaries.ext_all_claims_combined_20210131_with_salesforce mon
left join dbt_actuaries.cat_coding_w_loss_20210131 cc on (case when tbl_source = 'topa_tpa_claims' then trim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
left join (select policy_id, renewal_number from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2021-01-31') using(policy_id)
left join dbt_cchin.claims_mappings_202012 map on mon.peril = map.string_field_0
left join (select policy_id, date_first_effective from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) using (policy_id)
)
, loss as (
SELECT
    policy_id,
    -- mon.date_knowledge,
    accident_month,
    reinsurance_treaty,
    sum(case when CAT = 'Y' then total_incurred_calc else 0 end) as cat_incurred,
    sum(case when CAT = 'N' then total_incurred_calc else 0 end) as noncat_incurred,
    sum(case when CAT = 'Y' then 0 when total_incurred_calc > 100000 then 100000 else total_incurred_calc end) as noncat_capped_100k,
    sum(case when CAT = 'Y' then 0 when total_incurred_calc > 100000 then total_incurred_calc - 100000 else 0 end) as noncat_excess_100k,
    sum(total_incurred_calc) as total_incurred,
    sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as total_reported_claim_count_x_CNP,
    sum(case when claim_closed_no_total_payment is true then 0 when CAT = 'Y' then 1 else 0 end) as cat_reported_claim_count_x_CNP,
    sum(case when claim_closed_no_total_payment is true then 0 when CAT = 'N' then 1 else 0 end) as noncat_reported_claim_count_x_CNP,
    sum(case when new_peril_group = 'water' and CAT = 'N' then total_incurred_calc else 0 end) as noncat_water_incurred,
    sum(case when claim_closed_no_total_payment is true then 0 when new_peril_group = 'water' and CAT = 'N' then 1 else 0 end) as noncat_water_reported_claim_count_x_CNP
FROM
    claims_supp mon
  where is_ebsl is false
  and carrier <> 'canopius'
  and date_knowledge = '2021-01-31'
  group by 1,2,3
 )
, premium as (
    select 
        epud.policy_id
        , date_calendar_month_accounting_basis as date_accounting_start
        , reinsurance_treaty_property_accounting
        ,sum(written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl_x_fees
        ,sum(earned_base + earned_total_optionals - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl_x_fees
        ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure
        ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_policy_fee) as earned_policy_fee
        ,sum((written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line) * coalesce(on_level_factor,1)) as on_leveled_written_prem_x_ebsl_x_fees
        ,sum((earned_base + earned_total_optionals - earned_optionals_equipment_breakdown - earned_optionals_service_line) * coalesce(on_level_factor,1)) as on_leveled_earned_prem_x_ebsl_x_fees
from dw_prod_extracts.ext_policy_monthly_premiums epud
    left join (select policy_id, on_level_factor from dw_staging_extracts.ext_policy_snapshots where date_snapshot = '2021-01-31') seps on seps.policy_id = epud.policy_id
        where 1=1
        and date_knowledge = '2021-01-31'
        and carrier <> 'canopius'
group by 1,2,3
)
, aggregated as (
    select 
-- coalesce(date_knowledge,date_accounting_start) as calendar_month,
coalesce(accident_month,date_accounting_start) as accident_month,
coalesce(p.policy_id, l.policy_id) as policy_id,
coalesce(p.reinsurance_treaty_property_accounting, l.reinsurance_treaty) as reinsurance_treaty

,sum(coalesce(cat_incurred,0)) as cat_incurred
,sum(coalesce(noncat_incurred,0)) as noncat_incurred
,sum(coalesce(total_incurred,0)) as total_incurred
,sum(coalesce(noncat_capped_100k,0)) as noncat_capped_100k
,sum(coalesce(noncat_excess_100k,0)) as noncat_excess_100k
,sum(coalesce(total_reported_claim_count_x_CNP,0)) as total_reported_claim_count_x_CNP
,sum(coalesce(cat_reported_claim_count_x_CNP,0)) as cat_reported_claim_count_x_CNP
,sum(coalesce(noncat_reported_claim_count_x_CNP,0)) as noncat_reported_claim_count_x_CNP
,sum(coalesce(noncat_water_incurred,0)) as noncat_water_incurred
,sum(coalesce(noncat_water_reported_claim_count_x_CNP,0)) as noncat_water_reported_claim_count_x_CNP

,sum(coalesce(written_prem_x_ebsl_x_fees,0)) as written_prem_x_ebsl_x_fees
,sum(coalesce(earned_prem_x_ebsl_x_fees,0)) as earned_prem_x_ebsl_x_fees
,sum(coalesce(written_exposure,0)) as written_exposure
,sum(coalesce(earned_exposure,0)) as earned_exposure
,sum(coalesce(written_policy_fee,0)) as written_policy_fee
,sum(coalesce(earned_policy_fee,0)) as earned_policy_fee
,sum(coalesce(on_leveled_written_prem_x_ebsl_x_fees,0)) as on_leveled_written_prem_x_ebsl_x_fees
,sum(coalesce(on_leveled_earned_prem_x_ebsl_x_fees,0)) as on_leveled_earned_prem_x_ebsl_x_fees

from premium p
full join loss l ON
-- p.date_accounting_start = l.date_knowledge AND
p.date_accounting_start = l.accident_month AND
p.policy_id = l.policy_id AND
p.reinsurance_treaty_property_accounting = l.reinsurance_treaty
group by 1,2,3
)
, final as (
select 
policy_id, policy_number,
extract(year from accident_month) as accident_year,
-- calendar_month,
accident_month
-- case when accident_month < '2020-01-01' then '2019'
--  when accident_month >= '2020-08-01' then 'Post August 2020'
--  when accident_month >= '2020-01-01' and accident_month < '2020-08-01' then 'Pre August 2020'
--  else 'blank'
-- end as accident_cohort
,org_id
,org_name
,product
,state
,channel
,zip
,uw_model_score
-- ,zip_status
,year_built
,tenure
-- ,policy_cohort
,term_policy_effective_month
,orig_policy_effective_month

,sum(coalesce(cat_incurred,0)) as cat_incurred
,sum(coalesce(noncat_incurred,0)) as noncat_incurred
,sum(coalesce(total_incurred,0)) as total_incurred
,sum(coalesce(noncat_capped_100k,0)) as noncat_capped_100k
,sum(coalesce(noncat_excess_100k,0)) as noncat_excess_100k
,sum(coalesce(total_reported_claim_count_x_CNP,0)) as total_reported_claim_count_x_CNP
,sum(coalesce(cat_reported_claim_count_x_CNP,0)) as cat_reported_claim_count_x_CNP
,sum(coalesce(noncat_reported_claim_count_x_CNP,0)) as noncat_reported_claim_count_x_CNP
,sum(coalesce(noncat_water_incurred,0)) as noncat_water_incurred
,sum(coalesce(noncat_water_reported_claim_count_x_CNP,0)) as noncat_water_reported_claim_count_x_CNP

,sum(coalesce(written_prem_x_ebsl_x_fees,0)) as written_prem_x_ebsl_x_fees
,sum(coalesce(earned_prem_x_ebsl_x_fees,0)) as earned_prem_x_ebsl_x_fees
,sum(coalesce(a.written_exposure,0)) as written_exposure
,sum(coalesce(a.earned_exposure,0)) as earned_exposure
,sum(coalesce(a.written_policy_fee,0)) as written_policy_fee
,sum(coalesce(a.earned_policy_fee,0)) as earned_policy_fee
-- ,sum(coalesce(on_leveled_written_prem_x_ebsl_x_fees,0)) as on_leveled_written_prem_x_ebsl_x_fees
-- ,sum(coalesce(on_leveled_earned_prem_x_ebsl_x_fees,0)) as on_leveled_earned_prem_x_ebsl_x_fees

from aggregated a
left join policies using(policy_id) 
where 1=1
and state = 'ca'
and product <> 'ho5'
-- and accident_month >= '2019-01-01'
-- and policy_id = 2051353
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
select 
*
-- sum(total_incurred), sum(cat_incurred), sum(written_prem_x_ebsl_x_fees)
from final
where accident_month is not null
-- and calendar_month <> accident_month
order by 2