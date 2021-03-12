with claims_supp as (
SELECT DISTINCT
    mon.*
--       , is_ebsl
    , case when cc.cat_ind is true then true
    when cc.cat_ind is false then false
    when peril = 'wind' or peril = 'hail' then true
    when cat_code is not null then true
        else 'N' end as CAT
    , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
    , case when cc.recoded_loss_event is null then 'NA' else cc.recoded_loss_event end as recoded_loss_event
      ,dp.org_id
      ,dp.channel
      , date_trunc(date_effective, MONTH) as term_effective_month
      , case when renewal_number = 0 then "New" else "Renewal" end as tenure
      , case when property_data_address_state = 'tx' and calculated_fields_cat_risk_class = 'referral' then 'cat referral' 
              when calculated_fields_non_cat_risk_class is null or date_effective <= '2020-05-01' then 'not_applicable'
              else calculated_fields_non_cat_risk_class end as rated_uw_action
    , loss_description
    , damage_description
  FROM dw_prod_extracts.ext_all_claims_combined mon
  left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id, channel from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  left join (select policy_id, calculated_fields_non_cat_risk_class, calculated_fields_cat_risk_class, renewal_number from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2021-02-28') eps on eps.policy_id = mon.policy_id
  left join (select claim_number, loss_description, damage_description from dw_prod.dim_claims) fc on mon.claim_number = fc.claim_number
  left join (select date, last_day_of_quarter from dw_prod.utils_dates where date = date(last_day_of_quarter)) ud on mon.date_knowledge = date(ud.last_day_of_quarter)
  left join dbt_actuaries.cat_coding_w_loss_20210228_new cc on (case when tbl_source = 'topa_tpa_claims' then ltrim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
  WHERE 1=1
--   and date_knowledge = (select max(date_knowledge) from dw_prod_extracts.ext_all_claims_combined)
and date_knowledge = '2021-02-28'
  and carrier <> 'canopius'
  and is_ebsl is false
--   and last_day_of_quarter is not null
),
claims as (
  select
    claims_supp.policy_id,
    date_knowledge,
    sum(total_incurred) as total_incurred,
    sum(
      case
        when CAT is false then total_incurred
        else 0
      end
    ) as non_cat_incurred,
    sum(
      case
        when CAT is true then 0
        when total_incurred >= 100000 then 100000
        else total_incurred
      end
    ) as capped_non_cat_incurred,
    sum(
      case
        when CAT is true then 0
        when total_incurred >= 100000 then total_incurred - 100000
        else 0
      end
    ) as excess_non_cat_incurred,
    sum(
      case
        when CAT is true then total_incurred
        else 0
      end
    ) as cat_incurred,
    sum(
      case
        when claim_closed_no_total_payment is false then 1
        else 0
      end
    ) as reported_total_claim_count_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and CAT is false then 1
        else 0
      end
    ) as reported_non_cat_claim_count_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and CAT is false and peril_group = 'Water' then 1
        else 0
      end
    ) as reported_non_cat_water_cc_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and CAT is false and peril_group = 'Fire_Smoke' then 1
        else 0
      end
    ) as reported_non_cat_fire_smoke_cc_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and CAT is false and peril_group = 'Theft' then 1
        else 0
      end
    ) as reported_non_cat_theft_cc_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and CAT is false and peril_group = 'Liablity' then 1
        else 0
      end
    ) as reported_non_cat_liability_cc_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and CAT is false and peril_group = 'Other' then 1
        else 0
      end
    ) as reported_non_cat_other_cc_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and CAT is true then 1
        else 0
      end
    ) as reported_cat_claim_count_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and date_closed is not null then 1
        else 0
      end
    ) as closed_total_claim_count_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and date_closed is not null
        and CAT is false then 1
        else 0
      end
    ) as closed_non_cat_claim_count_x_cnp,
    sum(
      case
        when claim_closed_no_total_payment is false
        and date_closed is not null
        and CAT is true then 1
        else 0
      end
    ) as closed_cat_claim_count_x_cnp,
    sum(
      case
        when CAT is false
        and peril_group = 'Water' then total_incurred
        else 0
      end
    ) as non_cat_incurred_water
  from
    claims_supp
    where
    1 = 1
  group by
    1,2
),
msg as (
  select
    Policy_id,
    case
      when(message_code = 'A114') then 1
      else 0
    end as non_owner_occupied,
    case
      when(message_code = 'A113') then 1
      else 0
    end as vacancy,
    case
      when(message_code = 'F101') then 1
      else 0
    end as foreclosure,
    case
      when(message_code ='A205') then 1
      else 0
    end as owner_llc,
    case
      when(message_code = 'A212') then 1
      else 0
    end as commercial,case 
        when(message_code = 'A211') then 1
      else 0
    end as mobile_home,case
      when(message_code = 'A213') then 1
      else 0
    end as agricultural
  from
    dw_prod.dim_transunion_rvp_messages
),
msg_distinct as(
  select
    Policy_id,
    case when sum(non_owner_occupied)>0 then 1 else 0 end as non_owner_occupied,
    case when sum(vacancy)>0 then 1 else 0 end as vacancy,
    case when sum(foreclosure)>0 then 1 else 0 end as foreclosure,
    case when sum(owner_llc)>0 then 1 else 0 end as owner_llc,
    case when sum(commercial)>0 then 1 else 0 end as commercial,
    case when sum(mobile_home)>0 then 1 else 0 end as mobile_home,
    case when sum(agricultural)>0 then 1 else 0 end as agricultural
  from
    msg
  group by
    Policy_id
),

msg_pivot as(
  select
    Policy_id,
    non_owner_occupied + vacancy + foreclosure + owner_llc + commercial + mobile_home + agricultural as count_RVP_violation,
    case when non_owner_occupied>0 then 'Yes' else 'No' end as non_owner_occupied,
    case when vacancy>0 then 'Yes' else 'No' end as vacancy,
    case when foreclosure>0 then 'Yes' else 'No' end as foreclosure,
    case when owner_llc>0 then 'Yes' else 'No' end as owner_llc,
    case when commercial>0 then 'Yes' else 'No' end as commercial,
    case when mobile_home>0 then 'Yes' else 'No' end as mobile_home,
    case when agricultural>0 then 'Yes' else 'No' end as agricultural
  from
    msg_distinct
),

prior_claim as (
select distinct
    policy_id,
    json_extract_scalar(lh, '$.id') as prior_claim_id
    ,json_extract_scalar(lh, '$.category') as prior_claim_peril
    ,json_extract_scalar(lh, '$.catastrophe') as prior_claim_cat
from `datateam-248616.dw_prod_extracts.ext_policy_snapshots`
left join unnest(json_extract_array(loss_history_claims)) as lh
where date_snapshot = '2020-12-31'),

pivot_prior_claim as(
select policy_id,
    sum(case when prior_claim_cat ='true' then 1 else 0 end) as prior_cat_claim,
    sum(case when prior_claim_cat ='false' then 1 else 0 end) as prior_non_cat_claim,
    sum(case when prior_claim_cat ='false' and prior_claim_peril = 'water' then 1 else 0 end) as prior_noncat_water_claim,
    sum(case when prior_claim_cat ='false' and prior_claim_peril in ('wind', 'hail') then 1 else 0 end) as prior_noncat_wind_hail_claim,
    sum(case when prior_claim_cat ='false' and prior_claim_peril in ('fire', 'smoke') then 1 else 0 end) as prior_noncat_fire_smoke_claim,
    sum(case when prior_claim_cat ='false' and prior_claim_peril = 'theft' then 1 else 0 end) as prior_noncat_theft_claim,
    sum(case when prior_claim_cat ='false' and prior_claim_peril = 'liability' then 1 else 0 end) as prior_noncat_liability_claim,
    sum(case when prior_claim_cat ='false' and prior_claim_peril = 'ice' then 1 else 0 end) as prior_noncat_ice_claim,
    sum(case when prior_claim_cat ='false' and prior_claim_peril = 'other' then 1 else 0 end) as prior_noncat_other_claim,
    count(prior_claim_id) as prior_claims
    from (select * from prior_claim where prior_claim_id is not null)
    group by 1
), 

one_month as (
select s1.policy_id
    , s1.policy_number
    , s1.state
    , s1.property_data_address_zip
    , s1.date_policy_effective
    , s1.date_snapshot
	, c1.date_knowledge
	, s1.earned_exposure
	, s1.earned_base
	, s1.earned_policy_fee
	, (s1.earned_base + s1.earned_total_optionals - s1.earned_optionals_equipment_breakdown - s1.earned_optionals_service_line) as earned_premium_x_EBSL
	, c1.total_incurred
	, c1.non_cat_incurred
	, c1.cat_incurred
	, c1.reported_total_claim_count_x_cnp
	, c1.reported_non_cat_claim_count_x_cnp
	, c1.reported_cat_claim_count_x_cnp
	, c1.capped_non_cat_incurred
	, c1.excess_non_cat_incurred
	from dw_prod_extracts.ext_policy_snapshots s1
	left join claims c1 on s1.policy_id = c1.policy_id and date_add(s1.date_policy_effective, interval 1 month) = c1.date_knowledge
	where s1.date_snapshot = date_add(s1.date_policy_effective, interval 1 month)
)
, two_month as (
select s2.policy_id
    , s2.policy_number
    , s2.state
    , s2.property_data_address_zip
    , s2.date_policy_effective
    , s2.date_snapshot
	, c2.date_knowledge
	, s2.earned_exposure
	, s2.earned_base
	, s2.earned_policy_fee
	, (s2.earned_base + s2.earned_total_optionals - s2.earned_optionals_equipment_breakdown - s2.earned_optionals_service_line) as earned_premium_x_EBSL
	, c2.total_incurred
	, c2.non_cat_incurred
	, c2.cat_incurred
	, c2.reported_total_claim_count_x_cnp
	, c2.reported_non_cat_claim_count_x_cnp
	, c2.reported_cat_claim_count_x_cnp
	, c2.capped_non_cat_incurred
	, c2.excess_non_cat_incurred
	from dw_prod_extracts.ext_policy_snapshots s2
	left join claims c2 on s2.policy_id = c2.policy_id and date_add(s2.date_policy_effective, interval 2 month) = c2.date_knowledge
	where s2.date_snapshot = date_add(s2.date_policy_effective, interval 2 month)
),

early_term as (
    select one.policy_id
    , one.date_snapshot as one_month_snapshot
    , one.earned_exposure as one_month_earned_exposure
    , one.earned_policy_fee as one_month_earned_policy_fee
    , one.earned_premium_x_EBSL as one_month_earned_premium_x_EBSL
    , one.total_incurred as one_month_total_incurred
    , one.non_cat_incurred as one_month_non_cat_incurred
    , one.reported_total_claim_count_x_cnp as one_month_total_reported_claim_count_x_cnp
    , one.reported_non_cat_claim_count_x_cnp as one_month_non_cat_reported_claim_count_x_cnp
    , two.date_snapshot as two_month_snapshot
    , two.earned_exposure as two_month_earned_exposure
    , two.earned_policy_fee as two_month_earned_policy_fee
    , two.earned_premium_x_EBSL as two_month_earned_premium_x_EBSL
    , two.total_incurred as two_month_total_incurred
    , two.non_cat_incurred as two_month_non_cat_incurred
    , two.reported_total_claim_count_x_cnp as two_month_total_reported_claim_count_x_cnp
    , two.reported_non_cat_claim_count_x_cnp as two_month_non_cat_reported_claim_count_x_cnp
    from one_month one left join two_month two on one.policy_id = two.policy_id
),
aggregate as(
select distinct
  eps.policy_id,
  eps.policy_number,
  cast(right(eps.policy_number,2) as int64) + 1 as policy_term,
  CONCAT(left(eps.policy_number,LENGTH(eps.policy_number)-2),'00') as original_policy_number,
  date_trunc(date_policy_effective, Week) as policy_inception_Week,
  date_policy_effective,
  carrier,
  state,
  product,
  status,
  case
    when renewal_number = 0 then "New"
    else "Renewal"
  end as tenure,
  new_purchase_indicator,
  DATE_DIFF(CURRENT_DATE(),Purchase_Date, MONTH) AS Months_from_Purchase,
  Purchase_Price,
  Market_Value,
  Residents_Per_Bedroom,
  case when count_RVP_violation is null and new_purchase_indicator = 'old_purchase' then 'Null - Old Purchase'
    when count_RVP_violation is null and new_purchase_indicator = 'new_purchase' then 'Null - New Purchase'
    when count_RVP_violation is null and new_purchase_indicator = 'old_purchase' then 'Null - Unknown Purchase Date'
    else CAST(count_RVP_violation AS string) end as count_RVP_violation,
  case when non_owner_occupied is null and new_purchase_indicator = 'old_purchase' then 'Null - Old Purchase'
    when non_owner_occupied is null and new_purchase_indicator = 'new_purchase' then 'Null - New Purchase'
    when non_owner_occupied is null and new_purchase_indicator = 'old_purchase' then 'Null - Unknown Purchase Date'
    else non_owner_occupied end as non_owner_occupied,
  case when vacancy is null and new_purchase_indicator = 'old_purchase' then 'Null - Old Purchase'
    when vacancy is null and new_purchase_indicator = 'new_purchase' then 'Null - New Purchase'
    when vacancy is null and new_purchase_indicator = 'old_purchase' then 'Null - Unknown Purchase Date'
    else vacancy end as vacancy,
  case when foreclosure is null and new_purchase_indicator = 'old_purchase' then 'Null - Old Purchase'
    when foreclosure is null and new_purchase_indicator = 'new_purchase' then 'Null - New Purchase'
    when foreclosure is null and new_purchase_indicator = 'old_purchase' then 'Null - Unknown Purchase Date'
    else foreclosure end as foreclosure,
  case when owner_llc is null and new_purchase_indicator = 'old_purchase' then 'Null - Old Purchase'
    when owner_llc is null and new_purchase_indicator = 'new_purchase' then 'Null - New Purchase'
    when owner_llc is null and new_purchase_indicator = 'old_purchase' then 'Null - Unknown Purchase Date'
    else owner_llc end as owner_llc,
  case when commercial is null and new_purchase_indicator = 'old_purchase' then 'Null - Old Purchase'
    when commercial is null and new_purchase_indicator = 'new_purchase' then 'Null - New Purchase'
    when commercial is null and new_purchase_indicator = 'old_purchase' then 'Null - Unknown Purchase Date'
    else commercial end as commercial,
  case when mobile_home is null and new_purchase_indicator = 'old_purchase' then 'Null - Old Purchase'
    when mobile_home is null and new_purchase_indicator = 'new_purchase' then 'Null - New Purchase'
    when mobile_home is null and new_purchase_indicator = 'old_purchase' then 'Null - Unknown Purchase Date'
    else mobile_home end as mobile_home,
  case when agricultural is null and new_purchase_indicator = 'old_purchase' then 'Null - Old Purchase'
    when agricultural is null and new_purchase_indicator = 'new_purchase' then 'Null - New Purchase'
    when agricultural is null and new_purchase_indicator = 'old_purchase' then 'Null - Unknown Purchase Date'
    else agricultural end as agricultural,
  prior_cat_claim,
  prior_non_cat_claim,
  prior_noncat_water_claim,
  prior_noncat_wind_hail_claim,
  prior_noncat_fire_smoke_claim,
  prior_noncat_theft_claim,
  prior_noncat_liability_claim,
  prior_noncat_ice_claim,
  prior_noncat_other_claim,
  prior_claims,
  coverage_a,
  round(Market_Value/coverage_a,2) as mv_to_cov_a,
  written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line as written_prem_x_ebsl_x_pol_fee,
  earned_base + earned_total_optionals - earned_optionals_equipment_breakdown - earned_optionals_service_line as earned_prem_x_ebsl_x_pol_fee,
  written_base,
  earned_base,
  written_exposure,
  earned_exposure,
  written_policy_fee,
  earned_policy_fee,
  coalesce(total_incurred, 0) as total_incurred,
  coalesce(non_cat_incurred, 0) as non_cat_incurred,
  coalesce(capped_non_cat_incurred, 0) as capped_non_cat_incurred,
  coalesce(excess_non_cat_incurred, 0) as excess_non_cat_incurred,
  coalesce(cat_incurred, 0) as cat_incurred,
  coalesce(reported_total_claim_count_x_cnp, 0) as reported_total_claim_count_x_cnp,
  coalesce(reported_non_cat_claim_count_x_cnp, 0) as reported_non_cat_claim_count_x_cnp,
  coalesce(reported_non_cat_water_cc_x_cnp, 0) as reported_non_cat_water_cc_x_cnp,
  coalesce(reported_non_cat_fire_smoke_cc_x_cnp, 0) as reported_non_cat_fire_smoke_cc_x_cnp,
  coalesce(reported_non_cat_theft_cc_x_cnp, 0) as reported_non_cat_theft_cc_x_cnp,
  coalesce(reported_non_cat_liability_cc_x_cnp, 0) as reported_non_cat_liability_cc_x_cnp,
  coalesce(reported_non_cat_other_cc_x_cnp, 0) as reported_non_cat_other_cc_x_cnp,
  coalesce(reported_cat_claim_count_x_cnp, 0) as reported_cat_claim_count_x_cnp,
  coalesce(closed_total_claim_count_x_cnp, 0) as closed_total_claim_count_x_cnp,
  coalesce(closed_non_cat_claim_count_x_cnp, 0) as closed_non_cat_claim_count_x_cnp,
  coalesce(closed_cat_claim_count_x_cnp, 0) as closed_cat_claim_count_x_cnp,
  one_month_snapshot,
  one_month_earned_exposure,
  one_month_earned_policy_fee,
  one_month_earned_premium_x_EBSL,
  one_month_total_incurred,
  one_month_non_cat_incurred,
  one_month_total_reported_claim_count_x_cnp,
  one_month_non_cat_reported_claim_count_x_cnp,
  two_month_snapshot,
  two_month_earned_exposure,
  two_month_earned_policy_fee,
  two_month_earned_premium_x_EBSL,
  two_month_total_incurred,
  two_month_non_cat_incurred,
  two_month_total_reported_claim_count_x_cnp,
  two_month_non_cat_reported_claim_count_x_cnp
from
  dw_prod_extracts.ext_policy_snapshots eps
  left join (
    select
      policy_id,
      case when property_purchase_age is null then 'unknown'
        when property_purchase_age= 'over_1_year_ago' then 'old_purchase'
       else 'new_purchase' end as new_purchase_indicator
    from
      dw_prod.dim_policies
  ) dp on eps.policy_id = dp.policy_id
  left join claims c on eps.policy_id = c.policy_id and eps.date_snapshot = c.date_knowledge
  left join msg_pivot m on m.policy_id = eps.policy_id
  left join dbt_actuaries.verisk_homeowners ho on ho.policy_id = eps.policy_id
  left join pivot_prior_claim pr on pr.policy_id = eps.policy_id
  left join early_term et on et.policy_id = eps.policy_id
where
  1 = 1
--   and date_snapshot = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and date_snapshot = '2021-02-28'
  and carrier <> 'canopius'
--   and status <> 'pending_active'
  )
--   select *
--   from aggregate
--     left join(
--     select distinct policy_number as original_policy_number_1
--     ,coalesce(non_cat_risk_class, 'not_applicable') as UW_Action
--     from dw_prod.dim_quotes) q on q.original_policy_number_1 = aggregate.original_policy_number
select sum(total_incurred), sum(non_cat_incurred), sum(written_prem_x_ebsl_x_pol_fee), sum(earned_prem_x_ebsl_x_pol_fee) from aggregate
where carrier = 'spinnaker'
    