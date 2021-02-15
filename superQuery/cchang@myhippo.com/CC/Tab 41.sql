with rvp as (
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

rvp_distinct as(
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
    rvp
  group by
    Policy_id
),

rvp_pivot as(
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
    rvp_distinct
),

x as (SELECT
  prem.date_knowledge,
  prem.policy_id,
  prem.date_report_period_start,
  prem.date_report_period_end,
  prem.product,
  prem.state,
  dp.channel,
  dp.organization_id,
  snap.property_data_address_county,
  snap.property_data_address_zip,
  prem.carrier,
  case when calculated_fields_cat_risk_class = 'referral' then 'cat referral' 
       when calculated_fields_non_cat_risk_class is null or date_effective <= '2020-05-01' then 'not_applicable'
       else calculated_fields_non_cat_risk_class
  end as rated_uw_action,
  prem.written_base,
  prem.earned_base,
  prem.earned_total_optionals - prem.earned_optionals_equipment_breakdown - prem.earned_optionals_service_line as earned_optexebsl,
  prem.earned_base + prem.earned_total_optionals - prem.earned_optionals_equipment_breakdown - prem.earned_optionals_service_line as earned_ex_ebsl,
  prem.earned_optionals_equipment_breakdown as earned_eb,
  prem.earned_optionals_service_line as earned_sl,
  prem.earned_exposure,
  snap.coverage_a,
  snap.coverage_b,
  snap.coverage_c,
  snap.coverage_d,
  snap.on_level_factor,
  prem.earned_policy_fee,
  case when snap.renewal_number = 0 then 'NB' else 'RB' end as NB_Ind
  ,prem.date_report_period_end as month_knowledge
  ,snap.region_code
  ,count_RVP_violation
  ,non_owner_occupied
  ,vacancy
  ,foreclosure
  ,owner_llc
  ,commercial
  ,mobile_home
  ,agricultural
  
FROM
  dw_prod_extracts.ext_policy_monthly_premiums prem
  join dw_prod_extracts.ext_policy_snapshots snap ON prem.date_knowledge = snap.date_snapshot AND prem.policy_id = snap.policy_id
  left join (select distinct policy_id, organization_id, channel from dw_prod.dim_policies) dp on prem.policy_id = dp.policy_id
  left join rvp_pivot rvpp on rvpp.Policy_id = prem.policy_id
  
WHERE
  date_knowledge = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
  AND prem.carrier <> 'canopius'
  AND prem.state = 'tx'
)

select 
NB_Ind
,state
,product
,carrier
,channel
,organization_id
,month_knowledge
,region_code
,property_data_address_county
,property_data_address_zip
,rated_uw_action
,count_RVP_violation
,non_owner_occupied
,vacancy
,foreclosure
,owner_llc
,commercial
,mobile_home
,agricultural
,count(distinct policy_id) as policy_count
,sum(earned_ex_ebsl) as earned_ex_ebsl
,sum(earned_base) as earned_base
,sum(earned_optexebsl) as earned_optexebsl
,sum(earned_exposure) as ee
,sum(earned_policy_fee) as earned_policy_fee
from x 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
