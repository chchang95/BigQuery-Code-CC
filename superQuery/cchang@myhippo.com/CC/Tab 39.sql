with r as(
  select policy_number, quote_premium_total
  from dw_prod.dim_renewals
)
,fpu as(
  select 
    fpu.policy_id, concat(fpu.payment_method, ' ', fpu.payment_frequency) as payment_method
  from dw_prod.fct_policy_updates fpu
  where 1=1
    and fpu.is_update_most_recent = true
)
,eps_upd_to_use as(
  select eps.policy_id, max(eps.date_snapshot) as date_snapshot
  from dw_prod_extracts.ext_policy_snapshots eps
  group by 1
)
,eps as(
  select eps.policy_id, (eps.written_base + eps.written_total_optionals + eps.written_policy_fee) AS written_total, eps.earned_total, eps.status, eps.date_cancellation, eps.cancellation_reason1 as r, eps.is_cancellation_flat
  from dw_prod_extracts.ext_policy_snapshots eps
    join eps_upd_to_use upd on eps.policy_id = upd.policy_id
  where 1=1
    and eps.date_snapshot = upd.date_snapshot
)
,org as(
  select 
    org.organization_id as org_id
    , org.name as org_name
    , parent_org.name as parent_org_name
    , parent_org.is_producer
  from dw_prod.dim_organizations org
    join (select * from dw_prod.dim_organizations where root_organization_id = organization_id) parent_org 
      on org.root_organization_id = parent_org.organization_id
)
, quotes as(
  select 
    policy_id
    , max(hippo_home_care_opt_in) as hippo_home_care_opt_in
  from dw_prod.dim_quotes
  where
    policy_id IS NOT NULL
    AND hippo_home_care_opt_in IS TRUE
  group by 1 
)
,t1 as(
  select 
    p.policy_number
    , p.policy_id
    , p.policy_group_number
    , p.date_bound
    , p.date_effective
    , p.date_activation_update_made
    , CASE WHEN date_effective >= date_activation_update_made THEN date_effective ELSE date_activation_update_made END AS date_target
    , p.timestamp_renewal_offered
    , p.timestamp_non_renewal_notice
    , p.carrier
    , eps.earned_total
    , eps.written_total
    , eps.status
    , eps.date_cancellation
    , eps.r
    , fpu.payment_method
    , org.org_name
    , org.parent_org_name
    , CASE  WHEN p.channel IN ('Online', 'Agent') THEN 'Direct'
            ELSE p.channel END AS channel
    , p.street
    , p.city
    , p.state
    , p.zip_code
    , p.product
    , p.renewal_number
    , eps.is_cancellation_flat as t1_is_cancellation_flat
    , q.hippo_home_care_opt_in as t1_hhc
  from dw_prod.dim_policies p
    join eps on p.policy_id = eps.policy_id
    join fpu on p.policy_id = fpu.policy_id
    left join org on p.attributed_organization_id = org.org_id
    left join quotes q on p.policy_id = q.policy_id
  where 1=1
    and p.renewal_number = 0
    and p.is_rewritten = false
)
,t2 as(
  select 
    p.policy_number
    , p.policy_id
    , p.policy_group_number
    , p.date_bound
    , p.date_effective
    , p.date_activation_update_made
    , CASE WHEN date_effective >= date_activation_update_made THEN date_effective ELSE date_activation_update_made END AS date_target
    , p.timestamp_renewal_offered
    , p.timestamp_non_renewal_notice
    , r.quote_premium_total
    , eps.status
    , eps.date_cancellation
    , eps.earned_total
    , eps.written_total
    , p.renewal_number
    , eps.is_cancellation_flat as t2_is_cancellation_flat
    , q.hippo_home_care_opt_in as t2_hhc
  from dw_prod.dim_policies p
    join eps on p.policy_id = eps.policy_id
    join r on p.policy_number = r.policy_number
    left join quotes q on p.policy_id = q.policy_id
  where 1=1
    and p.is_rewritten = false
    and p.renewal_number = 1
)
,t3 as(
  select 
    p.policy_number
    , p.policy_id
    , p.policy_group_number
    , p.date_bound
    , p.date_effective
    , p.date_activation_update_made
    , CASE WHEN date_effective >= date_activation_update_made THEN date_effective ELSE date_activation_update_made END AS date_target
    , p.timestamp_renewal_offered
    , p.timestamp_non_renewal_notice
    , r.quote_premium_total
    , eps.status
    , eps.earned_total
    , eps.written_total
    , eps.date_cancellation
    , p.renewal_number
    , eps.is_cancellation_flat as t3_is_cancellation_flat
    , q.hippo_home_care_opt_in as t3_hhc
  from dw_prod.dim_policies p
    join eps on p.policy_id = eps.policy_id
    join r on p.policy_number = r.policy_number
    left join quotes q on p.policy_id = q.policy_id
  where 1=1
    and p.is_rewritten = false
    and p.renewal_number = 2
)
,t4 as(
  select 
    p.policy_number
    , p.policy_id
    , p.policy_group_number
    , p.date_bound
    , p.date_effective
    , p.date_activation_update_made
    , CASE WHEN date_effective >= date_activation_update_made THEN date_effective ELSE date_activation_update_made END AS date_target
    , p.timestamp_renewal_offered
    , p.timestamp_non_renewal_notice
    , r.quote_premium_total
    , eps.status
    , eps.earned_total
    , eps.written_total
    , eps.date_cancellation
    , p.renewal_number
    , eps.is_cancellation_flat as t4_is_cancellation_flat
    , q.hippo_home_care_opt_in as t4_hhc
  from dw_prod.dim_policies p
    join eps on p.policy_id = eps.policy_id
    join r on p.policy_number = r.policy_number
    left join quotes q on p.policy_id = q.policy_id
  where 1=1
    and p.is_rewritten = false
    and p.renewal_number = 3
)
,t5 as(
  select 
    p.policy_number
    , p.policy_id
    , p.policy_group_number
    , p.date_bound
    , p.date_effective
    , p.date_activation_update_made
    , CASE WHEN date_effective >= date_activation_update_made THEN date_effective ELSE date_activation_update_made END AS date_target
    , p.timestamp_renewal_offered
    , p.timestamp_non_renewal_notice
    , r.quote_premium_total
    , eps.status
    , eps.earned_total
    , eps.written_total
    , eps.date_cancellation
    , p.renewal_number
    , eps.is_cancellation_flat as t5_is_cancellation_flat
    , q.hippo_home_care_opt_in as t5_hhc
  from dw_prod.dim_policies p
    join eps on p.policy_id = eps.policy_id
    join r on p.policy_number = r.policy_number
    left join quotes q on p.policy_id = q.policy_id
  where 1=1
    and p.is_rewritten = false
    and p.renewal_number = 4
)
,t6 as(
  select 
    p.policy_number
    , p.policy_id
    , p.policy_group_number
    , p.date_bound
    , p.date_effective
    , p.date_activation_update_made
    , CASE WHEN date_effective >= date_activation_update_made THEN date_effective ELSE date_activation_update_made END AS date_target
    , p.timestamp_renewal_offered
    , p.timestamp_non_renewal_notice
    , r.quote_premium_total
    , eps.status
    , eps.earned_total
    , eps.written_total
    , eps.date_cancellation
    , p.renewal_number
    , eps.is_cancellation_flat as t6_is_cancellation_flat
    , q.hippo_home_care_opt_in as t6_hhc
  from dw_prod.dim_policies p
    join eps on p.policy_id = eps.policy_id
    join r on p.policy_number = r.policy_number
    left join quotes q on p.policy_id = q.policy_id
  where 1=1
    and p.is_rewritten = false
    and p.renewal_number = 5
)
,agg as(
  select 
    t1.policy_number as t1_policy_number
    , t1.renewal_number as t1_renewal_number
    , t2.policy_number as t2_policy_number
    , t2.renewal_number as t2_renewal_number
    , t3.policy_number as t3_policy_number
    , t3.renewal_number as t3_renewal_number
    , t4.policy_number as t4_policy_number
    , t4.renewal_number as t4_renewal_number
    , t5.policy_number as t5_policy_number
    , t5.renewal_number as t5_renewal_number
    , t6.policy_number as t6_policy_number
    , t6.renewal_number as t6_renewal_number
    , t1.date_bound as t1_policy_date_bound
    , t1.date_effective as t1_date_effective
    , t2.date_effective as t2_date_effective
    , t3.date_effective as t3_date_effective
    , t4.date_effective as t4_date_effective
    , t5.date_effective as t5_date_effective
    , t6.date_effective as t6_date_effective
    , t1.date_target as t1_date_target
    , t2.date_target as t2_date_target
    , t3.date_target as t3_date_target
    , t4.date_target as t4_date_target
    , t5.date_target as t5_date_target
    , t6.date_target as t6_date_target
    , t1.org_name
    , t1.parent_org_name
    , t1.channel
    , t1.carrier
    , t1.product
    , t1.street
    , t1.city
    , t1.state
    , t1.zip_code
    , t1.payment_method
    , t1_is_cancellation_flat
    , t2_is_cancellation_flat
    , t3_is_cancellation_flat
    , t4_is_cancellation_flat
    , t5_is_cancellation_flat
    , t6_is_cancellation_flat
    , t1_hhc
    , t2_hhc
    , t3_hhc
    , t4_hhc
    , t5_hhc
    , t6_hhc
    , CAST(t1.written_total AS FLOAT64) as t1_written_total
    , CAST(t2.written_total AS FLOAT64) as t2_written_total
    , CAST(t2.quote_premium_total AS FLOAT64) as t2_premium_offered
    , CAST(t2.quote_premium_total - t1.written_total AS FLOAT64) as t2_premium_increase_abs
    , CAST(t3.written_total AS FLOAT64) as t3_written_total
    , CAST(t3.quote_premium_total AS FLOAT64) as t3_premium_offered
    , CAST(t3.quote_premium_total - t2.written_total AS FLOAT64) as t3_premium_increase_abs
    , CAST(t4.written_total AS FLOAT64) as t4_written_total
    , CAST(t4.quote_premium_total AS FLOAT64) as t4_premium_offered
    , CAST(t4.quote_premium_total - t3.written_total AS FLOAT64) as t4_premium_increase_abs
    , CAST(t5.written_total AS FLOAT64) as t5_written_total
    , CAST(t5.quote_premium_total AS FLOAT64) as t5_premium_offered
    , CAST(t5.quote_premium_total - t4.written_total AS FLOAT64) as t5_premium_increase_abs
    , CAST(t6.written_total AS FLOAT64) as t6_written_total
    , CAST(t6.quote_premium_total AS FLOAT64) as t6_premium_offered
    , CAST(t6.quote_premium_total - t5.written_total AS FLOAT64) as t6_premium_increase_abs
    , CAST(case when t1.written_total is null or t1.written_total = 0 then 0 else 1.0 * (t2.quote_premium_total - t1.written_total) / t1.written_total end  AS FLOAT64) as t2_premium_increase_pct
    , CAST(case when t2.written_total is null or t2.written_total = 0 then 0 else 1.0 * (t3.quote_premium_total - t2.written_total) / t2.written_total end  AS FLOAT64) as t3_premium_increase_pct
    , CAST(case when t3.written_total is null or t3.written_total = 0 then 0 else 1.0 * (t4.quote_premium_total - t3.written_total) / t3.written_total end  AS FLOAT64) as t4_premium_increase_pct
    , CAST(case when t4.written_total is null or t4.written_total = 0 then 0 else 1.0 * (t5.quote_premium_total - t4.written_total) / t4.written_total end  AS FLOAT64) as t5_premium_increase_pct
    , CAST(case when t5.written_total is null or t5.written_total = 0 then 0 else 1.0 * (t6.quote_premium_total - t5.written_total) / t5.written_total end  AS FLOAT64) as t6_premium_increase_pct
    , 1 as num_bound
    , CAST(case when t1.date_activation_update_made is not null then 1 else 0 end  AS FLOAT64)as t1_num_activated
    , CAST(case when t2.date_activation_update_made is not null then 1 else 0 end  AS FLOAT64)as t2_num_activated
    , CAST(case when t3.date_activation_update_made is not null then 1 else 0 end  AS FLOAT64)as t3_num_activated
    , CAST(case when t4.date_activation_update_made is not null then 1 else 0 end  AS FLOAT64)as t4_num_activated
    , CAST(case when t5.date_activation_update_made is not null then 1 else 0 end  AS FLOAT64)as t5_num_activated
    , CAST(case when t6.date_activation_update_made is not null then 1 else 0 end  AS FLOAT64)as t6_num_activated
    , case when t1.status = 'expired' 
        or (t1.status = 'terminated' and t1.date_cancellation > date_add(t1.date_effective, interval 90 day))
        or (t1.status = 'active' and t1.date_effective < date_add(current_date, interval -90 day))
        then 1 else 0 end as num_completed_uw
    , CAST(case when t1.timestamp_renewal_offered is not null then 1 else 0 end  AS FLOAT64)as t1_num_renewal_offered
    , CAST(case when t2.timestamp_renewal_offered is not null then 1 else 0 end  AS FLOAT64)as t2_num_renewal_offered
    , CAST(case when t3.timestamp_renewal_offered is not null then 1 else 0 end  AS FLOAT64)as t3_num_renewal_offered
    , CAST(case when t4.timestamp_renewal_offered is not null then 1 else 0 end  AS FLOAT64)as t4_num_renewal_offered       
    , CAST(case when t5.timestamp_renewal_offered is not null then 1 else 0 end  AS FLOAT64)as t5_num_renewal_offered  
    , CAST(case when t6.timestamp_renewal_offered is not null then 1 else 0 end  AS FLOAT64)as t6_num_renewal_offered  
    , CAST(case when t1.timestamp_renewal_offered is not null and t2.date_activation_update_made is not null AND (t2_is_cancellation_flat is false OR t2_is_cancellation_flat is NULL)
        then 1 else 0 end  AS FLOAT64)as t1_num_renewed 
    , CAST(case when t2.timestamp_renewal_offered is not null and t3.date_activation_update_made is not null AND (t3_is_cancellation_flat is false OR t3_is_cancellation_flat is NULL)
        then 1 else 0 end  AS FLOAT64)as t2_num_renewed 
    , CAST(case when t3.timestamp_renewal_offered is not null and t4.date_activation_update_made is not null AND (t4_is_cancellation_flat is false OR t4_is_cancellation_flat is NULL) 
        then 1 else 0 end  AS FLOAT64)as t3_num_renewed 
    , CAST(case when t4.timestamp_renewal_offered is not null and t5.date_activation_update_made is not null AND (t5_is_cancellation_flat is false OR t5_is_cancellation_flat is NULL) 
        then 1 else 0 end  AS FLOAT64)as t4_num_renewed
    , CAST(case when t5.timestamp_renewal_offered is not null and t6.date_activation_update_made is not null AND (t6_is_cancellation_flat is false OR t6_is_cancellation_flat is NULL) 
        then 1 else 0 end  AS FLOAT64)as t5_num_renewed 
, case when t1.r is null then null else
      case when t1.r like '%insured_request%' then 'insured_request' else
        case when t1.r like '%terms_acceptance%' then 'terms_acceptance' else
          case when t1.r like '%company%' or t1.r like '%void%' or t1.r like '%rescission%' then 'company_request' else
            case when t1.r like '%free_form%' then 'unknown' else
              case when t1.r like '%previous_term_cancelled%' then 'previous_term_cancelled' else
                case when t1.r like '%risk%' or t1.r like '%failure%' or t1.r like '%claim%' or t1.r like '%hazard%' then 'underwriting' else
                  case when t1.r like '%payment%' and t1.r not like '%escrow%' then 'cc_non_payment' else
                    case when t1.r like '%payment%' and t1.r  like '%escrow%' then 'escrow_non_payment' else 'other'
                    end
                  end
                end
              end
            end
          end
        end
      end 
    end as cancel_reason
  from t1
    left join t2 on t1.policy_group_number = t2.policy_group_number
      and t1.timestamp_renewal_offered is not null 
    left join t3 on t1.policy_group_number = t3.policy_group_number
      and t1.timestamp_renewal_offered is not null 
    left join t4 on t1.policy_group_number = t4.policy_group_number
      and t1.timestamp_renewal_offered is not null 
    left join t5 on t1.policy_group_number = t5.policy_group_number
      and t1.timestamp_renewal_offered is not null 
    left join t6 on t1.policy_group_number = t6.policy_group_number
      and t1.timestamp_renewal_offered is not null 
  where 1=1 
),

final_tbl AS (select 
  agg.*,
  case
    when (t1_hhc IS NOT NULL OR t2_hhc IS NOT NULL OR t3_hhc IS NOT NULL OR t4_hhc IS NOT NULL OR t5_hhc IS NOT NULL OR t6_hhc IS NOT NULL)  
        THEN 'true' ELSE 'false'
    end as is_hhc
from 
  agg
where 
  t1_date_effective <= CURRENT_DATE())
  select * from final_tbl
  
  
  