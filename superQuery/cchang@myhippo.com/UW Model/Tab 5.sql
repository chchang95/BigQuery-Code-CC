with bind_updates as
(
  select
   a.policy_id,
   calculated_fields_non_cat_risk_score, 
   calculated_fields_cat_risk_score, 
   calculated_fields_non_cat_risk_class, 
   calculated_fields_cat_risk_class,
   case 
    when state = 'TX' and calculated_fields_cat_risk_class = "referral" then "referral" 
    else calculated_fields_non_cat_risk_class
   end as calculated_fields_non_cat_risk_with_cat_risk_class
  from
    dw_prod.fct_policy_updates a
    left join dw_prod.dim_policies b on a.policy_id = b.policy_id
    left join dw_prod.dim_policy_histories c on b.policy_history_id = c.policy_history_id
  where
    1=1
    and update_action = 'bind'
)
,
quoted_premium AS
( 
    select  
        policy_id,
        quote_premium_base,
        quote_premium_optionals,
        quote_premium_fees,
        quote_rater_version,
        coverage_a,
        year_built,
        deductible
    from 
        dw_prod.dim_quotes 
    where 
        is_bound = true
)
, 

policy_channels AS 
(
    SELECT 
        *,
        o.name AS organization_name,
        CASE 
            WHEN o.is_b2b2c IS true 
             THEN o.name
             ELSE NULL
        END AS partners,
        CASE 
            WHEN producer_id IS NOT NULL 
                AND attributed_organization_id IS NOT NULL 
                AND (o.is_b2b2c IS false OR o.is_b2b2c IS NULL)
            THEN producer_id
            ELSE NULL 
        END AS producers,
        CASE WHEN attributed_agent_bound_by IS NOT NULL 
                  AND (producer_id IS NULL)
                  AND (attributed_organization_id IS NULL OR attributed_organization_id != 213 OR attributed_organization_id != 10)
             THEN attributed_agent_bound_by
             ELSE NULL
        END AS agents
    FROM 
        dw_prod.dim_policies p
        LEFT JOIN dw_prod.dim_organizations o on p.organization_id = o.organization_id
    WHERE 1=1
    AND renewal_number = 0 
    AND rewritten_policy_id IS NULL
)

,


converted_policies as 
(
    select 
        a.policy_number,
        date_trunc(a.date_bound, MONTH) AS Bind_Month, 
        date_trunc(a.date_bound, WEEK) AS Bind_Week, 
        cast(a.date_quote_first_seen as date) as quote_date,
        CAST(DATETIME(a.timestamp_bound, "America/Los_Angeles") AS DATE) AS Bind_Date,
        DATE_TRUNC(a.date_effective, MONTH) AS Effective_Month, 
        DATE_TRUNC(a.date_effective, WEEK) AS Effective_Week, 
        a.date_effective AS Effective_Date, 
        a.state,
        a.city,
        a.zip_code,
        a.product,
--         year_built,
        case when year_built is null then 'Missing'
              when cast(year_built as numeric) >= 2000 then 'Post 2000' 
              when cast(year_built as numeric) >= 1980 then 'Post 1980' 
              else 'Pre 1980' end as year_built,
        CASE 
            WHEN partners is not null then 'Partner' 
            WHEN producers is not null then 'Producer' 
            WHEN agents is not null then 'Agent'
            ELSE 'Online'
        END AS channel,
        CASE WHEN a.property_purchase_age = 'last_12_months'THEN 'New Customer'ELSE 'Switcher'END AS customer_type,
        o.name as organization_name, 
        o2.name as root_organization_name,
        lower(a.attribution_source) as attribution_source,
        lower(coalesce(o2.name, a.attribution_source)) AS attribution_source_combined,
        a.carrier,
        a.is_rewritten,
        f.calculated_fields_cat_risk_class,
        f.calculated_fields_non_cat_risk_class,
        f.calculated_fields_non_cat_risk_with_cat_risk_class,
        f.calculated_fields_non_cat_risk_score,
        pgl.visible_damage,
        d.quote_rater_version,
        d.coverage_a,
        d.deductible,
        a.date_activation,
        case 
        	when current_date() >= a.date_expires then 'Inactive'
        	when e.is_status_active = false then 'Inactive'
        	else 'Active'
        end AS is_policy_in_effect,
        COUNT(distinct a.policy_number) AS num_bound_policies,
        cast(SUM(d.quote_premium_base + d.quote_premium_optionals + d.quote_premium_fees) as FLOAT64) AS tot_bound_premium,
        cast(SUM(d.quote_premium_base) as FLOAT64) AS base_bound_premium,
   FROM 
    dw_prod.dim_policies a
    LEFT JOIN dw_prod.dim_policy_histories b on a.policy_history_id = b.policy_history_id
    left join policy_channels c on a.policy_id = c.policy_id
    left join quoted_premium d on a.policy_id = d.policy_id
    left join dw_prod.dim_organizations o on a.attributed_organization_id = o.organization_id
    left join dw_prod.dim_organizations o2 on a.attributed_root_organization_id = o2.organization_id
    left join dw_prod.fct_policy_updates e on a.policy_id = e.policy_id
    left join bind_updates f on a.policy_id = f.policy_id
    left join (with leads as (
select cast(id as string) as id,json_extract_scalar(transaction,'$.property_data.visible_damage') as visible_damage from postgres_public.leads pgl)
, quotes as (select cast(id as string) as id,json_extract_scalar(coalesce(transaction, policy_info),'$.property_data.visible_damage') as visible_damage from postgres_public.policies pgl)
select * from leads union all select * from quotes) as pgl on coalesce(a.lead_id,cast(a.policy_id as string)) = pgl.id
   WHERE 
    1=1
    AND a.renewal_number = 0 
    AND a.rewritten_policy_id IS NULL
    and e.is_update_most_recent = true 
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
)

SELECT 
    policy_number,
    attribution_source_combined,
    attribution_source,
    quote_date,
    date_trunc(quote_date, WEEK) as quote_week,
    case when extract(DAYOFWEEK from quote_date) = 1 then 'Sunday'
      when extract(DAYOFWEEK from quote_date) = 2 then 'Monday'
      when extract(DAYOFWEEK from quote_date) = 3 then 'Tuesday'
      when extract(DAYOFWEEK from quote_date) = 4 then 'Wednesday'
      when extract(DAYOFWEEK from quote_date) = 5 then 'Thursday'
      when extract(DAYOFWEEK from quote_date) = 6 then 'Friday'
      when extract(DAYOFWEEK from quote_date) = 7 then 'Saturday'
      end as quote_weekday,
    state,
    channel as Channel,
    carrier,
    city,
    zip_code,
    customer_type,
--     year_built,
    Bind_Date AS day_of_sale,
    case when extract(DAYOFWEEK from Bind_Date) = 1 then 'Sunday'
      when extract(DAYOFWEEK from Bind_Date) = 2 then 'Monday'
      when extract(DAYOFWEEK from Bind_Date) = 3 then 'Tuesday'
      when extract(DAYOFWEEK from Bind_Date) = 4 then 'Wednesday'
      when extract(DAYOFWEEK from Bind_Date) = 5 then 'Thursday'
      when extract(DAYOFWEEK from Bind_Date) = 6 then 'Friday'
      when extract(DAYOFWEEK from Bind_Date) = 7 then 'Saturday'
      end as bind_weekday,
    Bind_Week AS week_of_sale,
    Bind_Month AS month_of_sale, 
    organization_name,
    product,
    calculated_fields_non_cat_risk_score,
    case when bind_date <= '2020-04-29' then 'not_applicable' else coalesce(calculated_fields_non_cat_risk_class,'not_applicable') end as UW_Action,
    case  when bind_date <= '2020-04-29' then 'not_applicable'
    when calculated_fields_non_cat_risk_class = 'exterior_inspection_required' or calculated_fields_non_cat_risk_class = 'interior_inspection_required' or calculated_fields_non_cat_risk_class = 'referral' then 'rocky'
    when calculated_fields_non_cat_risk_class = 'no_action' then 'happy'
    else 'not_applicable' end as UW_Path,
    visible_damage,
    is_policy_in_effect,
--     Effective_Date,
--     date_activation,
--     quote_rater_version,
    coverage_a,
    deductible,
    SUM(num_bound_policies) tot_bound_pol,
    SUM(tot_bound_premium) AS tot_bound_premium
FROM 
    converted_policies
    where Bind_Date >= '2020-01-01'
    and product <> 'HO5'
    and carrier <> 'Canopius'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25