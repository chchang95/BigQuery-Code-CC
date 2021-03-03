with quotes_supp as (
      select *
            ,o.*
            ,o.name as org_name
            ,dq.organization_id as org_id
            , channel
             ,case when policy_number is not null then 1 else 0 end as bound
             ,1 as quote_count
      from dw_prod.dim_quotes dq
            LEFT JOIN dw_prod.dim_organizations o on dq.organization_id = o.organization_id
            left join (select policy_number, rewritten_policy_id from dw_prod.dim_policies) dp USING(policy_number)
      where 1=1
            and dp.rewritten_policy_id is null
            and date_quote_first_seen is not null
            and quote_premium_total is not null 
            and (is_intent is not false or is_bound = true)
)
,quotes_dedup as (
      select opportunity_policy_id,
            date_trunc(cast(date_quote_first_seen as DATE), WEEK) as quote_month,
            FIRST_VALUE(is_bound) OVER (PARTITION by opportunity_policy_id ,date_trunc(cast(date_quote_first_seen as DATE), WEEK) ORDER BY is_bound DESC, timestamp_created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS is_bound,
            FIRST_VALUE(quote_id) OVER (PARTITION by opportunity_policy_id ,date_trunc(cast(date_quote_first_seen as DATE), WEEK) ORDER BY is_bound DESC, timestamp_created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS quote_id,
            FIRST_VALUE(org_id) OVER (PARTITION by opportunity_policy_id ,date_trunc(cast(date_quote_first_seen as DATE), WEEK) ORDER BY is_bound DESC, timestamp_created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS organization_attribution
      FROM quotes_supp
)
, quotes_dedup_first_attribution AS ( -- using the first seen attribution to choose the attribution
      SELECT opportunity_policy_id, quote_id, organization_attribution, is_bound, quote_month, count(*) as cnt 
      from quotes_dedup
      GROUP BY opportunity_policy_id, quote_id, organization_attribution, is_bound, quote_month
      ORDER BY opportunity_policy_id
)
SELECT
    --   q.policy_number,
    --   q.policy_id,
    --   cast(q.date_quote_first_seen as DATE) as quote_date,
      date_trunc(cast(q.date_quote_first_seen as DATE), WEEK) as quote_week
      ,date_trunc(cast(q.date_quote_first_seen as DATE), MONTH) as quote_month
      ,qs.org_name as organization_name
      ,q.organization_id
      ,q.state
      ,q.product
    --   ,q.carrier
--       ,q.is_bulk_quoted
    --   ,case when q.year_built is null then 'Missing'
    --   when cast(q.year_built as numeric) >= 2000 then 'Post 2000' 
    --   when cast(q.year_built as numeric) > 1980 then 'Pre 2000' 
    --   else 'Pre 1980' end as year_built
      ,q.zip_code
      ,q.county
    --   ,q.roof_type as quote_roof_type
    --   ,q.prefilled_roof_type as prefilled_roof_type
    --   ,property_data_roof_type as policy_roof_type
    --   ,q.construction_type
    --   ,q.square_footage
    --   ,2020 - q.year_built + 1 as age_of_home
    --   ,q.year_built as year_home_built
    --   ,q.coverage_a
    --   ,q.deductible
    --   ,q.wind_deductible
    --   ,q.year_roof_built
    --   ,q.insurance_score
    --   ,q.non_cat_risk_score
    --   ,q.cat_risk_score
    --   ,q.non_cat_risk_class
    --   ,q.cat_risk_class
    --   ,q.ready_for_risk_score
    --   ,coalesce(q.non_cat_risk_class, 'not_applicable') as UW_Action
      ,case when q.non_cat_risk_class is null then 'not_applicable'
      when q.state = 'tx' and q.cat_risk_class = 'referral' then 'cat_referral'
      else q.non_cat_risk_class end as UW_Class_with_TX
    --   , case when q.non_cat_risk_class is null then 'not_applicable'
    --     when q.state = 'tx' and q.cat_risk_class = 'referral' and q.ready_for_risk_score = 'true' then 'cat_referral_saw_message'
    --     when q.state = 'tx' and q.cat_risk_class = 'referral' and q.ready_for_risk_score is null then 'cat_referral_no_message'
    --     when q.ready_for_risk_score is null and q.non_cat_risk_class = 'referral' then 'referral_no_message' 
    --     when q.ready_for_risk_score = 'true' and q.non_cat_risk_class = 'referral' then 'referral_saw_message'
    --     else q.non_cat_risk_class end as upd_non_cat_risk_class
    -- ,coalesce(zips.status,'Open') as tx_moratorium 
    --   ,case when coalesce(q.date_bound, cast(q.date_quote_first_seen as date)) <= '2020-04-29' then 'not_applicable'
    --   when q.non_cat_risk_class = 'exterior_inspection_required' or q.non_cat_risk_class = 'interior_inspection_required' or q.non_cat_risk_class = 'referral' then 'rocky'
    --   when q.non_cat_risk_class = 'no_action' then 'happy'
    --   else 'not_applicable' end as UW_Path
    --   ,q.date_bound
      ,date_trunc(cast(q.date_bound as DATE), WEEK) as bound_week
      ,date_trunc(cast(q.date_bound as DATE), MONTH) as bound_month
      ,q.channel
      ,SUM(CASE WHEN ddp.is_bound IS TRUE THEN 1 ELSE 0 END) AS bound_count
      ,COUNT(*) as quote_count
      ,sum(q.quote_premium_total) as total_quote_premium
      FROM quotes_dedup_first_attribution ddp
            LEFT JOIN dw_prod.dim_quotes q USING (quote_id)
            LEFT JOIN quotes_supp qs using (quote_id)
            LEFT JOIN dw_prod.dim_policies dp on (q.policy_number = dp.policy_number)
            left join (select policy_id, property_data_roof_type from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2020-12-08') ps on q.policy_id = ps.policy_id
            left join dw_prod.tx_moratorium_zips zips on safe_cast(q.zip_code as numeric) = safe_cast(zips.zip_code as numeric)
      where q.date_quote_first_seen >= '2020-01-01'
      and q.state = 'ca'
    --   and q.product <> 'ho5'
      and q.carrier <> 'canopius'
      group by 1,2,3,4,5,6,7,8,9,10,11,12
