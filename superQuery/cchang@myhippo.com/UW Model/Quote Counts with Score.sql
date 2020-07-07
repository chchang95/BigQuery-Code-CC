with pg_quotes_supp as (
    with leads as (
        select cast(id as string)   as policy_number
             , least(cast(json_extract_scalar(transaction,'$.calculated_fields.age_of_home') as numeric),
                     77)                                                               as age_of_home
             , json_extract_scalar(transaction,'$.property_data.guard') as guard
            --  case
            --       when coalesce(json_extract_scalar(transaction,'$.property_data.guard'), 'false') = 'false' then 'No'
            --       else 'Yes' end                                                      as Guard
             , json_extract_scalar(transaction,'$.calculated_fields.age_of_insured')               as age_of_insured
             , json_extract_scalar(transaction,'$.calculated_fields.three_years_claims')           as Three_Years_Claims
             , json_extract_scalar(transaction,'$.property_data.number_of_stories')                as Number_Of_Stories
             , json_extract_scalar(transaction,'$.property_data.hoa_membership') as Hoa_Membership
             , JSON_EXTRACT(data,'$.promotional_score.report.insurance_score') as pg_insurance_score
            --  case
            --       when coalesce(json_extract_scalar(transaction,'$.property_data.hoa_membership'), 'false') = 'false' then 'No'
            --       else 'Yes' end                                                      as Hoa_Membership
             , cast('lead' as string)                                                                  as quote_type
        from postgres_public.leads a
                 left join (select lead_id, bound, status
                            from postgres_public.policies
                            where policy_number like '%-00'
                              and bound = true) b
                           on cast(a.id as string) = cast(b.lead_id as string)
        where 1 = 1
          and cast(initial_quote_date as date) >= '2019-10-20'
          and carrier <> 'canopius'
          and product not in ('ho5')
    )
       , quotes as (
select cast(id as string)   as policy_number
             , least(cast(json_extract_scalar(coalesce(policy_info,transaction),'$.calculated_fields.age_of_home') as numeric),
                     77)                                                               as age_of_home
             , json_extract_scalar(coalesce(policy_info,transaction),'$.property_data.guard') as guard
            --  case
            --       when coalesce(json_extract_scalar(coalesce(policy_info,transaction),'$.property_data.guard'), 'false') = 'false' then 'No'
            --       else 'Yes' end                                                      as Guard
             , json_extract_scalar(coalesce(policy_info,transaction),'$.calculated_fields.age_of_insured')               as age_of_insured
             , json_extract_scalar(coalesce(policy_info,transaction),'$.calculated_fields.three_years_claims')           as Three_Years_Claims
             , json_extract_scalar(coalesce(policy_info,transaction),'$.property_data.number_of_stories')                as Number_Of_Stories
             , json_extract_scalar(coalesce(policy_info,transaction),'$.property_data.hoa_membership') as hoa_membership
             , '0'                                         as pg_insurance_score
            --  case
            --       when coalesce(json_extract_scalar(coalesce(policy_info,transaction),'$.property_data.hoa_membership'), 'false') = 'false' then 'No'
            --       else 'Yes' end                                                      as Hoa_Membership
             , cast('quote' as string)                                                                  as quote_type
        from postgres_public.policies a
        where 1 = 1
          and cast(initial_quote_date as date) >= '2019-10-20'
          and carrier <> 'canopius'
          and product not in ('ho5')
    )
       , combined as (
        select *
        from leads
        union all
        select *
        from quotes
    )
    select *
    from combined
)
, quotes as (
select quote_id, policy_id, lead_id, product, carrier, state
,coverage_a 
,case when q.insurance_score is null and qs.quote_type = 'lead' then pg_insurance_score else insurance_score end as insurance_score
,qs.guard as property_data_guard
,cast(num_bathroom as string) as property_data_bathroom
,roof_type as property_data_roof_type
,roof_shape as property_data_roof_shape
,qs.Hoa_Membership as property_data_hoa_membership
,construction_type as property_data_construction_type
,qs.Number_Of_Stories as property_data_number_of_stories
,qs.age_of_home as calculated_fields_age_of_home
,qs.age_of_insured as calculated_fields_age_of_insured
,qs.Three_Years_Claims as calculated_fields_three_years_claims
,deductible as coverage_deductible
,extended_rebuilding_cost as coverage_extended_rebuilding_cost
,cast(water_backup as string) as coverage_water_backup
,coverage_e
,round(cast(q.non_cat_risk_score as numeric),5) as non_cat_risk_score
from dw_prod.dim_quotes q
left join pg_quotes_supp qs on coalesce(q.lead_id,cast(q.policy_id as string)) = qs.policy_number
      where q.date_quote_first_seen >= '2020-01-01'
      and q.product <> 'HO5'
)
, scoring_begin as (
select 
quote_id, state, carrier, product, non_cat_risk_score
-- *
,coverage_a as cov_a
,ln(coverage_a) * -0.141714902  as score_cov_a
,insurance_score  
,case when state = 'CA' or state = 'MD' then -0.078330927  
when insurance_score is null or insurance_score = 'no_hit' or insurance_score = '"no_hit"' then 0.093400276  
when insurance_score = 'no_score' or insurance_score = '"no_score"' then -0.078330927  
when cast(insurance_score as numeric) < 500 then 0  
when cast(insurance_score as numeric) >= 500 and cast(insurance_score as numeric) < 575 then -0.062269523  
when cast(insurance_score as numeric) >= 575 and cast(insurance_score as numeric) < 625 then -0.181704818  
when cast(insurance_score as numeric) >= 625 and cast(insurance_score as numeric) < 650 then -0.308159554  
when cast(insurance_score as numeric) >= 650 and cast(insurance_score as numeric) < 675 then -0.435992925  
when cast(insurance_score as numeric) >= 675 and cast(insurance_score as numeric) < 690 then -0.435992925  
when cast(insurance_score as numeric) >= 690 and cast(insurance_score as numeric) < 705 then -0.435992925  
when cast(insurance_score as numeric) >= 705 and cast(insurance_score as numeric) < 720 then -0.474051875  
when cast(insurance_score as numeric) >= 720 and cast(insurance_score as numeric) < 735 then -0.474051875  
when cast(insurance_score as numeric) >= 735 and cast(insurance_score as numeric) < 750 then -0.474051875  
when cast(insurance_score as numeric) >= 750 and cast(insurance_score as numeric) < 765 then -0.486243466  
when cast(insurance_score as numeric) >= 765 and cast(insurance_score as numeric) < 780 then -0.54424551  
when cast(insurance_score as numeric) >= 780 and cast(insurance_score as numeric) < 997 then -0.60838874  
when cast(insurance_score as numeric) >= 997 then -0.60838874 else -999 end as score_insurance_score
,property_data_guard  
,case when property_data_guard is null or property_data_guard = 'false' then 0 else 0.11462938  end as score_guard
,property_data_bathroom  
,case when property_data_bathroom is null or property_data_bathroom = '' or property_data_bathroom = ' ' then 0  
when cast(property_data_bathroom as numeric) in (0,1,0.5) then 0  
when property_data_bathroom = '1.5' then 0.019802627  
when property_data_bathroom = '2' then 0.039605255  
when property_data_bathroom = '2.5' then 0.059407882  
when property_data_bathroom = '3' then 0.073066986  
when property_data_bathroom = '3.5' then 0.083017317  
when property_data_bathroom = '4' then 0.112576119  
when property_data_bathroom = '4.5' then 0.12252645  
when cast(property_data_bathroom as numeric) >= 5 then 0.132476781 else -999 end as score_bathroom
,property_data_roof_type  
,case when property_data_roof_type is null or property_data_roof_type = 'architectural_shingles' or property_data_roof_type = 'asphalt_fiberglass_shingles' then 0  
when property_data_roof_type = 'clay_tile' then 0.154598171  
when property_data_roof_type = 'concrete_tile' then 0.104715239  
when property_data_roof_type = 'flat_roof' then 0.440948887  
when property_data_roof_type = 'other' then 0.001791321  
when property_data_roof_type = 'slate_tile' then 0.236729794  
when property_data_roof_type = 'steel_or_metal' then 0.201607323  
when property_data_roof_type = 'wood_shingle_or_shake' then 0.266324777 else -999 end as score_roof_type
,property_data_roof_shape  
,case when property_data_roof_shape is null or property_data_roof_shape = 'gable' or property_data_roof_shape = 'G' then 0  
when property_data_roof_shape = 'custom' or property_data_roof_shape = 'C' then 0.242261038  
when property_data_roof_shape = 'flat' or property_data_roof_shape = 'F' then -0.356824316  
when property_data_roof_shape = 'gambrel' or property_data_roof_shape = 'GM' then 0.071007869  
when property_data_roof_shape = 'hip' or property_data_roof_shape = 'H' then -0.016101627  
when property_data_roof_shape = 'mansard' or property_data_roof_shape = 'M' then 0.167549902  
when property_data_roof_shape = '' or property_data_roof_shape = '' then 0.043299838  
when property_data_roof_shape = 'shed' or property_data_roof_shape = 'S' then 0.097538198 else -999 end as score_roof_shape
,property_data_hoa_membership  
,case when property_data_hoa_membership is null or property_data_hoa_membership = 'false' then 0 else -0.044562395  end as score_hoa
,property_data_construction_type  
,case when property_data_construction_type is null or property_data_construction_type = 'frame' then 0  
when property_data_construction_type = 'brick_veneer' then 0.062194839  
when property_data_construction_type = 'concrete' then -0.238805033  
when property_data_construction_type = 'masonry' then -0.038203603  
when property_data_construction_type = 'steel' then 0.134077456 else -999 end as score_construction_type
,property_data_number_of_stories  
,case when property_data_number_of_stories is null or property_data_number_of_stories = '1' then 0  
when property_data_number_of_stories = '1.5' then 0.159412731  
when property_data_number_of_stories = '2' then 0.212688628  
when property_data_number_of_stories = '2.5' then 0.215330329  
when property_data_number_of_stories = '3+' then 0.222004124 else -999 end as score_number_of_stories
,calculated_fields_age_of_home  
,case when cast(calculated_fields_age_of_home as numeric) < 3 then 0  
when cast(calculated_fields_age_of_home as numeric) >= 12 and cast(calculated_fields_age_of_home as numeric) < 15 then 0.313489020037788  
when cast(calculated_fields_age_of_home as numeric) >= 15 and cast(calculated_fields_age_of_home as numeric) < 18 then 0.453250962412947  
when cast(calculated_fields_age_of_home as numeric) >= 18 and cast(calculated_fields_age_of_home as numeric) < 21 then 0.610254711222612  
when cast(calculated_fields_age_of_home as numeric) >= 21 and cast(calculated_fields_age_of_home as numeric) < 24 then 0.758674716340885  
when cast(calculated_fields_age_of_home as numeric) >= 24 and cast(calculated_fields_age_of_home as numeric) < 27 then 0.863034731665128  
when cast(calculated_fields_age_of_home as numeric) >= 27 and cast(calculated_fields_age_of_home as numeric) < 30 then 0.91182489583456  
when cast(calculated_fields_age_of_home as numeric) >= 3 and cast(calculated_fields_age_of_home as numeric) < 6 then 0.00995033085316809  
when cast(calculated_fields_age_of_home as numeric) >= 30 and cast(calculated_fields_age_of_home as numeric) < 33 then 0.941383698076104  
when cast(calculated_fields_age_of_home as numeric) >= 33 and cast(calculated_fields_age_of_home as numeric) < 36 then 0.970942500317649  
when cast(calculated_fields_age_of_home as numeric) >= 36 and cast(calculated_fields_age_of_home as numeric) < 39 then 0.985831112811399  
when cast(calculated_fields_age_of_home as numeric) >= 39 and cast(calculated_fields_age_of_home as numeric) < 42 then 1.00071972530515  
when cast(calculated_fields_age_of_home as numeric) >= 42 and cast(calculated_fields_age_of_home as numeric) < 45 then 1.0156083377989  
when cast(calculated_fields_age_of_home as numeric) >= 45 and cast(calculated_fields_age_of_home as numeric) < 48 then 1.02803085779746  
when cast(calculated_fields_age_of_home as numeric) >= 48 and cast(calculated_fields_age_of_home as numeric) < 51 then 1.03798118865063  
when cast(calculated_fields_age_of_home as numeric) >= 51 and cast(calculated_fields_age_of_home as numeric) < 54 then 1.04047806884921  
when cast(calculated_fields_age_of_home as numeric) >= 54 and cast(calculated_fields_age_of_home as numeric) < 57 then 1.0429749490478  
when cast(calculated_fields_age_of_home as numeric) >= 57 and cast(calculated_fields_age_of_home as numeric) < 60 then 1.04547182924639  
when cast(calculated_fields_age_of_home as numeric) >= 6 and cast(calculated_fields_age_of_home as numeric) < 9 then 0.0869113719892964  
when cast(calculated_fields_age_of_home as numeric) >= 60 and cast(calculated_fields_age_of_home as numeric) < 63 then 1.04796870944497  
when cast(calculated_fields_age_of_home as numeric) >= 63 and cast(calculated_fields_age_of_home as numeric) < 66 then 1.05046558964356  
when cast(calculated_fields_age_of_home as numeric) >= 66 and cast(calculated_fields_age_of_home as numeric) < 69 then 1.05296246984215  
when cast(calculated_fields_age_of_home as numeric) >= 69 and cast(calculated_fields_age_of_home as numeric) < 72 then 1.05545935004074  
when cast(calculated_fields_age_of_home as numeric) >= 72 then 1.05795623023932  
when cast(calculated_fields_age_of_home as numeric) >= 9 and cast(calculated_fields_age_of_home as numeric) < 12 then 0.191271387313539 else -999 end as score_age_of_home
,calculated_fields_age_of_insured  
,case when calculated_fields_age_of_insured = 'Default' then -0.285823311  
when cast(calculated_fields_age_of_insured as numeric) < 28 then 0  
when cast(calculated_fields_age_of_insured as numeric) >= 28 and cast(calculated_fields_age_of_insured as numeric) < 32 then -0.01327383  
when cast(calculated_fields_age_of_insured as numeric) >= 32 and cast(calculated_fields_age_of_insured as numeric) < 36 then -0.074538598  
when cast(calculated_fields_age_of_insured as numeric) >= 36 and cast(calculated_fields_age_of_insured as numeric) < 40 then -0.233273741  
when cast(calculated_fields_age_of_insured as numeric) >= 40 and cast(calculated_fields_age_of_insured as numeric) < 44 then -0.378114121  
when cast(calculated_fields_age_of_insured as numeric) >= 44 and cast(calculated_fields_age_of_insured as numeric) < 48 then -0.418936115  
when cast(calculated_fields_age_of_insured as numeric) >= 48 and cast(calculated_fields_age_of_insured as numeric) < 52 then -0.45975811  
when cast(calculated_fields_age_of_insured as numeric) >= 52 and cast(calculated_fields_age_of_insured as numeric) < 56 then -0.490217317  
when cast(calculated_fields_age_of_insured as numeric) >= 56 and cast(calculated_fields_age_of_insured as numeric) < 60 then -0.47041469  
when cast(calculated_fields_age_of_insured as numeric) >= 60 and cast(calculated_fields_age_of_insured as numeric) < 65 then -0.412145782  
when cast(calculated_fields_age_of_insured as numeric) >= 65 and cast(calculated_fields_age_of_insured as numeric) < 70 then -0.335184741  
when cast(calculated_fields_age_of_insured as numeric) >= 70 and cast(calculated_fields_age_of_insured as numeric) < 75 then -0.305625938  
when cast(calculated_fields_age_of_insured as numeric) >= 75 then -0.285823311 else -999 end as score_age_of_insured
,calculated_fields_three_years_claims  
,case when cast(calculated_fields_three_years_claims as numeric) > 0 then 0.172978204 else 0  end as score_three_year_claims
,coverage_deductible  
,case when coverage_deductible = 1000 then 0  
when coverage_deductible = 1 then -0.279708767  
when coverage_deductible = 500 then 0.550768157  
when coverage_deductible = 1500 then -0.116259846  
when coverage_deductible = 2500 then -0.223127203  
when coverage_deductible = 3500 then -0.328487719  
when coverage_deductible = 5000 then -0.390363123  
when coverage_deductible = 10000 then -0.390363123  
when coverage_deductible = 25000 then -0.390363123  
when coverage_deductible = 50000 then -0.390363123 else -999 end as score_deductible
,coverage_extended_rebuilding_cost  
,case when cast(coverage_extended_rebuilding_cost as numeric) in (0.1,0.25) then 0  
when cast(coverage_extended_rebuilding_cost as numeric) = 0.5 then -0.055942716  
when coverage_extended_rebuilding_cost is null or cast(coverage_extended_rebuilding_cost as numeric) = 0 then 0.052529142 else -999 end as score_extended_rebuilding_cost
,coverage_water_backup  
,case when coverage_water_backup = '5000' then 0  
when coverage_water_backup = '10000' then 0  
when coverage_water_backup = '12500' then 0.029558802  
when coverage_water_backup = '15000' then 0.059117604  
when coverage_water_backup = '20000' then 0.154427784  
when coverage_water_backup = '25000' then 0.262364264  
when coverage_water_backup = '50000' then 0.262364264  
when coverage_water_backup = '7500' then 0  
when coverage_water_backup in ('75000','9999999') then 0.262364264  
when coverage_water_backup = '0' or coverage_water_backup is null then 0 else -999 end as score_water_backup
,coverage_e  
,case when coverage_e = 300000 then 0  
when coverage_e = 100000 then -0.095833597  
when coverage_e = 200000 then -0.106434488  
when coverage_e = 500000 or coverage_e = 1000000 then -0.073082463 else -999 end as score_coverage_e
,-1.63517384735612 as score_intercept
from quotes
where 1=1
and product <> 'HO5'
and carrier <> 'Canopius'
)
,scoring_inter as (
select *
,score_cov_a + 
score_insurance_score +
score_guard + 
score_bathroom + 
score_roof_type + 
score_roof_shape +
score_hoa + 
score_construction_type + 
score_number_of_stories + 
score_age_of_home +
score_age_of_insured +
score_three_year_claims + 
score_deductible +
score_extended_rebuilding_cost + 
score_water_backup + 
score_coverage_e + 
score_intercept
as lin_comb
from scoring_begin
)
, scoring_final as (
select 
quote_id as id
-- , lin_comb
-- , exp(lin_comb) as exponent
, ROUND(exp(lin_comb) / (1+ exp(lin_comb)),5) as risk_score
from scoring_inter
)
, quotes_supp as (
      select *
            ,o.*
            ,o.name as org_name
            ,dq.organization_id as org_id
            ,
            case
                when o.is_b2b2c is true and o.is_builder is true 
                then o.name
                else NULL
            end as builders,
            CASE 
                WHEN o.is_b2b2c IS true and o.is_builder is false
                THEN o.name
                ELSE NULL
                END AS partners,
            CASE 
                WHEN producer_id IS NOT NULL 
                  AND dq.organization_id IS NOT NULL 
                  AND (o.is_b2b2c IS false OR o.is_b2b2c IS NULL)
                THEN producer_id
                ELSE NULL 
                END AS producers,
            CASE WHEN attributed_agent_bound_by IS NOT NULL 
                AND (producer_id IS NULL)
                AND (dq.organization_id IS NULL OR dq.organization_id != 213 OR dq.organization_id != 10)
                THEN attributed_agent_bound_by
                ELSE NULL
                END AS agents
             ,case when policy_number is not null then 1 else 0 end as bound
             ,1 as quote_count
      from dw_prod.dim_quotes dq
            LEFT JOIN dw_prod.dim_organizations o on dq.organization_id = o.organization_id
            left join (select policy_number, rewritten_policy_id from dw_prod.dim_policies) dp USING(policy_number)
            left join scoring_final score on dq.quote_id = score.id
      where 1=1
            and dp.rewritten_policy_id is null
            and date_quote_first_seen is not null
            and quote_premium_total is not null 
            and (is_intent is not false or is_bound = true)
)
,quotes_dedup as (
      select street,
            city,
            state,
            date_trunc(cast(date_quote_first_seen as DATE), WEEK) as quote_month,
            FIRST_VALUE(is_bound) OVER (PARTITION by street,city,state,date_trunc(cast(date_quote_first_seen as DATE), WEEK) ORDER BY is_bound DESC, timestamp_created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS is_bound,
            FIRST_VALUE(quote_id) OVER (PARTITION by street,city,state,date_trunc(cast(date_quote_first_seen as DATE), WEEK) ORDER BY is_bound DESC, timestamp_created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS quote_id,
            FIRST_VALUE(org_id) OVER (PARTITION by street,city,state,date_trunc(cast(date_quote_first_seen as DATE), WEEK) ORDER BY is_bound DESC, timestamp_created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS organization_attribution
      FROM quotes_supp
), quotes_dedup_first_attribution AS ( -- using the first seen attribution to choose the attribution
      SELECT street, city, state, quote_id, organization_attribution, is_bound, quote_month, count(*) as cnt 
      from quotes_dedup
      GROUP BY street, city, state, quote_id, organization_attribution, is_bound, quote_month
      ORDER BY street, city, state
)
SELECT
--       policy_number
      cast(q.date_quote_first_seen as DATE) as quote_date
      ,date_trunc(cast(q.date_quote_first_seen as DATE), WEEK) as quote_week
      ,date_trunc(cast(q.date_quote_first_seen as DATE), MONTH) as quote_month
    --   ,qs.org_name as organization_name
    --   ,q.organization_id
      ,q.state
      ,q.product
      ,q.carrier
--       ,q.is_bulk_quoted
      ,case when q.year_built is null then 'Missing'
      when cast(q.year_built as numeric) >= 2000 then 'Post 2000' 
      when cast(q.year_built as numeric) > 1980 then 'Pre 2000' 
      else 'Pre 1980' end as year_built
    --   ,q.zip_code
      ,q.county
--       ,q.roof_type
--       ,q.construction_type
--       ,2020 - q.year_built + 1 as age_of_home
      ,q.coverage_a
      ,q.deductible
      ,q.insurance_score
      ,round(cast(q.non_cat_risk_score as numeric),5) as non_cat_risk_score
--       ,q.cat_risk_score
      ,q.non_cat_risk_class
--       ,q.cat_risk_class
      ,coalesce(q.non_cat_risk_class, 'not_applicable') as UW_Action
      ,risk_score as calculated_risk_score
--       ,case when q.non_cat_risk_class is null then 'not_applicable'
--       when q.state = 'TX' and q.cat_risk_class = 'referral' then 'referral'
--       else q.non_cat_risk_class end as UW_Class_with_TX
      ,case when coalesce(q.date_bound, cast(q.date_quote_first_seen as date)) <= '2020-04-29' then 'not_applicable'
      when q.non_cat_risk_class = 'exterior_inspection_required' or q.non_cat_risk_class = 'interior_inspection_required' or q.non_cat_risk_class = 'referral' then 'rocky'
      when q.non_cat_risk_class = 'no_action' then 'happy'
      else 'not_applicable' end as UW_Path
      ,q.date_bound
      ,date_trunc(cast(q.date_bound as DATE), WEEK) as bound_week
      ,date_trunc(cast(q.date_bound as DATE), MONTH) as bound_month
      ,CASE 
            WHEN partners is not null then 'Partner' 
            WHEN builders is not null then 'Builder'
            WHEN producers is not null then 'Producer' 
            WHEN agents is not null then 'Agent'
            ELSE 'Online'
            end as channel
      ,SUM(CASE WHEN ddp.is_bound IS TRUE THEN 1 ELSE 0 END) AS bound_count
      ,COUNT(*) as quote_count
      ,sum(q.quote_premium_total) as total_quote_premium
      FROM quotes_dedup_first_attribution ddp
            LEFT JOIN dw_prod.dim_quotes q USING (quote_id)
            LEFT JOIN quotes_supp qs using (quote_id)
            LEFT JOIN dw_prod.dim_policies dp on (q.policy_number = dp.policy_number)
      where q.date_quote_first_seen >= '2020-01-01'
--       and q.state = 'CA'
      and q.product <> 'HO5'
--       and q.carrier = 'Topa'
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20