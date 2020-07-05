with quotes_supp as (
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
            date_trunc(cast(date_quote_first_seen as DATE), MONTH) as quote_month,
            FIRST_VALUE(is_bound) OVER (PARTITION by street,city,state,date_trunc(cast(date_quote_first_seen as DATE), MONTH) ORDER BY is_bound DESC, timestamp_created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS is_bound,
            FIRST_VALUE(quote_id) OVER (PARTITION by street,city,state,date_trunc(cast(date_quote_first_seen as DATE), MONTH) ORDER BY is_bound DESC, timestamp_created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS quote_id,
            FIRST_VALUE(org_id) OVER (PARTITION by street,city,state,date_trunc(cast(date_quote_first_seen as DATE), MONTH) ORDER BY is_bound DESC, timestamp_created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS organization_attribution
      FROM quotes_supp
), quotes_dedup_first_attribution AS ( -- using the first seen attribution to choose the attribution
      SELECT street, city, state, quote_id, organization_attribution, is_bound, quote_month, count(*) as cnt 
      from quotes_dedup
      GROUP BY street, city, state, quote_id, organization_attribution, is_bound, quote_month
      ORDER BY street, city, state
)
SELECT
      date_trunc(cast(q.date_quote_first_seen as DATE), MONTH) as quote_date
      ,case when q.carrier = 'Topa' then 'Topa'
            when q.product = 'HO5' then 'Spkr19_HO5'
            when q.date_effective >= '2020-01-01' and q.carrier = 'Spinnaker' then 'Spkr20_Classic'
            when q.date_effective >= '2019-09-19' and q.carrier = 'Spinnaker' then 'Spkr19_GAP'
            when q.date_effective < '2019-09-19' and q.carrier = 'Spinnaker' then 'Spkr17_MRDP'
            when q.carrier = 'Canopius' then 'Canopius'
            else 'None' end as treaty
      ,lower(q.state) as state
      ,lower(q.product) as product
      ,lower(q.carrier) as carrier
      ,CASE 
            WHEN partners is not null then 'Partner' 
            WHEN builders is not null then 'Builder'
            WHEN producers is not null then 'Producer' 
            WHEN agents is not null then 'Agent'
            ELSE 'Online'
            end as channel
      ,q.county
      ,least(q.coverage_a - mod(q.coverage_a,100000),700000) as cov_a
      ,q.deductible as aop_deductible
      ,coalesce(q.wind_deductible,0) as wind_deductible
      ,coalesce(q.hurricane_deductible,0) as hurricane_deductible
      ,coalesce(q.square_footage - mod(q.square_footage,250),0) as square_footage
      ,least(coalesce((2020 - q.year_built) - mod(2020 - q.year_built,5),0),77) as age_of_home
      ,least(coalesce((2020 - q.year_roof_built) - mod(2020 - q.year_roof_built,5),0),77) as age_of_roof
      ,coalesce(q.construction_type,'Other') as construction_type
      ,COUNT(*) as quote_leads_count
      ,SUM(CASE WHEN ddp.is_bound IS TRUE THEN 1 ELSE 0 END) AS bound_count
      ,sum(q.quote_premium_total) as quote_total_prem
      FROM quotes_dedup_first_attribution ddp
            LEFT JOIN dw_prod.dim_quotes q USING (quote_id)
            LEFT JOIN quotes_supp qs using (quote_id)
            LEFT JOIN dw_prod.dim_policies dp on (q.policy_number = dp.policy_number)
      where q.date_quote_first_seen >= '2019-01-01'
--       and q.state = 'CA'
--       and q.product <> 'HO5'
      and q.carrier <> 'Canopius'
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15