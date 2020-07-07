with quotes_supp as (
    with leads as (
        select cast(id as string)   as policy_number
             , least(cast(json_extract_scalar(transaction,'$.calculated_fields.age_of_home') as numeric),
                     77)                                                               as age_of_home
             , case
                   when coalesce(json_extract_scalar(transaction,'$.property_data.guard'), 'false') = 'false' then 'No'
                   else 'Yes' end                                                      as Guard
             , json_extract_scalar(transaction,'$.calculated_fields.age_of_insured')               as age_of_insured
             , json_extract_scalar(transaction,'$.calculated_fields.three_years_claims')           as Three_Years_Claims
             , json_extract_scalar(transaction,'$.property_data.number_of_stories')                as Number_Of_Stories
             , case
                   when coalesce(json_extract_scalar(transaction,'$.property_data.hoa_membership'), 'false') = 'false' then 'No'
                   else 'Yes' end                                                      as Hoa_Membership
             , cast('lead' as string)                                                                  as quote_type
        from postgres_public.leads a
                 left join (select lead_id, bound, status
                            from postgres_public.policies
                            where policy_number like '%-00'
                              and bound = true) b
                           on cast(a.id as string) = cast(b.lead_id as string)
        where 1 = 1
--       and bound = 'true'
--       and status not in ('pending_active', 'pending_bind')
--       and effective_date::date <= '2020-04-31'::date
          and cast(initial_quote_date as date) >= '2019-10-20'
--       and state = 'tx'
          and carrier <> 'canopius'
          and product not in ('ho5')
          and json_extract_scalar(transaction,'$.quote.premium.total') is not null
          and state is not null
          and initial_quote_date is not null
          and json_extract_scalar(transaction,'$.effective_date') is not null
        --   LIMIT 50000
    )
       , quotes as (
select cast(id as string)   as policy_number
             , least(cast(json_extract_scalar(coalesce(policy_info,transaction),'$.calculated_fields.age_of_home') as numeric),
                     77)                                                               as age_of_home
             , case
                   when coalesce(json_extract_scalar(coalesce(policy_info,transaction),'$.property_data.guard'), 'false') = 'false' then 'No'
                   else 'Yes' end                                                      as Guard
             , json_extract_scalar(coalesce(policy_info,transaction),'$.calculated_fields.age_of_insured')               as age_of_insured
             , json_extract_scalar(coalesce(policy_info,transaction),'$.calculated_fields.three_years_claims')           as Three_Years_Claims
             , json_extract_scalar(coalesce(policy_info,transaction),'$.property_data.number_of_stories')                as Number_Of_Stories
             , case
                   when coalesce(json_extract_scalar(coalesce(policy_info,transaction),'$.property_data.hoa_membership'), 'false') = 'false' then 'No'
                   else 'Yes' end                                                      as Hoa_Membership
             , cast('quote' as string)                                                                  as quote_type
        from postgres_public.policies a
        where 1 = 1
--       and bound = 'true'
--       and status not in ('pending_active', 'pending_bind')
--       and effective_date::date <= '2020-04-31'::date
          and cast(initial_quote_date as date) >= '2019-10-20'
--       and state = 'tx'
          and carrier <> 'canopius'
          and product not in ('ho5')
          and json_extract_scalar(coalesce(policy_info,transaction),'$.quote.premium.total') is not null
          and state is not null
          and initial_quote_date is not null
          and effective_date is not null
        --   LIMIT 100000
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
,insurance_score
,qs.guard as property_data_guard
,num_bathroom as property_data_bathroom
,roof_type as property_data_roof_type
,roof_shape as property_data_roof_shape
,qs.Hoa_Membership as property_data_hoa_membership
,construction_type as property_data_construction_type
,qs.Number_Of_Stories as property_data_number_of_stories
,qs.age_of_home as calculated_fields_age_of_home
,qs.age_of_insured as calculated_fields_age_of_insured
,qs.Three_Years_Claims as calculated_fields_three_years_claims
,deductible as coverage_deductible
,rebuilding_cost as coverage_extended_rebuilding_cost
,water_backup as coverage_water_backup
,coverage_e
from dw_prod.dim_quotes q
left join quotes_supp qs on coalesce(q.lead_id,cast(q.policy_id as string)) = qs.policy_number
      where q.date_quote_first_seen >= '2020-01-01'
      and q.product <> 'HO5'
)
select * from quotes