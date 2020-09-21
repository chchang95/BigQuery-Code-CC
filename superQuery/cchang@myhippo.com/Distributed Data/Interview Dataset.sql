with claims_supp as (
    select distinct
        mon.*
        ,dp.org_id
        ,case when peril = 'equipment_breakdown' or peril = 'service_line' then 'Y'
                when is_EBSL = true then 'Y'
                else 'N' end as EBSL
        ,case when peril = 'wind' or peril = 'hail' then 'Y'
                when cat_code is not null then 'Y'
                else 'N' end as CAT
        FROM dw_prod_extracts.ext_all_claims_combined mon
            left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
        WHERE 1=1
            and date_report_period_end = '2020-08-31'
            and carrier <> 'Canopius'
    )
, claims as (
    select
        policy_id
        ,date_trunc(date_of_loss, MONTH) as accident_month
        
        ,sum(total_incurred) as total_incurred
        ,sum(case when CAT = 'N' then total_incurred else 0 end) as non_cat_incurred
        ,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then 100000 else total_incurred end) as capped_non_cat_incurred
        ,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then total_incurred - 100000 else 0 end) as excess_non_cat_incurred
        ,sum(case when CAT = 'Y' then total_incurred else 0 end) as cat_incurred
        
        ,sum(case when claim_closed_no_total_payment is false then 1 else 0 end) as reported_total_claim_count_x_cnp
        ,sum(case when claim_closed_no_total_payment is false and CAT = 'N' then 1 else 0 end) as reported_non_cat_claim_count_x_cnp
        ,sum(case when claim_closed_no_total_payment is false and CAT = 'Y' then 1 else 0 end) as reported_cat_claim_count_x_cnp
        
        ,sum(case when claim_closed_no_total_payment is false and date_closed is not null then 1 else 0 end) as closed_total_claim_count_x_cnp
        ,sum(case when claim_closed_no_total_payment is false and date_closed is not null and CAT = 'N' then 1 else 0 end) as closed_non_cat_claim_count_x_cnp
        ,sum(case when claim_closed_no_total_payment is false and date_closed is not null and CAT = 'Y' then 1 else 0 end) as closed_cat_claim_count_x_cnp
        
        ,sum(case when CAT = 'N' and peril_group = 'Water' then total_incurred else 0 end) as non_cat_incurred_water
    from claims_supp
    where 1=1
        and ebsl = 'N'
    group by 1,2
    ) 
select 
(cast(eps.policy_id as numeric) * 2) + 18 as identifier
-- , eps.policy_number
-- ,date_trunc(date_policy_effective, MONTH) as policy_inception_month
-- ,date_policy_effective
-- ,carrier
,state
,product
-- ,status 
-- ,quote_rater_version
-- ,timstamp_quote_created as quote_date
-- ,org_id as organization_id
,property_data_address_city as city
,property_data_address_zip as zip
,property_data_address_county as county
-- ,case when renewal_number = 0 then "New" else "Renewal" end as tenure

,coverage_a
-- ,coverage_b
-- ,coverage_c
-- ,coverage_d
-- ,coverage_e
-- ,coverage_f

,coverage_deductible
-- ,coverage_wind_deductible
-- ,coverage_hurricane_deductible
-- ,coverage_deductible_amount
-- ,coverage_wind_deductible_amount
-- ,coverage_hurricane_deductible_amount

-- ,coverage_water_backup
-- ,coverage_ordinance_or_law
-- ,coverage_loss_assessment
,coverage_personal_property_replacement_cost
-- ,coverage_acv_on_roof
-- ,coverage_mortgage_payment_protection
,coverage_extended_rebuilding_cost

-- ,property_data_year_built
,property_data_square_footage
-- ,property_data_updated_electric
-- ,property_data_updated_heating
-- ,property_data_updated_plumbing
-- ,property_data_number_of_family_units
-- ,property_data_occupant_type
-- ,property_data_residence_type
,property_data_protection_class
-- ,property_data_miles_from_fire_department
,property_data_fire_ext
,property_data_sprinkler
,property_data_guard
-- ,property_data_affinity_discount
,property_data_deadbolt
-- ,property_data_swimming_pool
,property_data_number_of_stories
,property_data_roof_type
-- ,property_data_roof_shape
-- ,property_data_foundation_type
-- ,property_data_year_roof_built
,property_data_construction_type
,property_data_hail_resistant_roof
,property_data_bathroom
-- ,property_data_fireline_score
-- ,property_data_rebuilding_cost
,property_data_building_quality
-- ,property_data_fireplaces
-- ,property_data_garage_size
-- ,property_data_garage_type
-- ,property_data_held_in_trust
-- ,property_data_roof_material
-- ,property_data_scheduled_personal_property
-- ,property_data_slope_angle
-- ,property_data_smoke_alarm
-- ,case when state = 'tx' and calculated_fields_cat_risk_score = 'referral' then 'referral' 
--         when calculated_fields_non_cat_risk_class is null then 'not_applicable' 
--         else calculated_fields_non_cat_risk_class end as rated_uw_action 
-- ,coalesce(cast(calculated_fields_non_cat_risk_score as numeric),-99) as rated_non_cat_risk_score
-- ,payment_frequency
-- ,payment_method
-- ,auto_renewal
-- ,insurance_score
-- ,has_multiple_policies_discount
-- ,loss_history_claims

,calculated_fields_age_of_home
,calculated_fields_age_of_roof
-- ,calculated_fields_early_quote_days
,calculated_fields_five_years_claims
-- ,calculated_fields_three_years_claims
-- ,calculated_fields_property_claims_last_year
,calculated_fields_age_of_insured
-- ,calculated_fields_other_structures_increased_limit
-- ,calculated_fields_personal_property_increased_limit
-- ,calculated_fields_increased_loss_of_use_liability_limit

,(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) / 2 as written_premium
,(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) / 2 as earned_premium
-- ,written_base
-- ,earned_base
,written_exposure/2 as written_exposure
,earned_exposure/2 as earned_exposure
-- ,quote_premium_base + quote_premium_optionals + quote_policy_fee - quote_optionals_service_line - quote_optionals_equipment_breakdown as quote_prem_x_ebsl_inc_pol_fee
-- ,quote_premium_base as quote_base

,coalesce(total_incurred,0)/2 as total_incurred
,coalesce(non_cat_incurred,0)/2 as non_cat_incurred
,coalesce(capped_non_cat_incurred,0)/2 as capped_at_100k_non_cat_incurred
,coalesce(excess_non_cat_incurred,0)/2 as excess_of_100k_non_cat_incurred
,coalesce(cat_incurred,0)/2 as cat_incurred
,coalesce(reported_total_claim_count_x_cnp,0)/2 as reported_total_claim_count
,coalesce(reported_non_cat_claim_count_x_cnp,0)/2 as reported_non_cat_claim_count
,coalesce(reported_cat_claim_count_x_cnp,0)/2 as reported_cat_claim_count
-- ,coalesce(closed_total_claim_count_x_cnp,0)/2 as closed_total_claim_count
-- ,coalesce(closed_non_cat_claim_count_x_cnp,0)/2 as closed_non_cat_claim_count
-- ,coalesce(closed_cat_claim_count_x_cnp,0)/2 as closed_cat_claim_count

-- ,coalesce(non_cat_incurred_water,0) as non_cat_incurred_water

from dw_prod_extracts.ext_policy_snapshots eps
    left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on eps.policy_id = dp.policy_id
    left join claims c on eps.policy_id = c.policy_id
where 1=1
    and date_snapshot = '2020-08-31'
    and carrier <> 'canopius'
    and product <> 'ho5'
    and state not in ('tx','ca')
-- and state = 'CA'