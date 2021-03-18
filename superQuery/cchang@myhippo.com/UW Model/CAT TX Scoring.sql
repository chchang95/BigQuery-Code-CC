with scoring_begin as (
select 
-- policy_id, state, carrier, product, calculated_fields_non_cat_risk_score
*
,coverage_a as cov_a
,property_data_roof_type  
,case when property_data_roof_type is null or property_data_roof_type = 'architectural_shingles' then 0
when property_data_roof_type = 'asphalt_fiberglass_shingles' then 0.000613821931192313
when property_data_roof_type = 'clay_tile' then 0.00231222491077966  
when property_data_roof_type = 'concrete_tile' then -0.0207037825403463 
when property_data_roof_type = 'flat_roof' then 0.0257419376117609  
when property_data_roof_type = 'other' then 0.12463401371447 
when property_data_roof_type = 'slate_tile' then 0.0331615551825624  
when property_data_roof_type = 'steel_or_metal' then 0.0849643025059396  
when property_data_roof_type = 'wood_shingle_or_shake' then -0.00886032189745794 else -999 end as score_roof_type
-- roof type done


,property_data_roof_shape  
,case when property_data_roof_shape = 'gable' or property_data_roof_shape = 'G' then 0
when property_data_roof_shape = 'custom' or property_data_roof_shape = 'C' then -0.0202889599684784  
when property_data_roof_shape = 'flat' or property_data_roof_shape = 'F' then -0.0165963627205213  
when property_data_roof_shape = 'gambrel' or property_data_roof_shape = 'GM' then -0.0181676293055674  
when property_data_roof_shape = 'hip' or property_data_roof_shape = 'H' then -0.10659170887541 
when property_data_roof_shape = 'mansard' or property_data_roof_shape = 'M' then -0.000442469090184813 
when property_data_roof_shape is null then -0.0138500613665034 
when property_data_roof_shape = 'shed' or property_data_roof_shape = 'S' then 0.0191857599456752 else -999 end as score_roof_shape
-- roof shape done


,property_data_construction_type  
,case when property_data_construction_type is null or property_data_construction_type = 'frame' then 0  
when property_data_construction_type = 'brick_veneer' then -0.0365846876325438  
when property_data_construction_type = 'concrete' then -0.192984756837173 
when property_data_construction_type = 'masonry' then -0.203746880350082  
when property_data_construction_type = 'steel' then -0.239148490416563 else -999 end as score_construction_type
-- construction type done


,calculated_fields_age_of_home  
,case when cast(calculated_fields_age_of_home as numeric) < 3 then 0  
when cast(calculated_fields_age_of_home as numeric) >= 12 and cast(calculated_fields_age_of_home as numeric) < 15 then 0.025  
when cast(calculated_fields_age_of_home as numeric) >= 15 and cast(calculated_fields_age_of_home as numeric) < 18 then 0.055
when cast(calculated_fields_age_of_home as numeric) >= 18 and cast(calculated_fields_age_of_home as numeric) < 21 then 0.09  
when cast(calculated_fields_age_of_home as numeric) >= 21 and cast(calculated_fields_age_of_home as numeric) < 24 then 0.1175 
when cast(calculated_fields_age_of_home as numeric) >= 24 and cast(calculated_fields_age_of_home as numeric) < 27 then 0.1425  
when cast(calculated_fields_age_of_home as numeric) >= 27 and cast(calculated_fields_age_of_home as numeric) < 30 then 0.155  
when cast(calculated_fields_age_of_home as numeric) >= 3 and cast(calculated_fields_age_of_home as numeric) < 6 then 0  
when cast(calculated_fields_age_of_home as numeric) >= 30 and cast(calculated_fields_age_of_home as numeric) < 33 then 0.159 
when cast(calculated_fields_age_of_home as numeric) >= 33 and cast(calculated_fields_age_of_home as numeric) < 36 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 36 and cast(calculated_fields_age_of_home as numeric) < 39 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 39 and cast(calculated_fields_age_of_home as numeric) < 42 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 42 and cast(calculated_fields_age_of_home as numeric) < 45 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 45 and cast(calculated_fields_age_of_home as numeric) < 48 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 48 and cast(calculated_fields_age_of_home as numeric) < 51 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 51 and cast(calculated_fields_age_of_home as numeric) < 54 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 54 and cast(calculated_fields_age_of_home as numeric) < 57 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 57 and cast(calculated_fields_age_of_home as numeric) < 60 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 6 and cast(calculated_fields_age_of_home as numeric) < 9 then 0  
when cast(calculated_fields_age_of_home as numeric) >= 60 and cast(calculated_fields_age_of_home as numeric) < 63 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 63 and cast(calculated_fields_age_of_home as numeric) < 66 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 66 and cast(calculated_fields_age_of_home as numeric) < 69 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 69 and cast(calculated_fields_age_of_home as numeric) < 72 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 72 then 0.159  
when cast(calculated_fields_age_of_home as numeric) >= 9 and cast(calculated_fields_age_of_home as numeric) < 12 then 0.005 else -999 end as score_age_of_home
-- age of home done


,calculated_fields_age_of_roof  
,case when calculated_fields_age_of_roof is null or cast(calculated_fields_age_of_roof as numeric) < 3 then 0 
when cast(calculated_fields_age_of_roof as numeric) >= 12 and cast(calculated_fields_age_of_roof as numeric) < 15 then 0.0225 
when cast(calculated_fields_age_of_roof as numeric) >= 15 and cast(calculated_fields_age_of_roof as numeric) < 18 then 0.0425 
when cast(calculated_fields_age_of_roof as numeric) >= 18 and cast(calculated_fields_age_of_roof as numeric) < 21 then 0.0575  
when cast(calculated_fields_age_of_roof as numeric) >= 21 and cast(calculated_fields_age_of_roof as numeric) < 24 then 0.0725 
when cast(calculated_fields_age_of_roof as numeric) >= 24 and cast(calculated_fields_age_of_roof as numeric) < 27 then 0.0875  
when cast(calculated_fields_age_of_roof as numeric) >= 27 and cast(calculated_fields_age_of_roof as numeric) < 30 then 0.105
when cast(calculated_fields_age_of_roof as numeric) >= 3 and cast(calculated_fields_age_of_roof as numeric) < 6 then 0  
when cast(calculated_fields_age_of_roof as numeric) >= 30 and cast(calculated_fields_age_of_roof as numeric) < 33 then 0.1225
when cast(calculated_fields_age_of_roof as numeric) >= 33 and cast(calculated_fields_age_of_roof as numeric) < 36 then 0.14
when cast(calculated_fields_age_of_roof as numeric) >= 36 and cast(calculated_fields_age_of_roof as numeric) < 39 then 0.1575
when cast(calculated_fields_age_of_roof as numeric) >= 39 and cast(calculated_fields_age_of_roof as numeric) < 42 then 0.175  
when cast(calculated_fields_age_of_roof as numeric) >= 42 and cast(calculated_fields_age_of_roof as numeric) < 45 then 0.1925 
when cast(calculated_fields_age_of_roof as numeric) >= 45 and cast(calculated_fields_age_of_roof as numeric) <  48 then 0.21
when cast(calculated_fields_age_of_roof as numeric) >= 48 and cast(calculated_fields_age_of_roof as numeric) <  51 then 0.2125
when cast(calculated_fields_age_of_roof as numeric) >= 51 and cast(calculated_fields_age_of_roof as numeric) <  54 then 0.2325
when cast(calculated_fields_age_of_roof as numeric) >= 54 and cast(calculated_fields_age_of_roof as numeric) <  57 then 0.27
when cast(calculated_fields_age_of_roof as numeric) >= 57 and cast(calculated_fields_age_of_roof as numeric) <  60 then 0.2825
when cast(calculated_fields_age_of_roof as numeric) >= 6 and cast(calculated_fields_age_of_roof as numeric) <  9 then 0.005
when cast(calculated_fields_age_of_roof as numeric) >= 60 and cast(calculated_fields_age_of_roof as numeric) <  63 then 0.3125
when cast(calculated_fields_age_of_roof as numeric) >= 63 and cast(calculated_fields_age_of_roof as numeric) <  66 then 0.3425
when cast(calculated_fields_age_of_roof as numeric) >= 66 and cast(calculated_fields_age_of_roof as numeric) <  69 then 0.3825
when cast(calculated_fields_age_of_roof as numeric) >= 69 and cast(calculated_fields_age_of_roof as numeric) <  72 then 0.4225
when cast(calculated_fields_age_of_roof as numeric) >= 72 then 0.4625
when cast(calculated_fields_age_of_roof as numeric) >= 9 and cast(calculated_fields_age_of_roof as numeric) <  12 then 0.0125 else -999 end as score_age_of_roof
-- age of roof done


,case when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 0 and  cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) < 650 then 0
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1050 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1125 then -0.0192862505368703
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1125 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1200 then -0.0215557846851666
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1200 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1275 then -0.0234390532959475
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1275 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1350 then -0.0239637614726053
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1350 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1425 then -0.0240385555684884
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1425 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1500 then -0.02354269948457
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1500 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1575 then -0.0232592011793575
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1575 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1650 then -0.0231671020793432
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1650 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1725 then -0.0233291325043258
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1725 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1800 then -0.0232690677356905
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1800 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1875 then -0.0233999776953878
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1875 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1950 then -0.0237174146447346
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 1950 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  2025 then -0.0241706653468528
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 2025 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  2100 then -0.023616218513518
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 2100 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  2200 then -0.0219987946813693
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 2200 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  2300 then -0.0194882429774756
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 2300 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  2400 then -0.0165803642847895
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 2400 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  2500 then -0.0141027415071328
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 2500 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  2600 then -0.0130103947168472
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 2600 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  2750 then -0.0143033994946144
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 2750 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  2950 then -0.0165360064947729
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 2950 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  3150 then -0.0188383708629006
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 3150 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  3350 then -0.0197945604238339
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 3350 then -0.020837730861211
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 650 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  750 then -0.00992578840279334
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 750 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  850 then -0.0117924797573053
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 850 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  950 then -0.0143948495762861
when cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) >= 950 and cast(property_data_square_footage as numeric) / (case when property_data_number_of_stories <> "3+" then cast(property_data_number_of_stories as numeric) else 3 end) <  1050 then -0.0170416522944532 else -999 end as score_square_foot_to_stories
-- square foot to stories done


, case when (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >=0 and (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else  coverage_deductible_amount end) < 1001 then 0
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 1001 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  1499 then 0.0374869978188135
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 1499 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  1501 then 0.029098109276572
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 1501 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  2000 then 0.0107964412730233
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 2000 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  2499 then -0.00685527941326763
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 2499 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  2501 then -0.0288655440665447
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 2501 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  2750 then -0.0474370697033899
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 2750 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  3000 then -0.0663923810710794
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 3000 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  3250 then -0.084582180548258
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 3250 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  3500 then -0.10467007065022
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 3500 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  4000 then -0.129151234664007
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 4000 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  4250 then -0.150869356407342
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 4250 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  4500 then -0.170326009070322
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 4500 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  4750 then -0.189004756581711
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 4750 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  5000 then -0.210533752624218
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 5000 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  6000 then -0.242038887106931
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 6000 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  7000 then -0.274551101014638
when  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 7000 and  (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) <  9000 then -0.308544080492525
when (case when coverage_wind_deductible_amount >= coverage_deductible_amount then coverage_wind_deductible_amount else coverage_deductible_amount end) >= 9000 then -0.376621172745304 else -999 end as score_dollar_deductible

, coverage_a
, case when coverage_a >= 260000 and coverage_a < 280000 then 0
when coverage_a <= 60000 then 0
when coverage_a >= 100000 and coverage_a  <  120000 then -0.194009045552369
when coverage_a >= 120000 and coverage_a  <  140000 then -0.154308919554023
when coverage_a >= 140000 and coverage_a  <  160000 then -0.121359542766967
when coverage_a >= 160000 and coverage_a  <  180000 then -0.0944379416907716
when coverage_a >= 180000 and coverage_a  <  200000 then -0.0716760523074786
when coverage_a >= 200000 and coverage_a  <  220000 then -0.0519588068725416
when coverage_a >= 220000 and coverage_a  <  240000 then -0.0345669638275199
when coverage_a >= 240000 and coverage_a  <  260000 then -0.0183129049069389
when coverage_a >= 280000 and coverage_a  <  300000 then 0.016254058920581
when coverage_a >= 300000 and coverage_a  <  320000 then 0.032508117841162
when coverage_a >= 320000 and coverage_a  <  340000 then 0.048762176761743
when coverage_a >= 340000 and coverage_a  <  360000 then 0.0650162356823241
when coverage_a >= 360000 and coverage_a  <  380000 then 0.0812702946029051
when coverage_a >= 380000 and coverage_a  <  400000 then 0.0975243535234861
when coverage_a >= 400000 and coverage_a  <  420000 then 0.112153006552009
when coverage_a >= 420000 and coverage_a  <  440000 then 0.125156253688474
when coverage_a >= 440000 and coverage_a  <  460000 then 0.134908689040822
when coverage_a >= 460000 and coverage_a  <  480000 then 0.142223015555084
when coverage_a >= 480000 and coverage_a  <  500000 then 0.148724639123316
when coverage_a >= 500000 and coverage_a  <  520000 then 0.152788153853462
when coverage_a >= 520000 and coverage_a  <  540000 then 0.156851668583607
when coverage_a >= 540000 and coverage_a  <  560000 then 0.160915183313752
when coverage_a >= 560000 and coverage_a  <  580000 then 0.164978698043897
when coverage_a >= 580000 and coverage_a  <  600000 then 0.169042212774043
when coverage_a >= 60000 and coverage_a  <  80000 then -0.210298728571489
when coverage_a >= 600000 and coverage_a  <  620000 then 0.173105727504188
when coverage_a >= 620000 and coverage_a  <  640000 then 0.177169242234333
when coverage_a >= 640000 and coverage_a  <  660000 then 0.181232756964478
when coverage_a >= 660000 and coverage_a  <  680000 then 0.185296271694624
when coverage_a >= 680000 and coverage_a  <  700000 then 0.189359786424769
when coverage_a >= 700000 and coverage_a  <  720000 then 0.192610598208885
when coverage_a >= 720000 and coverage_a  <  740000 then 0.195861409993001
when coverage_a >= 740000 and coverage_a  <  760000 then 0.198299518831089
when coverage_a >= 760000 and coverage_a  <  780000 then 0.200737627669176
when coverage_a >= 780000 and coverage_a  <  800000 then 0.203175736507263
when coverage_a >= 80000 and coverage_a  <  100000 then -0.206092754000059
when coverage_a >= 800000 and coverage_a  <  820000 then 0.20561384534535
when coverage_a >= 820000 and coverage_a  <  840000 then 0.207239251237408
when coverage_a >= 840000 and coverage_a  <  860000 then 0.208864657129466
when coverage_a >= 860000 and coverage_a  <  880000 then 0.210490063021524
when coverage_a >= 880000 and coverage_a  <  900000 then 0.212115468913582
when coverage_a >= 900000 and coverage_a  <  920000 then 0.21374087480564
when coverage_a >= 920000 and coverage_a  <  940000 then 0.215366280697699
when coverage_a >= 940000 and coverage_a  <  960000 then 0.216991686589757
when coverage_a >= 960000 and coverage_a  <  980000 then 0.218617092481815
when coverage_a >= 980000 then 0.220242498373873 else -999 end as score_coverage_a
-- done
,1.630639505 as score_intercept

from dw_prod_extracts.ext_policy_snapshots 
where 1=1
and date_snapshot = '2020-06-29'
and product <> 'HO5'
and carrier = 'Spinnaker'
and state = 'TX'
-- and status = 'active'
-- and date_policy_effective >= '2020-05-15'
and renewal_number = 0
)
,scoring_inter as (
select *
,score_roof_type +
score_roof_shape +
score_construction_type +
score_age_of_home +
score_age_of_roof +
score_square_foot_to_stories +
score_dollar_deductible +
score_coverage_a+
score_intercept
as lin_comb
from scoring_begin
)
, scoring_final as (
select 
policy_id, state, carrier, product, case when renewal_number > 0 then 'Renewal' else 'New' end as tenure
, calculated_fields_cat_risk_class
, calculated_fields_cat_risk_score
, cov_a
, written_base
, property_data_number_of_family_units
, JSON_EXTRACT_SCALAR(property_data_zillow, '$.zestimate') as zillow_estimate
, property_data_rebuilding_cost
, property_data_swimming_pool
, case when property_data_year_built is null then 'Missing'
      when cast(property_data_year_built as numeric) >= 2000 then 'Post 2000' 
      when cast(property_data_year_built as numeric) > 1980 then 'Pre 2000' 
      else 'Pre 1980' end as year_built
-- , lin_comb
-- , exp(lin_comb) as exponent
, lin_comb as risk_score
from scoring_inter
)
select policy_id, state, product, carrier, tenure, risk_score, calculated_fields_cat_risk_class
, CAST(calculated_fields_cat_risk_score as numeric) - risk_score
from scoring_final
where 1=1
and tenure = 'New' 
and state = 'TX'
and CAST(calculated_fields_cat_risk_score as numeric) - risk_score > 0