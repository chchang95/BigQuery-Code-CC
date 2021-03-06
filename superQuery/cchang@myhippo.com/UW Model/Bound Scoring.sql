with scoring_begin as (
select 
policy_id, state, carrier, product, renewal_number, calculated_fields_non_cat_risk_class, calculated_fields_non_cat_risk_score, written_base, property_data_number_of_family_units, property_data_zillow, property_data_rebuilding_cost
, property_data_swimming_pool
, property_data_year_built
-- ,*
,coverage_a as cov_a
,ln(coverage_a) * -0.141714902  as score_cov_a
,insurance_score  
,case when state = 'CA' or state = 'MD' then -0.078330927  
when insurance_score is null or insurance_score = 'no_hit' then 0.093400276  
when insurance_score = 'no_score' then -0.078330927  
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
from dw_prod_extracts.ext_policy_snapshots 
where 1=1
and date_snapshot = '2020-12-31'
and product <> 'ho5'
and carrier <> 'canopius'
-- and status = 'active'
-- and date_policy_effective >= '2020-05-15'
and calculated_fields_non_cat_risk_score is not null
-- and renewal_number = 0
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
policy_id, state, carrier, product, case when renewal_number > 0 then 'Renewal' else 'New' end as tenure
, calculated_fields_non_cat_risk_class
, calculated_fields_non_cat_risk_score
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
, round(exp(lin_comb) / (1+ exp(lin_comb)),6) as risk_score
from scoring_inter
)
select policy_id
, CAST(calculated_fields_non_cat_risk_score as numeric) - risk_score
from scoring_final
-- where policy_id = 3027598
where abs(CAST(calculated_fields_non_cat_risk_score as numeric) - risk_score) > 0.005
-- 2513405
