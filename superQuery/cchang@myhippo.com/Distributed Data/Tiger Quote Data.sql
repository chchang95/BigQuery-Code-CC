-- construction_type
-- property_data_number_of_stories, year_built, hurricane_deductible, wind_deductible, coverage_a, coverage_b, coverage_c, coverage_d, deductible, property_data_number_of_family_units

-- with x as (
select 
quote_id
,state
,county
,zip_code
,street
,city
, coverage_a
, coverage_b
, coverage_c
, coverage_d
, deductible
, hurricane_deductible
, wind_deductible
, construction_type
, property_data_number_of_family_units
, property_data_number_of_stories
, year_built 
, calculated_fields_wind_exclusion as wind_exclusion
from dw_prod.dim_quotes
where carrier <> 'Canopius'
and product = 'ho5'
-- and date_quote_first_seen >= '2019-01-01'
and date_quote_first_seen <= '2020-09-30'
and (calculated_fields_wind_exclusion <> 'true' or calculated_fields_wind_exclusion is null)
-- )
-- select state, count(*) from x
-- group by 1
-- order by 2

-- select calculated_fields_wind_exclusion, count(*) from dw_prod.dim_quotes
-- group by 1
-- order by 2