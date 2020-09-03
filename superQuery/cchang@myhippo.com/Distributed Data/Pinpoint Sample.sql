select policy_id
, personal_information_first_name as first_name
, personal_information_last_name as last_name
, personal_information_email as email
, personal_information_phone_number as phone_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
, 'r' as type
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-08-31'
and carrier <> 'Canopius'
and product <> 'HO5'
and date_policy_effective >= '2020-01-01'