with policy_claims as (
select distinct policy_id 
, date_policy_effective
, personal_information_first_name as first_name
, personal_information_last_name as last_name
, personal_information_email as email
, personal_information_phone_number as phone_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
-- , 'c' as type
from dw_prod_extracts.ext_policy_snapshots
left join (select policy_id from dw_prod_extracts.ext_actuarial_monthly_loss_ratios_loss where date_bordereau = '2020-08-31') claims using(policy_id)
where date_snapshot = '2020-08-31'
and claims.policy_id is not null
and carrier <> 'Canopius'
and product <> 'HO5'
), active_policies as (
select policy_id
, date_policy_effective
, personal_information_first_name as first_name
, personal_information_last_name as last_name
, personal_information_email as email
, personal_information_phone_number as phone_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
-- , 'r' as type
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-08-31'
and carrier <> 'Canopius'
and product <> 'HO5'
and status = 'active'
and earned_exposure > 0
-- and date_policy_effective >= '2020-01-01'
), since_2019_policies as (
select policy_id
, date_policy_effective
, personal_information_first_name as first_name
, personal_information_last_name as last_name
, personal_information_email as email
, personal_information_phone_number as phone_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
-- , 'r' as type
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-08-31'
and carrier <> 'Canopius'
-- and product <> 'HO5'
and earned_exposure > 0
-- and status = 'active'
and date_policy_effective >= '2019-01-01'
), since_2018_policies as (
select policy_id
, date_policy_effective
, personal_information_first_name as first_name
, personal_information_last_name as last_name
, personal_information_email as email
, personal_information_phone_number as phone_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
-- , 'r' as type
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-08-31'
and carrier <> 'Canopius'
and product <> 'HO5'
and earned_exposure > 0.05
-- and status = 'active'
and date_policy_effective < '2019-01-01'
and date_policy_effective > '2017-01-01'
)
, combined as (
select * from policy_claims
union all
select * from active_policies
union all
select * from since_2019_policies
union all
select * from since_2018_policies
)
select distinct * from combined