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
from dw_prod_extracts.ext_policy_snapshots eps
    left join (select policy_id from dw_prod_extracts.ext_actuarial_monthly_loss_ratios_loss where date_bordereau = '2020-08-31') claims using(policy_id)
    left join (select id, policy_number, json_extract_scalar(policy_info,'$.property_data.purchase_date') as purchase_date
    from postgres_public.policies 
    where bound = true ) pd on pd.id = eps.policy_id
where date_snapshot = '2020-08-31'
and claims.policy_id is not null
and (pd.purchase_date = 'next_3_months' or pd.purchase_date = 'last_12_months')
and carrier <> 'Canopius'
and product <> 'HO5'