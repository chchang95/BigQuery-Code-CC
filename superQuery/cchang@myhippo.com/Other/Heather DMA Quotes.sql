select state, property_data_address_county, count(distinct policy_id) as total_policy_holders
, sum(written_base + written_total_optionals + written_policy_fee) as total_written_premium
, sum(written_base + written_total_optionals + written_policy_fee) / count(distinct policy_id) as average_bound_policy_cost 
from dw_staging_extracts.ext_policy_snapshots eps
where eps.date_snapshot = '2020-07-31'
and product <> 'HO5'
and carrier <> 'Canopius'
and status = 'active'
-- and property_data_address_county --= 'DeKalb'
-- in ('Cook', 'DeKalb', 'Dupage', 'Grundy', 'Kane', 'Kankakee', 'Kendall', 'LaSalle', 'Lake', 'McHenry', 'Will') 
--in ('Jasper', 'La Porte', 'Lake', 'Newton', 'Porter')
-- and state = 'IL'
--and state = 'IN'
group by property_data_address_county, state
order by property_data_address_county