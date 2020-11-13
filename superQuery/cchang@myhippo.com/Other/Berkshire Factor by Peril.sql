with policies as (
select policy_id, policy_number, state, product, carrier, date_policy_effective, status, quote_rater_version, latest_policy_update_id
,case when region_code is null then 'D' else region_code end as tx_region
from dw_prod_extracts.ext_policy_snapshots eps
left join dw_prod.texas_region_mappings map on safe_cast(eps.property_data_address_zip as numeric) = map.zip_code
where state = 'tx'
and product = 'ho3'
and status = 'active'
and carrier = 'spinnaker'
and date_snapshot = '2020-09-30'
and quote_rater_version in ('5.0','6.0','6.1','7.0')
)
, combined as (
select 
-- efg.*
p.*
,name
,safe_cast(index as numeric) as index
,safe_cast(p1 as numeric) as p1
,safe_cast(p2 as numeric) as p2
,safe_cast(p3 as numeric) as p3
,safe_cast(p4 as numeric) as p4
,safe_cast(p5 as numeric) as p5
,safe_cast(p6 as numeric) as p6
,safe_cast(p7 as numeric) as p7
,safe_cast(p8 as numeric) as p8
,safe_cast(p9 as numeric) as p9
,safe_cast(p10 as numeric) as p10
from dw_prod_extracts.ext_factor_grids efg
left join policies p on
efg.policy_id = p.policy_id
and efg.policy_update_id = p.latest_policy_update_id
where p.policy_id is not null
and (SAFE_CAST(index as numeric) is not null or name = 'Total')
-- and p.policy_number = 'HTX-2633748-00'
)
select index, name, quote_rater_version
-- , tx_region
, avg(p1) as p1
, avg(p2) as p2
, avg(p3) as p3
, avg(p4) as p4
, avg(p5) as p5
, avg(p6) as p6
, avg(p7) as p7
, avg(p8) as p8
, avg(p9) as p9
, avg(p10) as p10
from combined
group by 1, 2, 3
order by 1

-- select *, case when SAFE_CAST(index as numeric) is null then 'N' else 'Y' end as numeric from dw_prod_extracts.ext_factor_grids
-- where policy_number = 'HTX-1863461-00'