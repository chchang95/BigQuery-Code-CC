with x as (
select cd.*
-- ,org_id
-- ,renewal_number
-- ,uw_action
,date_trunc(date_of_loss, MONTH) as accident_month
,date_trunc(date_first_notice_of_loss, MONTH) as report_month
,date_trunc(date_effective, MONTH) as inception_month
, case when peril = 'equipment_breakdown' or peril = 'service_line' then 'Y'
      else 'N' end as EBSL
, case when peril = 'wind' or peril = 'hail' then 'Y'
        when cat_code is not null then 'Y'
        else 'N' end as CAT
from dw_prod_extracts.ext_claims_inception_to_date cd
-- left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on cd.policy_id = dp.policy_id
-- left join snapshot eps 
    -- on cd.policy_id = eps.policy_id
  WHERE date_knowledge = '2020-06-30'
  and carrier <> 'Canopius'
  )
  select * from x where CAT <> 'N' or CAT <> 'Y'