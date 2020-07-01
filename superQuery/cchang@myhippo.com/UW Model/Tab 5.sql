select
policy_id
,state
,carrier
,product
,accident_month
,inception_month
,reinsurance_treaty
,org_id as organization_id
,uw_action
,case when renewal_number = 0 then "New" else "Renewal" end as tenure
,sum(total_incurred) as total_incurred
,sum(case when CAT = 'N' then total_incurred else 0 end) as non_cat_incurred
,sum(case when CAT = 'Y' then total_incurred else 0 end) as cat_incurred
,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as total_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is true and CAT = 'N' then 0 else 1 end) as non_cat_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is true and CAT = 'Y' then 0 else 1 end) as cat_claim_count_x_cnp
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then 100000 else total_incurred end) as capped_non_cat_incurred
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then total_incurred - 100000 else 0 end) as excess_non_cat_incurred
from claims_supp
where ebsl = 'N'
and DATE_DIFF(report_month, inception_month, MONTH) <= 1
group by 1,2,3,4,5,6,7,8,9,10