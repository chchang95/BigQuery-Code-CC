select policy_id, policy_number
,calculated_fields_cat_risk_class
,calculated_fields_non_cat_risk_class
from dw_prod_extracts.ext_policy_snapshots
where date_policy_effective >= '2020-05-01'
and date_snapshot = '2020-08-31'
where calculated_fields_non_cat_risk_class is null