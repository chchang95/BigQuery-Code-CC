WITH spinnaker_claims AS (
 SELECT *
 FROM `datateam-248616`.`dw_staging_extracts`.`ext_all_claims_combined_running_month`
 WHERE carrier = 'Spinnaker'
),
topa_claims AS (
  SELECT *
 FROM `datateam-248616`.`dw_staging_extracts`.`ext_all_claims_combined_running_month`
 WHERE carrier = 'Topa'
),
spin_desired_dates AS (
 SELECT DISTINCT date_report_period_end
 FROM spinnaker_claims
 WHERE tbl_source = 'hippo_claims'
),
topa_desired_dates AS (
  SELECT DISTINCT date_report_period_end
 FROM topa_claims
 WHERE tbl_source = 'hippo_claims'
),
spin_most_recent_tpa_report_date AS (
 SELECT spin_desired_dates.date_report_period_end AS desired_report_date,
        MAX(tpa.date_report_period_end) AS most_recent_report_date_before_desired
 FROM spin_desired_dates
 INNER JOIN spinnaker_claims tpa
 ON tpa.date_report_period_end <= spin_desired_dates.date_report_period_end
 WHERE tpa.tbl_source = 'spinnaker_tpa_claims'
 GROUP BY 1
),
topa_most_recent_tpa_report_date AS (
 SELECT topa_desired_dates.date_report_period_end AS desired_report_date,
        MAX(tpa.date_report_period_end) AS most_recent_report_date_before_desired
 FROM topa_desired_dates
 INNER JOIN topa_claims tpa
 ON tpa.date_report_period_end <= topa_desired_dates.date_report_period_end
 WHERE tpa.tbl_source = 'topa_tpa_claims'
 GROUP BY 1
),
spin_tpa_data_filled AS (
 SELECT desired_report_date AS date_report_period_end,
        combined.* EXCEPT(date_report_period_end),
 FROM spinnaker_claims combined
 INNER JOIN spin_most_recent_tpa_report_date
 ON combined.date_report_period_end = spin_most_recent_tpa_report_date.most_recent_report_date_before_desired
 WHERE combined.tbl_source = 'spinnaker_tpa_claims'
),
topa_tpa_data_filled AS (
 SELECT desired_report_date AS date_report_period_end,
        combined.* EXCEPT(date_report_period_end),
 FROM topa_claims combined
 INNER JOIN topa_most_recent_tpa_report_date
 ON combined.date_report_period_end = topa_most_recent_tpa_report_date.most_recent_report_date_before_desired
 WHERE combined.tbl_source = 'topa_tpa_claims'
)
, pre_claims AS (
SELECT *
FROM spin_tpa_data_filled
WHERE claim_closed_no_total_payment IS FALSE
UNION ALL
SELECT *
FROM topa_tpa_data_filled
WHERE claim_closed_no_total_payment IS FALSE
UNION ALL
SELECT *
FROM spinnaker_claims
WHERE tbl_source = 'hippo_claims'
AND claim_closed_no_total_payment IS FALSE
UNION ALL
SELECT *
FROM topa_claims
WHERE tbl_source = 'hippo_claims'
AND claim_closed_no_total_payment IS FALSE
),
new_cats as (
SELECT * EXCEPT(is_cat)
, CASE WHEN tbl_source = 'hippo_claims' and peril in ('wind', 'hail')
  THEN TRUE
  ELSE is_cat end as is_cat,
FROM pre_claims
),
claims AS (
 SELECT *,
   CASE
     WHEN organization_id IS NULL AND attribution_channel = 'Online' then 0
     WHEN organization_id IS NULL AND attribution_channel = 'Agent' then -99
     ELSE organization_id  END
     AS fixed_attributed_organization_id,
   CASE
     WHEN is_ebsl is true AND reinsurance_treaty NOT IN ('Spkr19_HSBNew', 'Spkr19_HSBOld')
     THEN concat(reinsurance_treaty,'_EBSL')
     ELSE reinsurance_treaty   END
     AS reinsurance_treaty_add,
   CASE
     WHEN is_cat IS TRUE THEN 0
     WHEN is_cat IS FALSE AND total_incurred >= 100000 THEN 100000
     ELSE total_incurred END
     AS non_cat_total_incurred_capped_at_100k,
   CASE
     WHEN is_cat IS TRUE THEN 0
     WHEN is_cat IS FALSE AND total_incurred >= 100000 THEN total_incurred - 100000
     ELSE 0 END
     AS excess_loss_non_cat,
   date_report_period_end AS date_bordereau,
   accident_Month AS date_accident_month_begin,
   DATE_SUB(DATE_TRUNC(DATE_ADD(date_of_loss, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) AS date_accident_month_end,
   CASE
     WHEN policy_number LIKE '%-00' THEN 'new'
     ELSE 'renewal' END
     AS tenure,
 FROM new_cats
) select 
        case when tbl_source = 'hippo_claims' then 'Hippo' 
        when tbl_source = 'topa_tpa_claims' then 'TPA'
        when tbl_source = 'spinnaker_tpa_claims' then 'TPA'
        else 'ERROR' end as ClaimsHandler
        ,lower(Carrier) as carrier
        ,property_data_address_state as Policy_State
        ,date_trunc(date_of_loss, MONTH) as accident_month
        ,lower(Product) as Product
        ,claims_policy_number
        ,date_effective
        ,date_expires
        ,Claim_Number
        ,date_of_loss
        ,date_first_notice_of_loss
        ,property_data_address_city
        ,property_data_address_state
        ,property_data_address_zip
        ,claim_status
        ,peril
        ,date_close
        ,case when is_cat is false then 'N' else 'Y' end as CAT_indicator
        ,'' as placeholder
        ,is_ebsl
        ,loss_paid
        ,Loss_Net_Reserve
        ,expense_paid
        ,expense_net_reserve
        ,recoveries
        ,loss_paid + loss_net_reserve + expense_paid + expense_net_reserve - recoveries as incurred
        ,fixed_attributed_organization_id as organization_id
        ,CAT_code as internal_CAT_code
from claims
where is_ebsl is false
and date_bordereau = '2020-07-31'