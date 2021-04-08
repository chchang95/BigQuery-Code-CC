-- Claims outside policy lifecycle 
WITH
  top AS (
  SELECT
    claim_id,
    claim_number,
    ext.policy_id,
    dp.policy_number,
    dp.date_effective,
    dp.date_expires,
    dp.tenure,
    dp.renewal_number,
    dp.next_policy_id,
    dp.previous_terms,
    dp.policy_group_number,
    date_of_loss,
    date_first_notice_of_loss,
    total_incurred,
    claim_status,
    loss_calculated_incurred,
    total_calculated_net_paid,
    total_net_reserves,
    total_recoverable_depreciation,
    cat_code,
    claim_closed_no_total_payment,
    CASE
      WHEN ext.date_effective IS NULL THEN CAST(NULL AS BOOL)
      WHEN (date_of_loss < ext.date_effective
      OR date_of_loss > ext.date_expires) THEN TRUE
    ELSE
    FALSE
  END
    AS is_outside_policy_lifecyle
  FROM
    dw_prod_extracts.ext_claims_inception_to_date ext
  JOIN
    dw_prod.dim_policies dp
  ON
    ext.policy_id = dp.policy_id
  WHERE
    date_knowledge = '2021-03-31' ),
  mid AS (
  SELECT
    *,
    CASE
      WHEN tenure = 'new' AND date_of_loss < date_effective THEN TRUE
    ELSE
    FALSE
  END
    AS is_loss_prior_to_first_term_start,
    CASE
      WHEN date_of_loss > date_expires AND next_policy_id IS NULL THEN TRUE
    ELSE
    FALSE
  END
    AS is_after_expiration_no_term_following,
    CASE
      WHEN date_of_loss < date_effective AND tenure = 'renewal' THEN TRUE
    ELSE
    FALSE
  END
    AS claims_should_be_on_previous_term,
    CASE
      WHEN date_of_loss > date_effective AND next_policy_id IS NOT NULL THEN TRUE
    ELSE
    FALSE
  END
    AS claim_should_be_on_next_term,
  FROM
    top
  WHERE
    is_outside_policy_lifecyle IS TRUE ),
  mid1 AS (
  SELECT
    mid.*,
    dp1.policy_number AS next_policy_number,
    dp1.date_effective AS next_policy_date_effective,
    dp1.date_expires AS next_policy_date_expires,
    dp1.date_activation_update_made AS next_policy_date_went_effective,
    dp0.policy_number AS previous_policy_number,
    dp0.date_effective AS previous_policy_date_effective,
    dp0.date_expires AS previous_policy_date_expires
  FROM
    mid
  LEFT JOIN
    dw_prod.dim_policies dp1
  ON
    mid.next_policy_id = dp1.policy_id
  LEFT JOIN
    dw_prod.dim_policies dp0
  ON
    mid.policy_group_number = dp0.policy_group_number
    AND mid.renewal_number -1 = dp0.renewal_number
  WHERE 1 = 1
 --  and claims_should_be_on_previous_term IS TRUE -- or claim_should_be_on_next_term is true
    ), mid2 as (
SELECT
  *,
 CASE
    WHEN claims_should_be_on_previous_term IS TRUE THEN previous_policy_number
    WHEN claim_should_be_on_next_term IS TRUE THEN next_policy_number
  ELSE
  NULL
END
  AS desired_policy_number_for_claim
FROM
  mid1
  where  1 = 1 -- and claim_should_be_on_next_term --is_after_expiration_no_term_following is true
  )
  select * from mid2