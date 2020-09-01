with loss as (
select * 
from dw_staging_extracts.ext_actuarial_monthly_loss_ratios_loss
)
, premium as (
select * 
from dw_staging_extracts.ext_actuarial_monthly_loss_ratios_premium
)
, policies as (
select *
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-07-31'
)
, combined AS (
  SELECT COALESCE(premium.policy_id, loss.policy_id) AS policy_id,
         COALESCE(premium.reinsurance_treaty, loss.reinsurance_treaty_add) AS reinsurance_treaty,
         COALESCE(premium.date_knowledge, loss.date_bordereau) AS date_bordereau,
         COALESCE(premium.date_report_period_start, loss.date_accident_month_begin) AS date_accident_month_begin,
         COALESCE(premium.date_report_period_end, loss.date_accident_month_end) AS date_accident_month_end,
         premium.* EXCEPT(policy_id,reinsurance_treaty,date_knowledge,date_report_period_start,date_report_period_end),
         loss.* EXCEPT(policy_id,reinsurance_treaty_add,date_bordereau,date_accident_month_begin,date_accident_month_end)
  FROM loss
  FULL OUTER JOIN premium
  ON premium.policy_id = loss.policy_id
  AND premium.reinsurance_treaty = loss.reinsurance_treaty_add
  AND premium.date_knowledge = loss.date_bordereau
  AND premium.date_report_period_start = loss.date_accident_month_begin
)
-- select date_accident_month_begin, 
-- SUM(coalesce(total_incurred_loss_and_alae,0)) as total_incurred,
-- sum(coalesce(incurred_loss_cat,0)) as total_cat,
-- sum(coalesce(incurred_loss_noncat,0)) as total_noncat
-- from combined
-- where date_bordereau = '2020-07-31'
-- group by 1
-- order by 1
select distinct reinsurance_treaty
from combined

-- , enhanced AS (
--   SELECT combined.*,
--          EXTRACT(year FROM date_accident_month_begin) AS accident_year,
--          policies.carrier,
--          policies.organization_id,
--          policies.channel,
--          CASE
--           WHEN policies.attributed_organization_id IS NULL AND channel = 'Online' THEN 0
--           WHEN policies.attributed_organization_id IS NULL AND channel = 'Agent' THEN -99
--           ELSE organization_id
--          END AS fixed_attributed_organization_id,
--          policies.state,
--          policies.product,
--          CASE
--           WHEN policies.renewal_number = 0 THEN 'new'
--           ELSE 'renewal'
--          END AS tenure
--   FROM combined
--   INNER JOIN policies
--   USING(policy_id)
-- )
-- SELECT reinsurance_treaty,
--       state,
--       carrier,
--       product,
--       date_bordereau,
--       accident_year,
--       date_accident_month_begin,
--       date_accident_month_end,
--       organization_id,
--       channel,
--       fixed_attributed_organization_id,
--       tenure,
--       COALESCE(CAST(SUM(earned) AS FLOAT64),0) AS earned,
--       COALESCE(CAST(SUM(written) AS FLOAT64),0) AS written,
--       COALESCE(CAST(SUM(earned_exposure) AS FLOAT64),0) AS earned_exposure,
--       COALESCE(CAST(SUM(earned_tiv) AS FLOAT64),0) AS earned_tiv,
--       COALESCE(SUM(total_Claim_Count),0) AS total_Claim_Count,
--       COALESCE(SUM(Total_Paid_Indemnity),0) AS Total_Paid_Indemnity,
--       COALESCE(SUM(Total_Case_Reserve_Indemnity),0) AS Total_Case_Reserve_Indemnity,
--       COALESCE(SUM(Total_Paid_ALAE),0) AS Total_Paid_ALAE,
--       COALESCE(SUM(Total_Case_Reserve_ALAE),0) AS Total_Case_Reserve_ALAE,
--       COALESCE(SUM(Salvage_Subro),0) AS Salvage_Subro,
--       COALESCE(SUM(Total_incurred_Indemnity),0) AS Total_Incurred_Indemnity,
--       COALESCE(SUM(Total_Incurred_ALAE),0) AS Total_Incurred_ALAE,
--       COALESCE(SUM(Total_Incurred_Loss_and_ALAE),0) AS Total_Incurred_Loss_and_ALAE,
--       COALESCE(SUM(Claim_Count_CAT),0) AS Claim_Count_CAT,
--     --   COALESCE(SUM(total_cat_incurred_loss_and_alae),0) AS total_cat_incurred_loss_and_alae,
--       COALESCE(SUM(Incurred_Loss_CAT),0) AS Total_Incurred_Loss_CAT,
--       COALESCE(SUM(Claim_Count_NonCAT),0) AS Claim_Count_NonCAT,
--       COALESCE(SUM(Capped_NonCAT_loss_and_ALAE),0) AS Capped_NonCAT_loss_and_ALAE,
--       COALESCE(SUM(Excess_Loss_NonCAT),0) AS Excess_Loss_NonCAT,
--       COALESCE(SUM(Incurred_Loss_NonCAT), 0) AS Total_Incurred_Loss_NonCAT,
--       COALESCE(SUM(Excess_Count_NonCAT),0) AS Excess_Count_NonCAT,
--     --   {% for peril_type in peril_type_list %}
--     --     COALESCE(SUM(Claim_Count_{{ peril_type }}_CAT),0) AS Claim_Count_{{ peril_type }}_CAT,
--     --     COALESCE(SUM(Incurred_Loss_{{ peril_type }}_CAT),0) AS Incurred_Loss_{{ peril_type }}_CAT,
--     --     COALESCE(SUM(Claim_Count_{{ peril_type }}_NonCAT),0) AS Claim_Count_{{ peril_type }}_NonCAT,
--     --     COALESCE(SUM(Capped_Loss_{{ peril_type }}_NonCAT),0) AS Capped_Loss_{{ peril_type }}_NonCAT,
--     --     COALESCE(SUM(Excess_Loss_{{ peril_type }}_NonCAT),0) AS Excess_Loss_{{ peril_type }}_NonCAT,
--     --     COALESCE(SUM(Incurred_Loss_{{ peril_type }}_NonCAT),0) AS Incurred_Loss_{{ peril_type }}_NonCAT,
--     --     COALESCE(SUM(Excess_Count_{{ peril_type }}_NonCAT),0) AS Excess_Count_{{ peril_type }}_NonCAT,
--     --   {% endfor %}
-- FROM enhanced
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12