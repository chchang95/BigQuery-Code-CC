with loss as (
select * 
from dw_staging_extracts.ext_actuarial_monthly_loss_ratios_loss
)
, premium as (
select * 
from dw_staging_extracts.ext_actuarial_monthly_loss_ratios_premium
)
, policies as (
select eps.policy_id, eps.policy_number
, carrier
, state
, product
, date_trunc(date_policy_effective, MONTH) as effective_month
, case when renewal_number > 0 then 'Renewal' else 'New' end as tenure 
, channel
, org_id as organization_id
from dw_prod_extracts.ext_policy_snapshots eps
    left join (select policy_id, policy_number, channel, attributed_organization_id
    , case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on eps.policy_id = dp.policy_id
where date_snapshot = '2020-08-31'
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
, enhanced AS (
  SELECT combined.*,
         EXTRACT(year FROM date_accident_month_begin) AS accident_year,
         policies.carrier,
         policies.organization_id,
         policies.channel,
         policies.state,
         policies.product,
         policies.tenure,
         policies.effective_month
  FROM combined
  INNER JOIN policies
  USING(policy_id)
)
, final as (
SELECT 
date_bordereau
,state
,carrier
,product
,accident_year
,date_accident_month_begin
,date_accident_month_end
,reinsurance_treaty
,organization_id
,channel
,tenure
,effective_month
      ,COALESCE(CAST(SUM(earned) AS FLOAT64),0) AS earned,
      COALESCE(CAST(SUM(written) AS FLOAT64),0) AS written,
      COALESCE(CAST(SUM(earned_exposure) AS FLOAT64),0) AS earned_exposure,
      COALESCE(CAST(SUM(earned_tiv) AS FLOAT64),0) AS earned_tiv,
      COALESCE(SUM(total_Claim_Count),0) AS total_Claim_Count,
      COALESCE(SUM(Total_Paid_Indemnity),0) AS Total_Paid_Indemnity,
      COALESCE(SUM(Total_Case_Reserve_Indemnity),0) AS Total_Case_Reserve_Indemnity,
      COALESCE(SUM(Total_Paid_ALAE),0) AS Total_Paid_ALAE,
      COALESCE(SUM(Total_Case_Reserve_ALAE),0) AS Total_Case_Reserve_ALAE,
      COALESCE(SUM(Salvage_Subro),0) AS Salvage_Subro,
      COALESCE(SUM(Total_incurred_Indemnity),0) AS Total_Incurred_Indemnity,
      COALESCE(SUM(Total_Incurred_ALAE),0) AS Total_Incurred_ALAE,
      COALESCE(SUM(Total_Incurred_Loss_and_ALAE),0) AS Total_Incurred_Loss_and_ALAE,
      COALESCE(SUM(Claim_Count_CAT),0) AS Claim_Count_CAT,
    --   COALESCE(SUM(total_cat_incurred_loss_and_alae),0) AS total_cat_incurred_loss_and_alae,
      COALESCE(SUM(Incurred_Loss_CAT),0) AS Incurred_Loss_CAT,
      COALESCE(SUM(Claim_Count_NonCAT),0) AS Claim_Count_NonCAT,
      COALESCE(SUM(Capped_NonCAT_loss_and_ALAE),0) AS Capped_NonCAT_loss_and_ALAE,
      COALESCE(SUM(Excess_Loss_NonCAT),0) AS Excess_Loss_NonCAT,
      COALESCE(SUM(Incurred_Loss_NonCAT), 0) AS Incurred_Loss_NonCAT,
      COALESCE(SUM(Excess_Count_NonCAT),0) AS Excess_Count_NonCAT,
FROM enhanced
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12
)
, aggregated as (
select date_accident_month_begin, 
SUM(coalesce(total_incurred_loss_and_alae,0)) as total_incurred,
sum(coalesce(Incurred_Loss_CAT,0)) as total_cat,
sum(coalesce(Incurred_Loss_NonCAT,0)) as total_noncat
from final
where date_bordereau = '2020-08-31'
and reinsurance_treaty not in ('Spkr17_MRDP_EBSL','Topa_EBSL','Spkr19_HSBOld','Spkr19_HSBNew','Canopius')
group by 1
order by 1
)
select * from final
where date_bordereau = '2020-08-31'
and reinsurance_treaty not in ('Spkr17_MRDP_EBSL','Topa_EBSL','Spkr19_HSBOld','Spkr19_HSBNew','Canopius')
-- select * from aggregated