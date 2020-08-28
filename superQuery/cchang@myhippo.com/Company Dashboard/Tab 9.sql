select reinsurance_treaty
, sum(Total_Incurred_Loss_and_ALAE) as total_incurred, sum(earned) as EP, sum(Total_Incurred_Loss_and_ALAE) /sum(earned) as total_LR, 
,sum(total_claim_count) as total_CC, sum(Total_Incurred_Loss_and_ALAE) /sum(total_claim_count) as total_SEV, 
,sum(earned_exposure) as total_EE, sum(total_claim_count) / sum(earned_exposure) as total_FREQ
from dw_staging_extracts.ext_actuarial_monthly_loss_ratios_combined
where date_bordereau = '2020-06-30'
group by 1
order by 1