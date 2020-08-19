select date_accident_month_begin, date_accident_month_end, tenure, state, product, reinsurance_treaty, tbl_source
, sum(total_claim_count) as total_claim_count
, sum(Total_Paid_Indemnity) as Total_Paid_Indemnity
, sum(Total_Case_Reserve_Indemnity) as Total_Case_Reserve_Indemnity
, sum(Total_incurred_Indemnity) as Total_incurred_Indemnity
, sum(Total_Paid_ALAE) as Total_Paid_ALAE
, sum(Total_Case_Reserve_ALAE) as Total_Case_Reserve_ALAE
, sum(Salvage_Subro) as Salvage_Subro
, sum(Total_Incurred_Loss_and_ALAE) as Total_Incurred_Loss_and_ALAE
, sum(Claim_Count_CAT) as Claim_Count_CAT	
, sum(coalesce(total_cat_incurred_loss_and_alae, 0)) as total_cat_incurred_loss_and_alae
, sum(coalesce(Incurred_Loss_CAT, 0)) as Incurred_Loss_CAT
, sum(Claim_Count_NonCAT) as Claim_Count_NonCAT
, sum(Capped_NonCAT) as Capped_NonCAT
, sum(Incurred_Loss_NonCAT) as Incurred_Loss_NonCAT
, sum(Excess_Count_NonCAT) as Excess_Count_NonCAT
from dbt_corina.losses_aug19_v1 
where date_bordereau = '2020-06-30'--'2020-05-31' -- group by 2
--and lower(state) = 'ga'
--and date_accident_month_begin = '2020-01-01'
group by 1, 2, 3, 4, 5, 6, 7
order by 2 desc