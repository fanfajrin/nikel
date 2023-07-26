CREATE OR REPLACE TABLE pt-mp-gcp-data.development_gposlite.ln_lvl_summary
AS
WITH BASE AS (
  SELECT 
    CASE 
      WHEN current_dpd = 0 THEN 'CURRENT'
      WHEN current_dpd >= 1 AND current_DPD <= 30 THEN 'DPD160'
      WHEN current_dpd >= 30 AND current_DPD <= 60 THEN 'DPD3060'
      ELSE 'DPD61' END AS DPD_BUCKET
    ,AVG(loan_term) AS AVG_LOAN_TERM
    ,COUNT(CASE WHEN current_dpd > 0 THEN 1 END) AS COUNT_NPL
  
  FROM pt-mp-gcp-data.development_gposlite.ln_lvl
  WHERE status != 'CANCELLED'
  GROUP BY 1
)
SELECT * FROM BASE
ORDER BY DPD_BUCKET
