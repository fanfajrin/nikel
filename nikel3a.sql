WITH lateloan AS (
  SELECT 
    COUNT(*) AS cnt_late_loan
  FROM pt-mp-gcp-data.development_gposlite.ln_lvl
  WHERE status != 'CANCELLED' AND current_dpd > 0
)
,loan AS (
  SELECT 
    COUNT(*) AS cnt_loan
  FROM pt-mp-gcp-data.development_gposlite.ln_lvl
  WHERE status != 'CANCELLED' AND current_dpd = 0
)
SELECT ROUND(lateloan.cnt_late_loan/(SELECT cnt_loan FROM loan),2) AS lateloan_pct FROM lateloan