WITH BASE AS(
  SELECT
    'BANK_A' AS partner_code
    ,SUM(loan_amount) AS loan_amt_banka
  FROM pt-mp-gcp-data.development_gposlite.ln_lvl
  WHERE partner = 'BANK_A'
)
,BASE2 AS(
  SELECT * FROM pt-mp-gcp-data.development_gposlite.pn_lvl
)

SELECT BASE.*,BASE2.name AS Partners_Name FROM BASE
LEFT JOIN BASE2
ON BASE.partner_code = BASE2.partner