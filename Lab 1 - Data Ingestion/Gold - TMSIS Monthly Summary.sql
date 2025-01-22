-- Databricks notebook source
-- MAGIC %run "../Lab 0 - Setup/SetCatalogName"

-- COMMAND ----------


SELECT 
  DATE_TRUNC('day', DSCHRG_DT) AS discharge_date, 
  ROUND(SUM(tot_mdcd_pd_amt),0) AS total_amount
FROM 
  scott_dev.silver.tmsis_claims
WHERE DSCHRG_DT IS NOT NULL
GROUP BY 
 discharge_date
ORDER BY discharge_date

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.tmsis_daily_billing_summary AS
SELECT 
  DATE_TRUNC('day', DSCHRG_DT) AS discharge_date, 
  ROUND(SUM(tot_mdcd_pd_amt),0) AS total_amount
FROM 
  scott_dev.silver.tmsis_claims
WHERE DSCHRG_DT IS NOT NULL
GROUP BY 
 discharge_date
ORDER BY discharge_date

-- COMMAND ----------


