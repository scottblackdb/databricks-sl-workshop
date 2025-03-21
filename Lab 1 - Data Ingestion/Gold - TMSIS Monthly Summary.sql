-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Lab 1: Creating TMSIS Gold Layer
-- MAGIC
-- MAGIC In this notebook, we will create monthly aggregration of TMSIS and store the table in the gold layer.
-- MAGIC This gold table will be used later to power a dashboard.
-- MAGIC
-- MAGIC ## Objectives
-- MAGIC 2. **Data Transformation**: Aggregate the TMSIS data by month and adding the total amount of the claims.
-- MAGIC 3. **Create a New Table**: Create a new table in the gold layer to store the data.
-- MAGIC
-- MAGIC Let's get started!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Setup
-- MAGIC First step in all the labs will be to run a setup notebook. This notebook to create a catalog to store data and to set your default catalog 

-- COMMAND ----------

-- MAGIC %run "../Lab 0 - Setup/SetCatalogName"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using the ROUND and SUM functions calculate the total claims for the month. The DATE_TRUNC function is also used to remove any time component of the timestamp.

-- COMMAND ----------


SELECT 
  DATE_TRUNC('day', DSCHRG_DT) AS discharge_date, 
  ROUND(SUM(tot_mdcd_pd_amt),0) AS total_amount
FROM 
  silver.tmsis_claims
WHERE DSCHRG_DT IS NOT NULL
GROUP BY 
 discharge_date
ORDER BY discharge_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using the SQL query from above create the gold TMSIS table

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.tmsis_daily_billing_summary AS
SELECT 
  DATE_TRUNC('day', DSCHRG_DT) AS discharge_date, 
  ROUND(SUM(tot_mdcd_pd_amt),0) AS total_amount
FROM 
  silver.tmsis_claims
WHERE DSCHRG_DT IS NOT NULL
GROUP BY 
 discharge_date
ORDER BY discharge_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Congratulations on Completing the Notebook
