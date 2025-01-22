-- Databricks notebook source
-- MAGIC %run "../Lab 0 - Setup/SetCatalogName"

-- COMMAND ----------


select * from bronze.medicaid_enrollees

-- COMMAND ----------

SELECT TO_TIMESTAMP(month, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS') AS month,
* EXCEPT(month)
FROM bronze.medicaid_enrolless

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW clean_enrollees AS
SELECT TO_TIMESTAMP(month, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS') AS month,
* EXCEPT(month)
FROM bronze.medicaid_enrolless

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Using the Databricks Assistant to a Create Silver Table from Temporary View
-- MAGIC ####Try the prompt _using spark sql create or replace silver.medicaid_enrollees tables from clean_enrollees_

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.medicaid_enrollees AS
SELECT *
FROM clean_enrollees
