-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Lab 1: Creating TMSIS Silver Layer
-- MAGIC
-- MAGIC In this notebook, we will apply transformations from the bronze layer of the TMSIS data to create the silver layer.
-- MAGIC
-- MAGIC ## Objectives
-- MAGIC 1. **Enhance Bronze Reviews**: Learn how to import and preview medical center data from SQL Server using Lakehouse Federation.
-- MAGIC 2. **Data Transformation**: Add a categorical column based on the numerical value of the review
-- MAGIC 3. **Create a New Table**: Create a new table in the silver layer to store the data.
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
-- MAGIC Run the SQL query below. From the column names it appears several date columns are being selected but numbers are being returned.
-- MAGIC The data was receive as unix timestamps and we will convert them to date/time to make them easier to use.

-- COMMAND ----------

select adjdctn_dt, admission_dt,birth_dt, mdcd_pd_dt,srvc_bgnng_dt,srvc_endg_dt,dschrg_dt
from bronze.tmsis_claims

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the SQL query two functions are used. The first function from_unixtime takes a number and returns a date as a string. We want to store the value as an actual date not a string so the function to_timestamp is used to convert the string into a timestamp data type.
-- MAGIC
-- MAGIC A new column CLAIM_RCV_DT will also be added to store the current timestamp on when the data was added to the table

-- COMMAND ----------

select * except (adjdctn_dt,admission_dt,birth_dt,mdcd_pd_dt,srvc_bgnng_dt,srvc_endg_dt,dschrg_dt),
  to_timestamp(from_unixtime(dschrg_dt/1000)) as DSCHRG_DT, 
  to_timestamp(from_unixtime(adjdctn_dt/1000)) as ADJDCTN_DT, 
  to_timestamp(from_unixtime(admission_dt/1000)) as ADMISSION_DT,
  to_timestamp(from_unixtime(birth_dt/1000)) as BIRTH_DT,
  to_timestamp(from_unixtime(mdcd_pd_dt/1000)) as MDCD_PD_DT,
  to_timestamp(from_unixtime(srvc_bgnng_dt/1000)) as SRVC_BGNNG_DT,
  to_timestamp(from_unixtime(srvc_endg_dt/1000)) SRVC_ENDG_DT,
  current_timestamp as CLAIM_RCV_DT
from bronze.tmsis_claims

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using the SQL statement above we are going to create or replace a table but with a slight twist.
-- MAGIC Databricks offers many performance optimizations to make querying very large datasets fast called liquid clustering.
-- MAGIC
-- MAGIC **Databricks Liquid Clustering**: Automatically optimizes data layout and file sizes for efficient querying. Simply specifiy which columns will frequently be used in WHERE or JOIN clauses and Databricks takes care of the rest.

-- COMMAND ----------

--Complete the beginning of the SQL clause to create a table. The table name has been provided for you
 silver.tmsis_claims
as
select * except (adjdctn_dt,admission_dt,birth_dt,mdcd_pd_dt,srvc_bgnng_dt,srvc_endg_dt,dschrg_dt),
  to_timestamp(from_unixtime(dschrg_dt/1000)) as DSCHRG_DT, 
  to_timestamp(from_unixtime(adjdctn_dt/1000)) as ADJDCTN_DT, 
  to_timestamp(from_unixtime(admission_dt/1000)) as ADMISSION_DT,
  to_timestamp(from_unixtime(birth_dt/1000)) as BIRTH_DT,
  to_timestamp(from_unixtime(mdcd_pd_dt/1000)) as MDCD_PD_DT,
  to_timestamp(from_unixtime(srvc_bgnng_dt/1000)) as SRVC_BGNNG_DT,
  to_timestamp(from_unixtime(srvc_endg_dt/1000)) SRVC_ENDG_DT,
  current_timestamp as CLAIM_RCV_DT
from bronze.tmsis_claims
cluster by DSCHRG_DT,ADMISSION_DT,CLAIM_RCV_DT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Congratulations on Completing the Notebook
