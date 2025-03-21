-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 1: Ingesting and Processing Medical Center Data with Databricks
-- MAGIC
-- MAGIC Welcome to the Databricks Lab 1! In this notebook, we will guide you through the process of ingesting and processing medical center data using Databricks. This lab is designed for users with no prior experience with Databricks.
-- MAGIC <br>
-- MAGIC The department maintains t 
-- MAGIC
-- MAGIC ## Objectives
-- MAGIC 1. **Ingest Medical Center Locations**: Learn how to import and preview medical center data from SQL Server using Lakehouse Federation.
-- MAGIC 2. **Data Transformation**: Understand how to transform the metadata by replacing spaces in column names with underscores.
-- MAGIC 3. **Create a New Table**: Create a new table in the bronze layer to store the cleaned medical center data.
-- MAGIC
-- MAGIC ## Outline
-- MAGIC 1. **Step 1: Ingest Medical Center Locations**
-- MAGIC    - Import the medical center data into Databricks.
-- MAGIC    - Preview the imported data to understand its structure and content.
-- MAGIC 2. **Step 2: Data Transformation**
-- MAGIC    - Select all columns from the imported data.
-- MAGIC    - Replace spaces in column names with underscores.
-- MAGIC    - Create a new table called `bronze.medical_providers` to store the transformed data.
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
-- MAGIC ####Preview Medical Center Data
-- MAGIC
-- MAGIC Databricks Lakehouse Federation allows seamless access to data across various sources without users needing to know anything about the data source. It provides a unified interface to query and analyze data stored in different systems, making the process of accessing foreign tables transparent to the user. This means users can interact with external data sources as if they were native tables within Databricks, simplifying data integration and analysis workflows.
-- MAGIC
-- MAGIC In a SQL Server database is a table that contains information about medical providers that will be the source of the data pipeline. However there are spaces in the colum names and this could cause the next two cells to fail.

-- COMMAND ----------

select * from medicaid_providers.dbo.medicaid_providers

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.medical_providers
AS SELECT * FROM medicaid_providers.dbo.medicaid_providers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The solution is to replace spaces with underscores. One method is to manually type the SQL however another and faster way is to use the Databricks Assistant. Copy the prompt in the following cell to the Databricks Assistant. The output should be a SQL query which replaces spaces with underscores.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```select all the columns from medicaid_providers.dbo.medicaid_providers replace spaces in the column names with underscore and create a new table called bronze.medical_providers```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the next cell is what the output should be. Run this query to create a bronze table by select all the data from the SQL Server. 

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze.medical_providers AS
SELECT 
  `MEDICAID PROVIDER ID` AS MEDICAID_PROVIDER_ID, 
  NPI, 
  `PROVIDER OR FACILITY NAME` AS PROVIDER_OR_FACILITY_NAME, 
  `MEDICAID TYPE` AS MEDICAID_TYPE, 
  `PROFESSION OR SERVICE` AS PROFESSION_OR_SERVICE, 
  `PROVIDER SPECIALTY` AS PROVIDER_SPECIALTY, 
  `SERVICE ADDRESS` AS SERVICE_ADDRESS, 
  CITY, 
  STATE, 
  `ZIP CODE` AS ZIP_CODE, 
  COUNTY, 
  TELEPHONE, 
  LATITUDE, 
  LONGITUDE, 
  `ENROLLMENT BEGIN DATE` AS ENROLLMENT_BEGIN_DATE, 
  `NEXT ANTICIPATED REVALIDATION DATE` AS NEXT_ANTICIPATED_REVALIDATION_DATE, 
  `FILE DATE` AS FILE_DATE, 
  `MEDICALLY FRAGILE CHILDREN AND ADULTS DIRECTORY IND` AS MEDICALLY_FRAGILE_CHILDREN_AND_ADULTS_DIRECTORY_IND, 
  `PROVIDER EMAIL` AS PROVIDER_EMAIL
FROM medicaid_providers.dbo.medicaid_providers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Congratulations on Completing the Notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ```sql 
-- MAGIC CREATE OR REPLACE TABLE bronze.medical_providers AS
-- MAGIC SELECT
-- MAGIC   `MEDICAID PROVIDER ID` AS MEDICAID_PROVIDER_ID,
-- MAGIC   NPI,
-- MAGIC   `PROVIDER OR FACILITY NAME` AS PROVIDER_OR_FACILITY_NAME,
-- MAGIC   `MEDICAID TYPE` AS MEDICAID_TYPE,
-- MAGIC   `PROFESSION OR SERVICE` AS PROFESSION_OR_SERVICE,
-- MAGIC   `PROVIDER SPECIALTY` AS PROVIDER_SPECIALTY,
-- MAGIC   `SERVICE ADDRESS` AS SERVICE_ADDRESS,
-- MAGIC   CITY,
-- MAGIC   STATE,
-- MAGIC   `ZIP CODE` AS ZIP_CODE,
-- MAGIC   COUNTY,
-- MAGIC   TELEPHONE,
-- MAGIC   LATITUDE,
-- MAGIC   LONGITUDE,
-- MAGIC   `ENROLLMENT BEGIN DATE` AS ENROLLMENT_BEGIN_DATE, 
-- MAGIC   `NEXT ANTICIPATED REVALIDATION DATE` AS NEXT_ANTICIPATED_REVALIDATION_DATE, 
-- MAGIC   `FILE DATE` AS FILE_DATE, 
-- MAGIC   `MEDICALLY FRAGILE CHILDREN AND ADULTS DIRECTORY IND` AS MEDICALLY_FRAGILE_CHILDREN_AND_ADULTS_DIRECTORY_IND, 
-- MAGIC   `PROVIDER EMAIL` AS PROVIDER_EMAIL
-- MAGIC FROM medicaid_providers.dbo.medicaid_providers
-- MAGIC ```
