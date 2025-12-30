-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 1: Ingesting and Processing Medical Center Data with Databricks
-- MAGIC
-- MAGIC Welcome to the Databricks Lab 1! In this notebook, we will guide you through the process of ingesting and processing medical center data using Databricks. This lab is designed for users with no prior experience with Databricks.
-- MAGIC <br>
-- MAGIC  
-- MAGIC
-- MAGIC ## Objectives
-- MAGIC 1. **Ingest Medical Center Locations**: Learn how to import and preview medical center data from a different Databricks enviroment using Delta Sharing.
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
-- MAGIC Delta Sharing is an open protocol for secure data sharing across organizations, enabling users to access shared data in real time without data replication.
-- MAGIC
-- MAGIC Users are able to access data provided by the delta share without knowing any of the connection details. Administrators can manage access to the data as if it was in their local Databricks enviroment.
-- MAGIC
-- MAGIC In the following sections you will explore the medical provider data and ETL the data into your enviroment.
-- MAGIC
-- MAGIC The following SQL command displays the data coming from a Delta Share. Notice there is no additional work required by the users to query remote data.

-- COMMAND ----------

select * from medical_providers.default.medical_providers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC We want to create a local copy of the data. You can create SQL code yourself or use the Databricks Assistant. In the top right corner click on the multi-color diamond and enter this prompt.
-- MAGIC
-- MAGIC ```copy the data from medical_providers.default.medical_providers into a new table called bronze.medical_providers```
-- MAGIC
-- MAGIC A new cell with the code to copy the data into a new table should be created.

-- COMMAND ----------

create or replace table bronze.medical_providers as
select * from medical_providers.default.medical_providers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Congratulations on Completing the Notebook
