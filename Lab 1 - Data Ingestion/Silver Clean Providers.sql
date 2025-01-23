-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Lab 1: Creating Review Silver Layer
-- MAGIC
-- MAGIC In this notebook, we will enhance the reviews from the bronze layer to create the silver version of the reviews.
-- MAGIC
-- MAGIC ## Objectives
-- MAGIC 1. **Enhance Bronze Reviews**: Learn how to use SQL commands to enhance data.
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
-- MAGIC The following select statement uses CASE to label a new column based on the numerical value of the review

-- COMMAND ----------

select nip, rating,
       case 
           when rating <= 2 then 'low' 
           when rating < 4 then 'average' 
           else 'high' 
       end as ranking,
       review
from bronze.provider_reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using the same SELECT query from above add a create or replace statement to save the transformed data to the silver layer

-- COMMAND ----------

create or replace table silver.medical_providers
as
select nip, rating,
       case 
           when rating <= 2 then 'low' 
           when rating < 4 then 'average' 
           else 'high' 
       end as ranking,
       review
from bronze.provider_reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Congratulations on Completing the Notebook
