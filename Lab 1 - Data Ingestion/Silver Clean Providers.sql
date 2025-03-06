-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Lab 1: Creating Review Silver Layer
-- MAGIC
-- MAGIC In this notebook, we will enhance the reviews from the bronze layer to create the silver version of the reviews. Traditionally ETL has been limited to basic transformations of numbers, dates and texts. In this lab we use will new GenAI functions that will enable us to enrich our data.
-- MAGIC
-- MAGIC ## Objectives
-- MAGIC 1. **Enhance Bronze Reviews**: Learn how to GenAI to enhance ETL processing
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
-- MAGIC Next create a temporary view for the previous SQL to make it easier for the next transformation

-- COMMAND ----------

create or replace temp view rated_reviews as
select *,
       case 
           when rating <= 2 then 'low' 
           when rating < 4 then 'average' 
           else 'high' 
       end as ranking
from bronze.provider_reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC With provider reviews patients are able to give a yes or no recommendation as well as free form text response. It has been noticed that patients may recommend a provider but their free form text response can be negitave in tone. We have been asked to identify these types of instances. Instead of having to manually read every comment we can use GenAI. Using the ai_analyze_sentiment function the text can be analyzed and assigned an overall sentiment. 

-- COMMAND ----------

select *, ai_analyze_sentiment(review) as review_sentiment
from rated_reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Again we will make a temporary view we can reference when we create the silver table

-- COMMAND ----------

create or replace temp view final_reviews as
select *, ai_analyze_sentiment(review) as review_sentiment
from rated_reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using the temporary view created in the previous step finish the SQL query to create the silver table

-- COMMAND ----------

create or replace table silver.medical_providers
as
select *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Congratulations on Completing the Notebook
