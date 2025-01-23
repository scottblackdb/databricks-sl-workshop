-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Lab 1: Creating Review Gold Layer
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
-- MAGIC
-- MAGIC For this gold table, we are going to use AI to summarize the review. Instead of creating a summary for each review and then a summary of the summary, we are going to create a temporary view that for each unique rating will have a single row of all the reviews.
-- MAGIC
-- MAGIC A Spark temporary view is a virtual table created using a SQL query. It exists only for the duration of the Spark session and is not persisted.

-- COMMAND ----------

create or replace temp view collected_reviews
as
select
       rating,
       collect_set(review) as combined_ratings
from silver.medical_providers
group by rating

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using the temporary view we can use builtin Databricks AI functions.
-- MAGIC
-- MAGIC Databricks AI functions allow users to leverage generative AI directly in SQL queries. Tasks including sentiment analysis, text classiication and translation. There is also open ended function that allows users to customize the prompt opening endless possiblities.
-- MAGIC In this case we will use ai_summarize function to extract the key topics from all the reviews.

-- COMMAND ----------

select rating, combined_ratings, ai_summarize(array_join(combined_ratings, ', ')) review_summary
from collected_reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using the query from above we will create a table in the gold layer.

-- COMMAND ----------

create or replace table gold.provider_review_summary
select rating, ai_summarize(array_join(combined_ratings, ', ')) review_summary
from collected_reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Congratulations on Completing the Notebook
