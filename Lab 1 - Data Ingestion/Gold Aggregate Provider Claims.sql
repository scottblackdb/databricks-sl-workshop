-- Databricks notebook source
-- MAGIC %run "../Lab 0 - Setup/SetCatalogName"

-- COMMAND ----------

create or replace temp view collected_reviews
as
select
       rating,
       collect_set(review) as combined_ratings
from silver.medical_providers
group by rating

-- COMMAND ----------

select rating, combined_ratings, ai_summarize(array_join(combined_ratings, ', ')) review_summary
from collected_reviews

-- COMMAND ----------

create or replace table gold.provider_review_summary
select rating, ai_summarize(array_join(combined_ratings, ', ')) review_summary
from collected_reviews
