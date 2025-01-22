-- Databricks notebook source
-- MAGIC %run "../Lab 0 - Setup/SetCatalogName"

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
