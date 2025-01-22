# Databricks notebook source
# MAGIC %md
# MAGIC ## Lab 1 Data Ingestion
# MAGIC ###Notebook 4

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.budget

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * except (publication_date, first_actual_fiscal_year, year_1_actual,year_2_actual,year_3_actual,plan_amount_year_1,plan_amount_year_2),
# MAGIC to_date(publication_date, 'yyyyMMdd') publication_date, bigint(year_1_actual) year_1_actual, 
# MAGIC bigint(year_2_actual) year_2_actual, bigint(plan_amount_year_1) plan_amount_year_1, bigint(plan_amount_year_2) plan_amount_year_2
# MAGIC from bronze.budget

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table silver.budget as
# MAGIC select * except (publication_date, first_actual_fiscal_year, year_1_actual,year_2_actual,year_3_actual,plan_amount_year_1,plan_amount_year_2),
# MAGIC to_date(publication_date, 'yyyyMMdd') publication_date, bigint(year_1_actual) year_1_actual, 
# MAGIC bigint(year_2_actual) year_2_actual, bigint(plan_amount_year_1) plan_amount_year_1, bigint(plan_amount_year_2) plan_amount_year_2
# MAGIC from bronze.budget

# COMMAND ----------


