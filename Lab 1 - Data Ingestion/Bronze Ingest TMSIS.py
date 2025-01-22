# Databricks notebook source
# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

df = spark.read.json("/Volumes/quickstart_catalog/quickstart_schema/ext/tmsis_claims/")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table bronze.tmsis_claims
# MAGIC as
# MAGIC select *
# MAGIC from json.`/Volumes/quickstart_catalog/quickstart_schema/ext/tmsis_claims/`
