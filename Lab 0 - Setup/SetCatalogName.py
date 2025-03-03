# Databricks notebook source
ctlg = spark.sql("SELECT regexp_extract(current_user(), '^[^.@]+', 0) as name").collect()[0]['name']
ctlg = ctlg + "_dev"

# COMMAND ----------

if ctlg in [catalog.name for catalog in spark.catalog.listCatalogs()]:
  print(f"Setting Catalog to {ctlg}")
  spark.sql(f"use catalog {ctlg}")
else:
  print(f"Catalog {ctlg} does not exist.")


# COMMAND ----------


