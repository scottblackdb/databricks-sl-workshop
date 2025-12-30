# Databricks notebook source
ctlg = spark.sql("SELECT regexp_replace(regexp_extract(current_user(), '([^@]+)', 1), '\\\\.', '') as name").collect()[0][0]
ctlg = ctlg + "_dev"

# COMMAND ----------

if ctlg in [catalog.name for catalog in spark.catalog.listCatalogs()]:
  print(f"Setting Catalog to {ctlg}")
  spark.sql(f"use catalog {ctlg}")
else:
  print(f"Catalog {ctlg} does not exist.")


# COMMAND ----------


