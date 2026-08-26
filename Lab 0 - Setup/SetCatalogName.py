# Databricks notebook source
# Local-part of the user's email: drop dots, then turn any remaining
# non-alphanumeric char (e.g. hyphens) into '_' so the catalog name is a
# valid unquoted identifier. Must match workshop_setup/setup_workshop.py.
ctlg = spark.sql(
    "SELECT lower(regexp_replace(regexp_replace(regexp_extract(current_user(), '([^@]+)', 1), '\\\\.', ''), '[^a-zA-Z0-9]', '_')) as name"
).collect()[0][0]
ctlg = ctlg + "_dev"

# COMMAND ----------

if ctlg in [catalog.name for catalog in spark.catalog.listCatalogs()]:
  print(f"Setting Catalog to {ctlg}")
  spark.sql(f"use catalog {ctlg}")
else:
  print(f"Catalog {ctlg} does not exist.")


# COMMAND ----------


