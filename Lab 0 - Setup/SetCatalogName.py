# Databricks notebook source
# Local-part of the user's email: drop dots, then turn any remaining
# non-alphanumeric char (e.g. hyphens) into '_' and trim leading/trailing
# '_' so the catalog name is a valid unquoted identifier. Fall back to
# 'user' when nothing alphanumeric remains. Must match
# workshop_setup/setup_workshop.py.
ctlg = spark.sql(
    "SELECT coalesce(nullif(trim(BOTH '_' FROM lower(regexp_replace(regexp_replace(regexp_extract(current_user(), '([^@]+)', 1), '\\\\.', ''), '[^a-zA-Z0-9]', '_'))), ''), 'user') as name"
).collect()[0][0]
ctlg = ctlg + "_dev"

# COMMAND ----------

if ctlg in [catalog.name for catalog in spark.catalog.listCatalogs()]:
  print(f"Setting Catalog to {ctlg}")
  spark.sql(f"use catalog {ctlg}")
else:
  print(f"Catalog {ctlg} does not exist.")


# COMMAND ----------


