# Databricks notebook source
# MAGIC %md
# MAGIC ##Create Required Unity Catalog Objects
# MAGIC ####Create a catalog for the username if not the catalog does not already exist
# MAGIC ####Create bronze, silver and gold schemas into the catalog

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------


spark.sql(f"CREATE CATALOG IF NOT EXISTS {ctlg}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctlg}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctlg}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctlg}.gold")

# COMMAND ----------


