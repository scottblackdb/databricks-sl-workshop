# Databricks notebook source
# MAGIC %md
# MAGIC ##Create Required Unity Catalog Objects
# MAGIC ####Create a catalog for the username if not the catalog does not already exist
# MAGIC ####Create bronze, silver and gold schemas into the catalog

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####By default the first external location will be used when creating the catalog. To override at a new line to the cell below and set the path variable to the URL where to create the catalog

# COMMAND ----------

df = spark.sql("show external locations").limit(1).toPandas()
path = df['url'][0]

# COMMAND ----------


spark.sql(f"CREATE CATALOG IF NOT EXISTS {ctlg} MANAGED LOCATION '{path}'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctlg}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctlg}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctlg}.gold")

# COMMAND ----------


