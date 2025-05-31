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
# MAGIC By default the first external location will be used when creating the catalog. To override at a new line to the cell below and set the path variable to the URL where to create the catalog

# COMMAND ----------

df = spark.sql("show external locations").limit(1).toPandas()
path = df['url'].iloc[0] if not df.empty else None

# COMMAND ----------

# MAGIC %md
# MAGIC If there are no external locations the default location for the catalog will be used.

# COMMAND ----------

if path is None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {ctlg}")
else:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {ctlg} MANAGED LOCATION '{path}'")
    
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctlg}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctlg}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctlg}.gold")

# COMMAND ----------


