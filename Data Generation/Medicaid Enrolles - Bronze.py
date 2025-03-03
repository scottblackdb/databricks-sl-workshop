# Databricks notebook source
# MAGIC %md
# MAGIC ###Makes a REST call to get NYC medicaid enrollees
# MAGIC ###Create a Spark datafrand then save the raw data to a table

# COMMAND ----------

# MAGIC %run "./SetCatalogName"

# COMMAND ----------

spark.catalog.currentCatalog()

# COMMAND ----------

import requests

resp = requests.get("https://data.cityofnewyork.us/resource/33db-aeds.json")
df = spark.createDataFrame(resp.json())
display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("bronze.medicaid_enrollees")

# COMMAND ----------


