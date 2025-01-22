# Databricks notebook source
# MAGIC %md
# MAGIC ## Lab 1 Data Ingestion
# MAGIC ###Notebook 2
# MAGIC ####Ingest 311 Service Calls Requests
# MAGIC [Source](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data)

# COMMAND ----------

# MAGIC %run "./SetCatalogName"

# COMMAND ----------

import requests

resp = requests.get("https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=10000")

data =resp.json()

data

# COMMAND ----------

df = spark.createDataFrame(data)
df = df.drop(*[c for c in df.columns if c.startswith(":@")])
display(df)

# COMMAND ----------

#Complete the rest of the following line to save the data to a table called bronze.311_service_calls. Set the option to overwrite the table if it already exists.
#df.write.
df.write.mode("append").saveAsTable("bronze.311_service_calls")

# COMMAND ----------


