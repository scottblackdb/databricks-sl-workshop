# Databricks notebook source
# MAGIC %md
# MAGIC ## Lab 1 Data Ingestion
# MAGIC ###Notebook 3
# MAGIC Ingestion NYC Budget
# MAGIC [Source]https://data.cityofnewyork.us/City-Government/Expense-Budget/mwzb-yiwb/about_data

# COMMAND ----------

# MAGIC %run "./SetCatalogName"

# COMMAND ----------

# MAGIC %pip install sodapy

# COMMAND ----------

from sodapy import Socrata

client = Socrata("data.cityofnewyork.us",None,username=None,password=None)

data = []
for d in client.get_all("gzfs-3h4m"):
  data.append(d)

data

# COMMAND ----------

df = spark.createDataFrame(data)
df.write.mode("overwrite").saveAsTable("bronze.budget")

# COMMAND ----------


