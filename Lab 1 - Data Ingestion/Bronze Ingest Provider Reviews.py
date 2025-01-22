# Databricks notebook source
# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

import pandas as pd

df = spark.createDataFrame(pd.read_json("https://sl-workspace-reviews-c6eqa3btdjhna6hm.eastus2-01.azurewebsites.net/reviews"))
display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("bronze.provider_reviews")
