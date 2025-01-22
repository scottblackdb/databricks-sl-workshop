# Databricks notebook source
# MAGIC %md
# MAGIC ## Lab 1 Data Ingestion
# MAGIC ###Notebook 1
# MAGIC ####Ingest Medicaid Enrolles By Month For New York City into Bronze Layer
# MAGIC ####Data Provided By [NYC Open Data Portal ](https://data.cityofnewyork.us/Social-Services/Citywide-HRA-Administered-Medicaid-Enrollees/33db-aeds/about_data)

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

import pandas as pd

# URL of the dataset
url = "https://data.cityofnewyork.us/resource/33db-aeds.json"

# Read the JSON data into a DataFrame
df = pd.read_json(url)
df = spark.createDataFrame(df)
# Display the DataFrame
display(df)

# COMMAND ----------

tbl = f"{ctlg}.bronze.medicaid_enrollees"
df.write.format("delta").mode("overwrite").saveAsTable(tbl)

# COMMAND ----------


