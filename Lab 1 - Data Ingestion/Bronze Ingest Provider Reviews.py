# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2: Ingest Provider Reviews
# MAGIC
# MAGIC In this lab you will ingest medical provider reivews from a Rest API into the bronze layer.
# MAGIC
# MAGIC ## Objectives
# MAGIC 1. **Setup Environment**: Initialize the environment by running the setup notebook.
# MAGIC 2. **Ingest Data**: Learn how to import data into Databricks from a Rest Source.
# MAGIC 3. **Create Tables**: Create and manage tables to store your data.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Setup
# MAGIC First step in all the labs will be to run a setup notebook. This notebook to create a catalog to store data and to set your default catalog 

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

# MAGIC %md
# MAGIC Using pandas make a Rest call to the data source. Databricks provides common Python libraries including Pandas so often there is no need for users to spend time installing libraries.
# MAGIC
# MAGIC The data is returned to pandas which is directly used to create a Spark dataframe. The dataframe is then display.

# COMMAND ----------

import pandas as pd

df = spark.createDataFrame(pd.read_json("https://sl-workspace-reviews-c6eqa3btdjhna6hm.eastus2-01.azurewebsites.net/reviews"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Save the data to the bronze layer.

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("bronze.provider_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Congratulations on Completing the Notebook
