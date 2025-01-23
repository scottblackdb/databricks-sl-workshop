# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1: Ingesting and Processing Medical Center Data with Databricks
# MAGIC
# MAGIC  In this notebook, we will guide you through the process of ingesting and processing TMSIS JSON files. TMSIS is part of the federal medicaid and medicare program and contains information on the medical services provided to patients.
# MAGIC
# MAGIC ## Objectives
# MAGIC 1. **Ingest TMSIS Data**: Learn how to import and preview TMSIS data coming from JSON files.
# MAGIC 2. **Create a New Table**: Create a new table in the bronze layer to store the TSIS data.
# MAGIC
# MAGIC Let's get started!
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Setup
# MAGIC First step in all the labs will be to run a setup notebook. This notebook to create a catalog to store data and to set your default catalog 

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks Autoloader
# MAGIC
# MAGIC Databricks Autoloader is a feature that allows for efficient and scalable ingestion of data from cloud storage. It automatically detects new files as they arrive and incrementally processes them, making it ideal for streaming and batch data pipelines.
# MAGIC
# MAGIC However for this lab we will use standard method to read JSON files. Every time the command is run all JSON files will be read regardless if they were previously read.

# COMMAND ----------

df = spark.read.json("/Volumes/quickstart_catalog/quickstart_schema/ext/tmsis_claims/")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create or replace a table in the bronze table with the data from the JSON files.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table bronze.tmsis_claims
# MAGIC as
# MAGIC select *
# MAGIC from json.`/Volumes/quickstart_catalog/quickstart_schema/ext/tmsis_claims/`

# COMMAND ----------

# MAGIC %md
# MAGIC ####Congratulations on Completing the Notebook
