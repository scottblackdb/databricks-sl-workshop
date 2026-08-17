# Databricks notebook source
# MAGIC %md
# MAGIC # Data Engineering Lab 1: Building a Modern Data Pipeline with Databricks Medallion Architecture
# MAGIC <br>
# MAGIC
# MAGIC ## Setting the table
# MAGIC The first step in any data project is getting access to the data. Your first task in evaluating Databricks is how to ingest data. The major data sources used in the department are tradiontal RDBMS, JSON files and making REST calls.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this hands-on workshop, you'll learn how to implement a robust data pipeline using Databricks' medallion architecture - a multi-layered data organization framework that helps ensure data quality, reliability, and usability at scale. The medallion architecture, also known as "multi-hop" architecture, consists of bronze (raw), silver (validated), and gold (enriched) layers that progressively refine and transform data.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ## Workshop Objectives
# MAGIC
# MAGIC By the end of this lab, you will:
# MAGIC
# MAGIC * Understand the core principles and benefits of the medallion architecture
# MAGIC * Ingest data from a variety of data sources including databases, files and APIs
# MAGIC * Build data pipelines that move data through bronze, silver, and gold layers
# MAGIC * Implement data transformation
# MAGIC * Create optimized tables for downstream analytics and ML workloads
# MAGIC * Apply best practices for performance and maintainability
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC To get the most out of this workshop, you should have:
# MAGIC
# MAGIC * Basic familiarity with SQL and Python
# MAGIC * Understanding of basic data engineering concepts
# MAGIC * Access to a Databricks workspace
# MAGIC * Basic knowledge of Delta Lake operations
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ## Environment
# MAGIC * Ingest data from three sources, Lakebase Postgres, JSON files and Rest API into bronze layer
# MAGIC * Transform data to create silver layer
# MAGIC * Create aggregate data source in the gold layer
# MAGIC * All labs will leverage serverless compute

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Getting Started
# MAGIC In a separate browser or browser tab, go to your Databricks workspace for 
# MAGIC * Data Ingestion/Bronze Ingest Medical Providers Locations
# MAGIC
# MAGIC Then complete the notebooks in this order
# MAGIC * Bronze Ingest Provider Reviews
# MAGIC * Bronze Ingest TMSIS
# MAGIC
# MAGIC ####With the bronze layer built next build the silver layer
# MAGIC * Silver Clean Providers
# MAGIC * Silver Clean TMSIS
# MAGIC
# MAGIC ####Finally build the gold layer
# MAGIC * Gold - TMSIS Monthtly Summary
# MAGIC * Gold Aggregate Provider Claims

# COMMAND ----------

# MAGIC %md
# MAGIC ## End of Lab 1
