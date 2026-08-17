# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Python Lab
# MAGIC <br>
# MAGIC
# MAGIC Databricks notebooks support more than one language in the same workspace.
# MAGIC By supporting multiple languages, Databricks lets users work in their preferred language while avoiding extra data copies, enabling sharing through Unity Catalog, and reducing cost.
# MAGIC
# MAGIC This lab mirrors the Extra Labs R notebook, but in **Python**:
# MAGIC 1. Read a Unity Catalog table with Spark
# MAGIC 2. Run an analysis with PySpark / pandas
# MAGIC 3. Write results back to Unity Catalog
# MAGIC 4. Use a SQL cell for a heavier aggregation, then read that result in Python

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup
# MAGIC Set your default catalog (`{username}_dev`). Complete Lab 1 first so `silver.tmsis_claims` exists.

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connecting Python to Spark
# MAGIC
# MAGIC In a Databricks Python notebook, a Spark session is already available as the built-in `spark` variable. You do not need a separate connection library like `sparklyr` in R.
# MAGIC
# MAGIC Access to Unity Catalog uses your Databricks identity automatically. There is no separate database username/password to configure for UC tables.

# COMMAND ----------

print(f"Spark version: {spark.version}")
print(f"Current catalog: {spark.catalog.currentCatalog()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read TMSIS claims from Unity Catalog
# MAGIC
# MAGIC `spark.table()` reads through Spark, so filtering and aggregations can run distributed on the cluster / serverless compute. Calling `.limit()` or converting to pandas brings a smaller result set to the driver for local Python work.

# COMMAND ----------

tmsis_claims = spark.table("silver.tmsis_claims")
display(tmsis_claims.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute statistics by diagnosis code
# MAGIC
# MAGIC Next we compute min / max / average / standard deviation of claim payment amount (`TOT_MDCD_PD_AMT`) for each primary diagnosis code (`DGNS_CD_1`).
# MAGIC
# MAGIC The aggregation runs in Spark. Collecting the summarized result to pandas is fine because the grouped output is much smaller than the full claims table.

# COMMAND ----------

from pyspark.sql import functions as F

stats_df = (
    tmsis_claims.groupBy("DGNS_CD_1")
    .agg(
        F.min("TOT_MDCD_PD_AMT").alias("min_claim_amt"),
        F.max("TOT_MDCD_PD_AMT").alias("max_claim_amt"),
        F.avg("TOT_MDCD_PD_AMT").alias("avg_claim_amt"),
        F.stddev("TOT_MDCD_PD_AMT").alias("sd_claim_amt"),
    )
)

display(stats_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC Optional: convert the aggregated Spark DataFrame to a pandas DataFrame for local Python tooling (charts, scikit-learn, etc.).

# COMMAND ----------

stats_pdf = stats_df.toPandas()
stats_pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write results back to Unity Catalog
# MAGIC
# MAGIC Save the diagnosis statistics as `silver.tmsis_stats`. Using `mode("overwrite")` replaces the table if it already exists (same idea as `spark_write_table(..., mode = "overwrite")` in the R lab).

# COMMAND ----------

(
    stats_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.tmsis_stats")
)

print("Wrote silver.tmsis_stats")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.tmsis_stats LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to use SQL for heavy processing
# MAGIC
# MAGIC Joining large tables or building complex aggregations entirely in pandas on the driver can be slow or run out of memory.
# MAGIC
# MAGIC Databricks notebooks can mix languages: use `%sql` so Spark performs the heavy work, create a temporary view, then continue in Python.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dgns_by_gndr AS
# MAGIC SELECT
# MAGIC   DGNS_CD_1,
# MAGIC   GNDR_CD,
# MAGIC   AVG(TOT_MDCD_PD_AMT) AS avg_TOT_MDCD_PD_AMT,
# MAGIC   MIN(TOT_MDCD_PD_AMT) AS min_TOT_MDCD_PD_AMT,
# MAGIC   MAX(TOT_MDCD_PD_AMT) AS max_TOT_MDCD_PD_AMT,
# MAGIC   STDDEV(TOT_MDCD_PD_AMT) AS sd_TOT_MDCD_PD_AMT
# MAGIC FROM silver.tmsis_claims
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC Read the temporary view from Python. Spark still executes the SQL; Python receives the result as a DataFrame.

# COMMAND ----------

claims_dg_gnd = spark.table("dgns_by_gndr")
display(claims_dg_gnd.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Congratulations on Completing the Databricks Python Lab
# MAGIC
# MAGIC You:
# MAGIC - Read `silver.tmsis_claims` from Unity Catalog with Spark
# MAGIC - Aggregated claim amounts by diagnosis in Python
# MAGIC - Wrote `silver.tmsis_stats` back to Unity Catalog
# MAGIC - Used a SQL temp view for a multi-dimension aggregation and consumed it from Python
# MAGIC
# MAGIC The original R lab (`Claims Analysis Using R`) shows the same pattern with sparklyr and dplyr.
