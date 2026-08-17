# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1: Creating TMSIS Silver Layer — Visual Data Prep Edition
# MAGIC
# MAGIC This notebook walks through the same TMSIS silver-layer cleanup as **1.5 Silver Clean TMSIS**, but instead of hand-writing SQL we will use Databricks' **visual data prep** tool, Data Wrangler, to build the transformation interactively and let it generate the code for us.
# MAGIC
# MAGIC ## Objectives
# MAGIC 1. **Load Bronze Data**: Read the TMSIS bronze table into a dataframe.
# MAGIC 2. **Launch Data Wrangler**: Use the visual, point-and-click interface to explore and transform the data.
# MAGIC 3. **Convert Timestamps Visually**: Turn unix-timestamp columns into real timestamp columns without writing any code by hand.
# MAGIC 4. **Create a New Table**: Save the cleaned data into the silver layer with liquid clustering.
# MAGIC
# MAGIC Let's get started!

# COMMAND ----------

# MAGIC %md
# MAGIC ####Setup
# MAGIC First step in all the labs will be to run a setup notebook. This notebook to create a catalog to store data and to set your default catalog

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the Bronze Table
# MAGIC
# MAGIC Read the `bronze.tmsis_claims` table into a dataframe. Notice several columns look like they should be dates (`adjdctn_dt`, `admission_dt`, `birth_dt`, `mdcd_pd_dt`, `srvc_bgnng_dt`, `srvc_endg_dt`, `dschrg_dt`) but are actually stored as unix timestamp numbers.

# COMMAND ----------

df = spark.table("bronze.tmsis_claims")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Launch Data Wrangler
# MAGIC
# MAGIC Databricks **Data Wrangler** is a visual data prep tool built directly into the notebook. It lets you explore, clean, and transform a dataframe using point-and-click UI controls — no code required — and it writes the equivalent PySpark/pandas code back into the notebook for you as you go.
# MAGIC
# MAGIC To open it on the dataframe displayed above:
# MAGIC 1. Run the cell above so the dataframe preview is displayed.
# MAGIC 2. Click the grid icon in the top-left of the results table, next to **Table**, **Bar chart**, etc.
# MAGIC 3. Select **View as DataFrame** or **Data Wrangler** — this opens the interactive Data Wrangler pane.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform the Date Columns Visually
# MAGIC
# MAGIC Inside Data Wrangler, use the visual transformation panel to apply the following steps — for each one, pick the column, choose the transformation from the menu, and Data Wrangler will preview the result immediately:
# MAGIC
# MAGIC 1. **Convert unix timestamps to dates**: For each of `dschrg_dt`, `adjdctn_dt`, `admission_dt`, `birth_dt`, `mdcd_pd_dt`, `srvc_bgnng_dt`, and `srvc_endg_dt`, select the column and choose a **Format date/time** or **Convert data type** transformation (Unix time in milliseconds → Timestamp).
# MAGIC 2. **Rename the converted columns**: Use the **Rename column** transformation to give each converted column an uppercase name matching the original (e.g. `dschrg_dt` → `DSCHRG_DT`).
# MAGIC 3. **Add a received-at column**: Use **Add column by formula** (or **Derive column**) to add a new column named `CLAIM_RCV_DT` set to the current timestamp.
# MAGIC 4. **Drop the raw numeric columns**: Use the **Drop column** transformation to remove the original unix-timestamp columns now that you have their converted replacements.
# MAGIC
# MAGIC As you apply each step, watch the **generated code** panel on the right — Data Wrangler writes working PySpark for every click.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export the Generated Code
# MAGIC
# MAGIC When you are happy with the transformation, click **Export** (or **Add to notebook**) in Data Wrangler. It will insert a new cell containing generated PySpark code equivalent to the steps you performed visually.
# MAGIC
# MAGIC For reference, the cell below shows what that generated code looks like for the transformation described above — compare it against what Data Wrangler produced for you.

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, to_timestamp, current_timestamp, col

df_clean = (
    df
    .withColumn("DSCHRG_DT", to_timestamp(from_unixtime(col("dschrg_dt") / 1000)))
    .withColumn("ADJDCTN_DT", to_timestamp(from_unixtime(col("adjdctn_dt") / 1000)))
    .withColumn("ADMISSION_DT", to_timestamp(from_unixtime(col("admission_dt") / 1000)))
    .withColumn("BIRTH_DT", to_timestamp(from_unixtime(col("birth_dt") / 1000)))
    .withColumn("MDCD_PD_DT", to_timestamp(from_unixtime(col("mdcd_pd_dt") / 1000)))
    .withColumn("SRVC_BGNNG_DT", to_timestamp(from_unixtime(col("srvc_bgnng_dt") / 1000)))
    .withColumn("SRVC_ENDG_DT", to_timestamp(from_unixtime(col("srvc_endg_dt") / 1000)))
    .withColumn("CLAIM_RCV_DT", current_timestamp())
    .drop(
        "adjdctn_dt", "admission_dt", "birth_dt", "mdcd_pd_dt",
        "srvc_bgnng_dt", "srvc_endg_dt", "dschrg_dt",
    )
)

display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to the Silver Layer
# MAGIC
# MAGIC Databricks offers many performance optimizations to make querying very large datasets fast called liquid clustering.
# MAGIC
# MAGIC **Databricks Liquid Clustering**: Automatically optimizes data layout and file sizes for efficient querying. Simply specify which columns will frequently be used in WHERE or JOIN clauses and Databricks takes care of the rest.
# MAGIC
# MAGIC Write `df_clean` out to the silver layer as a managed table, clustered on the columns most likely to be filtered or joined on.

# COMMAND ----------

(
    df_clean.write
    .mode("overwrite")
    .clusterBy("DSCHRG_DT", "ADMISSION_DT", "CLAIM_RCV_DT")
    .saveAsTable("silver.tmsis_claims")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Congratulations on Completing the Notebook
