# Databricks notebook source
url = "https://health.data.ny.gov/resource/vn5v-hh5r.json"

# COMMAND ----------

df = spark.read.csv("abfss://filedrops@scottblackadls.dfs.core.windows.net/landing/Medicaid_Enrolled_Provider_Listing_20241230.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

url = "jdbc:sqlserver://mainedb.database.windows.net:1433;database=sl_workshop;user=maine_sa@mainedb;password=databricks1#;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


# COMMAND ----------

df.write \
  .format("jdbc") \
  .option("url", url) \
  .option("dbtable", "medicaid_providers") \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .mode("overwrite") \
  .save()

# COMMAND ----------


