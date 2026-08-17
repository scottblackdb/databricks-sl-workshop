# Databricks notebook source
# MAGIC %md
# MAGIC # Machine Learning Lab 2: Python Claims Forecast
# MAGIC <br>
# MAGIC
# MAGIC ## Know the Future Today
# MAGIC Having a forecast of claim payments in the near future helps with capital planning. In the past, forecasting often lived in desktop tools: models were hard to share, ran on stale extracts, and results had to be copied back into the warehouse by hand.
# MAGIC
# MAGIC In this lab you will build a simple time-series forecast **in a Databricks Python notebook**, using the gold TMSIS daily billing table from Lab 1, and write the results to Unity Catalog so Lab 4 dashboards can use them.
# MAGIC
# MAGIC ## Objectives
# MAGIC - Review `gold.tmsis_daily_billing_summary`
# MAGIC - Prepare a daily time series in pandas
# MAGIC - Train a forecast model in Python (Prophet)
# MAGIC - Write predictions to `gold.tmsis_forecast` for Lab 4
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Complete Lab 1 (especially the gold daily billing notebook)
# MAGIC - Use a compute environment that can run Python notebooks (Serverless or a standard cluster)
# MAGIC
# MAGIC ## Output table (used by Lab 4)
# MAGIC `gold.tmsis_forecast` with columns:
# MAGIC - `discharge_date`
# MAGIC - `total_amount`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install Prophet
# MAGIC Prophet is a forecasting library that works well for daily business metrics with trend and weekly seasonality.
# MAGIC
# MAGIC Note: The next cell restarts the Python process. After it finishes, continue from the following cell (or click Run all again). Do this once per session.

# COMMAND ----------

# MAGIC %pip install prophet --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup
# MAGIC Set your default catalog (`{username}_dev`).

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Review the gold daily billing table
# MAGIC Lab 1 created one row per discharge day with the total Medicaid payment amount.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.tmsis_daily_billing_summary ORDER BY discharge_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load history into pandas
# MAGIC Spark reads the Unity Catalog table; pandas is used for the forecasting API. Prophet can't work directly with Spark dataframes so Spark will be used to read the data and then convert into a pandas dataframe.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explanation of the Python Code
# MAGIC
# MAGIC This code loads historical billing data from the Unity Catalog table `gold.tmsis_daily_billing_summary` into a pandas DataFrame for time series forecasting.
# MAGIC
# MAGIC - `import pandas as pd`: Imports the pandas library for data manipulation.
# MAGIC - `spark.table(...).orderBy(...).toPandas()`: Reads the Spark table, orders by `discharge_date`, and converts it to a pandas DataFrame.
# MAGIC - `pd.to_datetime(...)`: Ensures the `discharge_date` column is in datetime format.
# MAGIC - `dropna(...)`: Removes rows with missing values in `discharge_date` or `total_amount`.
# MAGIC - `sort_values(...).reset_index(...)`: Sorts the DataFrame by date and resets the index.
# MAGIC - `display(history_pdf.tail(10))`: Shows the last 10 rows for inspection.
# MAGIC - `print(...)`: Outputs the number of training rows and the date range covered by the data.
# MAGIC
# MAGIC This prepares clean, ordered daily payment data for model training and forecasting.

# COMMAND ----------

import pandas as pd

history_pdf = (
    spark.table("gold.tmsis_daily_billing_summary")
    .orderBy("discharge_date")
    .toPandas()
)

history_pdf["discharge_date"] = pd.to_datetime(history_pdf["discharge_date"])
history_pdf = history_pdf.dropna(subset=["discharge_date", "total_amount"])
history_pdf = history_pdf.sort_values("discharge_date").reset_index(drop=True)

display(history_pdf.tail(10))
print(f"Training rows: {len(history_pdf):,}")
print(
    f"Date range: {history_pdf['discharge_date'].min().date()} "
    f"→ {history_pdf['discharge_date'].max().date()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize historical payments
# MAGIC A quick line chart helps confirm the series looks continuous enough to forecast.

# COMMAND ----------

import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(12, 4))
ax.plot(history_pdf["discharge_date"], history_pdf["total_amount"], linewidth=1)
ax.set_title("Historical daily TMSIS payments")
ax.set_xlabel("Discharge date")
ax.set_ylabel("Total amount")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train a Prophet model
# MAGIC Prophet expects columns named `ds` (date) and `y` (value to forecast).
# MAGIC We forecast **30 days** beyond the last date in the gold table.

# COMMAND ----------

from prophet import Prophet

FORECAST_HORIZON_DAYS = 30

train_pdf = history_pdf.rename(columns={"discharge_date": "ds", "total_amount": "y"})[
    ["ds", "y"]
]

model = Prophet(
    daily_seasonality=False,
    weekly_seasonality=True,
    yearly_seasonality=False,
)
model.fit(train_pdf)

future_pdf = model.make_future_dataframe(periods=FORECAST_HORIZON_DAYS, freq="D")
forecast_pdf = model.predict(future_pdf)

display(forecast_pdf[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Plot forecast vs history
# MAGIC Blue is the model fit/forecast; black points are actual history.

# COMMAND ----------

fig = model.plot(forecast_pdf)
ax = fig.gca()
ax.set_title("Prophet forecast of daily TMSIS payments")
ax.set_xlabel("Discharge date")
ax.set_ylabel("Total amount")
fig.set_size_inches(12, 4)
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build the Lab 4 output table
# MAGIC Keep only **future** days (after the last historical date) and map columns to:
# MAGIC - `discharge_date`
# MAGIC - `total_amount`
# MAGIC
# MAGIC Negative predictions are clipped to zero because payment totals cannot be negative.

# COMMAND ----------

last_history_date = train_pdf["ds"].max()

forecast_out = (
    forecast_pdf.loc[forecast_pdf["ds"] > last_history_date, ["ds", "yhat"]]
    .rename(columns={"ds": "discharge_date", "yhat": "total_amount"})
    .copy()
)
forecast_out["total_amount"] = forecast_out["total_amount"].clip(lower=0).round(0)

display(forecast_out)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write `gold.tmsis_forecast`
# MAGIC Lab 4 dashboards and queries read this table.

# COMMAND ----------

forecast_df = spark.createDataFrame(forecast_out)
forecast_df.write.mode("overwrite").saveAsTable("gold.tmsis_forecast")

print(f"Wrote {forecast_out.shape[0]} forecast rows to gold.tmsis_forecast")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.tmsis_forecast ORDER BY discharge_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optional: compare last 14 actual days vs model fit
# MAGIC This is a quick sanity check, not a full model-validation exercise.

# COMMAND ----------

fitted = forecast_pdf.merge(train_pdf, on="ds", how="inner")
recent = fitted.sort_values("ds").tail(14).copy()
recent["abs_error"] = (recent["y"] - recent["yhat"]).abs()
recent["pct_error"] = recent["abs_error"] / recent["y"].replace(0, pd.NA)

print(
    "Last 14 days — mean absolute error: "
    f"{recent['abs_error'].mean():,.0f}"
)
display(recent[["ds", "y", "yhat", "abs_error"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## End of Lab 2 (Python Forecast)
# MAGIC
# MAGIC You now have:
# MAGIC - A Prophet model trained on `gold.tmsis_daily_billing_summary`
# MAGIC - Future predictions stored in `gold.tmsis_forecast`
# MAGIC
# MAGIC Continue to **Lab 4 - Data Warehouse** to chart historical payments with this forecast.
# MAGIC
# MAGIC > Tip: The original AutoML Lab 2 notebook remains available if you want to compare the UI-driven forecasting experience.
