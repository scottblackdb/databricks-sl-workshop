-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Lab 1: Program Integrity — Improper Payment Flags (Gold)
-- MAGIC
-- MAGIC Reducing **improper payments** is one of the largest program-integrity missions for
-- MAGIC a state Medicaid agency. CMS reports Medicaid improper payments in the tens of
-- MAGIC billions of dollars a year, and TMSIS is the data agencies use to find them.
-- MAGIC
-- MAGIC In this notebook we build a gold table that applies a set of **claim edit checks**
-- MAGIC (deterministic rules) to the silver TMSIS claims and flags claims that look like
-- MAGIC improper payments or potential fraud. Nothing in the data marks a claim as
-- MAGIC fraudulent — we find them by writing the rules.
-- MAGIC
-- MAGIC ## Objectives
-- MAGIC 1. **Apply edit checks**: encode common Medicaid improper-payment rules in SQL.
-- MAGIC 2. **Create a flags table**: one row per (claim, rule) in the gold layer.
-- MAGIC 3. **Summarize**: rank providers by flagged dollars for investigator follow-up.
-- MAGIC
-- MAGIC ## Rules applied
-- MAGIC | Rule | What it catches |
-- MAGIC |------|-----------------|
-- MAGIC | `DUPLICATE_CLAIM` | Same beneficiary, provider, service date, and procedure billed more than once |
-- MAGIC | `PAID_BEFORE_SERVICE` | Payment date precedes the service begin date |
-- MAGIC | `DISCHARGE_BEFORE_ADMISSION` | Discharge date precedes the admission date |
-- MAGIC | `SERVICE_BEFORE_BIRTH` | Service date precedes the beneficiary's birth date |
-- MAGIC | `OUTPATIENT_OVERPAYMENT` | Outpatient office visit paid far above the normal range |
-- MAGIC | `BENE_MULTI_STATE_SAME_DAY` | One beneficiary billed in two states on the same day |
-- MAGIC | `EXCESSIVE_DAILY_VOLUME` | Implausible number of visits for one beneficiary in a single day |
-- MAGIC
-- MAGIC Let's get started!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Setup
-- MAGIC First step in all the labs will be to run a setup notebook. This notebook to create a catalog to store data and to set your default catalog

-- COMMAND ----------

-- MAGIC %run "../Lab 0 - Setup/SetCatalogName"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Rule 1: Duplicate claims
-- MAGIC Billing the same service twice is one of the most common improper payments. We use a
-- MAGIC window function to count how many claims share the same beneficiary, provider,
-- MAGIC service date, and procedure code. Any group with more than one claim is flagged.

-- COMMAND ----------

SELECT ICN_NUM, BENE_ID, PRVDR_ID, SRVC_BGNNG_DT, PRCDR_CD_1, TOT_MDCD_PD_AMT
FROM (
  SELECT *,
    COUNT(*) OVER (
      PARTITION BY BENE_ID, PRVDR_ID, SRVC_BGNNG_DT, PRCDR_CD_1
    ) AS claim_count
  FROM silver.tmsis_claims
)
WHERE claim_count > 1
ORDER BY BENE_ID, SRVC_BGNNG_DT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Build the flags table
-- MAGIC Each rule below produces the same set of columns so we can `UNION ALL` them into a
-- MAGIC single gold table. A claim that trips more than one rule appears once per rule.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.tmsis_improper_payment_flags AS

-- Rule 1: Duplicate paid claims
WITH duplicates AS (
  SELECT *,
    COUNT(*) OVER (
      PARTITION BY BENE_ID, PRVDR_ID, SRVC_BGNNG_DT, PRCDR_CD_1
    ) AS claim_count
  FROM silver.tmsis_claims
),
-- Rule 6: Same beneficiary billed in more than one state on the same day.
-- Spark SQL does not allow COUNT(DISTINCT ...) as a window function, so we
-- collect the distinct states into a set over the window and measure its size.
multi_state AS (
  SELECT *,
    SIZE(COLLECT_SET(SUBMTG_STATE_CD) OVER (
      PARTITION BY BENE_ID, SRVC_BGNNG_DT
    )) AS state_count
  FROM silver.tmsis_claims
),
-- Rule 7: Implausible number of visits for one beneficiary in a single day
daily_volume AS (
  SELECT *,
    COUNT(*) OVER (
      PARTITION BY BENE_ID, DATE_TRUNC('day', SRVC_BGNNG_DT)
    ) AS daily_claim_count
  FROM silver.tmsis_claims
)

SELECT ICN_NUM, BENE_ID, PRVDR_ID, SUBMTG_STATE_CD, SRVC_BGNNG_DT, TOT_MDCD_PD_AMT,
       'DUPLICATE_CLAIM' AS flag_rule,
       'Same beneficiary, provider, service date, and procedure billed more than once' AS flag_description
FROM duplicates
WHERE claim_count > 1

UNION ALL

SELECT ICN_NUM, BENE_ID, PRVDR_ID, SUBMTG_STATE_CD, SRVC_BGNNG_DT, TOT_MDCD_PD_AMT,
       'PAID_BEFORE_SERVICE',
       'Medicaid payment date precedes the service begin date'
FROM silver.tmsis_claims
WHERE MDCD_PD_DT < SRVC_BGNNG_DT

UNION ALL

SELECT ICN_NUM, BENE_ID, PRVDR_ID, SUBMTG_STATE_CD, SRVC_BGNNG_DT, TOT_MDCD_PD_AMT,
       'DISCHARGE_BEFORE_ADMISSION',
       'Discharge date precedes the admission date'
FROM silver.tmsis_claims
WHERE DSCHRG_DT < ADMISSION_DT

UNION ALL

SELECT ICN_NUM, BENE_ID, PRVDR_ID, SUBMTG_STATE_CD, SRVC_BGNNG_DT, TOT_MDCD_PD_AMT,
       'SERVICE_BEFORE_BIRTH',
       'Service date precedes the beneficiary birth date'
FROM silver.tmsis_claims
WHERE SRVC_BGNNG_DT < BIRTH_DT

UNION ALL

SELECT ICN_NUM, BENE_ID, PRVDR_ID, SUBMTG_STATE_CD, SRVC_BGNNG_DT, TOT_MDCD_PD_AMT,
       'OUTPATIENT_OVERPAYMENT',
       'Outpatient claim paid far above the normal outpatient range'
FROM silver.tmsis_claims
WHERE BILL_TYPE_CD = '131' AND TOT_MDCD_PD_AMT > 1000

UNION ALL

SELECT ICN_NUM, BENE_ID, PRVDR_ID, SUBMTG_STATE_CD, SRVC_BGNNG_DT, TOT_MDCD_PD_AMT,
       'BENE_MULTI_STATE_SAME_DAY',
       'Beneficiary billed in more than one state on the same day'
FROM multi_state
WHERE state_count > 1

UNION ALL

SELECT ICN_NUM, BENE_ID, PRVDR_ID, SUBMTG_STATE_CD, SRVC_BGNNG_DT, TOT_MDCD_PD_AMT,
       'EXCESSIVE_DAILY_VOLUME',
       'Implausible number of claims for one beneficiary in a single day'
FROM daily_volume
WHERE daily_claim_count >= 15

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### How many claims did each rule flag?
-- MAGIC A quick sanity check and a useful summary for reporting.

-- COMMAND ----------

SELECT flag_rule,
       COUNT(*) AS flagged_claims,
       ROUND(SUM(TOT_MDCD_PD_AMT), 0) AS flagged_dollars
FROM gold.tmsis_improper_payment_flags
GROUP BY flag_rule
ORDER BY flagged_dollars DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Which providers should investigators look at first?
-- MAGIC Rank providers by the total dollars on their flagged claims and how many distinct
-- MAGIC rules they trip. Providers that appear across several rules with high dollars are
-- MAGIC the strongest leads for a program-integrity review.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.tmsis_provider_risk_summary AS
SELECT PRVDR_ID,
       COUNT(*) AS flagged_claims,
       COUNT(DISTINCT flag_rule) AS distinct_rules,
       ROUND(SUM(TOT_MDCD_PD_AMT), 0) AS flagged_dollars
FROM gold.tmsis_improper_payment_flags
GROUP BY PRVDR_ID
ORDER BY flagged_dollars DESC;

SELECT * FROM gold.tmsis_provider_risk_summary
ORDER BY flagged_dollars DESC
LIMIT 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Congratulations on Completing the Notebook
-- MAGIC You now have two gold tables the rest of the workshop can build on:
-- MAGIC * `gold.tmsis_improper_payment_flags` — one row per flagged claim and rule
-- MAGIC * `gold.tmsis_provider_risk_summary` — providers ranked by flagged dollars
-- MAGIC
-- MAGIC These feed naturally into a **program-integrity dashboard** (Lab 4) and into
-- MAGIC **Genie** (Lab 5), where you can ask questions like *"Which providers have the most
-- MAGIC duplicate claims?"* or *"Show me claims paid before the service date."*
