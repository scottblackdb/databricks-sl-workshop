-- Databricks notebook source
-- MAGIC %run "../Lab 0 - Setup/SetCatalogName"

-- COMMAND ----------

select adjdctn_dt, admission_dt,birth_dt, mdcd_pd_dt,srvc_bgnng_dt,srvc_endg_dt,dschrg_dt
from bronze.tmsis_claims

-- COMMAND ----------

select * except (adjdctn_dt,admission_dt,birth_dt,mdcd_pd_dt,srvc_bgnng_dt,srvc_endg_dt,dschrg_dt),
  to_timestamp(from_unixtime(dschrg_dt/1000)) as DSCHRG_DT, 
  to_timestamp(from_unixtime(adjdctn_dt/1000)) as ADJDCTN_DT, 
  to_timestamp(from_unixtime(admission_dt/1000)) as ADMISSION_DT,
  to_timestamp(from_unixtime(birth_dt/1000)) as BIRTH_DT,
  to_timestamp(from_unixtime(mdcd_pd_dt/1000)) as MDCD_PD_DT,
  to_timestamp(from_unixtime(srvc_bgnng_dt/1000)) as SRVC_BGNNG_DT,
  to_timestamp(from_unixtime(srvc_endg_dt/1000)) SRVC_ENDG_DT,
  current_timestamp as CLAIM_RCV_DT
from bronze.tmsis_claims

-- COMMAND ----------

create or replace table silver.tmsis_claims
as
select * except (adjdctn_dt,admission_dt,birth_dt,mdcd_pd_dt,srvc_bgnng_dt,srvc_endg_dt,dschrg_dt),
  to_timestamp(from_unixtime(dschrg_dt/1000)) as DSCHRG_DT, 
  to_timestamp(from_unixtime(adjdctn_dt/1000)) as ADJDCTN_DT, 
  to_timestamp(from_unixtime(admission_dt/1000)) as ADMISSION_DT,
  to_timestamp(from_unixtime(birth_dt/1000)) as BIRTH_DT,
  to_timestamp(from_unixtime(mdcd_pd_dt/1000)) as MDCD_PD_DT,
  to_timestamp(from_unixtime(srvc_bgnng_dt/1000)) as SRVC_BGNNG_DT,
  to_timestamp(from_unixtime(srvc_endg_dt/1000)) SRVC_ENDG_DT,
  current_timestamp as CLAIM_RCV_DT
from bronze.tmsis_claims
