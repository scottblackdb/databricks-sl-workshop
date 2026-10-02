# Databricks notebook source
import random
from datetime import datetime, timedelta

# Helper functions to generate realistic data
def generate_date(start_date, end_date):
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randrange(days_between)
    return start_date + timedelta(days=random_days)

def generate_npi():
    return f"NPI{''.join([str(random.randint(0, 9)) for _ in range(9)])}"

def generate_bene_id(state):
    return f"{state}{''.join([str(random.randint(0, 9)) for _ in range(9)])}"

# Common diagnosis codes with realistic frequencies
diagnoses = [
    ("E11.9", "Type 2 diabetes without complications"),
    ("I10", "Essential hypertension"),
    ("J44.9", "COPD, unspecified"),
    ("F41.1", "Generalized anxiety disorder"),
    ("M54.5", "Low back pain"),
    ("J02.9", "Acute pharyngitis"),
    ("N39.0", "Urinary tract infection"),
    ("K21.9", "GERD without esophagitis"),
    ("E78.5", "Dyslipidemia"),
    ("F32.9", "Major depressive disorder")
]

# Common procedure codes
procedures = [
    "99213",  # Office visit, established patient
    "99214",  # Office visit, established patient, moderate
    "99215",  # Office visit, established patient, complex
    "99283",  # Emergency department visit
    "71045",  # Chest X-ray
    "80053",  # Comprehensive metabolic panel
    "85025",  # Complete blood count
    "93000",  # ECG with interpretation
    "90471",  # Immunization administration
    "99232"   # Subsequent hospital care
]

# Generate 100 records
claims_data = []
base_date = datetime(2023, 1, 1)
end_date = datetime(2024, 1, 2)

for i in range(150000):
    service_date = generate_date(base_date, end_date)
    
    # Determine if inpatient stay
    is_inpatient = random.random() < 0.15  # 15% chance of inpatient
    
    if is_inpatient:
        length_of_stay = random.randint(1, 14)
        admission_dt = service_date
        discharge_dt = service_date + timedelta(days=length_of_stay)
        service_end_date = discharge_dt
        bill_type = "111"
        base_amount = random.uniform(2000, 8000)
    else:
        admission_dt = None
        discharge_dt = None
        service_end_date = service_date
        bill_type = "131"
        base_amount = random.uniform(50, 300)

    # Generate diagnoses
    num_diagnoses = random.randint(1, 3)
    selected_diagnoses = random.sample(diagnoses, num_diagnoses)
    
    # Generate procedures
    num_procedures = random.randint(1, 2)
    selected_procedures = random.sample(procedures, num_procedures)

    submitted_state = random.choice(["NY", "CA", "TX", "FL", "IL"])

    claim = {
        "TMSIS_RUN_ID": "20240102_154523",
        "SUBMTG_STATE_CD": submitted_state,
        "ICN_NUM": f"202401020{str(i+4).zfill(4)}",
        "ADJSTMT_CLM_NUM": "0",
        "ORGNL_CLM_NUM": None,
        "ADJDCTN_DT": service_date + timedelta(days=random.randint(3, 7)),
        "MDCD_PD_DT": service_date + timedelta(days=random.randint(5, 10)),
        "SRVC_BGNNG_DT": service_date,
        "SRVC_ENDG_DT": service_end_date,
        "ADMISSION_DT": admission_dt,
        "DSCHRG_DT": discharge_dt,
        "BENE_ID": generate_bene_id(random.choice(["NY", "CA", "TX", "FL", "IL"])),
        "BIRTH_DT": generate_date(datetime(1940, 1, 1), datetime(2020, 12, 31)),
        "GNDR_CD": random.choice(["F", "M"]),
        "RACE_CD": random.choice(["1", "2", "3", "4", "5"]),
        "ETHNICITY_CD": random.choice(["H", "N"]),
        "PRVDR_ID": generate_npi(),
        "PRVDR_TYPE_CD": random.choice(["01", "08", "11", "24", "82"]),
        "BILL_TYPE_CD": bill_type,
        "CLM_TYPE_CD": "1",
        "DGNS_CD_1": selected_diagnoses[0][0],
        "DGNS_CD_2": selected_diagnoses[1][0] if len(selected_diagnoses) > 1 else None,
        "DGNS_CD_3": selected_diagnoses[2][0] if len(selected_diagnoses) > 2 else None,
        "PRCDR_CD_1": selected_procedures[0],
        "PRCDR_CD_2": selected_procedures[1] if len(selected_procedures) > 1 else None,
        "TOT_MDCD_PD_AMT": round(base_amount * random.uniform(0.9, 1.1), 2),
        "TOT_COPAY_AMT": 0.00,
        "MANAGED_CARE_PLAN_ID": f"MCP{submitted_state}00{random.randint(1,5)}"
    }
    claims_data.append(claim)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seed improper-payment / fraud patterns
# MAGIC The claims above are fully random, so program-integrity and anomaly labs have
# MAGIC nothing to detect. Here we append a small population (~2%) of records that carry
# MAGIC recognizable improper-payment signatures. Each pattern is detected by the SQL
# MAGIC rules in `Lab 1 - Data Ingestion/1.8 Gold - Improper Payment Flags.sql`.
# MAGIC
# MAGIC The same logic is available as a standalone, re-runnable script
# MAGIC (`Data Generation/Inject Improper Payments.py`) that seeds the committed
# MAGIC `workshop_setup/tmsis_claims.json` directly.

# COMMAND ----------

import copy

DAY = timedelta(days=1)
SEEDED_ICN_PREFIX = "9900"
BAD_PROVIDER_NPIS = [f"NPI99900{i:04d}" for i in range(6)]
_seed_seq = 0

def _seed_clone(src):
    global _seed_seq
    rec = copy.deepcopy(src)
    rec["ICN_NUM"] = f"{SEEDED_ICN_PREFIX}{_seed_seq:09d}"
    _seed_seq += 1
    return rec

outpatient = [c for c in claims_data if c["BILL_TYPE_CD"] == "131"]
inpatient = [c for c in claims_data if c["ADMISSION_DT"] and c["DSCHRG_DT"]]
recent_birth = [c for c in claims_data if c["BIRTH_DT"] > datetime(1980, 1, 1)]
seeded = []

# A. Duplicate paid claims — exact re-bill with a new ICN
for src in random.sample(outpatient, 500):
    seeded.append(_seed_clone(src))

# B. Paid before service — payment dated before service began
for src in random.sample(outpatient, 300):
    rec = _seed_clone(src)
    rec["MDCD_PD_DT"] = rec["SRVC_BGNNG_DT"] - random.randint(5, 30) * DAY
    seeded.append(rec)

# C. Discharge before admission
for src in random.sample(inpatient, 200):
    rec = _seed_clone(src)
    rec["DSCHRG_DT"] = rec["ADMISSION_DT"] - random.randint(1, 5) * DAY
    seeded.append(rec)

# D. Service before birth
for src in random.sample(recent_birth, 150):
    rec = _seed_clone(src)
    rec["SRVC_BGNNG_DT"] = rec["BIRTH_DT"] - random.randint(30, 3650) * DAY
    seeded.append(rec)

# E. Provider outlier + upcoding — 6 NPIs billing inflated outpatient office visits
for _ in range(1200):
    rec = _seed_clone(random.choice(outpatient))
    rec["PRVDR_ID"] = random.choice(BAD_PROVIDER_NPIS)
    rec["PRCDR_CD_1"] = "99215"
    rec["BILL_TYPE_CD"] = "131"
    rec["TOT_MDCD_PD_AMT"] = round(random.uniform(1500, 3500), 2)
    seeded.append(rec)

# F. Same beneficiary billed in two states on the same day
for src in random.sample(outpatient, 150):
    rec = _seed_clone(src)
    rec["SUBMTG_STATE_CD"] = random.choice(
        [s for s in ["NY", "CA", "TX", "FL", "IL"] if s != src["SUBMTG_STATE_CD"]]
    )
    seeded.append(rec)

# G. Excessive daily visit volume — 5 beneficiaries with ~30 visits in one day
for _ in range(5):
    base = random.choice(outpatient)
    for _ in range(random.randint(25, 35)):
        rec = _seed_clone(base)
        rec["BENE_ID"] = base["BENE_ID"]
        rec["SRVC_BGNNG_DT"] = base["SRVC_BGNNG_DT"]
        rec["SRVC_ENDG_DT"] = base["SRVC_BGNNG_DT"]
        rec["PRCDR_CD_1"] = random.choice(["99213", "99214", "99215"])
        rec["PRVDR_ID"] = random.choice(BAD_PROVIDER_NPIS)
        seeded.append(rec)

claims_data.extend(seeded)
print("Seeded", len(seeded), "improper-payment records")

# COMMAND ----------

# Print first few records to verify
print("Generated", len(claims_data), "records")
print("\nExample record:")
for key, value in claims_data[0].items():
    print(f"{key}: {value}")

# COMMAND ----------

import pandas as pd

df = spark.createDataFrame(pd.DataFrame(claims_data))

df.coalesce(1).write.mode("overwrite").format("json").save("abfss://filedrops@scottblackadls.dfs.core.windows.net/json_files/tmsis_claims/")

# COMMAND ----------

import pandas as pd

df = pd.DataFrame(claims_data)
display(df)

df.to_json("/Volumes/quickstart_catalog/quickstart_schema/ext/tmsis_claims.json", orient="records")

# COMMAND ----------

df1 = spark.createDataFrame(df)
display(df1)

# COMMAND ----------

display(spark.read.json("/Volumes/quickstart_catalog/quickstart_schema/ext/tmsis_claims.json"))


# COMMAND ----------

dbutils.fs.ls("abfss://filedrops@scottblackadls.dfs.core.windows.net/json_files/tmsis_claims/")

# COMMAND ----------


