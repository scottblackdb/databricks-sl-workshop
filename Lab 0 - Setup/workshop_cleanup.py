# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Cleanup
# MAGIC
# MAGIC Removes everything the student notebooks created in **your own catalog**:
# MAGIC the bronze, silver, and gold tables from Labs 1, 2, and 4, and the Lakeview
# MAGIC dashboard imported in Lab 4.
# MAGIC
# MAGIC This notebook is **safe by default** — it does nothing until you set the
# MAGIC `confirm_delete` widget at the top to `yes`. Run it once as-is to see what it
# MAGIC would remove, then flip the widget and run it again.
# MAGIC
# MAGIC It only ever touches a fixed list of workshop table names, so anything else
# MAGIC you created in your catalog is left alone. Schemas and the catalog itself are
# MAGIC also left in place.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Setup
# MAGIC Resolve your catalog name the same way every other lab does.

# COMMAND ----------

# MAGIC %run "../Lab 0 - Setup/SetCatalogName"

# COMMAND ----------

dbutils.widgets.dropdown("confirm_delete", "no", ["no", "yes"], "Actually delete?")
dbutils.widgets.text("dashboard_name_contains", "TMSIS", "Dashboard name contains")

confirm = dbutils.widgets.get("confirm_delete") == "yes"
dashboard_filter = dbutils.widgets.get("dashboard_name_contains").strip()

print(f"Catalog:        {ctlg}")
print(f"Confirm delete: {confirm}")
if not confirm:
    print("\nDRY RUN — nothing will be deleted. Set 'Actually delete?' to 'yes' to remove.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables created by the labs
# MAGIC One entry per table the student notebooks write, with the notebook that
# MAGIC creates it. Nothing outside this list is touched.

# COMMAND ----------

WORKSHOP_TABLES = [
    # schema.table                        created by
    ("bronze.medical_providers",           "1.1 Bronze Ingest Medical Providers Locations"),
    ("bronze.provider_reviews",            "1.2 Bronze Ingest Provider Reviews"),
    ("bronze.tmsis_claims",                "1.3 Bronze Ingest TMSIS"),
    ("silver.medical_providers",           "1.4 Silver Clean Providers"),
    ("silver.tmsis_claims",                "1.5 Silver Clean TMSIS"),
    ("silver.tmsis_stats",                 "Lab 4 Create Silver Stats Table / Extra Labs"),
    ("gold.tmsis_daily_billing_summary",   "1.6 Gold - TMSIS Monthly Summary"),
    ("gold.provider_review_summary",       "1.7 Gold Aggregate Provider Claims"),
    ("gold.tmsis_improper_payment_flags",  "1.8 Gold - Improper Payment Flags"),
    ("gold.tmsis_provider_risk_summary",   "1.8 Gold - Improper Payment Flags"),
    ("gold.tmsis_forecast",                "Lab 2 - Python Forecast"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which of them actually exist?
# MAGIC A student who stopped partway through will not have all of these, so check
# MAGIC before trying to drop anything.

# COMMAND ----------

existing = []
missing = []

for name, source in WORKSHOP_TABLES:
    try:
        found = spark.catalog.tableExists(f"{ctlg}.{name}")
    except Exception:
        # Missing catalog or schema raises rather than returning False.
        found = False
    (existing if found else missing).append((name, source))

print(f"Found {len(existing)} of {len(WORKSHOP_TABLES)} workshop tables in {ctlg}\n")
for name, source in existing:
    print(f"  [found]   {ctlg}.{name:<38} ({source})")
for name, source in missing:
    print(f"  [absent]  {ctlg}.{name:<38} ({source})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop the tables
# MAGIC Views are dropped first where a lab left one behind, then tables. Each drop is
# MAGIC independent so one failure does not stop the rest.

# COMMAND ----------

dropped, failed = [], []

for name, _ in existing:
    full = f"{ctlg}.{name}"
    if not confirm:
        print(f"  [dry run] would drop {full}")
        continue
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full}")
        dropped.append(full)
        print(f"  [dropped] {full}")
    except Exception as e:
        failed.append((full, str(e)))
        print(f"  [failed]  {full} — {e}")

if confirm:
    print(f"\nDropped {len(dropped)} table(s), {len(failed)} failure(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Lab 4 dashboard
# MAGIC Lab 4 has you import a dashboard from a JSON file, so its name is whatever you
# MAGIC saved the file as. We look for dashboards in **your own workspace folder**
# MAGIC whose name contains the text in the `dashboard_name_contains` widget — by
# MAGIC default `TMSIS`. Widen or narrow that if you named yours something else.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
home = f"/Users/{me}/"

candidates = []
try:
    # The list response does not include the dashboard's workspace path, so match
    # on the name first and then fetch each match to confirm it is yours. That
    # keeps us from ever trashing a dashboard in someone else's folder.
    for d in w.lakeview.list():
        name = d.display_name or ""
        if dashboard_filter and dashboard_filter.lower() not in name.lower():
            continue
        try:
            path = w.lakeview.get(d.dashboard_id).path or ""
        except Exception as e:
            print(f"  [skipped] {name} — could not read its path ({e})")
            continue
        if not path.startswith(home):
            print(f"  [skipped] {name} — not in your folder ({path})")
            continue
        candidates.append((d, path))
except Exception as e:
    print(f"Could not list dashboards — {e}")
    print("Delete the Lab 4 dashboard by hand: Dashboards -> the three-dot menu -> Move to trash.")

print(f"\nMatched {len(candidates)} dashboard(s) under {home} containing '{dashboard_filter}'\n")
for d, path in candidates:
    print(f"  {d.display_name}")
    print(f"    id:   {d.dashboard_id}")
    print(f"    path: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Dashboards are moved to the **trash**, not purged, so a mistake here is
# MAGIC recoverable from the Dashboards trash view for a short window.

# COMMAND ----------

trashed, trash_failed = [], []

for d, path in candidates:
    if not confirm:
        print(f"  [dry run] would trash {d.display_name} ({d.dashboard_id})")
        continue
    try:
        w.lakeview.trash(d.dashboard_id)
        trashed.append(d.display_name)
        print(f"  [trashed] {d.display_name}")
    except Exception as e:
        trash_failed.append((d.display_name, str(e)))
        print(f"  [failed]  {d.display_name} — {e}")

if confirm:
    print(f"\nTrashed {len(trashed)} dashboard(s), {len(trash_failed)} failure(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

if not confirm:
    print("DRY RUN — nothing was deleted.")
    print(f"  {len(existing)} table(s) would be dropped")
    print(f"  {len(candidates)} dashboard(s) would be trashed")
    print("\nSet the 'Actually delete?' widget to 'yes' and re-run to remove them.")
else:
    print(f"Tables dropped:      {len(dropped)}")
    print(f"Dashboards trashed:  {len(trashed)}")
    if failed or trash_failed:
        print(f"\nFailures: {len(failed)} table(s), {len(trash_failed)} dashboard(s) — see the cells above.")
    print(f"\nThe {ctlg} catalog and its bronze/silver/gold schemas were left in place.")
    print("Re-run the Lab 1 notebooks to rebuild the tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cleanup Complete
# MAGIC Your catalog and its empty bronze, silver, and gold schemas are still there,
# MAGIC so you can re-run the labs from Lab 1 without any setup.
# MAGIC
# MAGIC Things this notebook deliberately leaves alone:
# MAGIC * the catalog and the three schemas
# MAGIC * any table you created yourself that is not on the workshop list
# MAGIC * the MLflow experiment and runs from Lab 2
# MAGIC * the Genie Agent, which is shared and owned by the instructor
