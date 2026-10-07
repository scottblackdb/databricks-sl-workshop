# Lab 5 — Instructor Setup (Genie Agent)

**Audience:** workshop instructor. Students do not read this file.

Lab 5 asks students to open a Genie Agent named **TMSIS Genie Agent** and ask it
questions. That Agent cannot be created before the workshop starts, because it
has to point at tables the students build during Lab 1 and Lab 2. This file is
the run-book for creating it mid-workshop.

The approach: pick **one** student's catalog, open it up for read access to
everyone, and build a single shared Genie Agent on top of it. Every student then
asks questions against that one catalog.

## When to do this

Run these steps **after students finish Lab 1 and Lab 2, and before they start
Lab 5.** The break between Lab 4 and Lab 5 is the natural window. Budget about
15 minutes.

The Agent depends on tables created in:

| Table | Created by |
|---|---|
| `silver.tmsis_claims` | Lab 1 — `1.5 Silver Clean TMSIS.ipynb` |
| `silver.medical_providers` | Lab 1 — `1.4 Silver Clean Providers.sql` |
| `gold.provider_review_summary` | Lab 1 — `1.7 Gold Aggregate Provider Claims.sql` |
| `gold.tmsis_improper_payment_flags` | Lab 1 — `1.8 Gold - Improper Payment Flags.sql` |
| `gold.tmsis_provider_risk_summary` | Lab 1 — `1.8 Gold - Improper Payment Flags.sql` |

## Prerequisites

- You are a **metastore admin** (or the chosen student runs the grant in Step 2 themselves — they own their catalog).
- A **Pro or Serverless SQL warehouse** is running. Genie will not work on a Classic warehouse.
- **Databricks Assistant is enabled** for the workspace (Settings → Advanced). Genie depends on it.
- The `account users` group exists. The workshop setup script (`workshop_setup/setup_workshop.py`) already grants to this principal, so it should be present.

---

## Step 1 — Pick a student catalog and verify it is complete

Participant catalogs are named from the student's email: local part, dots
removed, any other non-alphanumeric character turned into `_`, then `_dev`.
So `jane.doe@state.gov` → `janedoe_dev`.

List the candidates:

```sql
SHOW CATALOGS LIKE '*_dev';
```

**Verify before you commit to one.** Two of the five tables are deliberately
left as fill-in-the-blank exercises (`1.4` and `1.5` both end with "finish the
SQL statement"), so a student who got stuck will have an incomplete catalog.
Run this against your candidate, substituting the catalog name:

```sql
SELECT 'silver.tmsis_claims'               AS tbl, COUNT(*) AS rows FROM <catalog>.silver.tmsis_claims
UNION ALL SELECT 'silver.medical_providers',          COUNT(*) FROM <catalog>.silver.medical_providers
UNION ALL SELECT 'gold.provider_review_summary',      COUNT(*) FROM <catalog>.gold.provider_review_summary
UNION ALL SELECT 'gold.tmsis_improper_payment_flags', COUNT(*) FROM <catalog>.gold.tmsis_improper_payment_flags
UNION ALL SELECT 'gold.tmsis_provider_risk_summary',  COUNT(*) FROM <catalog>.gold.tmsis_provider_risk_summary;
```

All five must exist and return a non-zero count. If any error out or come back
empty, pick a different catalog. Your own catalog is the safest choice if you
worked the labs alongside the class.

Tell the class whose catalog you picked — the Agent's answers are that student's
data, and a couple of the Lab 5 questions are more interesting if the room knows
the numbers are real.

## Step 2 — Grant read access on the catalog to everyone

Genie runs each query **as the user asking the question**, not as the Agent's
creator. So every student needs `SELECT` on the chosen catalog or they will get
permission errors even though the Agent works fine for you.

Grant at the catalog level so it cascades to all schemas and tables:

```sql
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG <catalog> TO `account users`;
```

Confirm it took:

```sql
SHOW GRANTS ON CATALOG <catalog>;
```

> The student owns their catalog (the setup script transfers ownership to each
> participant). If you are not a metastore admin, have that student run the
> `GRANT` instead — as owner they can.

Students also need to be able to use the warehouse Genie runs on:

```bash
databricks permissions update warehouses <WAREHOUSE_ID> --profile <PROFILE> --json '{
  "access_control_list": [
    { "group_name": "account users", "permission_level": "CAN_USE" }
  ]
}'
```

## Step 3 — Create the Genie Agent

Create it from **Genie → New** in the left nav, or with the CLI (see the bottom
of this file).

**Title it exactly `TMSIS Genie Agent`.** Lab 5 walks students through the Genie
nav with a screenshot and then tells them to click on that exact name. A
different title means the room gets stuck on the first instruction.

**Description** (required — it is what makes the Agent usable and is also what
multi-agent routing reads):

> Medicaid TMSIS claims, provider reviews, and program-integrity data for a
> state Medicaid agency. Answers questions about claim volumes and paid amounts
> over time, provider review ratings and sentiment, and improper-payment flags
> and the providers behind them.

### Tables to add

Add exactly these five. They are all in the catalog you chose in Step 1:

| Table | Why it belongs | Lab 5 questions it answers |
|---|---|---|
| `silver.tmsis_claims` | Claim-level detail — paid amounts, service/discharge/payment dates, provider, diagnosis, bill type | Total claim amount by day; highest-amount day; average daily total; paid amount by discharge date; claims discharged each month |
| `silver.medical_providers` | Provider reviews joined with rating, the `low`/`average`/`high` ranking, and `review_sentiment` from `ai_analyze_sentiment` | Negative sentiment but high rating; percentage of reviews ranked low/average/high |
| `gold.provider_review_summary` | One pre-summarized row per rating, built with `ai_summarize` | Summarize reviews for rating 5; common complaints in low-rated reviews |
| `gold.tmsis_improper_payment_flags` | One row per flagged claim and rule, from the seven Lab 1 edit checks | Outpatient overpayments by provider; paid before service date; duplicate claims; beneficiaries billed in two states same day |
| `gold.tmsis_provider_risk_summary` | Providers ranked by flagged dollars and distinct rules tripped | Which providers should investigators review first |

**Do not add:**

- **Bronze tables.** They hold epoch-millisecond dates and raw column names. Genie will produce confusing answers from them.
- **`gold.tmsis_daily_billing_summary`** and **`silver.tmsis_stats`.** Both are plain aggregates of `silver.tmsis_claims`. Giving Genie a detail table and its own rollup makes it guess which to use, and the totals look inconsistent when it guesses differently across two questions.
- **`gold.tmsis_forecast`** unless you want forecast questions. It is not needed by any question in Lab 5, and the Lab 4 dashboard already covers the forecast story.

Five tables is deliberate — Genie answers better on a tight source set, and the
hard ceiling is 30. Resist adding the rest of the catalog.

### Sample questions

Paste in the same questions Lab 5 lists for students, so the Agent's suggestion
chips match the notebook:

```
What was the total claim amount by day for the last 30 days?
Which day had the highest total claim amount?
What percentage of reviews are ranked as low, average, or high?
Which providers billed the most in outpatient overpayments?
Which providers should investigators review first based on flagged claims?
```

### Instructions

Add these under the Agent's general instructions. They head off the three things
that reliably confuse Genie on this dataset:

```markdown
## PURPOSE
- Answer questions about Medicaid TMSIS claims, provider reviews, and improper payments for a state Medicaid agency.
- Users are non-technical program staff — no SQL knowledge assumed.

## DISAMBIGUATION
- "Improper payments", "fraud", "flagged claims", and "suspect claims" all mean rows in gold.tmsis_improper_payment_flags.
- "Overpayment" on its own means flag_rule = 'OUTPATIENT_OVERPAYMENT'.
- Use silver.tmsis_claims for claim counts and paid amounts; use the gold flag tables only for program-integrity questions.

## DATA QUALITY NOTES
- A claim appears once per rule it trips in gold.tmsis_improper_payment_flags, so COUNT(*) there counts flags, not distinct claims. Use COUNT(DISTINCT ICN_NUM) for claims.
- DSCHRG_DT and ADMISSION_DT are NULL on outpatient claims — exclude NULLs in date-based trends.
- silver.medical_providers is one row per review, not per provider.

## Instructions you must follow when providing summaries
- Always state the date range the answer covers.
- Round dollar amounts to whole dollars.
- When reporting flagged claims, name the rule or rules involved.
```

## Step 4 — Grant students access to the Agent

In the Agent, use **Share** and add the `account users` group with **Can run**.
Can run lets them ask questions without being able to change the Agent's
configuration — which is what you want with a room full of people.

Or via CLI, using the space ID from the Agent's URL:

```bash
databricks permissions update genie <SPACE_ID> --profile <PROFILE> --json '{
  "access_control_list": [
    { "group_name": "account users", "permission_level": "CAN_RUN" }
  ]
}'
```

`update` adds to the existing ACL; `set` would replace it and drop your own
access, so use `update`. To confirm the available levels on your workspace:

```bash
databricks permissions get-permission-levels genie <SPACE_ID> --profile <PROFILE>
```

## Step 5 — Verify as a student, not as yourself

You own the Agent and the warehouse, so it will work for you regardless of
whether Step 2 and Step 4 actually landed. Before sending the class in, confirm
from a non-admin account — or ask one student to run two questions while you
watch:

1. *"Which day had the highest total claim amount?"* — proves catalog `SELECT` and warehouse access, and always returns a row.
2. *"Which providers should investigators review first based on flagged claims?"* — proves the gold tables resolve.

Click **Show generated code** on an answer. That the SQL is visible and
reviewable is a point Lab 5 makes explicitly, and it is worth demoing once.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Student sees `PERMISSION_DENIED` on a table | Step 2 grant missing or run on the wrong catalog | Re-run the `GRANT`, confirm with `SHOW GRANTS ON CATALOG` |
| Student cannot see the Agent in the Genie list | Step 4 not applied | Add `account users` with Can run |
| "No warehouse available" or queries never start | Warehouse stopped, Classic type, or no `CAN_USE` | Start a Pro/Serverless warehouse and grant `CAN_USE` |
| Program-integrity questions return nothing | Notebook `1.8` never ran in the chosen catalog | Run `1.8` in that catalog, or pick a catalog where it completed |
| Review-sentiment questions fail | `1.4` left unfinished — `silver.medical_providers` missing or lacks `review_sentiment` | Pick a different catalog, or complete `1.4` there |
| Flagged-claim counts look too high | Genie counted flag rows instead of distinct claims | Covered by the `DATA QUALITY NOTES` instruction — confirm it was pasted in |
| Genie answers from the wrong table | Extra tables were added beyond the five | Remove the extras; keep the source set tight |

## After the workshop

The Agent points at one student's catalog, which outlives the session. If the
catalogs get cleaned up, trash the Agent too so it does not sit there broken:

```bash
databricks genie trash-space <SPACE_ID> --profile <PROFILE>
```

Also revoke the catalog grant if the catalog is being kept:

```sql
REVOKE USE CATALOG, USE SCHEMA, SELECT ON CATALOG <catalog> FROM `account users`;
```

## Appendix — creating the Agent from the CLI

The UI is usually faster for a one-off, but if you would rather script it:

```bash
# Find a Pro/Serverless warehouse
databricks experimental aitools tools get-default-warehouse --profile <PROFILE>

# The parent folder must already exist
databricks workspace mkdirs /Workspace/Users/<you>/genie_spaces --profile <PROFILE>

databricks genie create-space --profile <PROFILE> --json "{
  \"warehouse_id\": \"<WAREHOUSE_ID>\",
  \"title\": \"TMSIS Genie Agent\",
  \"description\": \"Medicaid TMSIS claims, provider reviews, and program-integrity data.\",
  \"parent_path\": \"/Workspace/Users/<you>/genie_spaces\",
  \"serialized_space\": $(cat genie_agent.json | jq -c '.' | jq -Rs '.')
}"
```

Where `genie_agent.json` carries the table list, sample questions, and
instructions from Step 3. Then apply Step 4 to share it.
