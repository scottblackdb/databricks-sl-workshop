# Databricks State & Local Workshop
Databricks workshop designed for State & Local departments and agencies.

The workshop involves ingesting healthcare and finance data.
After completing the workshop students should have a basic understanding of ingesting data with workflows, quesrying data with DBSQL, creating a ML model and using Genie to ask questions

One of the recurring themes is **program integrity** — using TMSIS claims to find improper payments and potential fraud. Lab 1 builds a gold `tmsis_improper_payment_flags` table (duplicate claims, payments before service, impossible dates, outpatient overpayments, and other classic Medicaid edit checks) and a provider risk summary that can be explored in DBSQL (Lab 4) and Genie (Lab 5).

## Background
You are a data architect for a state department responbilie for managing the state's medicaid program. Currently the majority of the data resides in on premise datacenters in traditional databases. Over the past several years the cost of manging software licenses and hardware has grown significantly. At the same time innovation has slowed as most of the time is spent managing existing data pipelines that fail or run too slow and helping users performance tune their SQL queries. There is a small group of health informaticists which now is expanding and have plans to do more with ML & AI. This is the straw that is broke their current architecture. You have been tasked with evaluting Databricks as the replacement architecture.

At the same time, leadership is under pressure to reduce **improper payments** — claims that were paid in error, billed twice, or that show signs of fraud. Medicaid improper payments run into the tens of billions of dollars a year nationally, and the TMSIS claims data is where a program-integrity team goes to find them. As you evaluate Databricks, you will also build the pipeline and analytics that surface these suspect claims and the providers behind them, so the agency can investigate and recover the money.

## For Instructors
Lab 5's Genie Agent has to be created mid-workshop, because it reads tables the students build in Labs 1 and 2. See [Lab 5 Instructor Setup](<Lab 5 - Asking Natural Language Questions/INSTRUCTOR_README.md>) for the run-book: choosing a participant catalog, granting access, and the tables to attach to the Agent.
