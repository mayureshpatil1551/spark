# Real World Interview Q&A — ECDP & FinOps Dataverse Projects
### Databricks | ADF | PySpark | Azure Data Engineering

> **How to use this:** Read the question, cover the answer, and speak it out loud. 
> These are written in first-person so you can say them directly in the interview.

---

## TABLE OF CONTENTS

1. [ADF — Real World Challenges](#adf)
2. [Databricks — Real World Challenges](#databricks)
3. [PySpark / Spark — Real World Challenges](#pyspark)
4. [Architecture & Design Challenges](#architecture)
5. [CI/CD & Deployment Challenges](#cicd)
6. [Data Quality & Pipeline Reliability](#dataquality)

---

<a name="adf"></a>
## 1. ADF — Real World Challenges

---

### Q1. Tell me about a time your ADF pipeline failed in production. What happened and how did you fix it?

**Situation:**
In our ECDP project, we had an ADF pipeline that ingested financial data from an Oracle source into ADLS Gen2. One morning the pipeline failed silently — it showed as "Succeeded" in ADF Monitor but the target Delta table had zero new records.

**Challenge:**
The pipeline had no row count validation. The Copy Activity completed without error because technically it ran fine — but the source query returned 0 rows due to a watermark logic bug. The watermark value in our Azure SQL control table had been accidentally updated to a future timestamp during a manual hotfix the previous night.

**How I resolved it:**
- I added a **Lookup Activity** before the Copy Activity to fetch the watermark value and validate it — if the watermark is in the future compared to `GETDATE()`, the pipeline raises an error immediately.
- I added a **row count check** after the Copy Activity using a Validation Activity — if `rowsCopied == 0`, the pipeline sends an alert via Logic App email notification and fails gracefully instead of silently succeeding.
- Long term, I restricted manual write access to the control table and made all watermark updates go through the pipeline itself.

**What I learned:**
A pipeline that completes without errors is not the same as a pipeline that did the right thing. Always validate the *outcome*, not just the *execution*.

---

### Q2. How did you handle dynamic pipelines in ADF to avoid code duplication across 20+ tables?

**Situation:**
In ECDP, we had 25+ source tables that needed to be ingested from Oracle into Bronze layer following the same pattern — extract, copy to ADLS, trigger Databricks notebook for transformation.

**Challenge:**
The initial approach had one ADF pipeline per table — 25 pipelines with nearly identical logic. Any change (like adding a new audit column or changing the retry policy) had to be applied 25 times. This was unsustainable.

**How I resolved it:**
I redesigned the architecture using a **metadata-driven pipeline pattern**:
- Created a **control table** in Azure SQL with one row per source table, storing: `source_table_name`, `target_path`, `watermark_column`, `load_type` (full/incremental), `is_active` flag.
- Built a **single master ADF pipeline** that uses a **Lookup Activity** to read the control table, then a **ForEach Activity** to iterate over each table config and call a **child pipeline** via Execute Pipeline Activity, passing the table config as parameters.
- The child pipeline is fully parameterized — no hardcoded table names, paths, or queries.

**Result:**
Adding a new source table means adding one row to the control table — zero pipeline changes. When we needed to add audit logging across all tables, I updated one child pipeline instead of 25.

---

### Q3. How did you manage secrets like database passwords and storage keys in ADF?

**Situation:**
We had connection strings, Oracle JDBC passwords, and Snowflake credentials that needed to be used in ADF Linked Services across DEV, UAT, and PROD environments.

**Challenge:**
Initially a team member had hardcoded credentials directly in the Linked Service JSON. This is a security risk — credentials in plain text, checked into Git, visible to everyone with repo access.

**How I resolved it:**
- Moved all secrets to **Azure Key Vault** — one Key Vault per environment (DEV-KV, UAT-KV, PROD-KV).
- Updated ADF Linked Services to use **Key Vault Secret Reference** instead of inline credentials — ADF fetches the secret at runtime using the pipeline's Managed Identity.
- Granted ADF's Managed Identity `Get` and `List` permissions on the Key Vault secrets via Access Policies.
- In ARM templates for CI/CD, the Key Vault name is an **environment-specific parameter**, so the same template deploys correctly to DEV, UAT, and PROD without any manual credential changes.

**Result:**
Zero hardcoded credentials in any config or code. Rotating a password means updating the Key Vault secret only — no pipeline changes needed.

---

### Q4. Your ADF pipeline runs fine in DEV but fails in UAT. How did you debug this?

**Situation:**
We deployed an ADF pipeline from DEV to UAT using our Azure DevOps CI/CD process. The pipeline ran successfully in DEV for weeks but failed immediately in UAT with a connection error on the Oracle Linked Service.

**Challenge:**
The error was generic: *"Unable to connect to Oracle database."* Not helpful on its own.

**How I debugged it:**
1. Checked the **ADF Monitor activity run details** — the error pointed to the Linked Service, not the pipeline logic.
2. Compared DEV and UAT Linked Service configurations — found that the UAT parameter override file had the wrong Oracle hostname (a copy-paste error from a previous sprint).
3. Fixed the parameter override file, redeployed — still failed.
4. Next checked **network connectivity** — UAT Integration Runtime (Self-Hosted IR) didn't have outbound access to the Oracle server's IP on port 1521. Raised a firewall rule request with the infrastructure team.
5. After firewall was opened, tested the Linked Service connection using the **Test Connection** button in ADF — passed. Pipeline ran successfully.

**What I learned:**
Environment-specific failures are almost always either wrong config values or missing network/firewall rules. Build a deployment checklist that explicitly validates Linked Service connections in every environment after deployment.

---

### Q5. How did you implement incremental loads in ADF and what challenges came up?

**Situation:**
In ECDP, we needed incremental ingestion from Oracle financial tables — pulling only new/changed records since the last run, not a full reload every day.

**Challenge:**
The source Oracle tables didn't have a reliable `updated_at` timestamp column on some tables. For others, the timestamp existed but wasn't indexed, making `WHERE updated_at > :watermark` queries extremely slow on 50M+ row tables.

**How I resolved it:**
- For tables **with a timestamp column**: used a watermark-based approach — store the max `updated_at` from the last successful run in the Azure SQL control table, use it as a query parameter in the next run's source query, update the watermark only after a successful pipeline run (not before, to avoid skipping records if the pipeline fails mid-run).
- For tables **without a timestamp**: worked with the source team to enable Oracle **Change Data Capture (CDC)** / LogMiner for those specific tables so we could get a change feed. Where CDC wasn't feasible, we used a **checksum-based approach** — hash key columns and compare against the previous load.
- For the **slow query issue**: requested the DBA to add an index on the watermark column. As an interim fix, used ADF's JDBC parallel read with `partitionColumn` to split the query into parallel chunks, which reduced full-scan time from 45 minutes to 8 minutes.

---

<a name="databricks"></a>
## 2. Databricks — Real World Challenges

---

### Q6. Tell me about a real performance issue you faced in Databricks and how you fixed it.

**Situation:**
In the FinOps Dataverse project, we had a PySpark job running on Databricks that was processing pharmaceutical data — joining a large transactions table (~15GB) with a master data reference table. The job consistently took 3+ hours and sometimes timed out.

**Challenge:**
When I looked at the Spark UI, I saw the join stage had 200 tasks but only 4-5 were taking 90% of the total time — classic data skew. The transactions table had a `country_code` column used as the join key, and one country (US) had 70% of all records.

**How I resolved it:**
1. **Short-term fix**: Enabled AQE — `spark.sql.adaptive.enabled = true` and `spark.sql.adaptive.skewJoin.enabled = true`. This helped Spark automatically split the skewed US partition into smaller sub-tasks. Job time dropped to 90 minutes.
2. **Long-term fix**: Implemented **salt key technique** — appended a random number (0-9) to the join key on the large table, exploded the small table to have 10 copies (one per salt value), joined on the salted key, then aggregated results post-join. This distributed the US records evenly across all tasks. Job time dropped to 35 minutes.
3. Also tuned `spark.sql.shuffle.partitions` from default 200 to 400 to give more parallelism for the large shuffle.

**Result:**
Job went from 3+ hours (with timeouts) to 35 minutes consistently.

---

### Q7. How did you handle schema evolution when source data changed unexpectedly?

**Situation:**
In ECDP, we were ingesting data from a financial source system into our Bronze Delta table. One day the pipeline failed with: *"Column 'tax_code' not found in schema."* The source team had added 3 new columns to their extract without informing us.

**Challenge:**
This was a production pipeline running at 6 AM. Business users were waiting for the data. We needed a fix that was fast but also safe — we couldn't just blindly accept any schema change.

**How I resolved it:**
**Immediate fix:**
- Temporarily set `mergeSchema` option to `true` on the Delta write — this allowed the new columns to be added to the Delta table without failing. Bronze layer was restored within 20 minutes.

**Proper solution (implemented same day):**
- Added a **schema validation step** at the start of the Databricks notebook, before any transformation:
  ```python
  expected_cols = set(expected_schema.fieldNames())
  incoming_cols = set(df.schema.fieldNames())
  
  new_cols = incoming_cols - expected_cols
  missing_cols = expected_cols - incoming_cols
  
  if missing_cols:
      raise Exception(f"CRITICAL: Missing columns: {missing_cols}")
  if new_cols:
      log_warning(f"New columns detected: {new_cols} — adding to schema")
  ```
- New columns are auto-added to Bronze (raw layer should capture everything), but Silver and Gold layers only promote columns that are explicitly mapped in the transformation config.
- Set up an **email alert** when new columns are detected so the team is notified and can update downstream mappings proactively.

**Process fix:**
Established a change notification process with the source team — any schema changes must be communicated 1 sprint ahead.

---

### Q8. How did you manage notebook dependencies and code reuse across multiple Databricks notebooks?

**Situation:**
In ECDP, we had 15+ Databricks notebooks for different source tables. Each notebook had its own copy of common functions — logging, data quality checks, schema validation, reading from Key Vault, etc. When we needed to update the logging format, we had to update 15 notebooks.

**Challenge:**
Code duplication was making maintenance a nightmare. A bug fix in one notebook wasn't being applied to others, leading to inconsistent behavior across the pipeline.

**How I resolved it:**
- Created a **utility notebook** (`/utils/common_functions`) with all shared functions: `get_secret()`, `log_audit()`, `validate_schema()`, `write_to_delta()`, `send_alert()`.
- Used Databricks `%run` to import the utility notebook at the top of every pipeline notebook:
  ```python
  %run ../utils/common_functions
  ```
- For more complex shared logic, packaged functions as a **Python wheel (.whl)** file, published it to DBFS, and installed it as a cluster library — so notebooks import it like any Python package.
- Stored the wheel in our Azure DevOps artifact feed so it goes through the same CI/CD process as everything else.

**Result:**
Any change to shared logic happens in one place. All 15 notebooks automatically pick it up on next run.

---

### Q9. How did you implement Delta Lake MERGE and what issues did you face?

**Situation:**
In ECDP, the Silver layer required upsert logic — if a financial record already existed (matched on `account_id` + `transaction_date`), update it; if new, insert it. We couldn't use simple overwrites because downstream teams had real-time queries running on the Silver table.

**Challenge:**
The first MERGE implementation worked in DEV but was extremely slow in UAT — taking 45 minutes for a 2GB dataset. Also, we hit a **concurrent write conflict** error when two pipeline runs triggered simultaneously due to a trigger timing issue.

**How I resolved it:**

**Performance issue:**
- The MERGE was doing a full table scan to find matching records. Added `ZORDER BY (account_id, transaction_date)` on the Delta table — this co-locates related data in the same files, so the MERGE only scans relevant file subsets. Time dropped from 45 minutes to 6 minutes.
- Also set `spark.databricks.delta.merge.enableLowShuffle = true` to reduce shuffle during MERGE.

**Concurrent write conflict:**
- Delta Lake uses optimistic concurrency — two simultaneous writes to the same table conflict. Fixed by ensuring only one pipeline instance runs at a time using ADF trigger de-duplication (set concurrency = 1 on the pipeline trigger).
- Added retry logic in ADF for transient conflicts with exponential backoff.

---

### Q10. How did you handle failed or partial pipeline runs to avoid reprocessing already-completed data?

**Situation:**
In FinOps Dataverse, the end-to-end pipeline had 5 stages: extract → bronze → silver → gold → export to Snowflake. If the pipeline failed at Stage 4 (gold), we didn't want to re-extract and reprocess all data from Stage 1 — that would take 3 hours unnecessarily.

**Challenge:**
By default, ADF re-runs the entire pipeline from the beginning on retry. We needed checkpoint-style resumption.

**How I resolved it:**
- Implemented a **pipeline state table** in Azure SQL:
  ```
  batch_id | stage_name | status       | start_time | end_time | rows_processed
  B001     | extract    | SUCCESS      | ...        | ...      | 1200000
  B001     | bronze     | SUCCESS      | ...        | ...      | 1200000
  B001     | silver     | FAILED       | ...        | null     | null
  B001     | gold       | NOT_STARTED  | ...        | null     | null
  ```
- Each Databricks notebook checks this table at the start — if its stage is already `SUCCESS` for the current `batch_id`, it skips execution and exits cleanly.
- On retry, only failed and not-started stages run. Already-successful stages are skipped.
- Used Delta Lake's `idempotent write` pattern — even if a stage reruns, it produces the same output (MERGE on business key, not append).

**Result:**
A Stage 4 failure now retries from Stage 4 only — saving 2+ hours of unnecessary reprocessing.

---

<a name="pyspark"></a>
## 3. PySpark / Spark — Real World Challenges

---

### Q11. Describe a situation where your Spark job ran out of memory. How did you debug and fix it?

**Situation:**
In FinOps Dataverse, a nightly Spark job processing 20TB of pharmaceutical data started failing with `java.lang.OutOfMemoryError: GC overhead limit exceeded` on executors. This job had been running fine for 2 weeks.

**Debugging steps:**
1. **Spark UI → Stages tab**: Found the failure in a groupBy + aggregation stage. Task attempt #3 on 4 specific executors was failing — same executors every time.
2. **Executor metrics**: Those executors were processing 10x more data than others — data skew on the `product_category` column. One category (`API_COMPOUND`) had 60% of all records.
3. **Storage tab**: Found we were caching a 15GB DataFrame unnecessarily — an earlier developer had added `.cache()` but never `.unpersist()`. This was eating executor memory before the heavy aggregation stage even started.

**Fixes applied:**
1. Removed the unnecessary `.cache()` — freed significant executor memory immediately.
2. Enabled AQE with skew join handling.
3. For the groupBy skew: used a **two-stage aggregation** (partial aggregation per salt key, then final aggregation) to distribute the `API_COMPOUND` records.
4. Increased `spark.executor.memoryOverhead` from default 10% to 20% since this job does heavy UDF operations that use off-heap memory.

**Result:**
Job went from failing to completing in 55 minutes reliably.

---

### Q12. How did you optimize reading from JDBC sources in Spark without killing the source database?

**Situation:**
In ECDP, we read from an Oracle production database. Initial implementation used a single JDBC connection — one task reading all data serially. For a 50M row table, this took 2 hours and caused noticeable load on the Oracle server during business hours.

**Challenge:**
The DBA team flagged that our job was consuming too many Oracle sessions and slowing down application queries running concurrently.

**How I resolved it:**

**Parallel read with partition bounds:**
```python
df = spark.read.format("jdbc") \
    .option("partitionColumn", "transaction_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "50000000") \
    .option("numPartitions", "20") \
    .option("fetchsize", "50000") \
    .load()
```
This creates 20 parallel JDBC connections, each reading a range of `transaction_id` — distributing load across 20 Oracle sessions instead of one long-running session.

**Additional controls:**
- Set `numPartitions` to 20 (not too high — Oracle had connection limits).
- Increased `fetchsize` to 50000 to reduce round trips per connection.
- Scheduled the job at **2 AM** to avoid overlap with business hour Oracle load.
- Added a **connection pool limit** on the Databricks side to ensure we never exceeded the agreed Oracle connection budget.

**Result:**
Read time dropped from 2 hours to 18 minutes. Oracle DBA confirmed minimal impact on production workloads.

---

### Q13. How did you handle small files problem in your Delta Lake tables?

**Situation:**
In ECDP, after running incremental loads daily for 3 months, our Silver Delta table had accumulated 50,000+ small Parquet files (some as small as 10KB). Query performance on the Gold layer that read from this table degraded significantly — a query that took 2 minutes initially was now taking 18 minutes.

**Root cause:**
Each incremental run was writing data with default partitioning, creating many small files per run. Over 90 days of daily runs, this became a severe small files problem.

**How I resolved it:**
1. **Immediate fix**: Ran `OPTIMIZE` on the Delta table to compact small files into larger ones (target 1GB per file):
   ```sql
   OPTIMIZE silver.financial_transactions
   ZORDER BY (account_id, transaction_date);
   ```
   Query time dropped from 18 minutes to 2.5 minutes after optimization.

2. **Preventive fix**: Before writing each incremental batch, added `coalesce()` to control output file count:
   ```python
   df.coalesce(10).write.format("delta").mode("append").save(target_path)
   ```

3. **Automated maintenance**: Set up a Databricks job to run `OPTIMIZE` and `VACUUM` weekly as a maintenance notebook, scheduled via ADF on Sunday nights.

4. **Auto-optimize**: Enabled Delta's auto-optimize and auto-compact features on the table:
   ```sql
   ALTER TABLE silver.financial_transactions
   SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,
                      delta.autoOptimize.autoCompact = true);
   ```

---

### Q14. How did you implement data quality checks in your pipeline and what happened when checks failed?

**Situation:**
In ECDP, the business team discovered that some Gold layer reports were showing incorrect totals. After investigation, we found duplicate records were making it into the Silver table — the deduplication logic had a bug that only surfaced for records with null values in the sort key.

**Challenge:**
The bug had been running for 3 weeks undetected. There was no automated data quality monitoring in place.

**How I resolved it:**

**Immediate fix:**
- Identified the affected date range using Delta Lake time travel:
  ```python
  df_now = spark.read.format("delta").load(silver_path)
  df_before_bug = spark.read.format("delta") \
      .option("timestampAsOf", "2024-01-15") \
      .load(silver_path)
  ```
- Compared row counts per day to find the exact date the issue started.
- Fixed the deduplication logic to handle null sort keys using `nulls last` ordering.
- Restored affected partitions from the pre-bug snapshot.

**Long-term fix — built a DQ framework:**
Added a data quality notebook that runs after each pipeline stage and checks:
```
- Row count today vs 7-day average (alert if > 20% deviation)
- Null count on non-nullable business key columns (must be 0)
- Duplicate count on primary key (must be 0)
- Referential integrity: all account_ids in transactions must exist in dim_account
- Amount range check: no negative amounts in this dataset
```
Results are written to a `dq_results` table. If any CRITICAL check fails, the pipeline raises an exception and stops. WARNING-level issues are logged but don't stop the pipeline.

---

<a name="architecture"></a>
## 4. Architecture & Design Challenges

---

### Q15. How did you design your pipeline to handle late-arriving data?

**Situation:**
In FinOps Dataverse, source systems sometimes sent data with a 24-48 hour delay — a transaction that happened on Day 1 would arrive in the pipeline on Day 3. Our daily incremental pipeline was partitioned by `ingestion_date`, so late data landed in the wrong partition and got missed by downstream aggregations.

**How I resolved it:**
- Switched Silver and Gold layer partitioning from `ingestion_date` to `business_date` (the actual transaction date from the source).
- Used Delta Lake's `MERGE` instead of append — late-arriving records for a past date upsert into the correct business_date partition rather than creating duplicates in today's partition.
- For Gold layer aggregations, added a **reprocessing trigger**: if a late record arrives for `business_date = D-2`, the Gold layer job for that date is re-triggered automatically, recalculating aggregates for that specific partition only.
- Set `VACUUM` retention to 30 days so we can always time-travel back if reprocessing needs investigation.

---

### Q16. A stakeholder asked why the pipeline takes 4 hours. How did you analyze and explain it?

**Situation:**
The ECDP project lead asked me to investigate why the end-to-end pipeline took 4 hours and present findings to the client.

**How I analyzed it:**
I broke the pipeline into timed segments using ADF Monitor + Databricks job run history:

| Stage | Time Taken | % of Total |
|-------|-----------|-----------|
| Oracle Extract (JDBC) | 110 min | 46% |
| Bronze Write (ADLS) | 25 min | 10% |
| Silver Transform (Databricks) | 65 min | 27% |
| Gold Aggregation | 30 min | 13% |
| Snowflake Export | 10 min | 4% |

The Oracle extract was taking 46% of total time. I drilled into this and found it was doing a full table scan on a 50M row table with no partition read parallelism.

**What I recommended and implemented:**
1. Parallel JDBC read with partitionColumn — reduced extract to 18 minutes.
2. Silver transform was slow due to a cartesian join caused by a missing join condition in one notebook — fixed the join, reduced from 65 to 15 minutes.
3. Total pipeline time went from 4 hours to 58 minutes.

**How I explained it to the stakeholder:**
I used a simple bar chart showing time per stage before and after, and explained the root cause in non-technical terms: *"The system was reading the entire database table from scratch every time instead of reading sections in parallel — like having one cashier serve 50,000 customers versus 20 cashiers working simultaneously."*

---

<a name="cicd"></a>
## 5. CI/CD & Deployment Challenges

---

### Q17. Tell me about a time a deployment broke production. What happened?

**Situation:**
In ECDP, we deployed an updated ADF pipeline from UAT to PROD on a Friday evening. Saturday morning, the scheduled pipeline ran and failed — 0 records loaded.

**What happened:**
The ARM template for the ADF pipeline had a parameter override file for PROD. A developer had updated the DEV parameter file but forgotten to update the PROD parameter file. So in PROD, the source table name parameter still pointed to a DEV table name that didn't exist in the PROD Oracle database.

**How I resolved it:**
- Rolled back the ADF pipeline to the previous version using the previous ARM template from Git history.
- Fixed the PROD parameter file, tested locally, and redeployed within 2 hours.
- Manually triggered the pipeline for the missed Saturday run once deployment was confirmed working.

**Process changes I introduced:**
1. Added a **parameter file validation step** in the CI/CD YAML pipeline — a Python script that checks all three parameter files (DEV/UAT/PROD) exist and have consistent keys before allowing deployment.
2. Made PROD deployments require **two-person approval** in Azure DevOps — the deployer and a second engineer must both approve.
3. Banned Friday evening deployments for production — release windows are Monday to Thursday before 3 PM.

---

### Q18. How did you manage Databricks notebooks in CI/CD?

**Situation:**
Initially, Databricks notebooks were edited directly in the Databricks workspace UI. There was no version control — if someone overwrote a notebook, it was gone. We also had no way to track who changed what.

**How I resolved it:**
- Integrated Databricks workspace with the **Azure DevOps Git repository** — notebooks are now stored as `.py` files (source format) in Git, synced to Databricks workspace via the Databricks Repos feature.
- Any notebook change goes through a **Pull Request** — peer review required before merge.
- The CI/CD YAML pipeline runs `databricks workspace import` to deploy notebooks to the target environment's workspace folder after merge to the environment branch.
- Cluster configurations and job definitions are stored as JSON in Git and deployed via Databricks CLI in the same pipeline.

**Result:**
Full audit trail of every notebook change, peer-reviewed code, and automated deployment — same discipline as application code.

---

<a name="dataquality"></a>
## 6. Data Quality & Reliability

---

### Q19. How do you ensure your pipeline is idempotent — safe to rerun without producing wrong results?

**Answer:**
Every pipeline I build is designed to be rerun safely. The key practices I follow:

1. **Use MERGE not INSERT**: Writing to Delta tables uses MERGE on a business key — so rerunning inserts the same records as updates, not duplicates.
2. **Watermark updates only on success**: The control table watermark is updated *after* a successful pipeline completion, not at the start. If the pipeline fails mid-run, the next run re-reads the same data from the same watermark.
3. **Batch ID tracking**: Every run generates a unique batch_id. The pipeline checks if a batch_id already has a SUCCESS status before processing — if yes, it skips. This prevents double-processing if ADF retries a run that actually succeeded but timed out reporting.
4. **Overwrite partitions, not full tables**: For full loads, I use `replaceWhere` to overwrite only the specific partition being loaded, not the entire table — so a rerun of Day 3's data only overwrites Day 3's partition.

---

### Q20. Describe a situation where you had to explain a data issue to a non-technical stakeholder.

**Situation:**
In ECDP, a finance manager escalated that their daily balance report showed a $2M discrepancy compared to the source system report. They were understandably alarmed.

**How I handled it:**
1. First confirmed the issue was real by querying both the pipeline output and running the same aggregation directly on the source Oracle DB — confirmed the gap.
2. Traced it to the Silver layer — found that records with a specific `account_type = 'INTERCOMPANY'` were being filtered out by a data quality rule that was too aggressive (it was excluding all INTERCOMPANY records thinking they were duplicates, because they had matching amounts).
3. Fixed the filter logic and ran a backfill for the affected date range.
4. Communicated to the stakeholder: *"We found that a filter in our data cleaning step was incorrectly excluding a specific category of transactions called intercompany accounts. These are legitimate transactions and should have been included. We've corrected the logic and recalculated the reports for the affected dates. The $2M gap is now reconciled."*

**What they cared about:**
- Is the data correct now? Yes.
- For how long was it wrong? 4 days.
- Will it happen again? No — we've added a check that specifically validates intercompany record counts match the source.

They didn't need to know about Delta Lake or PySpark. They needed to know their data was now reliable and what safeguards prevent recurrence.

---

## QUICK REFERENCE — KEY NUMBERS FROM YOUR PROJECTS

| Project | Scale | Key Tech |
|---------|-------|----------|
| FinOps Dataverse | 20TB+ pharmaceutical data | PySpark, Iceberg, S3, Oracle, Snowflake, Salesforce API |
| ECDP | 16TB+ financial data | ADF, Databricks, Delta Lake, ADLS Gen2, Azure SQL, Snowflake |

**Performance wins to mention:**
- Spark job: 3 hours → 35 minutes (skew fix + AQE)
- Oracle JDBC read: 2 hours → 18 minutes (parallel partitioned read)
- MERGE query: 45 minutes → 6 minutes (ZORDER + low shuffle merge)
- End-to-end pipeline: 4 hours → 58 minutes (parallelism + join fix)
- Small files: query 18 minutes → 2.5 minutes (OPTIMIZE + ZORDER)

---

*Last updated for interview preparation — ECDP & FinOps Dataverse projects*
