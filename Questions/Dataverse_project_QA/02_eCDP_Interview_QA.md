# eCDP Interview — 30 Questions with Full Model Answers
**Mayuresh | Data Engineer | Cognizant Technology Solutions**

> Answer structure for each question:
> - **What I did** — specific to eCDP
> - **Why that decision** — technical reasoning
> - **What breaks without it** — shows depth

---

## SECTION 1 — Medallion Architecture & Design

---

### Q1. You have Bronze, Silver, Gold — why three layers? Why not just ingest and serve directly?

**Answer:**

In eCDP we were ingesting 16TB+ of financial and clinical data from Oracle and CSV flat files. The data came in raw — nulls, duplicates, inconsistent date formats, mixed casing. If we served that directly to analysts or Power BI, every report would have data quality issues and there would be no way to trace where a problem started.

The three-layer Medallion Architecture solves this by separating concerns cleanly.

Bronze is our raw landing zone — exactly what came from source, nothing changed. This gives us a permanent audit trail and the ability to reprocess from scratch if something goes wrong downstream.

Silver is our cleaned, deduplicated, business-normalized layer. This is where transformation logic lives — type casting, null handling, MERGE for incremental updates. Only data engineers work at this layer.

Gold is purpose-built for consumption. It has pre-aggregated summary tables and flat denormalized tables that Power BI and analysts query directly. No complex JOINs needed on the BI side.

Without this separation, if a bug corrupts your data you have no clean starting point to recover from. You would have to re-ingest from source every time. With Medallion, Bronze is always untouched — you just rerun Silver and Gold from Bronze.

---

### Q2. Bronze is append-only but Silver uses MERGE. What problem does that create and how did you solve it?

**Answer:**

This is a real problem we had to solve carefully.

Because Bronze is append-only, the same `claim_id` can appear multiple times across different Bronze partitions. For example, a claim ingested on January 14th might appear again on January 15th if its status changed in Oracle. So Bronze has two rows for the same `claim_id` — one with status APPROVED, one with status REJECTED.

When we read today's Bronze partition and try to MERGE into Silver, Delta MERGE matches on `claim_id`. If our incoming batch also has two rows for the same `claim_id` — maybe because the CSV source sent it twice — Delta throws an `UnsupportedOperationException: Cannot perform MERGE as multiple source rows matched and attempted to modify the same target row`.

We solved this with deduplication before the MERGE. We used a `row_number()` window function partitioned by `claim_id` and ordered by `updated_at` descending, then kept only `rn = 1`. This ensures we pass exactly one row per primary key into the MERGE — the most recent version.

Without this deduplication step the MERGE would fail or produce unpredictable results depending on which duplicate row wins. It would not be a consistent or repeatable pipeline.

---

### Q3. Why did you store Bronze as Delta files on ADLS Gen2 but register Silver and Gold as External Tables in Unity Catalog?

**Answer:**

The access patterns are completely different for each layer, and that drove the decision.

Bronze is only ever accessed by Databricks notebooks in the bronze-to-silver pipeline. It is never queried by analysts, never connected to Power BI, never referenced by name in a SQL query. So there is no need to register it in a catalog. We access it purely by ADLS path — `spark.read.format("delta").load(bronze_path)`. Registering it would add catalog overhead and expose raw uncleaned data to anyone browsing Unity Catalog, which we did not want.

Silver and Gold have a completely different access pattern. Other Databricks notebooks read Silver by table name — `spark.read.table("ecdp.silver.claims")`. Power BI connects to Gold via Databricks SQL Endpoint and queries it as a standard SQL table — `SELECT * FROM ecdp.gold.claims_summary`. For this to work, the table must be registered in Unity Catalog with a name.

A Delta file is just data written in Delta format to a path. A Delta table is those same files plus a metastore entry that gives them a name, schema, and catalog location. Same underlying files — different access.

---

### Q4. What is the difference between an External Table and a Managed Table in Databricks? Why did you choose External Tables for Silver and Gold?

**Answer:**

The difference is about who controls the data lifecycle.

With a Managed Table, Databricks owns both the metadata and the physical data. If you run `DROP TABLE`, Databricks deletes the actual files from storage. You lose your data permanently.

With an External Table, you provide the LOCATION — which is our ADLS Gen2 path. Databricks owns only the metadata entry in Unity Catalog. If you run `DROP TABLE`, only the catalog entry is removed. The physical Delta files on ADLS Gen2 are completely untouched.

We chose External Tables for Silver and Gold because our data physically lives on ADLS Gen2 under our organization's control. The storage account is managed by our infrastructure team — separate from Databricks. If someone accidentally drops a table in Databricks, we do not lose 16TB of production data. We just re-register the table with `CREATE TABLE USING DELTA LOCATION` and it points back to the existing files immediately.

For a production system handling financial data, this safety boundary is non-negotiable.

---

### Q5. If a bug in your Silver transformation corrupts data, how do you recover? Walk me through the steps.

**Answer:**

Delta Lake's time travel feature makes this recoverable.

First, I identify when the corruption happened by checking the Silver table history:

```python
from delta.tables import DeltaTable
silver_dt = DeltaTable.forName(spark, "ecdp.silver.claims")
silver_dt.history().select("version", "timestamp", "operation").show()
```

This shows every commit to the Silver table with a version number and timestamp.

Second, I restore the Silver table to the last good version before the bad run:

```python
silver_dt.restoreToVersion(42)  # last known good version
```

Or by timestamp:

```python
silver_dt.restoreToTimestamp("2024-01-14 23:59:59")
```

Third, I fix the bug in the Silver notebook code, test it in Dev environment, then redeploy via Azure DevOps CI/CD.

Fourth, I re-read the affected Bronze partitions (Bronze is untouched — append only) and re-run the corrected Silver pipeline for those run_dates.

Fifth, Silver is corrected. I then re-run the Gold pipeline which reads from Silver — Gold is restored automatically.

Without Bronze being append-only and without Delta time travel, we would have no clean recovery point. We would have to re-ingest from Oracle and CSV from source — which could take hours or days at 16TB scale.

---

### Q6. Your Bronze layer has Oracle and CSV data landing in the same Delta path. How do you distinguish which records came from which source at the Silver layer?

**Answer:**

We add a `source_system` audit column at the Bronze write step — before anything is written to the Bronze Delta path. Oracle records get `source_system = 'oracle'` and CSV records get `source_system = 'csv'`. We also add `source_file` using `input_file_name()` which captures the exact filename for CSV records.

```python
df_raw = df_raw \
    .withColumn("source_system", lit("oracle")) \
    .withColumn("source_file",   input_file_name())
```

At the Silver layer, when we read today's Bronze partition, every row carries its `source_system` value. We can filter, route, or apply source-specific business rules if needed — for example, applying different date format parsing for Oracle vs CSV records.

We also use this column in our quarantine table — if a record gets rejected during Silver cleansing, we log `source_system` and `source_file` so the team knows exactly which source sent the bad data.

---

## SECTION 2 — Incremental Load & Watermark

---

### Q7. Explain your watermark-based incremental load for Oracle. What column do you track? Where do you store the watermark? When exactly do you update it?

**Answer:**

For the Oracle source in eCDP, we track the `updated_at` column — a timestamp that Oracle updates on every INSERT and UPDATE to a record. This is our change detection column.

We store the watermark in an Azure SQL control table — a simple table with columns `entity_name` and `last_watermark`. For example:

```
entity_name   | last_watermark
oracle_claims | 2024-01-14 23:59:59
```

The ADF pipeline works like this. First, a Lookup Activity reads the current watermark from the control table. Second, a Copy Activity runs a parameterized SQL query on Oracle — `SELECT * FROM claims WHERE updated_at > last_watermark`. Third, a Databricks Notebook Activity runs to convert the staged Parquet to Bronze Delta. Fourth — and this is the critical part — a Stored Procedure Activity updates the watermark to the current timestamp.

The watermark update is the LAST step in the pipeline, and it only runs if all previous steps succeed. This is intentional. If the Databricks notebook fails, ADF marks the pipeline as failed, skips the watermark update, and the watermark stays at the previous value. The next scheduled run will re-fetch from the same point — no data loss, no gap.

---

### Q8. What happens if your ADF pipeline fails after the Copy Activity but before the Databricks notebook runs? Will you lose data or reprocess it?

**Answer:**

We will reprocess it safely — no data loss.

Here is exactly what happens. The Copy Activity has already landed Parquet files into the Bronze staging path on ADLS Gen2. But the Databricks notebook that converts those Parquet files to Bronze Delta has not run. ADF marks the pipeline as failed and stops. The watermark update step never runs.

On the next scheduled run, ADF reads the watermark from the control table — it still has the old value from before this failed run. So the Copy Activity re-runs the same Oracle query with the same watermark filter. It re-lands the same records into the staging path, overwriting what was already there from the failed run. The Databricks notebook runs and writes to Bronze Delta with `mode = append`.

Now the question is — do we get duplicate Bronze records? Yes we might, but Bronze is designed for this. It is append-only and captures every load. Deduplication happens at Silver not Bronze. When the Silver MERGE runs, `row_number()` dedup ensures only the latest version of each `claim_id` goes into Silver regardless of how many times it appears in Bronze.

The key design principle is: watermark updated last, after all downstream steps succeed. This makes every pipeline run safely retryable.

---

### Q9. What if the Oracle source doesn't have an `updated_at` column? How would you handle incremental load then?

**Answer:**

If there is no `updated_at` column, we have three alternative approaches depending on what the source provides.

Option 1 — Sequence or row ID based. If Oracle has a sequence-generated primary key that always increases for new records, we track the max primary key instead of a timestamp. Each run fetches records WHERE `claim_id > last_max_id`. This works for inserts only — it misses updates to existing records.

Option 2 — Oracle Change Data Capture using LogMiner. Oracle's redo logs capture every change at the database level — inserts, updates, and deletes. We can read from the redo log using Oracle GoldenGate or a CDC tool integrated with ADF. This is the most complete solution but requires DBA-level access to the Oracle instance.

Option 3 — Full load with hash comparison. We pull all records from Oracle every run, compute a hash on each row's key columns, and compare against hashes stored from the previous run. Rows with changed hashes are treated as updates. This is expensive at scale but works when no change column exists.

In eCDP, our Oracle source did have `updated_at` so we used watermark-based approach. But understanding these alternatives shows you know the tradeoffs.

---

### Q10. For CSV incremental load, you used the archive pattern. What is the risk if the Delete Activity fails after Copy but before Databricks runs? How do you handle that?

**Answer:**

The risk is that the CSV file stays in the landing folder instead of being archived. On the next pipeline run, ADF's Get Metadata Activity lists that file again and processes it a second time. This means duplicate records go into Bronze Delta.

We handle this at two levels.

First, Bronze accepts duplicates by design — `mode = append` and `mergeSchema = true`. The duplicate records land in Bronze but carry the correct `run_date` of when they were processed, not duplicated `run_date`.

Second, Silver's deduplication step using `row_number()` on `claim_id` ordered by `updated_at` ensures only the latest version of each record survives into Silver. The MERGE then handles whether it is a new insert or an update to an existing Silver record. So even if Bronze has duplicate records from two separate CSV loads, Silver always has the correct deduplicated state.

The cleanest solution to prevent duplicate Bronze records entirely is to use the control table pattern instead of — or alongside — the archive pattern. You maintain a file tracking table with columns `file_name` and `processed`. Before processing, check if `processed = FALSE`. After successful Databricks run, update `processed = TRUE`. Even if the Delete Activity fails, the file is marked processed and will not be picked up again.

---

### Q11. You do deduplication in Silver using `row_number()`. Why is deduplication necessary before MERGE? What error occurs if you skip it?

**Answer:**

Delta Lake's MERGE operation has a strict requirement: for any given target row, at most one source row can match it. This is called the unique match constraint.

If our incoming Bronze batch has two rows with `claim_id = C001` — which can happen because Bronze is append-only and the same claim can appear in multiple runs — and we pass both rows to MERGE without deduplication, MERGE finds two source rows matching one target row. It does not know which one to use for the update. Delta throws this error:

```
UnsupportedOperationException: Cannot perform Merge as multiple source rows 
matched and attempted to modify the same target row.
```

The pipeline fails completely. No records are written to Silver.

The `row_number()` deduplication solves this by keeping only the most recent version of each `claim_id` within the incoming batch — ordered by `updated_at` descending. After deduplication, every `claim_id` in the source batch is unique. MERGE can now safely match at most one source row to each target row.

This is not an optional step. If you skip deduplication and your source ever sends duplicates — which in a production system at 16TB scale it will — your Silver pipeline breaks.

---

### Q12. What is the difference between a full load and an incremental load in your pipeline? When would you trigger a full load?

**Answer:**

Incremental load fetches only records that changed since the last run. For Oracle, this means records where `updated_at > last_watermark`. For CSV, this means only new files that arrived since the last run. Silver then MERGEs those records into the existing table — updating changed records and inserting new ones. This is our standard daily pipeline pattern.

Full load fetches every record from source regardless of when it changed. The Silver table is truncated and rebuilt from scratch. Or the watermark is reset to a very old date so the incremental query effectively pulls everything.

We trigger full loads in these scenarios. First, when we first onboard a new entity — the Silver table does not exist yet, so we must load all historical data. Second, when we discover that a watermark was set incorrectly and a window of records was missed — full load ensures no gaps. Third, when there is a major schema change in the source — adding new columns or changing data types — a full reload ensures Silver reflects the correct schema from the beginning. Fourth, when Silver data gets corrupted and `restoreToVersion` is not viable — full reload from Bronze.

Full loads are expensive at 16TB scale so we never trigger them without explicit sign-off. In eCDP, full loads were scheduled separately with a weekly or monthly trigger depending on the entity size.

---

## SECTION 3 — ADF Specific

---

### Q13. Walk me through your ADF pipeline for Oracle ingestion activity by activity — what each one does and why it's in that order.

**Answer:**

Our Oracle incremental pipeline has five activities in this order.

**Activity 1 — Lookup Activity.** Queries the Azure SQL control table to fetch the last successful watermark for the entity. Output is a timestamp like `2024-01-14 23:59:59`. This must run first because everything downstream depends on this value.

**Activity 2 — Copy Activity.** Connects to Oracle via JDBC Linked Service. Runs a parameterized query — `SELECT * FROM claims WHERE updated_at > @{activity('Lookup').output.firstRow.last_watermark}`. Sinks the result as Parquet files into the ADLS Gen2 Bronze staging path. This is the actual data movement step. We use Parquet here not Delta because ADF's Copy Activity to Delta is less reliable than letting Databricks handle the Delta write.

**Activity 3 — If Condition Activity.** Checks if the Copy Activity returned any records. If `rowsCopied = 0` we skip the Databricks step — no point running a notebook on empty data. This saves compute cost.

**Activity 4 — Databricks Notebook Activity.** Triggers the `oracle_claims_to_delta` notebook with parameters `run_date` and `entity`. Databricks reads the Parquet from staging, adds audit columns, and appends to Bronze Delta on ADLS Gen2.

**Activity 5 — Stored Procedure Activity.** Updates the control table — `SET last_watermark = CURRENT_TIMESTAMP WHERE entity_name = 'oracle_claims'`. This only runs if Activity 4 succeeds. If Databricks fails, this step is skipped, watermark stays unchanged, next run retries safely.

The order is non-negotiable. Watermark read first, watermark write last.

---

### Q14. What is a Linked Service in ADF? How many did you configure for eCDP and for what?

**Answer:**

A Linked Service in ADF is a connection definition — it stores the connection string, authentication method, and credentials for a data source or compute resource. Think of it as ADF's way of knowing how to talk to external systems. The actual credentials are never stored in ADF directly — they reference Azure Key Vault secrets.

For eCDP we configured four Linked Services.

First, Oracle Linked Service. Uses JDBC driver with the Oracle host, port, service name, and credentials fetched from Key Vault. Used by the Copy Activity source for incremental Oracle ingestion.

Second, ADLS Gen2 Linked Service. Uses a Service Principal or storage account key from Key Vault. Used by Copy Activity sink for landing Parquet files, and by Get Metadata Activity for listing CSV files in the landing folder.

Third, Azure Databricks Linked Service. Uses a Databricks workspace URL and Personal Access Token from Key Vault. Used by the Databricks Notebook Activity to trigger notebooks and pass parameters.

Fourth, Azure SQL Linked Service. Used by Lookup Activity and Stored Procedure Activity to read and write the watermark control table.

---

### Q15. How does ADF trigger your Databricks notebook? What parameters do you pass and why?

**Answer:**

ADF uses the Databricks Notebook Activity connected to the Databricks Linked Service. Under the hood, ADF calls the Databricks Jobs API to submit a one-time job run against an existing cluster or a new job cluster.

In the Notebook Activity configuration, we specify the notebook path in Databricks Repos — for example `/Repos/ecdp/notebooks/bronze/oracle_claims_to_delta`. We configure it to use a Job Cluster which auto-terminates after the notebook finishes — this saves cost compared to a running interactive cluster.

We pass three parameters to every notebook. First, `run_date` — the date of this pipeline run, formatted as `yyyy-MM-dd`. The notebook uses this to read the correct Bronze partition and write partition labels. Second, `entity` — the name of the entity being processed, like `claims`. This makes one notebook generic enough to handle multiple entities with different configurations. Third, `env` — the environment identifier, `dev`, `qa`, or `prod`. The notebook uses this to resolve the correct ADLS Gen2 storage account name and Unity Catalog schema.

Without parameters, each notebook would be hardcoded for one entity and one environment — we would need dozens of duplicate notebooks. Parameters make notebooks generic and reusable.

---

### Q16. What is a Tumbling Window Trigger? Why did you use it instead of a Schedule Trigger for daily incremental runs?

**Answer:**

A Schedule Trigger in ADF fires at a fixed clock time — for example every day at 6 AM. It is fire-and-forget. If the pipeline fails, the trigger fires again the next day at 6 AM regardless of what happened yesterday. There is no built-in awareness of whether previous runs succeeded or failed, and no concept of processing a specific time window.

A Tumbling Window Trigger is different in two key ways. First, it fires for non-overlapping, contiguous time windows — for example daily windows covering 00:00 to 23:59. Each window is a distinct run. Second, if a run fails, the trigger tracks the failed window and can retry it. It also ensures windows are processed in order — window for January 14th must succeed before January 15th is triggered.

For eCDP we chose Tumbling Window because our incremental Oracle load is time-window based. The watermark tracks which time window has been processed. If we miss a window — say January 14th fails — we need to ensure January 14th data is processed before January 15th. Tumbling Window gives us this dependency and sequential guarantee automatically.

With a Schedule Trigger, if January 14th fails and January 15th fires the next day, we could end up with a data gap that is hard to detect and harder to backfill.

---

### Q17. How do you handle pipeline failures in ADF? How does the team get notified when a pipeline fails?

**Answer:**

We handle failures at two levels — pipeline design and alerting.

At the pipeline design level, we set retry configuration on the Databricks Notebook Activity — typically two retries with a 5-minute interval between attempts. This handles transient failures like a cluster spinning up slowly or a brief network timeout. The watermark update step has an explicit dependency on the notebook step with condition `Succeeded` — so it never runs on failure.

For cross-activity failure handling, we use the red dependency arrow in ADF — `On Failure` path — connecting critical activities to a Web Activity that calls a webhook. This webhook posts a failure message to Microsoft Teams with the pipeline name, activity that failed, run ID, and error message.

For monitoring at scale, we configured Azure Monitor alerts on the ADF instance — specifically on the `PipelineFailedRuns` metric. When any pipeline fails, Azure Monitor fires an email alert to the team distribution list within 5 minutes.

In the Databricks notebook itself, we wrap the main logic in a try-except block. If the notebook throws an exception, it logs the error to a Delta error log table and re-raises the exception so ADF sees the notebook as failed and triggers its failure path correctly.

---

## SECTION 4 — Delta Lake & PySpark

---

### Q18. Explain how Delta MERGE works. What are the three scenarios it handles?

**Answer:**

Delta MERGE is the equivalent of a SQL MERGE statement — it compares a source dataframe against a target Delta table on a join condition (usually the primary key) and handles three scenarios in one atomic operation.

Scenario 1 — `whenMatchedUpdateAll()`. The join condition finds a matching row in the target. This means the record already exists in Silver. MERGE overwrites all columns in the target row with values from the source. Used for updating changed records — for example a claim whose status changed from PENDING to APPROVED.

Scenario 2 — `whenNotMatchedInsertAll()`. No matching row found in target. This is a brand new record that has never appeared in Silver before. MERGE inserts it as a new row.

Scenario 3 — `whenMatchedDelete()` — optional, not always used. The join condition matches but the source flags the record for deletion — used in CDC scenarios where source sends a delete marker. In eCDP we did not use hard deletes — we used soft deletes with an `is_active` flag instead.

The entire MERGE operation is atomic. Either all three actions commit together or none of them do. There is no partial state. If the MERGE fails halfway, Delta rolls back automatically and the target table is unchanged.

---

### Q19. What is the `_delta_log` folder? What does it contain and why is it important?

**Answer:**

Every Delta table — whether a path-based file or a registered table — has a `_delta_log` folder at the root of its storage path. This is the transaction log that makes Delta Lake what it is.

The `_delta_log` contains JSON files — one per transaction — numbered sequentially: `00000000000000000001.json`, `00000000000000000002.json`, and so on. Each JSON file records what happened in that transaction — which Parquet data files were added, which were removed, the schema at that point, the operation type (WRITE, MERGE, DELETE, OPTIMIZE), and operation metrics like `numOutputRows` and `numFiles`.

Every 10 transactions, Delta also writes a checkpoint file (`.parquet`) which is a snapshot of the full table state at that point — this makes reading the log fast without scanning all individual JSON files.

The `_delta_log` is what enables ACID transactions, time travel, schema enforcement, and MERGE. When you run `spark.read.format("delta").load(path)`, Spark reads the `_delta_log` to understand which files are currently valid and what the current schema is — not by scanning all files blindly.

For eCDP, the `_delta_log` on Bronze gave us a full history of every ingestion run — when it happened, how many records were written, which files were created. This was our audit trail for free.

---

### Q20. You used `replaceWhere` in Gold instead of full overwrite. Explain the difference and why replaceWhere is better at scale.

**Answer:**

Full overwrite with `mode = overwrite` rewrites every file in the entire Gold Delta table on every run. At 16TB scale, that means rewriting months of historical Gold data every single day just to add one day's new aggregations. This is extremely expensive in compute, I/O, and time. It also destroys Delta time travel — every overwrite creates a new version that replaces all previous data, so you cannot query what Gold looked like last week.

`replaceWhere` is a targeted overwrite. Instead of rewriting everything, you specify a predicate — for example `report_date = '2024-01-15'` — and Delta only rewrites the files that belong to that partition. All other partitions are completely untouched.

```python
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"report_date = '{run_date}'") \
    .partitionBy("claim_month") \
    .saveAsTable("ecdp.gold.claims_summary")
```

The benefits are significant. Only today's partition is rewritten — maybe 1GB out of 16TB. Historical partitions keep their Delta versions intact, so time travel works for past months. Compute cost drops dramatically. The write is also safer — a failure only affects today's partition, not the entire table.

Without `replaceWhere`, a Gold overwrite job that fails halfway could leave the entire 16TB table in a corrupted state. With `replaceWhere`, a failure only affects the single partition being rewritten — Delta rolls it back atomically.

---

### Q21. What does OPTIMIZE do in Delta Lake? What does ZORDER do? How did you decide which columns to ZORDER by?

**Answer:**

OPTIMIZE is Delta Lake's file compaction command. When you write data incrementally — especially with MERGE which writes small delta files for each changed record — you accumulate many small Parquet files. Spark reads files in parallel, so having thousands of tiny files means thousands of parallel tasks with high overhead and slow reads. OPTIMIZE compacts those small files into fewer, larger files — typically targeting 1GB per file. This significantly reduces read overhead.

ZORDER is a data co-location technique applied during OPTIMIZE. Without ZORDER, records are written in whatever order they arrive. With ZORDER BY (region, claim_date), Delta physically reorganizes data on disk so that records with the same `region` and `claim_date` values are stored in the same or adjacent files. Delta also updates its statistics in `_delta_log` for these columns.

When a query then runs `WHERE region = 'North' AND claim_date = '2024-01-15'`, Delta's data skipping reads those statistics and skips files that provably contain no matching data. Instead of scanning all 100 files, Spark might only read 3 files. This is a massive performance improvement.

We decided which columns to ZORDER by based on how Power BI and analysts actually filter data. We looked at the most common WHERE clause patterns in Gold queries — they almost always filtered by `region` and `claim_month`. So we ran `OPTIMIZE ecdp.gold.claims_summary ZORDER BY (region, claim_month)`. ZORDER is most effective on high-cardinality columns that are frequently used in filters.

---

### Q22. What is VACUUM in Delta Lake? What happens if you run VACUUM with 0 hours retention?

**Answer:**

Delta Lake's MERGE and OPTIMIZE operations do not physically delete old Parquet files immediately. When MERGE updates records, it writes new files and marks old files as removed in the `_delta_log` — but the old files remain on disk. This is intentional — it enables time travel, because you can query old Delta versions which reference those old files.

VACUUM is the cleanup command that physically deletes files that are no longer referenced by any current or recent Delta version. By default it retains files for 7 days (168 hours) — meaning you can time-travel up to 7 days back.

```python
spark.sql("VACUUM ecdp.silver.claims RETAIN 168 HOURS")
```

If you run `VACUUM RETAIN 0 HOURS`, Delta physically deletes all files that are not referenced by the current latest version. Time travel is completely destroyed — you can no longer query any historical version. Any long-running read query that started before the VACUUM and is referencing old files will fail immediately with a FileNotFoundException.

Delta actually prevents `RETAIN 0 HOURS` by default with a safety check. You must explicitly disable it:

```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM ecdp.silver.claims RETAIN 0 HOURS")  # extremely dangerous
```

In eCDP we never ran VACUUM below 168 hours. This gave us 7 days of time travel — enough to detect and recover from any Silver corruption within a normal business week.

---

### Q23. You used `mergeSchema = true` in Bronze but not in Silver. Why?

**Answer:**

`mergeSchema = true` tells Delta to automatically update the table schema if the incoming dataframe has new columns that do not exist in the current Delta schema. Without it, if the source sends a new column, the write fails with a schema mismatch error.

We enabled it in Bronze because Bronze must accept whatever the source sends — no rejections. Our Oracle and CSV sources can have schema changes — a new column added to the Oracle table, a new field in the CSV header. Bronze should absorb all of that without pipeline failures. We do not transform or enforce schema in Bronze.

We did NOT enable it in Silver because Silver is our schema-enforced layer. If a new column arrives from Bronze that Silver does not expect, we want the pipeline to fail and alert — not silently absorb an unknown column. The data team needs to consciously decide: what does this new column mean? Does it need cleansing? Does it need a Silver transformation rule? Does it change the MERGE key? These are deliberate decisions that should go through code review and CI/CD — not happen automatically.

If we allowed mergeSchema in Silver, a source schema change could silently propagate to Gold, break Power BI reports, and nobody would know until an analyst noticed wrong data in a dashboard.

---

## SECTION 5 — Unity Catalog & Table Registration

---

### Q24. You created Silver and Gold as External Tables using `CREATE TABLE USING DELTA LOCATION`. What happens if someone runs `DROP TABLE ecdp.silver.claims`? Is the ADLS data lost?

**Answer:**

No. The ADLS Gen2 data is completely safe.

When you `DROP TABLE` an External Table in Databricks Unity Catalog, only the catalog metadata entry is deleted — the table name, schema definition, and column descriptions are removed from Unity Catalog. The physical Delta files at the ADLS Gen2 LOCATION path are not touched at all.

This is the fundamental difference between External and Managed tables. For Managed tables, `DROP TABLE` deletes both the metadata and the physical storage. For External tables, storage is the organization's responsibility — Databricks only manages the catalog entry.

If someone accidentally drops `ecdp.silver.claims`, recovery is a single SQL command:

```sql
CREATE TABLE ecdp.silver.claims
USING DELTA
LOCATION 'abfss://silver@<storage>.dfs.core.windows.net/delta/claims/';
```

This re-registers the existing ADLS Delta files back as the Silver table. Because the `_delta_log` is intact on ADLS, the full history, schema, and time travel are immediately restored. The table is back online in seconds.

---

### Q25. How does Power BI connect to your Gold tables? Walk me through the connection path from Power BI to data on ADLS Gen2.

**Answer:**

The connection path has four hops.

First, Power BI Desktop connects to a Databricks SQL Endpoint — also called a SQL Warehouse in newer Databricks versions. This is a dedicated SQL compute resource in Databricks that exposes Unity Catalog tables over a JDBC/ODBC connection. In Power BI, we use the Azure Databricks connector and provide the Server Hostname and HTTP Path of the SQL Endpoint.

Second, Power BI authenticates to Databricks using a Personal Access Token or Azure Active Directory OAuth. This controls which Unity Catalog schemas and tables the Power BI service account can see.

Third, when a Power BI report refreshes, it sends a SQL query to the Databricks SQL Endpoint — for example `SELECT region, claim_month, total_claim_amount FROM ecdp.gold.claims_summary WHERE claim_month = '2024-01'`. The SQL Endpoint resolves `ecdp.gold.claims_summary` through Unity Catalog to get the ADLS Gen2 LOCATION path.

Fourth, the SQL Endpoint's Spark compute reads the Delta files from ADLS Gen2 at that path, executes the query with ZORDER-based data skipping, and returns results to Power BI.

Power BI never touches ADLS Gen2 directly. It only speaks SQL to Databricks. The ADLS path is invisible to the BI team — they just see a table name.

---

### Q26. What is Unity Catalog? How is it different from the older Hive Metastore in Databricks?

**Answer:**

Unity Catalog is Databricks' centralized governance layer introduced to replace the older per-workspace Hive Metastore. The core difference is in scope and governance capabilities.

The old Hive Metastore was workspace-scoped. Every Databricks workspace had its own isolated metastore. A table registered in workspace A was invisible to workspace B. If you had dev, QA, and prod as separate workspaces — which is the standard pattern — you had three separate metastores with no sharing between them. Access control was coarse — cluster-level or table-level ACLs only.

Unity Catalog is account-scoped. One Unity Catalog sits above all workspaces in the Databricks account. Tables registered in Unity Catalog are accessible from any workspace that is attached to that catalog — with fine-grained access control. You can grant access at catalog, schema, table, column, or row level.

Unity Catalog also introduces a three-level namespace: `catalog.schema.table` — which is why our tables are `ecdp.silver.claims` not just `silver.claims`. This prevents naming collisions across teams and projects.

For eCDP, Unity Catalog meant we could register Silver and Gold tables once and access them from dev, QA, and prod workspaces with environment-specific permissions — without duplicating table registrations across three separate metastores.

---

## SECTION 6 — CI/CD & Deployment

---

### Q27. You have three environments — Dev, QA, Prod. How does a code change in a notebook flow from a developer's laptop to production? Walk through each step.

**Answer:**

The flow has seven steps.

Step 1 — Developer creates a feature branch from dev in GitHub. For example `feature/silver-dedup-fix`. They make changes to the Silver notebook locally or in Databricks Repos.

Step 2 — Developer opens a Pull Request from `feature/silver-dedup-fix` into `dev` branch. The PR requires at least one reviewer approval.

Step 3 — Azure DevOps CI pipeline triggers automatically on PR creation. It runs unit tests for the changed notebooks. If tests pass, the reviewer approves the PR. The feature branch is merged into `dev`.

Step 4 — Azure DevOps CD pipeline triggers on merge to `dev`. It deploys notebooks to the Dev Databricks workspace via the Databricks CLI — uploading notebooks to `/Repos/ecdp/notebooks/`. It also deploys any ADF ARM template changes to the Dev ADF instance. Dev environment is now updated.

Step 5 — QA promotion. The team opens a PR from `dev` to `qa` branch. After review, it is merged. Azure DevOps pipeline deploys to QA Databricks workspace and QA ADF. QA team runs integration tests.

Step 6 — Production promotion. A PR from `qa` to `main` is opened. This requires senior engineer approval and a manual approval gate in Azure DevOps — nobody can deploy to prod without explicit human sign-off.

Step 7 — After approval, Azure DevOps deploys notebooks to Prod Databricks and ADF ARM templates to Prod ADF. The deployment is fully automated from this point — no manual file uploads or copy-paste.

Every deployment is linked to a specific commit hash in GitHub — full audit trail of who deployed what and when.

---

### Q28. What is an ADF ARM template? Why do you export and deploy it through Azure DevOps instead of making changes directly in the Prod ADF UI?

**Answer:**

An ARM template — Azure Resource Manager template — is a JSON file that describes Azure resources declaratively. For ADF, when you click the "Export ARM Template" button in the ADF UI, it generates JSON files that represent every pipeline, Linked Service, trigger, and dataset in that ADF workspace. These JSON files can be version-controlled in GitHub and deployed programmatically to other ADF instances.

We use ARM template deployment through Azure DevOps for three reasons.

First, consistency. If a developer manually creates a pipeline in the Prod ADF UI, there is no guarantee it matches what is tested in Dev and QA. Small differences in parameters, activity configs, or trigger settings can cause prod-specific failures. Deploying from ARM templates guarantees that exactly what was tested in QA goes to prod — bit for bit.

Second, audit trail. Every ADF change in prod is tied to a PR, a reviewer, a commit hash, and a DevOps pipeline run. If a production pipeline breaks, we know exactly what changed, when, and who approved it. Direct UI edits in prod leave no traceable history.

Third, rollback. If a prod deployment breaks something, we revert the GitHub commit and re-run the Azure DevOps pipeline to deploy the previous working ARM template. Rolling back a manual UI change requires someone to remember exactly what was changed and undo it manually — error-prone under pressure.

---

## SECTION 7 — Scenario & Troubleshooting

---

### Q29. Your Silver MERGE ran successfully yesterday but today the pipeline is running 3x slower than usual. What are the first five things you check?

**Answer:**

This is a performance regression — something changed between yesterday and today. I check in this order.

**Check 1 — Data volume.** How many records did today's Bronze partition contain versus yesterday's? Run `df_bronze.filter(col("run_date") == today).count()`. If today's batch is 3x larger — maybe a monthly data dump arrived, or an upstream backfill ran — that alone explains the slowdown. No code change needed, it is a data volume spike.

**Check 2 — Small file problem.** Run `DESCRIBE DETAIL ecdp.silver.claims` and check `numFiles`. After many incremental MERGE runs, Silver accumulates thousands of small files — each MERGE writes new small files. Spark reading 10,000 small files is much slower than reading 100 large files. Fix: run `OPTIMIZE ecdp.silver.claims ZORDER BY (region, claim_date)`. This compacts files and should immediately improve performance.

**Check 3 — Skewed data.** Check if today's batch has records concentrated on a few `claim_id` values or a few partition values. Data skew means some Spark tasks process 10x more data than others — the job waits for the slowest task. Check using the Spark UI's Stage tab — look for tasks with dramatically different durations. Fix: add salting or use `repartition()` before the MERGE.

**Check 4 — Cluster resources.** Check if the Databricks job cluster has the same configuration as yesterday. Did someone change the cluster policy, reduce the number of workers, or is the cluster auto-scaling hitting its maximum? Check the cluster event log in Databricks.

**Check 5 — Shuffle partitions.** Check `spark.conf.get("spark.sql.shuffle.partitions")`. Default is 200. If today's batch is much larger, 200 shuffle partitions may be too few — each partition handles too much data. Increase to 400 or 800 for large batches. Also check if a broadcast join threshold change caused a sort-merge join instead of broadcast — visible in the Spark UI query plan.

---

### Q30. A business user says the Gold summary table shows different revenue numbers than what finance has in their Oracle system. How do you debug this end to end — from Gold back to source?

**Answer:**

This is a data reconciliation issue. I trace it backwards through the layers — Gold → Silver → Bronze → Oracle.

**Step 1 — Understand the discrepancy.** Ask the business user for specifics. What entity? What time period? What exact number does Gold show vs Oracle? This narrows the scope immediately. For example: "Gold shows $2.1M total revenue for North region in January 2024. Oracle shows $2.4M."

**Step 2 — Check Gold.** Query Gold directly for the discrepancy window:
```sql
SELECT region, claim_month, total_claim_amount, total_claims
FROM ecdp.gold.claims_summary
WHERE region = 'North' AND claim_month = '2024-01'
```
Note the Gold value. Check when Gold was last refreshed via `DESCRIBE HISTORY ecdp.gold.claims_summary` — if Gold is stale, the pipeline may not have run recently.

**Step 3 — Check Silver.** Query Silver raw records for the same window:
```sql
SELECT SUM(amount), COUNT(claim_id)
FROM ecdp.silver.claims
WHERE region = 'North' AND claim_date BETWEEN '2024-01-01' AND '2024-01-31'
AND is_current = TRUE AND status = 'APPROVED'
```
If Silver's sum matches Gold — the discrepancy is in how Silver data was ingested from Bronze. If Silver's sum is different from Gold — the discrepancy is in the Gold aggregation logic. Check the Gold notebook for incorrect filters, wrong status conditions, or aggregation column errors.

**Step 4 — Check Bronze.** If Silver shows a different value from Oracle, read Bronze for the affected run_dates:
```python
df_bronze.filter(
    (col("region") == "North") &
    (col("run_date").between("2024-01-01", "2024-01-31"))
).agg(sum("amount")).show()
```
Compare Bronze sum to Oracle. If Bronze matches Oracle — the issue is in Silver transformation. If Bronze does not match Oracle — the issue is in ADF ingestion. Either records were missed (watermark gap), duplicated (archive pattern failure), or the Oracle query filtered incorrectly.

**Step 5 — Check ADF pipeline runs.** Go to ADF Monitor → Pipeline Runs. Check if any Oracle ingestion run failed or was skipped for January 2024. Check the watermark control table — is there a gap in `last_watermark` values for the entity?

**Step 6 — Compare Oracle directly.** Run the same Oracle query our ADF uses — `SELECT SUM(amount) FROM claims WHERE region='North' AND claim_date BETWEEN '2024-01-01' AND '2024-01-31' AND status='APPROVED'`. Compare to Bronze. This isolates whether the gap is in Oracle itself (upstream data issue) or in our ingestion.

By this point, the discrepancy is isolated to one specific layer and one specific cause — wrong filter in Gold aggregation, a missed ingestion window in ADF, a Silver dedup that incorrectly dropped valid records, or the Oracle source data itself being wrong.

---

*Last Updated: June 2026*
