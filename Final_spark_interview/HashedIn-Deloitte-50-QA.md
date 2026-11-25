# HashedIn by Deloitte — Data Engineer Interview Prep
**50 Questions + Answers | Tailored to JD + Mayuresh's Resume**
**Role:** Senior Data Engineer / Data Engineer | Pune | 2.5–5 Years

---

> **How to use:** Each question has a **Short Answer** (the 1-line takeaway) and an **Interview Script** (say this verbatim). Follow the script, then drill into the technical detail if the interviewer probes.

---

## SECTION 1 — INTRODUCTION & BACKGROUND (Q1–Q4)

---

### Q1. Tell me about yourself and your experience as a Data Engineer.

**Short Answer:** 3.5 years at Cognizant building Medallion Lakehouses on Azure and AWS, with two production projects handling 20–26 TB of data.

**Interview Script:**
> "I'm Mayuresh, a Data Engineer at Cognizant with 3.5 years of experience specializing in Azure Databricks, PySpark, Delta Lake, and Azure Data Factory. I've worked on two major production platforms — eCDP, a pharmaceutical MDM platform on AWS using PySpark and Apache Iceberg with 20+ TB of data, and FinOps Dataverse, a financial data lakehouse on Azure migrating 26+ TB to ADLS Gen2. I hold the Databricks Certified Data Engineer Associate certification and have hands-on experience with Medallion architectures, incremental pipelines, data governance via Unity Catalog, and CI/CD using Databricks Asset Bundles. At HashedIn, I'm excited to bring this end-to-end pipeline ownership experience to build scalable, production-grade lakehouse platforms."

---

### Q2. Why HashedIn by Deloitte specifically?

**Short Answer:** Product mindset + Databricks/Snowflake focus + AI-ready pipelines aligns with where I want to grow.

**Interview Script:**
> "HashedIn's positioning — engineering talent combined with product thinking for global enterprises — resonates with how I've worked at Cognizant. My current work is production-grade but within a single client domain. At HashedIn, I'd get to solve diverse business problems across industries, which is where I want to push my growth. The JD specifically mentions building AI-ready pipelines for RAG, LLM, and ML use cases — that's exactly the direction I'm actively exploring. I also appreciate that HashedIn emphasizes ownership and fast-paced learning, both things I value and practice."

---

### Q3. Walk me through your most impactful project.

**Short Answer:** FinOps Dataverse — 26+ TB financial migration with 50% faster onboarding and 40% query latency reduction.

**Interview Script:**
> "My most impactful project is FinOps Dataverse at Cognizant — a financial data modernization platform where I was responsible for architecting the entire Medallion Lakehouse on Azure. I migrated 26+ TB of financial data from on-premises Oracle and flat-file sources to ADLS Gen2 using Azure Data Factory for ingestion and Databricks for Silver and Gold transformations. The key achievement I'm proud of is building a configuration-driven ingestion framework that reduced onboarding time for new data sources by 50% — no hardcoded pipelines, everything driven by a control table. I also optimized Delta Lake workloads using Z-Ordering and file compaction, cutting report query latency by 40% across 300+ business reports. That project taught me the full lifecycle — design, build, optimize, monitor, and govern."

---

### Q4. What certifications do you hold, and how relevant are they to this role?

**Short Answer:** Databricks Certified Data Engineer Associate + Azure Fundamentals + AWS CCP — directly map to this JD's core platforms.

**Interview Script:**
> "I hold three certifications. The most relevant is the Databricks Certified Data Engineer Associate — it covers Delta Lake, Auto Loader, Structured Streaming, Medallion architecture, and cluster tuning, all of which are core to this JD. The Microsoft Azure Fundamentals DP-900 covers Azure data services including Databricks, ADF, ADLS, and Synapse — directly aligned with my day-to-day work. The AWS CCP validates my AWS knowledge which I used in the eCDP project with S3, IAM, and Iceberg. I'm planning to pursue the Databricks Data Engineer Professional certification as the next step."

---

## SECTION 2 — PYSPARK & PYTHON (Q5–Q12)

---

### Q5. How do you optimize a slow PySpark job in production?

**Short Answer:** Profile in Spark UI → fix root cause: skew, shuffle, partition size, or missing pushdown.

**Interview Script:**
> "In production on eCDP, we had a PySpark job that took 45 minutes on 10M+ rows. My optimization approach is systematic: first I open Spark UI and look at the longest stage — that's where the bottleneck is. I check the Data Skew tab: if one partition holds 80% of the data, I add a salt key to distribute it. I check shuffle read/write bytes: if shuffle is huge, I increase spark.sql.shuffle.partitions from default 200 to match the actual data size. I look at whether Catalyst is pushing filters to the source — if not, I rewrite the query to let the predicate hit the source. For joins, I check if one side is small enough for broadcast — if yes, broadcast hint cuts the shuffle entirely. On FinOps, these combined changes reduced one MERGE job from 45 minutes to 8 minutes."

**Key techniques:**
```python
# Broadcast hint for small table joins
from pyspark.sql.functions import broadcast
df_result = df_large.join(broadcast(df_small), "key")

# Salt key for skewed column
from pyspark.sql.functions import concat, lit, (rand()*10).cast("int")
df_salted = df.withColumn("salt_key", concat(col("REGION"), lit("_"), (rand()*10).cast("int")))

# Tune shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "400")  # for 10GB+ shuffles

# AQE for auto skew handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

---

### Q6. Explain PySpark's execution model: Driver, DAG, Stages, Tasks.

**Short Answer:** Driver builds DAG → Catalyst optimizes → stages split at shuffles → tasks = partitions run on executors.

**Interview Script:**
> "When I submit a PySpark job, the Driver program is the brain — it reads my code, constructs a logical plan, and passes it to the Catalyst optimizer, which produces an optimized physical plan as a DAG of RDD operations. The DAG Scheduler then splits this DAG into stages at shuffle boundaries — anytime data needs to be redistributed across nodes (like a groupBy or join), that's a stage boundary. Each stage is a set of tasks, and each task processes one partition of data. The Task Scheduler sends those tasks to Executors — worker JVM processes that have memory and CPU cores. Each executor can run multiple tasks in parallel (one per core). Lazy evaluation means nothing actually runs until an action like .write() or .collect() is called — transformations just build up the plan."

---

### Q7. How do you handle data skew in PySpark?

**Short Answer:** Identify skewed key → salting for groupBy/join, AQE for auto-handling, or repartition by better key.

**Interview Script:**
> "In eCDP, our PATIENTS table was skewed on REGION — the 'US' region had 70% of rows, causing one task to run 10× longer than others. I identified this using Spark UI's Stage Detail tab where I could see one task running for 20 minutes while others finished in 2 minutes. For the aggregation, I used salting: I appended a random integer 0–9 to the REGION key, aggregated, then did a second aggregation to remove the salt. For the MERGE operation, I enabled Adaptive Query Execution which auto-detects and splits skewed partitions at runtime — that handled most cases without code changes. For new pipelines, I now pick the join/group key carefully upfront to avoid skew by design."

---

### Q8. Write a PySpark MERGE (upsert) for a Silver layer SCD Type 1.

**Short Answer:** DeltaTable.forPath → merge → whenMatchedUpdateAll → whenNotMatchedInsertAll.

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp

# Read incremental Bronze data
df_source = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_checkpoint) \
    .load("/mnt/bronze/patients/")

# Deduplicate: keep latest record per PATIENT_ID
from pyspark.sql.window import Window
w = Window.partitionBy("PATIENT_ID").orderBy(col("UPDATED_DATE").desc())
df_deduped = df_source.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")

# MERGE into Silver (SCD Type 1 — overwrite on match)
silver = DeltaTable.forPath(spark, "/mnt/silver/patients/")

silver.alias("tgt").merge(
    df_deduped.alias("src"),
    "tgt.PATIENT_ID = src.PATIENT_ID"
).whenMatchedUpdateAll(
    condition="src.UPDATED_DATE > tgt.UPDATED_DATE"   # only update if newer
).whenNotMatchedInsertAll().execute()
```

---

### Q9. How do you implement incremental loading in PySpark with Delta Lake?

**Short Answer:** Store watermark → filter source WHERE timestamp > watermark → process → update watermark only on success.

**Interview Script:**
> "My incremental loading pattern uses a control table that stores the last successful watermark per table. At the start of each run, I read the old watermark, then read from the source WHERE updated_date > old_watermark AND updated_date <= new_watermark (snapshot boundary). I use the upper bound to avoid reading rows that arrive mid-run. After successful MERGE into Silver, I update the watermark. The critical rule: watermark update happens ONLY after MERGE succeeds — if MERGE fails, the watermark stays at the old value, so the next run re-reads the same window. MERGE handles duplicate inserts as updates, so the pipeline is fully idempotent."

---

### Q10. How do you read JSON/XML/semi-structured data in PySpark?

**Short Answer:** JSON: spark.read.json with schema; nested: use explode() and get_json_object(); XML: spark-xml library.

```python
# JSON with explicit schema (never infer schema in prod — slow and unstable)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("patient_id", IntegerType()),
    StructField("name",       StringType()),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("zip",  StringType())
    ]))
])

df = spark.read.schema(schema).json("/mnt/landing/patients/*.json")

# Flatten nested struct
df_flat = df.select(
    col("patient_id"),
    col("name"),
    col("address.city").alias("city"),
    col("address.zip").alias("zip")
)

# Explode array column (one row per element)
from pyspark.sql.functions import explode
df_exploded = df.withColumn("diagnosis", explode(col("diagnoses_array")))

# XML (needs com.databricks.spark.xml library)
df_xml = spark.read.format("xml") \
    .option("rowTag", "Patient") \
    .load("/mnt/landing/patients.xml")
```

---

### Q11. How do you implement data quality checks in PySpark?

**Short Answer:** Custom validation functions or Great Expectations/Deequ — check nulls, ranges, referential integrity, row count thresholds.

**Interview Script:**
> "In eCDP and FinOps, I built a custom validation layer before writing to Silver. The checks are: null check on primary key (PATIENT_ID must never be null), range check (AGE between 0 and 150), referential integrity (REGION_CODE must exist in region_lookup table), duplicate check (count distinct PKs vs count total rows), and row count threshold (current run must have at least 80% of last week's average row count). Any row failing hard rules goes to a dead-letter Delta table with a rejection_reason column, and I raise a soft warning (not pipeline failure) for threshold anomalies. For this role, I'd be excited to implement Great Expectations — their suite of expectations and automatic documentation is production-grade and handles all these checks with built-in reporting."

```python
# Custom validation — null PK check
bad_rows = df.filter(col("PATIENT_ID").isNull())
if bad_rows.count() > 0:
    bad_rows.withColumn("rejection_reason", lit("NULL_PRIMARY_KEY")) \
            .write.format("delta").mode("append") \
            .save("/mnt/silver/dead_letter/patients/")
    df = df.filter(col("PATIENT_ID").isNotNull())
```

---

### Q12. How do you use window functions in PySpark? Give a real example.

**Short Answer:** row_number() for deduplication, lag()/lead() for change detection, rank() for top-N per group.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lag, col, datediff

# Deduplication — keep latest record per PATIENT_ID
w_dedup = Window.partitionBy("PATIENT_ID").orderBy(col("UPDATED_DATE").desc())
df_deduped = df.withColumn("rn", row_number().over(w_dedup)).filter("rn = 1").drop("rn")

# Change detection — flag rows where REGION changed from previous record
w_change = Window.partitionBy("PATIENT_ID").orderBy("UPDATED_DATE")
df_changes = df.withColumn("prev_region", lag("REGION").over(w_change)) \
               .withColumn("region_changed",
                   when(col("REGION") != col("prev_region"), True).otherwise(False))

# Running total by region (FinOps use case — cumulative financial amounts)
w_running = Window.partitionBy("REGION").orderBy("TRANSACTION_DATE") \
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_cum = df.withColumn("running_total", sum("AMOUNT").over(w_running))
```

---

## SECTION 3 — DELTA LAKE & MEDALLION ARCHITECTURE (Q13–Q20)

---

### Q13. Explain the Medallion Architecture and how you've implemented it.

**Short Answer:** Bronze = raw append-only, Silver = clean deduped MERGE, Gold = aggregated replaceWhere — each layer adds quality and reduces breadth.

**Interview Script:**
> "In FinOps Dataverse I implemented a full three-layer Medallion on Azure. Bronze is append-only Delta — raw data exactly as it arrives from Oracle or flat files, with an ingestion timestamp column added. We never modify Bronze; it's the replay layer if something goes wrong downstream. Silver is the cleansed, deduplicated, conformed layer — I read incremental Bronze using CDF or Auto Loader, deduplicate on TRANSACTION_ID using row_number(), apply business rules, check quality, and MERGE into Silver Delta tables. Silver has schema enforcement on. Gold is the domain-specific aggregated layer for BI and reporting — I use replaceWhere to overwrite only the specific partition being refreshed, like a business date partition, so the operation is idempotent and doesn't touch other partitions."

---

### Q14. What is Delta Lake MERGE and how does it differ from a simple OVERWRITE?

**Short Answer:** MERGE does row-level upsert — matches on key, updates matched rows, inserts new ones. OVERWRITE replaces entire partition or table.

**Interview Script:**
> "MERGE is the backbone of my Silver layer. When I read incremental Bronze data, some rows are updates to existing patients (address change, status change) and some are genuinely new patients. OVERWRITE would have me re-process ALL history every run — not scalable at 10M+ records/day. MERGE uses the PATIENT_ID key to decide: if it exists in Silver, UPDATE it; if it's new, INSERT it. The key design decision is the merge condition — I add AND src.UPDATED_DATE > tgt.UPDATED_DATE to avoid overwriting Silver with an older Bronze row that arrived out of order. For Gold, I do use replaceWhere (a form of partition overwrite) because Gold is aggregated — I overwrite the whole business_date partition atomically, which is idempotent."

---

### Q15. What is OPTIMIZE and Z-ORDER in Delta Lake, and when do you use them?

**Short Answer:** OPTIMIZE compacts small files into larger ones; Z-ORDER co-locates related data physically for faster filter pushdown.

**Interview Script:**
> "In FinOps, we had a classic small-files problem — incremental MERGE writes created thousands of tiny Parquet files, and report queries were doing 10,000+ file opens per query, taking 3–4 minutes. OPTIMIZE fixed the file count by merging small files into 1GB target file size. Z-ORDER on TRANSACTION_DATE and REGION co-locates rows with the same date and region in the same files, so when Power BI queries WHERE TRANSACTION_DATE = '2024-10' AND REGION = 'APAC', Delta's data skipping reads only those files and skips 90%+ of the table. Result: query latency dropped 40%. I schedule OPTIMIZE as a Databricks Workflow job every morning after the nightly load, before BI hours."

```python
# Run OPTIMIZE with Z-ORDER
spark.sql("""
    OPTIMIZE delta.`/mnt/silver/transactions/`
    ZORDER BY (TRANSACTION_DATE, REGION)
""")

# VACUUM to clean up old files (keep 7 days for time travel)
spark.sql("VACUUM delta.`/mnt/silver/transactions/` RETAIN 168 HOURS")
```

---

### Q16. What is Delta Lake's Change Data Feed (CDF) and when do you use it?

**Short Answer:** CDF tracks row-level changes (insert/update/delete) in a Delta table so downstream jobs can read only what changed, without full table scan.

**Interview Script:**
> "CDF is a feature I use to pass incremental changes from Silver to Gold without scanning the entire Silver table. I enable it at the Silver table level with delta.enableChangeDataFeed = true. When Gold needs to refresh, it reads from Silver using spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', last_checkpoint). This returns only rows that changed since the last checkpoint, with a _change_type column: insert, update_postimage, or delete. This is much faster than re-reading all of Silver. The alternative without CDF is to use a watermark column, but CDF handles deletes natively — watermarks can't track deleted rows."

---

### Q17. How do you implement SCD Type 2 in Delta Lake?

**Short Answer:** Add is_active + start_date + end_date columns; MERGE closes the old row (update end_date, is_active=false) and inserts a new row.

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit, col

silver_scd2 = DeltaTable.forPath(spark, "/mnt/silver/patients_scd2/")

# Step 1: Expire existing active rows where key matches and something changed
silver_scd2.alias("tgt").merge(
    df_incoming.alias("src"),
    "tgt.PATIENT_ID = src.PATIENT_ID AND tgt.is_active = true"
).whenMatchedUpdate(
    condition="src.REGION != tgt.REGION OR src.DOCTOR_ID != tgt.DOCTOR_ID",
    set={
        "is_active":  "false",
        "end_date":   "current_date()"
    }
).execute()

# Step 2: Insert all incoming rows as new active rows
df_new_rows = df_incoming \
    .withColumn("is_active",  lit(True)) \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date",   lit(None).cast("date"))

df_new_rows.write.format("delta").mode("append") \
    .save("/mnt/silver/patients_scd2/")
```

---

### Q18. What is Delta Lake Time Travel and when is it useful?

**Short Answer:** Every Delta write creates a versioned snapshot in _delta_log; you can query any past version for auditing, debugging, or rollback.

**Interview Script:**
> "Delta Lake logs every transaction in the _delta_log folder as a JSON file. Each write increments the version number. Time Travel lets me query any past version. In production I use it in three scenarios: first, debugging — if a report shows wrong numbers after last night's load, I query version N-1 to see the data before the load and find the bad records. Second, compliance auditing — GDPR requires me to show what data we held on a specific date, so I query AS OF TIMESTAMP. Third, rollback — if a bad MERGE corrupted Silver, I can RESTORE to the last known good version in one command. I keep 7 days of history (VACUUM RETAIN 168 HOURS) in production — longer for regulated data."

```python
# Query as of a specific version
df_v5 = spark.read.format("delta").option("versionAsOf", 5) \
              .load("/mnt/silver/patients/")

# Query as of timestamp
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-10-23T00:00:00") \
    .load("/mnt/silver/patients/")

# Restore table to a previous version
spark.sql("RESTORE TABLE delta.`/mnt/silver/patients/` TO VERSION AS OF 5")
```

---

### Q19. How does Auto Loader work and why do you prefer it over spark.read.format('parquet') for Bronze?

**Short Answer:** Auto Loader (cloudFiles) tracks new files using ADLS event notifications or file listing, processes each file exactly once, and handles schema evolution automatically.

**Interview Script:**
> "Auto Loader is my Bronze ingestion tool of choice. The problem with spark.read.format('parquet') is that it reads all files in the folder every run — if 1,000 files arrive daily, by day 30 I'm reading 30,000 files just to process 1,000 new ones. Auto Loader solves this by maintaining a checkpoint of which files it has already processed. It uses one of two modes: file notification mode subscribes to ADLS event notifications (near-realtime, no listing overhead, needs Event Grid setup) or directory listing mode (simpler, scans for new files since last run). With trigger.availableNow = True, it processes all new files and stops — perfect for batch ETL. It also handles schema evolution: if a new column appears in source files, Auto Loader detects it and can either add the column to the schema or quarantine the file."

```python
df_bronze = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/bronze/patients/schema") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("/mnt/landing/oracle/patients/")

df_bronze.withColumn("ingestion_timestamp", current_timestamp()) \
    .writeStream.format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/bronze/patients/") \
    .option("mergeSchema", "true") \
    .trigger(availableNow=True) \
    .start("/mnt/bronze/patients/")
```

---

### Q20. What is the difference between Delta Live Tables (DLT) and regular Databricks notebooks?

**Short Answer:** DLT is a declarative pipeline framework — you define what the data should look like, DLT handles execution order, retries, expectations, and lineage automatically.

**Interview Script:**
> "In regular notebooks, I imperatively write: read this, transform that, write here. DLT flips this — I declare tables using @dlt.table decorators and define expectations with @dlt.expect. DLT figures out the execution graph (which table depends on which), runs them in order, retries failed tables, tracks lineage, and surfaces data quality metrics in a live graph UI. The key benefit for teams is governance — every DLT pipeline automatically logs how many rows failed each expectation, making data quality visible without extra code. For this role at HashedIn, I'd use DLT for the declarative Medallion pipelines where multiple tables depend on each other and quality metrics need to be surfaced to stakeholders."

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(name="bronze_patients", comment="Raw patients from Oracle")
def bronze_patients():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .load("/mnt/landing/oracle/patients/")

@dlt.table(name="silver_patients", comment="Cleaned, deduplicated patients")
@dlt.expect_or_drop("valid_patient_id", "PATIENT_ID IS NOT NULL")
@dlt.expect_or_drop("valid_age", "AGE BETWEEN 0 AND 150")
def silver_patients():
    return dlt.read_stream("bronze_patients") \
        .dropDuplicates(["PATIENT_ID"])
```

---

## SECTION 4 — AZURE DATABRICKS & PLATFORM (Q21–Q26)

---

### Q21. How do you tune Databricks cluster configuration for a large PySpark job?

**Short Answer:** Choose cluster type (memory-optimized for MERGE/joins, compute-optimized for Parquet reads); tune executor cores=5; size workers = data_GB / cores_per_worker.

**Interview Script:**
> "My cluster tuning formula: for memory-intensive jobs like MERGE on large Silver tables, I use Memory Optimized nodes (Standard_E16ds_v4 on Azure — 128GB RAM). For the executor, the optimal core count is 5 per executor — this is a PySpark community standard balancing parallelism vs resource contention. So for a 16-core machine, I set 2 executors per node × 5 cores each = 10 cores used, 2 reserved for OS. I set executor memory = (total_RAM × 0.9) / executors_per_node. I enable Autoscaling so the cluster scales up for heavy loads and back down during light processing — cost-efficient without over-provisioning. I also enable Photon (vectorized execution engine) for SQL-heavy workloads — it gave us a 2–3× speedup on Gold aggregations in FinOps."

---

### Q22. What is Unity Catalog and how have you used it?

**Short Answer:** Unity Catalog is Databricks' centralized governance layer — unified access control, lineage, audit, and cataloging across all workspaces.

**Interview Script:**
> "In FinOps Dataverse, I set up Unity Catalog as the governance layer. The three-level namespace is catalog.schema.table — for example, finops_prod.silver.transactions. I created separate catalogs per environment (finops_dev, finops_qa, finops_prod) and assigned RBAC: data engineers have full access to dev catalog, read+write on silver/bronze, read-only on gold for analysts. I used column-level security to mask PII columns like EMPLOYEE_NAME and ACCOUNT_NUMBER — analysts see the masked value, only the data team sees the actual value. Unity Catalog also gives automatic data lineage — when you query in Databricks, it records the lineage graph showing which table was read and written, without any extra code. This was important for our compliance requirements."

```sql
-- Create catalog and schema
CREATE CATALOG finops_prod;
CREATE SCHEMA finops_prod.silver;

-- Column masking for PII
CREATE FUNCTION finops_prod.mask_name(name STRING)
  RETURNS STRING
  RETURN CASE WHEN is_account_group_member('data-engineers') THEN name
              ELSE CONCAT(LEFT(name,1), '***') END;

ALTER TABLE finops_prod.silver.employees
  ALTER COLUMN FULL_NAME SET MASK finops_prod.mask_name;
```

---

### Q23. How do you use Databricks Workflows for orchestration?

**Short Answer:** Databricks Workflows = DAG of notebook/JAR/Python tasks with dependencies, retries, scheduling, and email alerts — no need for Airflow for Databricks-only pipelines.

**Interview Script:**
> "In FinOps, I orchestrate the daily ETL using Databricks Workflows. I define a multi-task job: Task 1 runs the Bronze ingestion notebook (Auto Loader), Task 2 runs Silver MERGE (depends on Task 1 success), Task 3 runs Gold aggregations (depends on Task 2 success), Task 4 sends a notification webhook (depends on Task 2/3 completion). Each task has its own retry policy — 2 retries, 30-minute timeout. The job is scheduled via a cron trigger at 2 AM daily. For CI/CD, I define the workflow in databricks.yml using Databricks Asset Bundles — the workflow definition is in Git, and deploying to prod means running databricks bundle deploy --target prod."

---

### Q24. What is Photon in Databricks and when does it help?

**Short Answer:** Photon is a C++-based vectorized query engine inside Databricks that replaces the JVM Spark engine for SQL workloads — typically 2–10× faster for aggregations and joins.

**Interview Script:**
> "Photon is Databricks' replacement for the JVM-based Spark engine for SQL operations. It processes data in vectorized batches using SIMD CPU instructions instead of row-by-row JVM interpretation. I enabled Photon in FinOps on the Gold layer cluster — the Gold jobs are SQL-heavy aggregations on large Silver tables. The speed improvement was significant: a daily financial aggregation that took 18 minutes dropped to 7 minutes with Photon, with no code changes. Photon helps most for SQL aggregations, filters, joins, and sort operations on structured data. It doesn't speed up Python UDFs — those still run on the JVM. For this role's analytics workloads, I'd always enable Photon on Gold and reporting clusters."

---

### Q25. How do you implement CI/CD for Databricks notebooks using Databricks Asset Bundles?

**Short Answer:** Define pipeline in databricks.yml with dev/qa/prod targets → push to Git → Azure DevOps pipeline runs bundle validate + bundle deploy --target prod.

**Interview Script:**
> "In FinOps, I use Databricks Asset Bundles for CI/CD. The bundle config file databricks.yml lives in the Git repo alongside the notebooks. It defines the job structure, cluster config, and three targets: dev, qa, prod — each pointing to different Databricks workspaces and storage paths. When I push code to main branch, the Azure DevOps pipeline runs: first lint + pytest on notebooks, then bundle validate to catch config errors, then bundle deploy --target dev. For QA, deploy is triggered by a release tag. For prod, there's a manual approval gate before deploy. This means every prod deployment has been validated in dev and QA, and the approval gate prevents accidental releases."

```yaml
# databricks.yml
bundle:
  name: finops_dataverse

targets:
  dev:
    workspace:
      host: https://adb-dev.azuredatabricks.net
  prod:
    workspace:
      host: https://adb-prod.azuredatabricks.net
    variables:
      storage_path: /mnt/prod/

resources:
  jobs:
    daily_etl:
      name: FinOps_Daily_ETL
      tasks:
        - task_key: bronze_ingestion
          notebook_task:
            notebook_path: notebooks/01_bronze_ingestion
```

---

### Q26. How do you handle schema evolution in a Delta Lake pipeline?

**Short Answer:** Bronze: mergeSchema=true (auto-add new columns); Silver: autoMerge in Spark config; schema drift detection code to alert on breaking changes.

**Interview Script:**
> "Schema changes are one of the biggest prod risks in long-running pipelines. My three-layer strategy: Bronze uses mergeSchema=true in the write options — if Oracle adds a new column, Auto Loader automatically adds it to the Bronze Delta table. Silver uses spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true') — same behavior. For breaking changes (a column is renamed or type changed), neither of these helps because Delta's mergeSchema only handles additive changes. So I add a schema drift detection step: before the Silver MERGE, I compare the incoming dataframe's columns against the Silver table schema. If any existing column is missing or has a changed type, I fail the pipeline immediately with a clear error and alert the team to review. This prevents silent data corruption."

---

## SECTION 5 — DATA QUALITY & GOVERNANCE (Q27–Q31)

---

### Q27. How do you implement data quality checks in production pipelines?

**Short Answer:** Pre-load: null checks, range checks, referential integrity. Post-load: row count threshold vs yesterday. Bad rows → dead-letter Delta table.

**Interview Script:**
> "My data quality framework has three phases. Pre-load checks run before MERGE: null check on PKs, range checks (AGE 0–150, AMOUNT > 0), format checks (TRANSACTION_DATE matches YYYY-MM-DD), referential integrity (REGION_CODE exists in lookup table). Any row failing a hard rule goes to a dead-letter Delta table with a rejection_reason column — it doesn't block the pipeline. Post-load checks run after MERGE: I compare row count in Silver against a rolling 7-day average — if today's count is less than 80% of the average, I raise a soft warning (log + Teams notification). I also check for null growth in key columns — if suddenly 5% of REGION is null but yesterday it was 0.1%, something broke upstream. For this role, I'd implement Great Expectations suites which give a much cleaner way to define and document all these checks."

---

### Q28. What is data lineage and how do you implement it?

**Short Answer:** Data lineage tracks how data moves from source to target — Unity Catalog provides automatic column-level lineage in Databricks; Azure Purview for cross-platform.

**Interview Script:**
> "In FinOps I implemented lineage at two levels. At the platform level, Unity Catalog automatically captures lineage — every read and write operation is recorded, so you can open any Gold table in the Databricks Data Explorer and see the full upstream lineage back to the Bronze source. At the pipeline level, I add a source_system column (value: 'ORACLE_CDB') and an ingestion_timestamp column to every Bronze row, and a pipeline_run_id column that maps back to the ADF or Databricks workflow run. This means for any Gold row, I can trace back to the exact pipeline run, the Bronze file, and the original source record. For cross-platform lineage (Oracle → ADF → ADLS → Databricks → Power BI), I'd use Microsoft Purview which scans all these connectors and builds an end-to-end lineage graph."

---

### Q29. How do you mask PII data in a lakehouse?

**Short Answer:** Unity Catalog column masking functions apply masking at query time based on the caller's group; row-level security filters entire rows.

**Interview Script:**
> "In eCDP, pharmaceutical data had strict PII requirements — patient names, DOBs, and SSNs could only be accessed by authorized teams. My approach: I store the actual values in Silver (data engineers need them for processing), but mask them at the Gold/serve layer using Unity Catalog's column masking. I create a SQL masking function that checks if the caller is in the 'clinical-data-team' group — if yes, return the real value; if no, return a masked version. This masking is applied at query time, transparently — analysts using the Gold table never see the real PII without code changes on their end. For the Gold layer I also apply row-level security to restrict regions: APAC analysts only see APAC patient rows."

---

### Q30. What is the difference between data governance and data quality?

**Short Answer:** Data quality = is the data correct and complete? Data governance = who can access what data, how is it cataloged, and what policies govern its lifecycle?

**Interview Script:**
> "Data quality answers: is this data fit for its intended use? Checks include completeness (no nulls in required fields), accuracy (values in valid ranges), consistency (same PATIENT_ID means the same patient), timeliness (data arrives within SLA). Data governance answers: who owns this data, who can see it, what does each field mean, and how long do we keep it? Governance covers access control (RBAC in Unity Catalog), data cataloging (what tables exist, what they mean), data lineage (where did this data come from), data retention policies (delete records older than 7 years per GDPR), and PII/GDPR compliance (mask SSN, right to be forgotten). In my experience, quality without governance leads to good data that only the data team can find. Governance without quality leads to well-documented junk data."

---

### Q31. How do you implement GDPR/data compliance in a data pipeline?

**Short Answer:** PII masking at serve layer, right-to-be-forgotten via DELETE + VACUUM, data retention policies, audit logging of all data access.

**Interview Script:**
> "GDPR has two critical technical requirements for a lakehouse. First, right to erasure — when a patient requests their data be deleted, I need to delete them from Silver and Gold Delta tables. Delta Lake supports DELETE FROM table WHERE PATIENT_ID = 123 which is logged in _delta_log. But the actual Parquet file still exists until VACUUM runs. So for GDPR deletes, I run VACUUM immediately with a short retention window. Second, PII masking — SSNs, DOBs, and names are stored encrypted at rest (Azure storage encryption) and masked at query time via Unity Catalog masking functions. I also maintain an audit log: every query on PII tables is logged via Databricks audit events to a central Azure Monitor Log Analytics workspace. For CCPA (California), the requirements are similar but add an opt-out mechanism which I implement as a flag in the PATIENTS table."

---

## SECTION 6 — CI/CD & DEVOPS (Q32–Q35)

---

### Q32. How do you structure CI/CD for a data pipeline project?

**Short Answer:** Dev branch → PR → automated tests (lint, unit, integration) → merge to main → deploy to Dev auto → QA on release tag → Prod with manual approval.

**Interview Script:**
> "My CI/CD setup uses Azure DevOps with two pipelines. The CI pipeline triggers on every PR: it runs flake8 linting on Python, pytest unit tests on transformation logic (mocking SparkSession with chispa), and checks databricks.yml bundle validation. The CD pipeline has three stages: Deploy-Dev (auto on merge to main), Deploy-QA (triggered by release tag), Deploy-Prod (manual approval required — a senior engineer must click approve). The deploy step runs databricks bundle deploy --target {env}, which deploys notebooks, jobs, and cluster configs from Git to the target workspace. No manual notebook uploads — everything is code, versioned in Git. This ensures dev and prod environments are always in sync and every change has a reviewable PR."

---

### Q33. How do you write unit tests for PySpark transformations?

**Short Answer:** Create a local SparkSession in conftest.py → build small test DataFrames → call the transformation function → assert output with assertEqual or checkAnswer.

```python
# conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("unit_tests") \
        .getOrCreate()

# test_silver_transforms.py
def test_deduplication_keeps_latest(spark):
    data = [
        (1, "Alice", "2024-01-01"),
        (1, "Alice", "2024-01-05"),   # same PATIENT_ID, newer date
        (2, "Bob",   "2024-01-01"),
    ]
    df = spark.createDataFrame(data, ["PATIENT_ID", "NAME", "UPDATED_DATE"])
    result = deduplicate(df, pk="PATIENT_ID", order_col="UPDATED_DATE")

    assert result.count() == 2
    alice = result.filter("PATIENT_ID = 1").collect()[0]
    assert alice["UPDATED_DATE"] == "2024-01-05"   # kept the latest

def test_null_pk_quarantined(spark):
    data = [(None, "Unknown", "2024-01-01"), (1, "Alice", "2024-01-01")]
    df = spark.createDataFrame(data, ["PATIENT_ID", "NAME", "UPDATED_DATE"])
    clean, bad = validate_pk(df, pk_col="PATIENT_ID")
    assert clean.count() == 1
    assert bad.count() == 1
```

---

### Q34. What is Terraform and how is it used in a data platform?

**Short Answer:** Terraform is IaC — declare Azure resources (Databricks workspace, ADLS, ADF) as code, version-controlled and deployed consistently across environments.

**Interview Script:**
> "Terraform defines infrastructure as HCL code. For a data platform, I'd define the Databricks workspace, ADLS Gen2 storage account, Azure Data Factory instance, Key Vault, and all IAM/RBAC assignments in .tf files. These files live in Git alongside the data pipeline code. When I need a new environment (say, a QA environment that mirrors prod), I run terraform apply --var-file=qa.tfvars and Terraform creates all the resources identically. The key benefit is drift detection — if someone manually changes a resource in the portal, Terraform plan shows the diff and you can decide whether to accept or revert the change. I haven't used Terraform directly in production yet, but I'm familiar with the concepts and have used Azure ARM templates for ADF CI/CD, which solves the same problem for the ADF resource."

---

### Q35. How do you monitor a production data pipeline?

**Short Answer:** Pipeline-level: Databricks Workflows alerts + ADF Monitor. Data-level: row count checks, quality metrics, SLA breach alerts via Teams/email.

**Interview Script:**
> "Monitoring operates at three levels. Infrastructure level: Azure Monitor alerts fire if the Databricks cluster fails to start, runs out of memory, or if ADF triggers fail — these go to PagerDuty or Teams. Pipeline level: Databricks Workflows sends email/webhook on job failure — I have a webhook to a Teams channel with job name, failed task, and error message. Data level: after each successful Silver MERGE, I log the row count, rejection count, and processing time to a Delta audit table. A Power BI dashboard reads this audit table and shows green/yellow/red SLA status per table. If the daily row count drops more than 20% from the previous run, an alert fires. These three levels mean I know within minutes if something breaks, and I know whether it's infra, pipeline code, or data quality."

---

## SECTION 7 — SNOWFLAKE (Q36–Q38)

---

### Q36. What is Snowflake's architecture and how does it differ from Databricks?

**Short Answer:** Snowflake = storage-compute separation + virtual warehouses (auto-suspend/resume) + cloud-native SQL DWH. Databricks = open lakehouse on Delta Lake with Spark compute.

**Interview Script:**
> "Snowflake separates storage (S3/ADLS under the hood) from compute (virtual warehouses). Each virtual warehouse is an independent MPP cluster — you can have multiple warehouses running simultaneously on the same data without contention. Warehouses auto-suspend when idle (you only pay when running) and auto-resume in seconds. This makes Snowflake excellent for multi-tenant, bursty workloads like BI dashboards. Databricks on the other hand runs Spark on Delta Lake — open format, more flexible for Python/ML workloads, and better for streaming and large-scale ETL. In eCDP, I used Snowflake as a target sink for SAP financial data — dual-write to Iceberg (for ML teams) and Snowflake (for finance dashboards). The Snowflake path was chosen because the finance team's SQL tools connected to Snowflake natively."

---

### Q37. What is Snowpipe and how does it work?

**Short Answer:** Snowpipe is Snowflake's continuous micro-batch ingestion service — it loads new files from S3/ADLS into Snowflake tables automatically as they arrive.

**Interview Script:**
> "Snowpipe uses an event-driven model. When a new Parquet or CSV file lands in an S3 bucket, an S3 event notification triggers an SQS queue, and Snowpipe picks it up within seconds and loads it into the target Snowflake table. The load is incremental — Snowpipe tracks which files it has loaded (in a metadata table) and never loads the same file twice. I used Snowpipe in eCDP for the Snowflake target path — after the PySpark job wrote Parquet files to S3, Snowpipe loaded them into Snowflake within 60 seconds, so finance dashboards were near-realtime. The alternative is COPY INTO command run on a schedule (a Snowflake Task), which is simpler but adds latency equal to the schedule interval."

---

### Q38. What are Snowflake Streams and Tasks?

**Short Answer:** Stream = CDC table that tracks DML changes (insert/update/delete) on a source table. Task = scheduled SQL job that processes the stream, like a mini-pipeline.

**Interview Script:**
> "Snowflake Streams are like a CDF (Change Data Feed) in Delta Lake — they track which rows were inserted, updated, or deleted in a source table since the stream was last consumed. Each stream row has a METADATA$ACTION column: INSERT or DELETE (updates appear as DELETE + INSERT pair). Tasks are Snowflake's scheduler — you define a Task that runs a SQL query on a schedule (cron expression) or when a stream has data (stream-based trigger). The classic pattern: Stream on the staging table → Task runs every 5 minutes → if stream.row_count > 0, run MERGE from stream into the target table. I understand this pattern conceptually from my Databricks CDF and watermark experience; for this role I'd apply the same incremental logic using Streams and Tasks in Snowflake."

---

## SECTION 8 — AI/GENAI & ML PIPELINES (Q39–Q41)

---

### Q39. What are RAG pipelines and how does a Data Engineer's work enable them?

**Short Answer:** RAG = Retrieval-Augmented Generation — the LLM retrieves relevant context from a vector store before answering. Data Engineers build the pipelines that chunk, embed, and load documents into that vector store.

**Interview Script:**
> "In a RAG system, when a user asks a question, the LLM doesn't answer from memory alone — it first retrieves the most relevant chunks of text from a vector database, then generates an answer grounded in those chunks. As a Data Engineer, my job in a RAG pipeline is upstream: I take raw documents (PDFs, contracts, clinical trial reports), chunk them into 500–1000 token segments, call an embedding model to convert each chunk to a vector, and load those vectors into a vector store like Databricks Vector Search or Pinecone. I need to do this incrementally — new documents arrive daily, so I need to process only new chunks and upsert them. I also maintain the metadata (source document, page number, timestamp) alongside each vector so the retrieval layer can filter and cite sources. This is a standard ETL pipeline — the novelty is the chunking and embedding step."

---

### Q40. How do you chunk unstructured data for LLM/RAG use cases?

**Short Answer:** Fixed-size chunking (500 tokens, 50-token overlap) for uniform docs; semantic/paragraph chunking for structured docs; always preserve metadata with each chunk.

**Interview Script:**
> "Chunking strategy depends on the document type. For dense, unstructured documents like clinical trial reports, I use fixed-size chunking with overlap: 500 tokens per chunk, 50-token overlap between consecutive chunks so context isn't cut mid-sentence at a boundary. For structured documents with sections (annual reports, contracts), I use semantic chunking — split at paragraph or section boundaries, keeping section headers with each chunk for better retrieval context. In PySpark, I implement this as a UDF that takes the raw text column, applies the chunking logic, and explodes the result into one row per chunk. I add metadata columns: source_doc_id, chunk_index, page_number, section_header. The resulting DataFrame is then sent to an embedding model endpoint and the vectors are written to the vector store."

```python
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

def chunk_text(text, chunk_size=500, overlap=50):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size - overlap):
        chunk = " ".join(words[i:i + chunk_size])
        chunks.append({"text": chunk, "chunk_index": len(chunks)})
    return chunks

chunk_udf = udf(chunk_text, ArrayType(StructType([
    StructField("text", StringType()),
    StructField("chunk_index", IntegerType())
])))

df_chunks = df_docs \
    .withColumn("chunks", chunk_udf(col("document_text"))) \
    .withColumn("chunk", explode(col("chunks"))) \
    .select("doc_id", "source", col("chunk.text"), col("chunk.chunk_index"))
```

---

### Q41. What is MLflow and how does a Data Engineer interact with it?

**Short Answer:** MLflow tracks ML experiments (parameters, metrics, models). Data Engineers build the feature tables and data pipelines that feed ML models logged in MLflow.

**Interview Script:**
> "As a Data Engineer, my interaction with MLflow is primarily on the data side — I build and maintain the feature store tables that ML models train on. MLflow's Model Registry helps me understand which model version is in production so I know which features it needs and which transformations must be stable. I use MLflow to log data pipeline metrics — row counts, quality check results, feature distribution statistics — alongside the ML experiments so the team can correlate data quality issues with model performance changes. For model serving, the Data Engineer's role is to ensure the real-time feature serving path (live feature lookups in Databricks Online Tables) is built and maintained. I haven't built ML models myself, but I've collaborated closely with ML engineers and understand the data contract between pipelines and models."

---

## SECTION 9 — STREAMING (Q42–Q43)

---

### Q42. What is Structured Streaming in PySpark? How does it work?

**Short Answer:** Structured Streaming treats a live data stream as an unbounded DataFrame — same PySpark API as batch, but continuously ingests and processes micro-batches.

**Interview Script:**
> "Structured Streaming is Spark's answer to streaming ETL — you write almost the same code as batch PySpark but instead of spark.read you use spark.readStream, and instead of df.write you use df.writeStream. Under the hood, Spark treats the stream as a continuously appending table. It reads micro-batches from the source (Kafka, Event Hubs, Auto Loader, Delta CDF), processes each batch with the same transformations and aggregations you'd write for batch, and writes results to the sink. The checkpoint directory stores the offset of the last processed message, so if the streaming job restarts, it resumes exactly where it left off — no duplicates, no data loss (exactly-once semantics with Delta sink). I've used Structured Streaming with Auto Loader (cloudFiles source) for near-realtime Bronze ingestion."

```python
# Kafka → Bronze Delta via Structured Streaming
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "patient_events") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_ts", current_timestamp())

df_parsed.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/bronze/patient_events/") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start("/mnt/bronze/patient_events/")
```

---

### Q43. What is Kafka and when would you use it over ADF for ingestion?

**Short Answer:** Kafka is a distributed event streaming platform for high-throughput, low-latency, real-time event streams. ADF is for batch/scheduled ETL from databases and files.

**Interview Script:**
> "Kafka and ADF solve different problems. ADF is batch-oriented — it pulls data from a source on a schedule (every hour, every day) and is excellent for database extracts, file processing, and orchestrating multi-step ETL. Kafka is event-streaming — producers push events to Kafka topics continuously (thousands/second), and consumers read them in near-realtime with sub-second latency. If I need to process click events from a website, IoT sensor readings, or financial transactions as they happen, Kafka is the right tool — ADF's minimum batch interval is too coarse. At Cognizant, I haven't used Kafka directly (our pipelines are batch, ADF-driven), but I understand the architecture conceptually and have read messages from Azure Event Hubs (which has a Kafka-compatible API) in a streaming proof-of-concept."

---

## SECTION 10 — SQL & DATA MODELING (Q44–Q47)

---

### Q44. Write a SQL query to find overlapping date ranges (e.g., double-booked appointments).

```sql
-- Find patients with overlapping insurance coverage periods
SELECT
    a.PATIENT_ID,
    a.POLICY_ID     AS policy_1,
    b.POLICY_ID     AS policy_2,
    a.START_DATE    AS start_1,
    a.END_DATE      AS end_1,
    b.START_DATE    AS start_2,
    b.END_DATE      AS end_2
FROM insurance_coverage a
JOIN insurance_coverage b
  ON a.PATIENT_ID = b.PATIENT_ID
 AND a.POLICY_ID  < b.POLICY_ID          -- avoid self-join and duplicates
 AND a.START_DATE <= b.END_DATE          -- overlap condition
 AND b.START_DATE <= a.END_DATE
ORDER BY a.PATIENT_ID;
```

**Key logic:** Two date ranges [A_start, A_end] and [B_start, B_end] overlap when A_start ≤ B_end AND B_start ≤ A_end.

---

### Q45. What is Star Schema vs Snowflake Schema? Which would you use for a BI-serving Gold layer?

**Short Answer:** Star = fact table + denormalized dimensions (1 join per dimension = fast BI queries). Snowflake = dimensions normalized into sub-dimensions (less storage, more joins = slower BI).

**Interview Script:**
> "In FinOps Gold layer I use Star Schema. The fact table is FACT_TRANSACTIONS with foreign keys to DIM_DATE, DIM_REGION, DIM_COST_CENTER, DIM_EMPLOYEE. Each dimension is denormalized — DIM_REGION has region code, region name, and country, all in one table, no separate COUNTRY table. This means a Power BI query needs at most 4 joins to answer 'total transactions by region by quarter' — simple, fast, understandable by business users. Snowflake schema would split DIM_REGION into DIM_REGION → DIM_COUNTRY → DIM_CONTINENT — saves some storage but adds joins and confuses non-technical users. For analytical BI queries, Star Schema wins every time. Snowflake schema makes sense for OLTP or when dimension tables are extremely large and normalized storage matters."

---

### Q46. How do you write an SQL query to calculate a 7-day rolling average?

```sql
-- 7-day rolling average transaction amount per region
SELECT
    TRANSACTION_DATE,
    REGION,
    DAILY_AMOUNT,
    AVG(DAILY_AMOUNT) OVER (
        PARTITION BY REGION
        ORDER BY TRANSACTION_DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW   -- 7 rows including current
    ) AS rolling_7d_avg
FROM (
    SELECT
        TRANSACTION_DATE,
        REGION,
        SUM(AMOUNT) AS DAILY_AMOUNT
    FROM fact_transactions
    GROUP BY TRANSACTION_DATE, REGION
) daily_totals
ORDER BY REGION, TRANSACTION_DATE;
```

---

### Q47. What are slowly changing dimensions (SCDs) and when do you use each type?

**Short Answer:** SCD1 = overwrite (no history), SCD2 = add new row with start/end date (full history), SCD3 = add previous value column (limited history).

**Interview Script:**
> "In FinOps I use SCD1 for DIM_COST_CENTER — when a cost center's name changes, we just update it. Historical reports use the new name — finance doesn't need to know what it was called 2 years ago. For DIM_EMPLOYEE, I use SCD2 — if an employee moves from APAC to EMEA, I need historical reports to show their APAC transactions under APAC and future transactions under EMEA. I achieve this in Delta Lake by closing the old row (set is_active=false, end_date=today) and inserting a new row (is_active=true, start_date=today, end_date=null). To get the current record for any employee, the query filters WHERE is_active = true. SCD3 (store previous value in a separate column like PREV_REGION) is rare — I'd only use it when users need quick access to the last value without a full history table."

---

## SECTION 11 — SCENARIO-BASED & BEHAVIORAL (Q48–Q50)

---

### Q48. Tell me about a time you improved pipeline performance significantly. What did you do?

**Short Answer:** FinOps: Delta MERGE took 45 min on 26+ TB. Fixed with Low Shuffle MERGE + Z-ORDER + executor tuning → 8 minutes.

**Interview Script:**
> "In FinOps, the nightly Silver MERGE job was taking 45 minutes, breaking our 2 AM SLA for the 6 AM report run. I opened Spark UI and found two issues: first, the MERGE was doing a full table shuffle because there was no data co-location — Silver table had no Z-ORDER. Second, the executor configuration was default (200 shuffle partitions for a 26+ TB table is comically small). I fixed both: ran OPTIMIZE ZORDER BY (TRANSACTION_DATE, COST_CENTER_ID) on Silver — this co-locates rows that are commonly joined/filtered together so MERGE reads fewer files. I changed shuffle partitions from 200 to 1600 (matching actual data size) and enabled Low Shuffle MERGE by setting spark.databricks.delta.merge.enableLowShuffle = true — this optimization keeps matching rows in-place and only shuffles the non-matching subset. Result: 45 minutes to 8 minutes. That's a 5× improvement that gave us 37 extra minutes of buffer in the SLA window."

---

### Q49. How do you handle a pipeline failure in production at 3 AM?

**Short Answer:** Automated alert fires → check Databricks run / ADF Monitor → identify failed task + error → fix or rerun → root cause analysis next morning.

**Interview Script:**
> "My production pipelines have three layers of alerting so I know within 5 minutes of a failure. The Databricks Workflow sends a webhook to a Teams channel — the message includes the job name, failed task, error message, and a direct link to the run page. I also have an Azure Monitor alert on the Databricks workspace for failed job runs. And the downstream Gold table doesn't get updated, so Power BI dashboards show stale data — the business team will notice by 7 AM. At 3 AM I'd open the Teams alert link, go to the Databricks run page, click on the failed task and read the exception. If it's a transient issue (OOM, cluster start failure), I rerun the job from the failed task — the watermark wasn't updated so the pipeline re-processes the same window safely. If it's a data issue (bad source row), I check the dead-letter table, understand the bad data, fix it, and rerun. Root cause analysis and a postmortem go into Confluence the next morning."

---

### Q50. Where do you see yourself growing in the next 2 years, and how does this role fit?

**Short Answer:** From pipeline builder to platform architect — lead end-to-end lakehouse design decisions, contribute to AI-ready data infrastructure, and mentor junior engineers.

**Interview Script:**
> "In the next 2 years, I want to grow from someone who builds well-engineered pipelines within a defined architecture to someone who designs the architecture itself — making decisions about Medallion layer boundaries, governance models, cost optimization strategy, and data contracts with downstream consumers. At HashedIn, working across multiple client industries would accelerate this significantly — I'd see patterns and anti-patterns across different business domains much faster than within a single client at Cognizant. I'm also genuinely excited about the AI/ML data engineering work in this JD — building the governed data pipelines for RAG and LLM use cases is the next frontier of data engineering, and I want to be part of that early. The Databricks Professional certification and potentially a Snowflake SnowPro certification are on my near-term plan to deepen platform expertise."

---

## QUICK REFERENCE — JD COVERAGE MAP

| JD Requirement | Questions Covered | Your Evidence |
|---|---|---|
| PySpark / Python | Q5, Q6, Q7, Q8, Q9, Q10 | eCDP (10M+/day), FinOps (26+ TB) |
| Delta Lake / Medallion | Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q20 | Both projects, Databricks cert |
| Azure Databricks | Q21, Q22, Q23, Q24, Q25, Q26 | FinOps (Azure), eCDP (AWS) |
| ADF / Orchestration | Q3, Q23, Q35 | FinOps (ADF + Databricks Workflows) |
| Data Quality | Q11, Q27 | Custom validation, dead-letter pattern |
| Data Governance / Unity Catalog | Q22, Q28, Q29, Q30, Q31 | Unity Catalog, PII masking, lineage |
| CI/CD / Databricks Asset Bundles | Q25, Q32, Q33, Q34 | DABs, Azure DevOps, pytest |
| Snowflake | Q36, Q37, Q38 | eCDP dual-write to Snowflake |
| AI/GenAI / RAG / LLM | Q39, Q40, Q41 | Conceptual + LLMs used (Claude, Copilot) |
| Streaming (Kafka/Event Hubs) | Q42, Q43 | Structured Streaming, Event Hubs PoC |
| SQL / Data Modeling | Q44, Q45, Q46, Q47 | Star Schema, SCDs, Window functions |
| Behavioral / Soft Skills | Q1, Q2, Q3, Q4, Q48, Q49, Q50 | FinOps results, ownership, achievements |

---

*HashedIn by Deloitte — 50 Q&A | Tailored to Mayuresh Patil Resume | Nov 2026*
