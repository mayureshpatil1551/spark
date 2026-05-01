# Interview Preparation Guide
## Mapping Your Real Experience → ADF & Databricks
### Real-World Project Q&A | eCDP & FinOps | Pharmaceutical Data Engineering

---

## ⚠️ IMPORTANT: Read This First

You are NOT lying in interviews. Your actual experience is **equally or more advanced** than ADF + Databricks. Here is the honest truth:

| What You Used | What Interviewers Ask About | Why You Are Qualified |
|---|---|---|
| Modak Nabu | Azure Data Factory (ADF) | Both are **orchestration tools** — same concepts, different UI |
| Apache Iceberg | Delta Lake | Both are **open table formats** — Iceberg is actually harder |
| GitHub + Nabu integration | Databricks Notebooks + Git | **Same CI/CD concept** — version control → pipeline execution |
| Medallion Architecture | Medallion Architecture | **Identical** — Raw, Trusted, Curated |
| PySpark on Nabu | PySpark on Databricks | **Same Spark engine** — compute is identical |

**Your strategy:** Present concepts confidently. When asked tool-specific questions, bridge using "In my project we used X which does the same thing as Y in ADF/Databricks."

---

## PART 1 — Concept Mapping: Your Stack → ADF + Databricks

---

### 1.1 Modak Nabu → Azure Data Factory

**What Nabu does (that you know):**
- Orchestrates data pipelines — triggers PySpark jobs in sequence
- Manages dependencies between pipeline stages
- Monitors pipeline runs — success, failure, retry
- Integrates with GitHub — pulls code, runs it
- Has a metadata-driven approach — reads config, generates pipelines

**How ADF does the same things:**

| Nabu Concept | ADF Equivalent | What to Say in Interview |
|---|---|---|
| Pipeline (in Nabu) | Pipeline (in ADF) | "Both define the flow of data activities" |
| Job execution trigger | Schedule/Event Trigger | "Both support time-based and event-based triggers" |
| Dependency management | Pipeline dependencies, If/Else activities | "Both control activity execution order" |
| Monitoring dashboard | ADF Monitor tab | "Both provide run history, status, error details" |
| GitHub integration | ADF Git integration (CI/CD mode) | "Both pull from Git, same concept" |
| Retry on failure | ADF retry policy on activities | "Both support configurable retry logic" |
| Metadata-driven config | Lookup + ForEach Activities | "Both use configuration to drive dynamic pipelines" |

**What you need to learn about ADF (gaps to fill):**
- **Integration Runtime types** (Azure IR, Self-hosted IR, SSIS IR) — spend 1 hour
- **Linked Services and Datasets** — how ADF connects to sources/sinks
- **Specific activities** — Copy Activity, Data Flow, Web Activity, Stored Procedure
- **Triggers** — Schedule, Tumbling Window, Event-based, Custom Event

**Key talking point:**
> "In my project I used Modak Nabu as the orchestration layer — it plays the same role as Azure Data Factory. Both tools schedule and monitor data pipeline jobs, handle retries, manage dependencies between stages, and integrate with version control. Nabu is actually purpose-built for enterprise data governance pipelines, so concepts like metadata-driven orchestration and data quality gate enforcement are things I implemented hands-on."

---

### 1.2 Apache Iceberg → Delta Lake

**This is your STRONGEST technical advantage.** Iceberg is harder and more complex than Delta Lake. Anyone who knows Iceberg deeply can switch to Delta Lake in a day.

| Iceberg Concept You Know | Delta Lake Equivalent |
|---|---|
| Snapshot model (snapshot → manifest list → manifest → data files) | Transaction log (_delta_log → checkpoint → JSON entries) |
| Time travel (VERSION AS OF, TIMESTAMP AS OF) | Time travel (same syntax) |
| ACID transactions | ACID transactions |
| Schema evolution (field_id based) | Schema evolution (mergeSchema) |
| Partition evolution (versioned partition specs) | No direct equivalent (Delta partitions are fixed) |
| Compaction (rewrite_data_files) | OPTIMIZE command |
| Copy-on-Write / Merge-on-Read | Copy-on-Write / Deletion Vectors |
| Hidden partitioning | No equivalent (Delta requires explicit partition columns) |
| Multi-engine support (Spark, Flink, Trino) | Primarily Spark/Databricks |

**Iceberg advantages to mention (makes you look MORE knowledgeable):**
- Iceberg's partition evolution is more flexible than Delta's fixed partitioning
- Iceberg works across multiple engines (Spark, Flink, Trino, Hive) — Delta is primarily Databricks
- Iceberg's snapshot model is arguably more transparent than Delta's log structure
- In pharma/regulated industries, Iceberg's audit trail capabilities are enterprise-grade

**Key talking point:**
> "In my project we used Apache Iceberg as the table format instead of Delta Lake. Iceberg and Delta solve the same problem — ACID transactions, schema evolution, time travel, and query performance — on cloud object storage. I'm deeply familiar with Iceberg internals including the snapshot model, manifest files, Copy-on-Write vs Merge-on-Read write modes, and compaction strategies. Transitioning to Delta Lake would take me a day — the core concepts are identical."

---

### 1.3 GitHub + Nabu → Databricks Notebooks + Git

**What you did:**
- Wrote PySpark code in GitHub repositories
- Nabu pulls code from GitHub and executes it
- Branching strategy for dev/staging/prod
- Code reviews via Pull Requests
- CI/CD: merge to main → auto-deploy to pipeline

**What Databricks does:**
- Write PySpark code in Databricks Notebooks
- Databricks Repos syncs notebooks with GitHub/Azure DevOps
- Same branching strategy applies
- CI/CD via Databricks CLI or Terraform

**The key insight:** The Spark code you write is **identical** whether it runs on Nabu or Databricks. The compute engine is Apache Spark in both cases. Databricks just adds a managed cluster and UI on top.

**Key talking point:**
> "My PySpark code runs on the Spark engine — the actual transformation logic is identical whether executed via Nabu or Databricks. In Databricks, I would write the same PySpark transformations in a notebook and use Databricks Repos to sync with GitHub, which mirrors our current Git integration pattern."

---

### 1.4 What to Study — Priority Order

#### 🔴 HIGH PRIORITY (Study in first week)

**Azure Data Factory:**
- Linked Services, Datasets, Integration Runtime concepts
- Copy Activity — parallel copy, DIU settings
- ForEach + Lookup (metadata-driven pipeline pattern — you already know the concept)
- Trigger types — Schedule, Tumbling Window, Storage Event
- Pipeline monitoring and alerting
- ADF CI/CD with GitHub integration

**Databricks:**
- Databricks workspace UI — clusters, notebooks, jobs
- Cluster configuration — driver vs worker, autoscaling
- Databricks Repos — Git integration
- Delta Lake on Databricks — OPTIMIZE, VACUUM, DESCRIBE HISTORY
- Unity Catalog basics — catalog.schema.table namespace
- Databricks Jobs — job clusters, task dependencies

#### 🟡 MEDIUM PRIORITY (Study in second week)

**ADF Advanced:**
- Data Flows (visual ETL transformations in ADF)
- Self-hosted Integration Runtime for on-prem
- ADF with ADLS Gen2 and Azure SQL

**Databricks Advanced:**
- Databricks SQL Warehouses
- MLflow basics (may come up in AI/ML-adjacent roles)
- Workflows (multi-task jobs with task dependencies)
- Z-Ordering and Liquid Clustering

#### 🟢 LOW PRIORITY (Nice to know)

- Azure DevOps Pipelines for ADF CI/CD
- Databricks Terraform provider
- Azure Monitor integration

---

## PART 2 — Your Project Story

---

### 2.1 eCDP Project — How to Describe It

**What you should say (mapped to ADF/Databricks vocabulary):**

> "I worked on the e-Clinical Data Platform (eCDP) — a pharmaceutical data engineering project where I built a Data Lakehouse architecture on Azure. The platform ingested 10TB+ of clinical trial data from Oracle databases via JDBC and Salesforce APIs, processed it through a Medallion Architecture (Raw → Trusted → Curated layers), and served it to downstream analytics teams and Salesforce for operational use.
>
> For orchestration, we used an enterprise data pipeline tool [Modak Nabu] that plays the same role as Azure Data Factory — scheduling jobs, managing dependencies, monitoring runs, and integrating with our GitHub repository. For the storage layer, we used Apache Iceberg tables on AWS S3 — which provides the same capabilities as Delta Lake: ACID transactions, schema evolution, time travel for regulatory audit trails, and query optimization through compaction and partition management.
>
> The transformation layer ran PySpark — the same Spark engine that powers Azure Databricks — processing 10 million+ records daily from multiple source systems, implementing a Golden Record identity resolution system using cross-reference mappings, and applying data quality validations at each Medallion layer boundary."

---

### 2.2 FinOps Project — How to Describe It

> "I also worked on the FinOps Dataverse project, which focused on financial operations data — processing XML-formatted data from legacy systems into a modern Hive-on-ADLS architecture. This involved building PySpark pipelines to parse and transform complex XML structures, implementing schema validation to enforce data contracts, and orchestrating the pipeline through our metadata-driven orchestration framework."

---

## PART 3 — Real-World Project Experience Q&A

*These are questions interviewers ask to verify you actually built what you say you built.*

---

### PROJECT SETUP & ARCHITECTURE

---

**Q1. Walk me through the architecture of your most complex data engineering project.**

**Answer:**

"My most complex project was the e-Clinical Data Platform (eCDP) for a pharmaceutical client. Let me walk you through it end to end.

**Source systems:** We had two primary sources. Oracle databases containing 10TB+ of clinical trial data — patient records, drug trial results, audit events — accessed via parallel JDBC reads. And Salesforce CRM containing customer and account data, accessed via their Bulk API 2.0 for large-scale extractions.

**Orchestration layer:** We used Modak Nabu — an enterprise orchestration platform that functions like Azure Data Factory. It reads a metadata configuration (table definitions, schedules, dependencies) and dynamically generates and executes the pipeline. This meant adding a new table to the pipeline was as simple as adding one row to the config — no code changes needed. In ADF terms, this is the Lookup → ForEach → parameterised Copy Activity pattern.

**Storage layer:** Raw data landed on AWS S3 in Apache Iceberg format — an open table format providing ACID transactions, schema evolution, and time travel. This is functionally equivalent to Delta Lake but engine-agnostic. We had three Medallion layers: Raw (data as-is from source, immutable), Trusted (cleaned, deduplicated, with Golden Record XREF applied), and Curated (business-ready aggregates for downstream analytics).

**Transformation layer:** PySpark jobs ran on a managed Spark cluster. The transformations included parallel JDBC ingestion, identity resolution for the Golden Record system, schema validation at each layer boundary, and optimised partition management for Iceberg tables.

**Output:** The Curated layer fed three consumers — a Power BI-connected analytical store, a downstream Salesforce sync (writing back unified data), and a regulatory audit system that used Iceberg's time travel capability to prove what data existed at specific historical dates.

The whole platform processed 10M+ records daily with 99.9% data uptime — achieved through idempotent MERGE operations, watermark-based incremental loading, and dead-letter queuing for malformed records."

---

**Q2. How did you design the Medallion Architecture in your project? What did each layer contain?**

**Answer:**

"We implemented a three-layer Medallion Architecture on Apache Iceberg (Iceberg is the equivalent of Delta Lake — same concepts, different implementation):

**Raw Layer (Bronze):**
- Data landed exactly as it came from the source — no transformations
- Schema enforcement was strict: unexpected columns from source were rejected, logged, and alerted on — we didn't silently ingest bad schema
- Partitioned by `ingestion_date` so each day's load was isolated
- Iceberg compaction ran nightly to merge streaming micro-file writes
- Retained for 90 days for reprocessing capability
- The key principle: Raw is your safety net. If anything goes wrong downstream, you can always reprocess from Raw without touching the source systems

**Trusted Layer (Silver):**
- This is where the real business logic lived
- Deduplication using PySpark window functions (ROW_NUMBER over partition by business key, order by updated_at DESC)
- XREF/Golden Record resolution — matching records across Oracle and Salesforce to the same entity using confidence scoring
- Schema was validated and enforced using our data quality framework (Modak Nabu's quality rule engine — equivalent to Great Expectations or dbt tests)
- Data quality score had to be above 95% to promote from Trusted to Curated
- Partitioned by `processing_date` and clustered by `patient_id` for query performance

**Curated Layer (Gold):**
- Pre-aggregated business views — department-level summaries, trial outcome metrics
- Joined with dimension tables (product catalog, site master, country reference)
- Schema was version-controlled — breaking changes went through an approval process
- Served directly to Power BI, downstream APIs, and Salesforce sync
- Iceberg time travel enabled regulatory responses without maintaining separate archive tables

The key insight I'd share: the value of Medallion Architecture isn't just the three layers — it's the contracts between them. Each layer promotion had automated quality gates. No bad data could silently pass through."

---

**Q3. How did you handle ingestion from Oracle and Salesforce in the same pipeline?**

**Answer:**

"We treated Oracle and Salesforce as two different ingestion patterns, each with their own optimisation strategies.

**Oracle via JDBC:**
The naive approach reads Oracle sequentially — one Spark executor pulling all data through a single JDBC connection. This doesn't scale for 10M+ records. We used JDBC parallelism:

```python
df = spark.read.format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", f"(SELECT * FROM {schema}.{table} WHERE updated_at > '{watermark}') t") \
    .option("partitionColumn", "patient_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "10000000") \
    .option("numPartitions", "100") \
    .load()
```

This spawned 100 parallel JDBC connections, each pulling a range of patient IDs. A pull that used to take 3 hours became 5 minutes.

**Salesforce via Bulk API 2.0:**
Salesforce has API rate limits (daily call limits). We used the Bulk API 2.0 which creates async batch jobs server-side — designed specifically for million-record extractions. We implemented OAuth 2.0 JWT Bearer authentication (server-to-server, no user interaction needed) and handled partial failures since Salesforce returns per-record success/failure in the response.

**Unified pattern:**
Both sources landed in the Raw Iceberg layer with the same schema structure — a `source_system` column tagged each record's origin. This allowed the Trusted layer's XREF logic to match a patient from Oracle (source='ORACLE') to the same patient in Salesforce (source='SF') using confidence-scored matching rules."

---

**Q4. Explain the Golden Record XREF system you built. How does it work technically?**

**Answer:**

"The Golden Record problem: the same patient might be 'John Smith' in Oracle, 'J. Smith' in Salesforce, and 'JOHN SMITH' in the clinical trial database. Three records, one person, zero awareness that they're the same.

The XREF (Cross-Reference) system resolves this. Here's the technical implementation:

**Step 1 — Blocking (reduce comparison space):**
Comparing every record against every other record is O(n²) — impossible at scale. We used blocking to group likely matches: same first letter of last name + same birth year + same country. This reduced 10M records to millions of small comparison groups.

**Step 2 — Matching within blocks:**
Within each block, we applied matching rules:
- Exact match on national health ID → confidence 1.0 (definitive)
- Jaro-Winkler distance on full name > 0.92 → confidence 0.8
- Exact match on date of birth → adds 0.1 to confidence
- Same address → adds 0.05

We implemented this as a PySpark UDF calling Python's `jellyfish` library for string distance calculations.

**Step 3 — XREF table:**
Records scoring above 0.85 total confidence were linked under a `golden_id`:

```python
# XREF table schema
# golden_id | source_system | source_id  | confidence | linked_at
# GLD-0001  | ORACLE        | ORA-78901  | 1.00       | 2024-01-15
# GLD-0001  | SALESFORCE    | SF-A12345  | 0.95       | 2024-01-15
```

**Step 4 — Survivorship:**
For the Golden Record itself, we applied survivorship rules — which source wins for each attribute:
- Date of birth: Oracle ERP wins (most validated)
- Email: Salesforce wins (most recently maintained)
- Address: most recently updated source wins

Implemented via PySpark Window function with source priority ordering.

**Step 5 — Conflict handling:**
Records where sources disagreed beyond a threshold went to a stewardship queue for human review rather than silently picking a winner. This was critical in pharma where a wrong patient match is a regulatory issue, not just a data quality issue."

---

**Q5. How did you achieve 99.9% data uptime in your pipelines?**

**Answer:**

"99.9% uptime means less than 9 hours of downtime per year across all pipelines. We achieved this through five layers of reliability — each covering a different failure mode:

**Layer 1 — Idempotent operations:**
Every pipeline run produces the same result whether run once or five times. We used MERGE (Iceberg's equivalent of Delta MERGE) instead of blind appends. If a job reruns due to failure, duplicate records are not created — the MERGE detects existing records and updates them.

**Layer 2 — Watermark-based incremental loading:**
Each pipeline tracked the maximum `updated_at` timestamp of the last successfully processed batch in a control table. On the next run, we only pull records WHERE updated_at > last_watermark. If a run fails, the watermark isn't updated — so the next run re-processes the failed batch. No data loss, no duplication.

**Layer 3 — Dead-letter queuing:**
Records that failed schema validation or data quality checks didn't fail the entire pipeline. They were routed to a quarantine Iceberg table with the failure reason. The pipeline continued processing valid records. We monitored quarantine queue volume and alerted if it exceeded 0.1% of total records.

**Layer 4 — Retry with exponential backoff:**
Transient failures (Salesforce API rate limits, Oracle connection timeouts) were retried automatically. We implemented exponential backoff: retry after 30s, then 60s, then 120s, then alert. Most transient failures resolve within 2 retries.

**Layer 5 — Row count anomaly detection:**
After every pipeline run, we compared today's record count to a rolling 30-day average. If today's count was < 80% of the average (pipeline might have missed records) or > 200% (possible data duplication), the pipeline would alert before writing to the Curated layer."

---

**Q6. Describe a major incident in your pipeline and how you debugged it.**

**Answer:**

"The most impactful incident I handled was an overnight pipeline that had been running reliably for months suddenly taking 4x longer than expected. Downstream teams reported stale data in dashboards at 9 AM — our pipeline was still running at 8:30 AM when it should finish by 6 AM.

**Step 1 — Identify the stage:**
I opened the Spark monitoring UI (accessible via the orchestration tool's job details). The job history showed 7 of 8 stages completing in expected times. Stage 6 — the Trusted layer write — was taking 3+ hours instead of 30 minutes.

**Step 2 — Identify the task:**
Within Stage 6, the stage DAG showed 200 tasks completing quickly but 2 tasks running for 2+ hours. Classic straggler pattern — data skew.

**Step 3 — Confirm the skew:**
```python
df.groupBy("patient_id_prefix").count().orderBy("count", ascending=False).show(10)
```
Two patient ID prefixes had 50x more records than any others. A batch migration from one hospital system had created a spike in records for those prefixes on that particular day.

**Step 4 — Fix:**
Applied salting to distribute the skewed keys across partitions:
```python
from pyspark.sql.functions import concat, col, lit, floor, rand

# Add a random salt (0-9) to the join key
df_salted = df.withColumn("salt", (rand() * 10).cast("int")) \
              .withColumn("salted_key", concat(col("patient_id"), lit("_"), col("salt")))

# Explode the other side of the join with all salt values
lookup_salted = lookup_df.withColumn("salt", explode(array([lit(i) for i in range(10)]))) \
                          .withColumn("salted_key", concat(col("patient_id"), lit("_"), col("salt")))

result = df_salted.join(lookup_salted, "salted_key")
```

**Step 5 — Prevent recurrence:**
Added a pre-flight check: before the Trusted layer write, compute the top 10 key distributions. If any single key accounts for > 10% of total records, log a warning and automatically switch to a salted join strategy.

**Result:** Pipeline completed in expected time on next run. We also added this check to the standard pre-processing template for all new pipelines."

---

**Q7. How did you implement incremental loading in your pipelines?**

**Answer:**

"We had multiple incremental patterns depending on the source system's capabilities.

**Pattern 1 — High-water mark (most common):**
For Oracle tables with an `updated_at` timestamp column:
```python
# Read current watermark from control table
last_run = spark.sql("""
    SELECT MAX(watermark_value) 
    FROM control.pipeline_watermarks 
    WHERE pipeline_name = 'oracle_patients'
""").collect()[0][0]

# Pull only new/updated records
df_incremental = spark.read.format("jdbc") \
    .option("dbtable", f"(SELECT * FROM patients WHERE updated_at > '{last_run}') t") \
    ...

# MERGE into Iceberg Trusted table
# (same as Delta MERGE)
spark.sql(f"""
    MERGE INTO trusted.patients t
    USING incremental_source s
    ON t.patient_id = s.patient_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Update watermark only after successful MERGE
spark.sql(f"""
    UPDATE control.pipeline_watermarks
    SET watermark_value = '{current_max_updated_at}', last_run = current_timestamp()
    WHERE pipeline_name = 'oracle_patients'
""")
```

**Pattern 2 — Partition-based incremental:**
For very large tables where `updated_at` wasn't indexed — we partitioned by `extract_date` on the source side. Each run, we only pulled today's date partition. Works perfectly for event-driven data like audit logs.

**Pattern 3 — CDC (Change Data Capture) via Debezium:**
For a few high-velocity tables, we used Oracle LogMiner through Debezium to stream row-level changes (INSERT/UPDATE/DELETE) as events. These events landed in a Kafka topic and were consumed by a Structured Streaming job. The stream applied changes to the Iceberg table using MERGE with a `change_type` column.

**Why idempotency matters here:**
If the watermark update fails (between the MERGE succeeding and the control table update), the next run will re-process the same records. Because we use MERGE (not append), this is safe — duplicates are detected and handled as updates, not new inserts."

---

**Q8. What data quality checks did you implement and how were they enforced?**

**Answer:**

"We implemented data quality as a first-class gate — not an afterthought. Quality checks ran at two boundaries: Raw → Trusted and Trusted → Curated.

**Categories of checks:**

1. **Completeness checks:**
```python
# Null rate per critical column must be below threshold
critical_cols = ["patient_id", "trial_id", "event_date"]
for col_name in critical_cols:
    null_rate = df.filter(col(col_name).isNull()).count() / df.count()
    assert null_rate < 0.01, f"QUALITY FAIL: {col_name} null rate {null_rate:.2%} > 1%"
```

2. **Uniqueness checks:**
```python
# No duplicate primary keys
dup_count = df.groupBy("patient_id").count().filter(col("count") > 1).count()
assert dup_count == 0, f"QUALITY FAIL: {dup_count} duplicate patient_ids found"
```

3. **Referential integrity:**
```python
# Every patient_id in events must exist in the patient master
orphan_events = events_df.join(patient_df, "patient_id", "leftanti")
assert orphan_events.count() == 0, "QUALITY FAIL: Events with no matching patient"
```

4. **Range checks:**
```python
# Age must be between 0 and 120
invalid_ages = df.filter((col("age") < 0) | (col("age") > 120)).count()
assert invalid_ages == 0, f"QUALITY FAIL: {invalid_ages} records with invalid age"
```

5. **Row count consistency:**
```python
# Today's count must be within ±20% of 30-day rolling average
avg_count = get_rolling_average("oracle_patients", days=30)
current_count = df.count()
ratio = current_count / avg_count
assert 0.8 <= ratio <= 1.5, f"QUALITY FAIL: Row count anomaly. Today: {current_count}, Avg: {avg_count}"
```

**How enforcement worked:**
- Quality checks ran as a separate Spark job after each layer's transformation
- Checks were codified in Modak Nabu's rule engine (YAML configuration)
- If a check failed, the pipeline stopped promotion — data stayed in the current layer
- A quality report was generated and sent to the data steward team
- We tracked quality scores over time — each dataset had a score (% checks passed) and a minimum threshold to promote (95% for Raw→Trusted, 99% for Trusted→Curated)

This prevented bad data from ever reaching the Curated layer — our 25% reduction in data quality errors came directly from catching issues at the Raw→Trusted boundary."

---

**Q9. How did you handle schema drift when source systems changed their schemas?**

**Answer:**

"Schema drift — when a source system silently changes its schema — was one of our most common operational incidents. We built a three-layer defence:

**Layer 1 — Detect drift at ingestion:**
Before writing any data to the Raw layer, we compared the incoming schema against the registered expected schema stored in a schema registry table:

```python
def detect_schema_drift(df, table_name, spark):
    # Load registered schema
    registered_cols = spark.sql(f"""
        SELECT column_name, data_type, is_required
        FROM control.schema_registry
        WHERE table_name = '{table_name}'
    """).collect()
    
    registered = {r.column_name: r.data_type for r in registered_cols}
    required = {r.column_name for r in registered_cols if r.is_required}
    actual = {f.name: str(f.dataType) for f in df.schema.fields}
    
    new_cols = set(actual.keys()) - set(registered.keys())
    missing_required = required - set(actual.keys())
    type_changes = {c for c in actual if c in registered and actual[c] != registered[c]}
    
    if missing_required:
        raise ValueError(f"SCHEMA DRIFT CRITICAL: Required columns missing: {missing_required}")
    if type_changes:
        raise ValueError(f"SCHEMA DRIFT: Type changes detected: {type_changes}")
    if new_cols:
        logger.warning(f"SCHEMA DRIFT WARNING: New columns detected: {new_cols}")
        # Route to drift review queue but continue processing
        
    return new_cols  # Caller decides whether to add or drop
```

**Layer 2 — Handle gracefully:**
- **New columns added by source:** Log and alert, then use Iceberg's schema evolution to add the column to the Raw table. Old rows have NULL for the new column.
- **Column removed by source:** If required → fail the pipeline and alert. If optional → fill with NULL.
- **Type changes:** If widening (INT→BIGINT) → allow with warning. If narrowing (DOUBLE→STRING) → fail and alert.

**Layer 3 — Enforce data contracts:**
We maintained written Data Contracts with each source system owner — a document specifying the expected schema and a process for communicating breaking changes 2 weeks in advance. This turned schema drift from a surprise into a managed process.

In practice: new columns from source systems happened frequently (about once a month). Missing required columns happened rarely (twice in my tenure, both due to source system bugs). Type changes never happened because we caught them in the contract review process."

---

**Q10. How did you optimise your Iceberg/Spark pipelines for performance?**

**Answer:**

"Performance optimisation was iterative — we profiled first and fixed the actual bottlenecks rather than optimising blindly. Here are the key improvements we made:

**1. JDBC Parallelism (biggest single improvement — 36x faster):**
Changed Oracle reads from single-threaded to 100-way parallel using `numPartitions`. Ingestion time dropped from 3 hours to 5 minutes.

**2. Partition pruning:**
Ensured all Iceberg tables were partitioned on the most commonly filtered column (`event_date` for time-series data, `patient_id_prefix` for patient data). Queries that previously scanned 1000 files now scanned 10-30.

**3. Iceberg compaction (addressed small files problem):**
Streaming micro-batch loads created thousands of small files. Scheduled nightly compaction:
```python
spark.sql("""
    CALL iceberg_catalog.system.rewrite_data_files(
        table => 'trusted.patient_events',
        strategy => 'sort',
        sort_order => 'event_date ASC, patient_id ASC'
    )
""")
```
Query times on the events table dropped by 60% after the first compaction run.

**4. Broadcast joins:**
Patient master table (500MB) was used in joins across multiple large fact tables. Switching to broadcast joins eliminated the shuffle:
```python
from pyspark.sql.functions import broadcast
result = large_events.join(broadcast(patient_master), "patient_id")
```

**5. Predicate pushdown into JDBC:**
Ensured Oracle-side filtering by using subquery syntax in the JDBC `dbtable` option. This pushed the WHERE clause to Oracle, reducing data transferred over the network by 80%.

**6. AQE (Adaptive Query Execution):**
Enabled Spark 3.x AQE which automatically handled shuffle partition skew:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Cumulative result:** End-to-end pipeline execution time reduced by 30% compared to the initial implementation."

---

**Q11. How did you implement CI/CD for your data pipelines?**

**Answer:**

"We had a proper CI/CD setup that prevented the common 'deploy directly to production' antipattern. Here's how it worked:

**Repository structure:**
```
/repo
  /pipelines        ← Nabu pipeline YAML configs
  /transforms       ← PySpark transformation code (Python files)
  /quality_rules    ← Data quality rule definitions (YAML)
  /tests            ← Unit tests with pytest + chispa
  /configs          ← Environment-specific configs (dev/staging/prod)
```

**Branching strategy:**
- `main` → Production environment
- `staging` → Staging/UAT environment
- `dev` → Development environment (shared)
- Feature branches for individual development

**CI pipeline (GitHub Actions):**
On every Pull Request:
1. Run `pytest` — unit tests for PySpark transformations
2. Run linting (`flake8`) and type checking (`mypy`)
3. Validate pipeline YAML configs (schema validation)
4. Run data quality rule syntax validation
5. Check that all new PySpark functions have test coverage

```yaml
# .github/workflows/ci.yml
name: CI
on: [pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install dependencies
        run: pip install pyspark chispa pytest
      - name: Run unit tests
        run: pytest tests/ -v
```

**CD pipeline:**
- Merge to `dev` → Auto-deploy to dev environment (Nabu picks up new configs from GitHub)
- Merge to `staging` → Deploy to staging + run integration tests on sample data
- Manual approval → Merge to `main` → Production deploy

**How Nabu integrates with GitHub:**
Nabu was configured to read pipeline configs directly from the GitHub repository. When a new version is pushed to the relevant branch, Nabu's next scheduled run uses the new config. In ADF terms — this is equivalent to ADF's Git-integrated mode where the main branch deploys to production.

**What I'd do differently in Databricks:**
The same pattern applies — use Databricks Repos to sync notebooks with GitHub, use the Databricks CLI or `dbx` for job config deployment, and `chispa` for DataFrame unit tests. The CI/CD philosophy is identical."

---

**Q12. Tell me about a time you designed a solution that you're most proud of.**

**Answer:**

"The solution I'm most proud of is the Python-based SQL configuration generator that eliminated 15+ hours of manual work per week.

**The problem:**
Every time we onboarded a new table to the eCDP platform, someone had to manually create SQL DDL files, Iceberg table creation scripts, schema registry entries, quality rule YAML files, and Nabu pipeline YAML configs — for three environments (dev, staging, prod). With 30+ tables in the migration, this was consuming entire sprint cycles and causing configuration drift between environments.

**The insight:**
Every piece of configuration was a transformation of the same core information — table name, column list, data types, partition columns, quality thresholds. Everything else was templated boilerplate.

**The solution:**
I built a Python code generator:

```python
# Input: simple YAML file
# table_config.yaml
tables:
  - name: patient_events
    schema: clinical
    source_system: oracle
    columns:
      - name: patient_id     type: string   required: true   pii: false
      - name: event_date     type: date     required: true   pii: false
      - name: event_type     type: string   required: true   pii: false
      - name: dosage_mg      type: double   required: false  pii: false
    partition_by: [event_date]
    cluster_by: [patient_id]
    quality:
      null_threshold: 0.01
      row_count_variance: 0.20
    watermark_column: updated_at
```

The generator reads this YAML and produces:
1. Iceberg `CREATE TABLE` DDL for all three environments
2. Schema registry `INSERT` statement
3. Quality rule YAML for Nabu
4. Pipeline configuration YAML for Nabu
5. PySpark ingestion code template with correct partitioning and watermark
6. Unit test boilerplate

Used Jinja2 for templating and Pydantic for YAML validation:
```python
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, validator

class ColumnConfig(BaseModel):
    name: str
    type: str
    required: bool = True
    pii: bool = False

class TableConfig(BaseModel):
    name: str
    columns: list[ColumnConfig]
    partition_by: list[str]
    
    @validator('partition_by')
    def partition_cols_must_exist(cls, v, values):
        col_names = [c.name for c in values.get('columns', [])]
        for p in v:
            if p not in col_names:
                raise ValueError(f'Partition column {p} not in column list')
        return v
```

**Impact:**
- 3-4 hours per table setup → 5-minute YAML edit + one command
- Zero configuration drift between environments (all generated from the same template)
- Onboarding a new table became something a junior engineer could do without senior oversight
- 15+ hours recovered per week across the team
- Awarded 'Owning It' for this initiative and the production delivery under deadline pressure

**In Databricks terms:**
This same pattern applies — the generator would produce Databricks notebook templates, Unity Catalog table DDL, and Databricks job YAML configurations instead of Nabu configurations. The generator logic is identical."

---

**Q13. How did you handle sensitive/PII data in your pharmaceutical pipelines?**

**Answer:**

"PII handling in pharma is regulated by HIPAA and GDPR — not just a best practice. We had multiple layers of protection:

**1. PII tagging at source:**
Every column in our schema registry was tagged with a `pii` flag (true/false). This tag drove all downstream controls.

**2. Column-level masking in the Trusted layer:**
Non-authorised users accessing the Trusted layer saw masked values:
- Names: First name initial + last name (J. Smith)
- DOB: Only the year (1985 instead of 1985-03-15)
- National IDs: Last 4 digits only (XXX-XX-1234)
- Addresses: City and state only (no street address)

We implemented this as Iceberg view-level masking — analysts querying the data saw masked values automatically based on their access group.

**3. Encryption in transit and at rest:**
- All data on AWS S3 encrypted using SSE-KMS (AES-256)
- All JDBC connections used TLS 1.2
- Salesforce API connections over HTTPS

**4. Access control:**
- Production Iceberg tables: only the pipeline service account had write access
- Analysts: read-only access to Curated layer views (masked)
- Data stewards: read access to Trusted layer for quality review
- No human ever had direct access to Raw layer

**5. Audit logging:**
Every query against the platform was logged — who, when, which table, which columns. These logs were retained for 7 years per FDA 21 CFR Part 11 requirements.

**In Databricks terms:**
This maps directly to Unity Catalog features — column masking functions, row-level filters, access control via groups, and audit logs to Azure Monitor/Delta Lake. My implementation used the same principles — I'd map it to Unity Catalog syntax in a Databricks environment."

---

**Q14. Your interviewer says: 'We use Databricks and Delta Lake. You've used different tools. Why should we hire you?'**

**Answer (how to respond):**

"I appreciate the directness. Let me be specific about what transfers directly and what I'd need a short ramp-up on.

**What transfers directly:**
- Apache Spark knowledge is 100% transferable. Whether my code runs on Databricks or on a Nabu-managed cluster, it's the same PySpark API, same Spark internals, same optimization techniques.
- Data Lakehouse concepts — Medallion Architecture, ACID transactions, time travel, schema evolution, incremental loading, MERGE operations — these are architectural patterns, not tool-specific knowledge. I've implemented them end-to-end.
- Data quality engineering, PII handling, regulatory compliance (FDA 21 CFR Part 11), CI/CD for data pipelines — all directly transferable.
- Iceberg is actually a superset of what Delta Lake offers. I understand open table format internals deeply — snapshot models, manifest files, compaction, Copy-on-Write vs Merge-on-Read. Delta Lake took me an afternoon to map from my Iceberg knowledge.

**What I'd ramp up on (honest about gaps):**
- ADF-specific UI and activity configuration — I've read the documentation but haven't built pipelines in the ADF UI. I'd need a week of hands-on time to be productive.
- Unity Catalog specifics — I know the concepts (column masking, row filters, lineage) from our governance implementation, but not the exact Unity Catalog syntax. This is a few days of practice.
- Databricks-specific features like Photon engine, serverless compute, and Workflows UI — these I'd learn on the job.

But here's what matters: in my project I built and operated a platform processing 10M+ records daily with 99.9% uptime, in a regulated pharmaceutical environment where data errors have serious consequences. The tool I used for orchestration is different from ADF — the orchestration problems I solved are identical. Give me a week in your environment and I'll be productive. Give me a month and I'll be a strong contributor."

---

**Q15. What would you do differently if you were starting the eCDP project from scratch today?**

**Answer:**

"Three things I'd change:

**1. Start with Liquid Clustering instead of manual partitioning strategy:**
Our initial Iceberg table design used hash partitioning on patient_id. Six months in, our query patterns evolved — analysts started filtering by `trial_site` and `event_type` more than `patient_id`. Repartitioning an existing Iceberg table was painful. If I started today, I'd use Liquid Clustering (or Iceberg's equivalent clustering approach) from day one — it's flexible and changeable without rewriting data.

**2. Implement data contracts from the beginning:**
We added formal data contracts with source system owners six months into the project — after two painful schema drift incidents. I'd negotiate and document contracts before writing a single line of ingestion code. The contract defines exactly what the source system commits to providing, and any breaking change requires a two-week notice. This single change would have prevented our most stressful production incidents.

**3. Use streaming where we used batch:**
We built the Oracle ingestion as nightly batch because that's what the requirements said. But the downstream teams actually needed near-real-time data for some dashboards — they worked around our batch limitation by querying the source system directly (defeating the purpose of the platform). With what I know now, I'd build incremental streaming ingestion using Oracle LogMiner → Kafka → Spark Structured Streaming for the high-priority tables, and keep batch only for the historical/analytical tables where latency doesn't matter.

This is also where I'd use Databricks' structured streaming capabilities and Delta Lake's `foreachBatch` + MERGE pattern — well-suited for this use case."

---

## PART 4 — ADF & Databricks Quick Study Cards

---

### ADF Cheat Sheet (Learn These Cold)

```
Linked Service    → Connection string to a data store (like a JDBC connection string)
Dataset           → Definition of data within a Linked Service (like a table definition)
Pipeline          → Container of activities (like a DAG in Nabu/Airflow)
Activity          → One step in a pipeline (Copy, ForEach, If Condition, Web, Notebook)
Integration Runtime → Compute that runs activities (Azure IR for cloud, SHIR for on-prem)
Trigger           → What starts a pipeline (Schedule, Event, Tumbling Window, Manual)
Dataflow          → Visual drag-drop ETL transformation (uses Spark under the hood)
```

### Databricks Cheat Sheet (Learn These Cold)

```
Workspace         → Your Databricks environment
Cluster           → Spark cluster (All-purpose vs Job cluster)
Notebook          → Where you write PySpark/SQL/Scala code
Repos             → Git integration for notebooks
Workflow/Job      → Scheduled notebook/script execution (like Nabu job)
Delta Lake        → Iceberg equivalent — ACID table format
Unity Catalog     → Governance layer (catalog.schema.table)
SQL Warehouse     → Compute for SQL-only queries (no cluster needed)
DBFS              → Databricks File System (don't use in production — use ADLS)
dbutils           → Utility library for Databricks (dbutils.fs, dbutils.secrets)
```

### Must-Know ADF Code Patterns

```python
# Equivalent to your Nabu metadata-driven pattern:
# 1. Lookup Activity reads config table
# 2. ForEach loops over results
# 3. Copy Activity uses @{item().table_name} as parameter

# Dynamic expression in ADF pipeline:
# Source table: @{item().schema}.@{item().table_name}
# Sink path:    @{concat('/mnt/raw/', item().table_name, '/', formatDateTime(utcnow(), 'yyyy-MM-dd'))}
```

### Must-Know Databricks Code Patterns

```python
# Read from ADLS Gen2 (mounted)
df = spark.read.parquet("/mnt/raw/employees/")

# Read from ADLS Gen2 (direct, no mount — modern approach)
df = spark.read.parquet("abfss://container@storage.dfs.core.windows.net/employees/")

# Get secrets from Databricks Secret Scope
db_password = dbutils.secrets.get(scope="my-scope", key="db-password")

# Create Delta table
df.write.format("delta").partitionBy("dept").save("/mnt/delta/employees")

# MERGE in Delta
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/mnt/delta/employees")
dt.alias("t").merge(source_df.alias("s"), "t.id = s.id") \
  .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Unity Catalog table
spark.sql("CREATE TABLE main.silver.employees USING DELTA CLUSTER BY (dept, salary)")
```

---

*End of Interview Preparation Guide*
*Mayuresh Patil | Data Engineer | eCDP & FinOps | Cognizant Technology Solutions*
