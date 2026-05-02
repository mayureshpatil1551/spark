# Mayuresh Patil — Resume-Based Interview Q&A
**50 Questions with Detailed Answers**  
Domains: PySpark & Spark · Apache Iceberg · Azure & Databricks · ETL & Architecture · MDM & Golden Record · Data Governance · Behavioral & Scenario

---

## 🔵 PySpark & Apache Spark

---

### Q1 (Basic) — What is the difference between a transformation and an action in Apache Spark?

**Transformations** are lazy operations that define a new RDD/DataFrame but don't execute immediately (e.g. `filter()`, `map()`, `select()`, `groupBy()`). Spark builds a **DAG** (Directed Acyclic Graph) of transformations.

**Actions** trigger actual computation and return results to the driver or write data (e.g. `collect()`, `count()`, `show()`, `write()`).

This lazy evaluation allows Spark to optimize the entire execution plan before running anything — it can combine multiple operations, push down filters, and avoid unnecessary shuffles.

---

### Q2 (Basic) — What is a shuffle in Spark and why is it expensive?

A **shuffle** occurs when data needs to be redistributed across partitions — typically during operations like `groupBy()`, `join()`, `orderBy()`, or `distinct()`.

**Why it's expensive:**
- Data must be serialized, written to disk, transferred over the network, and deserialized
- It breaks the pipeline — Spark can't execute the next stage until the shuffle is complete
- Produces temporary files that consume disk I/O

**In your work:** You reduced execution time by 30% by optimizing Spark partitioning and shuffle operations — this would involve techniques like reducing shuffle width, using `broadcast joins`, or repartitioning data to co-locate records before joining.

---

### Q3 (Basic) — What is the difference between `repartition()` and `coalesce()`?

Both change the number of partitions, but differ in how they do it:

**`repartition(n)`** — always performs a full shuffle. Can increase or decrease partitions. Produces evenly distributed partitions. Use when you need more partitions or balanced data.

**`coalesce(n)`** — avoids a full shuffle by combining existing partitions on the same node. Can only reduce partitions. May produce uneven partitions but is much faster.

**Rule of thumb:** Use `coalesce` when shrinking (e.g., before writing to fewer output files), use `repartition` when you need even distribution or are increasing partitions.

---

### Q4 (Medium) — You process 10M+ records daily from Oracle and Salesforce. How did you tune Spark for this volume?

Based on the ingestion frameworks you built, key tuning strategies include:

**1. Partitioning strategy** — partition data on a high-cardinality column (e.g. date, region) so Spark can process partitions in parallel without data skew.

**2. JDBC parallelism** — when reading from Oracle via JDBC, use the `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` options to read in parallel rather than a single-threaded sequential pull:

this is only where col is numeric

```python
spark.read.jdbc(url, table, column="id", lowerBound=1, upperBound=10000000, numPartitions=200, properties=props)
```

**3. Broadcast joins** — if joining large Oracle data with small lookup tables, broadcast the small table to avoid shuffles.

**4. Persist intermediate results** — cache DataFrames that are reused multiple times with `.cache()` or `.persist(StorageLevel.MEMORY_AND_DISK)`.

**5. Adaptive Query Execution (AQE)** — enabled by default in Spark 3.x, it dynamically adjusts partition count after shuffles and handles skewed joins automatically.

---

### Q5 (Medium) — Explain the Medallion Architecture (Raw, Trusted, Curated). How did you implement it?

The Medallion Architecture is a data design pattern with three progressive quality layers:

**Raw (Bronze)** — data ingested as-is from source systems. No transformations, preserves original schema and format. Acts as an immutable audit log. In your case: data from Oracle JDBC and Salesforce APIs lands here.

**Trusted (Silver)** — cleaned, validated, and conformed data. Deduplication, null handling, type casting, schema enforcement. This is where your Golden Record XREF logic runs — resolving identities across sources.

**Curated (Gold)** — business-ready aggregates and domain models. Joined, aggregated, and formatted for downstream consumers like analytics dashboards or Salesforce sync.

**Key benefit:** Each layer is independently queryable. If a downstream bug is found, you can reprocess from Raw without re-ingesting from the source.

---

### Q6 (Advanced) — How does Spark's Catalyst optimizer work, and how can you influence it?

The **Catalyst optimizer** transforms your logical query plan into an optimized physical execution plan in four phases:

1. **Analysis** — resolves column names, types, functions against the catalog.
2. **Logical optimization** — applies rule-based optimizations: predicate pushdown, column pruning, constant folding, Boolean simplification.
3. **Physical planning** — generates multiple physical plans and picks the cheapest using a cost model (considers row counts, join strategies).
4. **Code generation** — generates bytecode (via Tungsten's whole-stage code gen) that executes as a tight Java loop.

**How to influence it:**
- Use DataFrames/SQL (not RDDs) — Catalyst only optimizes these
- Collect table statistics: `ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS col1, col2` — improves cost-based join selection
- Use partition pruning — filter on partition columns so Catalyst skips irrelevant files
- Avoid UDFs where possible — they're black boxes to Catalyst and disable predicate pushdown

---

### Q7 (Advanced) — What is data skew and how do you handle it in PySpark?

**Data skew** occurs when some partitions have significantly more data than others, causing a few tasks to run much longer than the rest (the straggler problem).

**Detection:** Look at the Spark UI — if most tasks complete in 2s but a few take 200s, you have skew. Check with:
```python
df.groupBy(key).count().orderBy(desc("count")).show()
```

**Solutions:**
- **Salting** — append a random number (0–N) to the skewed join key, explode the right table N times with all salt values, then join on the salted key
- **AQE skew join handling** — Spark 3.x automatically splits skewed partitions (enable with `spark.sql.adaptive.skewJoin.enabled=true`)
- **Broadcast join** — if one side is small enough, broadcast it entirely to avoid the shuffle
- **Custom partitioning** — repartition on a composite key that distributes more evenly

---

### Q8 (Medium) — What is the difference between narrow and wide transformations?

**Narrow transformations** — each output partition depends on at most one input partition. No shuffle required. Examples: `map()`, `filter()`, `select()`, `withColumn()`. All execute within the same stage.

**Wide transformations** — each output partition may depend on multiple input partitions. Requires a shuffle. Examples: `groupBy()`, `join()`, `orderBy()`, `distinct()`. Each wide transformation creates a new stage boundary.

**Why it matters:** Minimizing wide transformations (and therefore stages and shuffles) is one of the primary Spark optimization strategies. In your pipeline refactoring work, restructuring SQL into modular PySpark pipelines would have involved identifying and reducing unnecessary wide operations.

---

## 🟢 Apache Iceberg

---

### Q9 (Basic) — What is Apache Iceberg and why did you choose it over a traditional data lake format?

**Apache Iceberg** is an open table format for huge analytic datasets. It sits on top of file storage (S3, ADLS) and provides table-level abstractions that traditional data lakes lack.

**Key advantages that likely drove your pharmaceutical migration:**
- **ACID transactions** — safe concurrent reads and writes without data corruption
- **Time travel** — query any historical snapshot: `SELECT * FROM table TIMESTAMP AS OF '2024-01-01'` — critical for audit trails in pharma
- **Schema evolution** — add, rename, or drop columns without rewriting the entire dataset
- **Partition evolution** — change partitioning strategy without migrating data
- **Hidden partitioning** — users don't need to know the partition layout to get partition pruning benefits

Compared to bare Parquet/ORC on S3, Iceberg adds a metadata layer (snapshots, manifests, manifest lists) that tracks exactly which files belong to each table version.

---

### Q10 (Basic) — Explain Iceberg's snapshot model. What is a manifest file and a manifest list?

Iceberg uses a three-level metadata hierarchy:

**Snapshot** — represents the state of a table at a point in time. Every write (insert, delete, update) creates a new snapshot. Contains a pointer to a manifest list.

**Manifest list** — a file that lists all manifest files belonging to a snapshot. Contains partition-level statistics (row counts, min/max values) used for pruning without opening manifest files.

**Manifest file** — lists the actual data files (Parquet/ORC/Avro) that make up the table. Contains column-level statistics for each file.

**Query flow:** Reader opens snapshot → reads manifest list (prunes partitions) → reads relevant manifests (prunes files using column stats) → reads only the needed data files.

This metadata-driven approach enables **O(1) table operations** like snapshot isolation and time travel without scanning the entire storage directory.

---

### Q11 (Medium) — How does time travel work in Iceberg and how did it help with audit trails in your pharmaceutical project?

**Time travel** in Iceberg works by querying a specific historical snapshot. Every write creates an immutable snapshot with a unique `snapshot_id` and timestamp.

**Syntax options:**
```sql
-- By timestamp
SELECT * FROM pharma_db.patients TIMESTAMP AS OF '2024-06-01T00:00:00'

-- By snapshot ID
SELECT * FROM pharma_db.patients VERSION AS OF 8574632891234
```

```python
# In PySpark
df = spark.read.option("as-of-timestamp", "1717200000000").table("pharma_db.patients")
```

**Pharmaceutical audit use case:** Regulators (FDA, EMA) often require proving what data looked like at a specific point in time — e.g., which patient records existed when a clinical trial was submitted. With time travel, you can reconstruct the exact table state without maintaining separate archive tables, eliminating storage duplication and the risk of archive drift.

---

### Q12 (Medium) — What is compaction in Iceberg and why is it necessary?

**Compaction** (also called rewriting data files) merges many small files into fewer larger ones. This addresses the "small files problem" that accumulates over time from frequent streaming or micro-batch writes.

**Why it matters:**
- Each small file requires a separate metadata entry, S3 API call, and JVM overhead to open
- 10,000 files of 1MB each performs far worse than 100 files of 100MB each

**Iceberg compaction in Spark:**
```python
from pyiceberg.expressions import AlwaysTrue
table.rewrite_data_files()
```

Or via Spark SQL:
```sql
CALL catalog.system.rewrite_data_files('db.table')
```

**Best practice:** Schedule compaction as a regular maintenance job (daily or weekly) separate from your ETL pipeline. Also run `expire_snapshots` to clean up old snapshot metadata and `remove_orphan_files` to delete files no longer referenced by any snapshot.

---

### Q13 (Advanced) — How does Iceberg handle schema evolution and partition evolution differently from Hive?

**Hive's limitations:**
- Schema changes often require rewriting the entire dataset or are simply unsupported
- Partition layout is fixed — changing it requires creating a new table and migrating all data
- No column-level tracking — renaming a column breaks queries

**Iceberg schema evolution:** Each column has a unique `field_id` independent of its name or position. Operations like add, rename, drop, reorder, and widen types are tracked in the schema metadata — existing files don't need to be rewritten. Old files are read with their schema; new files with the updated schema.

**Iceberg partition evolution:** Partition specs are versioned. New data written after a partition change uses the new spec; old data retains the old spec. Iceberg's query planner handles both simultaneously during scans — no migration required.

**Example:** If your pharma dataset originally partitioned by `month` but you later needed `day` granularity, Iceberg lets you add a day partition spec. New ingestions use day partitioning; historical data stays as-is.

---

### Q14 (Advanced) — What are the different Iceberg write modes (Copy-on-Write vs Merge-on-Read) and when would you use each?

**Copy-on-Write (CoW):**
- On every update/delete, affected data files are rewritten entirely with the changes applied
- Reads are fast — no merging needed at read time
- Writes are slow and expensive for frequent small updates
- Best for: batch workloads with infrequent updates and frequent reads

**Merge-on-Read (MoR):**
- Updates/deletes write small delta files (delete files + position/equality deletes)
- Writes are fast — only write the changed rows
- Reads are slower — must merge base files with delete files at query time
- Best for: streaming ingestion, CDC pipelines, frequent upserts

**In your pharmaceutical migration context:** CoW would be appropriate for the curated layer where reads dominate. MoR would suit the raw/trusted layers receiving frequent incremental loads from Oracle JDBC. Compaction then periodically converts MoR files back to clean base files.

---

## 🟡 Azure & Databricks

---

### Q15 (Basic) — What is Azure Data Factory and how does it differ from Databricks?

**Azure Data Factory (ADF)** is a cloud ETL orchestration service — a managed pipeline scheduler and data movement tool. It excels at:
- Scheduling and triggering workflows (time-based, event-based)
- Copying data between heterogeneous sources (SQL Server → ADLS, Salesforce → Blob)
- Low-code/no-code pipeline building with 90+ connectors
- Monitoring and alerting on pipeline runs

**Azure Databricks** is a managed Apache Spark platform optimized for heavy computation — large-scale transformations, ML training, streaming, complex joins.

**How they complement each other (your architecture):** ADF orchestrates the overall workflow — triggers Databricks notebooks/jobs at the right time, handles data movement from Oracle/Salesforce into ADLS. Databricks performs the heavy transformation work (PySpark, Medallion layers, Iceberg operations). ADF acts as the control plane; Databricks as the compute engine.

---

### Q16 (Basic) — What is Delta Lake and how does it relate to Apache Iceberg?

Both are open table formats that add ACID transactions, schema enforcement, and time travel to cloud storage. Key differences:

**Delta Lake:**
- Created by Databricks, tightly integrated with Spark/Databricks ecosystem
- Transaction log stored as JSON/Parquet in `_delta_log/`
- Best-in-class Databricks integration (optimized writes, Z-ordering, OPTIMIZE command)
- Open source under Linux Foundation (Delta Lake 3.x)

**Apache Iceberg:**
- Created at Netflix, designed to be engine-agnostic (works with Spark, Flink, Trino, Hive, Presto)
- Three-level metadata (snapshot → manifest list → manifest → data files)
- Better suited for multi-engine environments and cloud-native architectures
- Open governance via Apache Software Foundation

**Your choice:** You used Iceberg for the AWS S3 migration — this makes sense as Iceberg is more portable across engines and cloud providers, while Delta Lake is optimal when staying within the Databricks/Azure ecosystem.

---

### Q17 (Medium) — How does Unity Catalog work in Databricks and what governance benefits does it provide?

**Unity Catalog** is Databricks' unified governance layer for all data and AI assets across workspaces and clouds.

**Three-level namespace:** `catalog.schema.table` — e.g., `pharma_prod.trusted.patient_xref`

**Key governance features:**
- **Centralized access control** — one place to manage permissions across all workspaces instead of per-workspace Hive metastores
- **Column-level security** — mask or restrict access to specific columns (e.g., PII fields) per user/group
- **Row-level filters** — restrict rows based on user attributes
- **Lineage tracking** — automatic column-level lineage showing data flow from source to downstream tables
- **Audit logs** — who accessed what, when
- **Data discovery** — search, tag, and document assets

In pharma environments with strict data governance requirements, Unity Catalog's audit logs and column masking are essential for regulatory compliance.

---

### Q18 (Medium) — What is Azure Synapse Analytics and when would you use it over Databricks?

**Azure Synapse Analytics** is Microsoft's all-in-one analytics service combining data warehousing, big data processing, and data integration.

**Core components:**
- Dedicated SQL Pools (formerly SQL DW) — massively parallel processing (MPP) for structured data warehousing
- Serverless SQL Pool — on-demand SQL queries directly against ADLS files (Parquet, CSV) with no cluster management
- Synapse Spark — managed Spark clusters (similar to Databricks)
- Synapse Pipelines — ADF-like orchestration built in

**When to use Synapse over Databricks:**
- Heavy SQL workloads where teams are SQL-native (not Python/Scala)
- Tight integration with Power BI and Azure ecosystem
- Structured data warehouse use cases with dedicated SQL pools
- When licensing budget favors an all-in-one Microsoft solution

**When Databricks wins:** ML workloads, streaming, Python-heavy transformations, multi-cloud portability.

---

### Q19 (Advanced) — How would you implement a CI/CD pipeline for Databricks notebooks and jobs?

A mature CI/CD setup for Databricks involves several components:

**1. Source control** — all notebooks and job definitions in Git (Azure DevOps or GitHub). Use Databricks Repos to sync.

**2. Development workflow** — developers work in feature branches, commit notebooks as source. Avoid clicking-and-running in production workspaces.

**3. Pipeline stages:**
```
PR → Run unit tests (pytest + chispa for PySpark)
Merge → Deploy to Dev workspace via Databricks CLI / Terraform
QA approval → Deploy to Staging
Manual gate → Deploy to Production
```

**4. Deployment tools:**
- **Databricks CLI** — `databricks jobs create/update`, `databricks workspace import`
- **Databricks Terraform provider** — infrastructure-as-code for clusters, jobs, permissions, secrets
- **dbx** — Databricks deployment tool that manages job configs as YAML

**5. Testing:** Use `chispa` for DataFrame equality assertions, mock external data sources, run against a small fixture dataset in CI.

------------------------------------------------------------------------------------------------------------------------------------------------

01 - 
Version notebooks in GitStore all notebooks and job definitions in GitHub or Azure DevOps; sync with Databricks Repos.

02 - 
Develop in feature branchesWork in isolated branches and commit changes; avoid editing directly in production workspaces.

03 - 
Run CI unit testsOn pull requests, run pytest and chispa tests against fixture datasets to validate logic.

04 - 
Deploy to Dev environmentAfter merge, use Databricks CLI, Terraform, or dbx to deploy notebooks and jobs to the Dev workspace.

05 - 
Promote to StagingWith QA approval, deploy to Staging for integration testing and validation.

06 - 
Gate Production releaseUse manual approval before deploying to Production; ensure jobs, clusters, and secrets are correctly configured.

---

## 🟠 ETL & Architecture

---

### Q20 (Basic) — What is a Data Lakehouse and how does it differ from a data warehouse and a data lake?

**Data Lake** — raw file storage (S3, ADLS) with no schema enforcement. Cheap storage, flexible. But: no ACID, no performance guarantees, schema-on-read can be inconsistent.

**Data Warehouse** — structured, schema-on-write, highly optimized for SQL queries (Snowflake, Redshift, BigQuery). But: expensive, rigid schema, can't handle unstructured data well.

**Data Lakehouse** — combines both. Stores files cheaply on object storage (like a lake) but adds a table format layer (Iceberg, Delta, Hudi) that provides:
- ACID transactions
- Schema enforcement and evolution
- Query performance (metadata-based pruning)
- Support for multiple query engines

Your architecture is a Lakehouse: raw files on AWS S3 + Apache Iceberg table format + Spark compute, giving you warehouse-quality reliability at lake-scale economics.

---

### Q21 (Basic) — What is JDBC and how did you use it to ingest from Oracle?

**JDBC (Java Database Connectivity)** is a standard Java API for connecting to relational databases. Spark includes JDBC connectors that let you read/write from any JDBC-compatible database.

**Basic Oracle read:**
```python
df = spark.read \
  .format("jdbc") \
  .option("url", "jdbc:oracle:thin:@host:1521/service") \
  .option("dbtable", "schema.patient_data") \
  .option("user", dbutils.secrets.get("scope", "user")) \
  .option("password", dbutils.secrets.get("scope", "pwd")) \
  .option("driver", "oracle.jdbc.driver.OracleDriver") \
  .load()
```

**Parallel ingestion (critical for 10M+ records):**
```python
  .option("partitionColumn", "patient_id") \
  .option("lowerBound", "1") \
  .option("upperBound", "10000000") \
  .option("numPartitions", "200")
```

This spawns 200 parallel JDBC connections, each reading a range of patient_ids simultaneously — transforming a sequential 3-hour pull into a ~5-minute parallel read.

---

### Q22 (Medium) — Explain how you achieved 99.9% data uptime in your ingestion pipelines.

Achieving 99.9% uptime means less than ~9 hours of downtime per year. In a data pipeline context, this involves multiple reliability layers:

**1. Idempotent ingestion** — each pipeline run produces the same result whether run once or multiple times. Use Iceberg's `MERGE INTO` or Delta's `MERGE` to upsert rather than blind append, so re-runs don't duplicate records.

**2. Checkpointing** — for streaming/incremental loads, track watermarks (last processed timestamp or ID) in a control table. On failure, resume from the last checkpoint rather than reprocessing everything.

**3. Retry logic with backoff** — Salesforce API calls may transiently fail; implement exponential backoff with a maximum retry count.

**4. Dead-letter queues** — malformed records that fail validation are routed to a quarantine table rather than failing the entire pipeline.

**5. Monitoring and alerting** — ADF pipeline failure alerts, Databricks job notification emails, and data quality assertions that trigger alerts when row counts deviate significantly from baseline.

---

### Q23 (Medium) — What is incremental loading and how do you implement it in PySpark?

**Incremental loading** processes only new or changed data since the last run, rather than reloading the full dataset each time.

**Common patterns:**

**1. Watermark approach** — track the max timestamp of the last processed record:
```python
last_run = spark.sql(
    "SELECT MAX(processed_at) FROM control.watermarks WHERE table='patients'"
).collect()[0][0]

new_data = spark.read.jdbc(...).filter(f"updated_at > '{last_run}'")
```

**2. CDC (Change Data Capture)** — source database emits change logs (Oracle LogMiner, Debezium). Capture inserts, updates, deletes as events.

**3. Partition-based incremental** — source data is partitioned by date; only read today's partition each run.

**4. Upsert with MERGE:**
```sql
MERGE INTO trusted.patients t
USING new_data s ON t.patient_id = s.patient_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

With 10M+ daily records from Oracle, full loads would be prohibitively expensive. Incremental loads with watermarking and MERGE operations are the scalable approach.

---

### Q24 (Advanced) — How would you design a data pipeline for exactly-once semantics?

**Exactly-once** means each record is processed and written precisely once — no duplicates, no losses — even in the presence of failures.

**The challenge:** Failures happen after processing but before acknowledging completion, causing the system to retry and re-process records.

**Techniques for batch pipelines:**
- **Idempotent writes + deduplication keys** — use Iceberg/Delta MERGE with a unique business key. Even if a record is processed twice, the MERGE detects the duplicate and doesn't insert it
- **Two-phase commit pattern** — write data to a staging location, then atomically "commit" by updating the metadata (Iceberg snapshot commit is atomic)
- **Transactional control table** — record job run ID and status; skip runs already marked as complete

**For streaming (Spark Structured Streaming):**
- Enable checkpointing: `.option("checkpointLocation", "s3://bucket/checkpoints/job-name")`
- Use idempotent sinks (Kafka exactly-once, Delta Lake with `foreachBatch` + MERGE)
- Structured Streaming's WAL (write-ahead log) in checkpoints tracks offsets — on restart, only unprocessed offsets are replayed

---

### Q25 (Advanced) — What SQL optimizations did you apply to improve pipeline performance by 30%?

The 30% improvement on end-to-end execution time likely came from a combination of:

**1. Partition pruning** — ensure queries filter on partition columns (e.g., `WHERE processing_date = '2024-01-01'`). Iceberg/Delta skip irrelevant files entirely without scanning them.

**2. Predicate pushdown** — push filters as close to the source as possible. In JDBC reads:
```python
.option("pushDownPredicate", "true")
```

**3. Column pruning** — select only needed columns rather than `SELECT *`. Reduces Parquet bytes read significantly for wide tables.

**4. Broadcast joins** — for joins where one side < 10MB:
```python
from pyspark.sql.functions import broadcast
df = large_df.join(broadcast(small_df), "key")
```

**5. Z-ordering** (Delta) / **sort-merge** (Iceberg) — co-locate frequently filtered columns within files, dramatically reducing files scanned per query.

**6. Eliminating unnecessary shuffles** — reorganize query logic to minimize `groupBy`/`join` stages, or use window functions instead of self-joins.

---

## 🔴 MDM & Golden Record

---

### Q26 (Basic) — What is MDM (Master Data Management) and what problem does it solve?

**Master Data Management (MDM)** is the discipline of creating a single, authoritative, consistent view of key business entities (customers, products, locations) across an organization's systems.

**The problem it solves:** Large enterprises accumulate data in dozens of systems — CRM (Salesforce), ERP (SAP), clinical trial databases, billing systems. The same customer might be:
- "John Smith" in Salesforce
- "J. Smith" in Oracle ERP
- "JOHN SMITH" in the clinical database

These are the same person but appear as three separate records. Business decisions made on fragmented data lead to duplicate mailings, incorrect reporting, and regulatory compliance failures.

Your Golden Record solution creates one master record per entity by detecting and linking these duplicates, then designating one as the "golden" authoritative record that all downstream systems use.

---

### Q27 (Medium) — Explain how your Golden Record XREF (cross-reference) system works.

A **Cross-Reference (XREF)** system maintains a mapping table that links the same entity's identifiers across different source systems.

**Typical XREF table structure:**
```
golden_id   | source_system | source_id  | confidence
GLD-0001    | SALESFORCE    | SF-A12345  | 1.00
GLD-0001    | ORACLE_ERP    | ORA-78901  | 0.95
GLD-0001    | CLINICAL_DB   | CLN-00234  | 0.92
```

**How identity resolution works:**
- **Blocking** — group records by rough keys (same last name + DOB) to avoid comparing all pairs (O(n²) problem)
- **Matching** — apply matching rules within each block: exact match on national ID, fuzzy match on name (Jaro-Winkler, Levenshtein), address standardization
- **Scoring** — assign confidence scores based on match strength
- **Merge** — records above threshold are linked under one `golden_id`; survivorship rules determine which source's value wins for each attribute

**Survivorship rules:** e.g., "trust Salesforce for email, trust Oracle for date of birth, prefer most recently updated for address."

---

### Q28 (Medium) — What are survivorship rules in MDM and how do you implement them in PySpark?

**Survivorship rules** determine which value "wins" when the same attribute exists in multiple source systems for the same Golden Record.

**Common survivorship strategies:**
- **Most recent** — take the value from the record with the latest update timestamp
- **Most trusted source** — source priority ranking (e.g., Oracle ERP > Salesforce > Manual entry)
- **Most complete** — prefer non-null over null values
- **Most frequent** — take the value that appears most often across sources

**PySpark implementation example:**
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, coalesce, col

# Source priority: Oracle=1, Salesforce=2, Clinical=3
w = Window.partitionBy("golden_id").orderBy("source_priority", col("updated_at").desc())

df_ranked = df_xref.withColumn("rn", row_number().over(w))

golden = df_ranked.filter(col("rn") == 1).select(
    "golden_id",
    coalesce("email_oracle", "email_sf").alias("email"),       # Oracle preferred
    coalesce("dob_oracle", "dob_clinical").alias("date_of_birth"),
)
```

---

### Q29 (Advanced) — How do you handle the survivorship problem when source systems conflict in pharmaceutical data?

Pharmaceutical data has additional complexity because some attributes carry regulatory weight — the wrong value isn't just a data quality issue, it's a compliance violation.

**Layered approach:**

**1. Regulatory fields** — attributes like patient identifiers, drug codes (NDC), and study IDs should have a single authoritative source defined by policy. No fuzzy survivorship — the designated system wins unconditionally.

**2. Confidence-scored fields** — for attributes like contact info, rank sources by historical accuracy (track how often each source's value matches confirmed ground truth), and apply Bayesian scoring.

**3. Conflict flagging** — when sources disagree beyond a threshold, write a conflict record to a review queue rather than silently picking a winner. Human stewards resolve high-stakes conflicts.

**4. Audit trail** — always preserve which source contributed which value, and the timestamp of the golden record update. With Iceberg time travel, you can reconstruct the exact golden record state at any audit point.

**5. Feedback loops** — when a steward corrects a golden record, that correction is fed back to improve matching confidence scores for future runs.

---

## 🟣 Data Governance

---

### Q30 (Basic) — What is data governance and why is it critical in pharmaceutical environments?

**Data governance** is the set of policies, processes, roles, and standards that ensure data is accurate, consistent, secure, and used appropriately across an organization.

**In pharmaceuticals specifically, governance is non-negotiable because:**
- **FDA 21 CFR Part 11** — requires electronic records to be trustworthy, reliable, and equivalent to paper records. Requires audit trails, access controls, and validation
- **GDPR/HIPAA** — patient data is sensitive PII/PHI; access must be logged and restricted
- **Clinical trial integrity** — data used in drug approval submissions must be reproducible and auditable; a governance failure can invalidate years of research
- **GxP compliance** (Good Manufacturing/Laboratory/Clinical Practice) — requires data integrity controls throughout the data lifecycle

**In your work:** Modak Nabu enforces governance through programmatic validation rules — ensuring data meets defined quality thresholds before being promoted between Medallion layers.

---

### Q31 (Medium) — What is Modak Nabu and what governance capabilities did you use from it?

**Modak Nabu** is an enterprise data governance and quality management platform. It provides a framework for defining, enforcing, and monitoring data quality rules programmatically across data pipelines.

**Key capabilities you leveraged:**
- **Rule-based validation** — define quality rules (completeness, uniqueness, referential integrity, range checks) as code or configuration. Nabu runs these against each dataset layer
- **Profiling** — automatically computes statistics (null rates, cardinality, min/max, frequency distributions) to detect anomalies and drift
- **Data lineage** — tracks how data flows from source through transformations to consumption, enabling impact analysis
- **Quality scoring** — assigns a quality score per dataset, enabling threshold-gated pipeline promotion (e.g., only advance to Curated if quality score > 95%)
- **Audit logging** — records every validation run with results, timestamps, and record-level details for regulatory review

**Impact:** Your 25% reduction in data quality errors came from catching issues at the Raw→Trusted boundary rather than discovering them in production dashboards.

---

### Q32 (Medium) — How do you implement data lineage in a modern data platform?

**Data lineage** tracks the origin, movement, and transformation of data — answering "where did this value come from, and what touched it?"

**Implementation approaches:**

**1. Metadata-level lineage** — tools like Apache Atlas, OpenLineage, or Unity Catalog capture lineage by parsing SQL/Spark execution plans. You instrument your jobs with OpenLineage emitters that send events to a lineage backend:
```python
from openlineage.client import OpenLineageClient
# Databricks natively emits OpenLineage events when enabled
```

**2. Column-level lineage** — finer granularity: "this column was derived by summing columns A and B from table X after joining with table Y." Unity Catalog provides this automatically for SQL operations.

**3. Medallion Architecture as lineage** — your Raw→Trusted→Curated structure is itself a lineage guarantee. If a Curated value is wrong, you always know to look at the Trusted layer, then the Raw layer, then the source system.

**4. Control tables** — custom logging tables that record job run IDs, source files processed, record counts, and transformation parameters for manual lineage reconstruction.

---

### Q33 (Advanced) — How would you implement column-level security for sensitive pharmaceutical data in Databricks?

Column-level security in Databricks Unity Catalog has two mechanisms:

**1. Column masking:**
```sql
CREATE OR REPLACE FUNCTION mask_dob(dob DATE)
RETURNS DATE
RETURN CASE
  WHEN is_account_group_member('clinical_researchers') THEN dob
  ELSE DATE '1900-01-01'
END;

ALTER TABLE pharma.trusted.patients
ALTER COLUMN date_of_birth SET MASK mask_dob;
```
Non-authorized users see `1900-01-01`; researchers see the real DOB.

**2. Row filters:**
```sql
CREATE FUNCTION patient_region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member(CONCAT('region_', region));

ALTER TABLE pharma.trusted.patients
SET ROW FILTER patient_region_filter ON (patient_region);
```

**3. Dynamic views** — for legacy setups without Unity Catalog, create views that use `current_user()` to conditionally mask values.

**4. Audit all access** — enable Unity Catalog audit logs to Delta Lake, then use Databricks SQL to query who accessed sensitive columns and when.

-----
is_account_group_member() is a built‑in security function in Databricks SQL (and similar platforms) that checks whether the current user belongs to a specific account group.
---

## ⚪ Behavioral & Scenario

---

### Q34 (Basic) — Tell me about yourself and your experience as a Data Engineer.

**Structured approach (use the STAR method as a frame):**

Open with your current role and headline: *"I'm a Data Engineer at Cognizant with 3+ years of experience building scalable Data Lakehouse architectures on Azure and AWS."*

Then cover your key technical pillars: *"My core expertise is in PySpark, Apache Iceberg, and SQL optimization. I work primarily in pharmaceutical data engineering — a domain where data quality and auditability aren't optional."*

Highlight your signature achievement: *"One of my most impactful projects was migrating 10TB+ of legacy pharmaceutical data to Apache Iceberg on AWS S3 — which introduced ACID transactions and time-travel capabilities that are critical for regulatory audit trails."*

Close with your direction: *"I'm now focused on expanding into AI-ready data engineering — designing pipelines that serve ML and analytics workloads, not just traditional BI consumers."*

> Keep it under 90 seconds. Don't read from your resume — tell the story.

---

### Q35 (Basic) — Why do you want to move from Cognizant? What are you looking for next?

**Key principles for answering this:**
- Never criticize your current employer
- Frame it as growth-seeking, not escape
- Connect your answer to the target company's strengths

**Sample answer framework:** *"Cognizant gave me a strong foundation — I've led significant projects like the Iceberg migration and the Golden Record MDM system. I'm proud of what I've built there. But I'm at a point where I want to work on problems that push into AI and real-time data engineering, and I want to do that in an environment where data engineering is core to the product rather than a client service delivery model. [Target company]'s investment in [specific tech or domain] is exactly the direction I want to grow."*

> Adapt the closing to the company you're interviewing with. Research what they're building and connect your "what I want next" to what they actually do.

---

### Q36 (Medium) — Describe a time you had to debug a failing production pipeline under pressure. What was your approach?

**Structured answer using STAR (Situation, Task, Action, Result):**

**Situation:** *"One of our critical overnight ingestion pipelines — processing 10M+ records from Oracle for a pharmaceutical client — started failing silently. Downstream analytics teams reported stale data the next morning."*

**Task:** *"I needed to identify the root cause, fix the pipeline, and restore data continuity without losing records or violating data integrity."*

**Action:** *"I started with the Spark UI to identify which stage had failed and what the error was. I found an OOM (out of memory) exception in the JDBC partition read stage — the data volume for that day had spiked 3x due to a source system backfill. I increased the number of JDBC partitions to distribute the load, added a checkpoint to track the last successfully processed batch, and re-triggered the job from the failure point rather than reprocessing the entire dataset."*

**Result:** *"Pipeline was restored within 2 hours. We added automated row-count monitoring afterward to alert before failures rather than after."*

> Use a real incident if possible — specificity makes the answer credible.

---

### Q37 (Medium) — You received the 'Owning It' award. What did you do that earned it?

This is a chance to demonstrate initiative and ownership mindset — not just technical skill.

**Sample answer framework:** *"We had a critical production release on a tight deadline — a regulatory data submission window. Late in the sprint, we discovered a data quality issue in the curated layer that affected 15% of patient records. The formal process would have pushed the release to the next quarter.*

*I took ownership by staying through the weekend to trace the issue through the Medallion layers, identify the root cause (a survivorship rule conflict in the Golden Record system), write and test the fix, and run a full regression on the curated data. I also wrote documentation so the team understood the root cause, not just the fix.*

*The release went out on time. That's what 'owning it' meant — not just completing tasks assigned to me, but treating the outcome as my personal responsibility."*

> The answer should feel specific, not generic. Adapt it to what actually happened.

---

### Q38 (Medium) — How did you eliminate 15+ hours/week of manual setup using Python automation?

**The problem:** *"Every new data pipeline environment required manually creating SQL configuration files — table schemas, partition specs, column mappings, data type definitions. With dozens of tables and multiple environments (dev/staging/prod), this was error-prone and consumed significant engineering time each sprint."*

**The solution:** *"I built a Python code generator that read a YAML configuration file defining each table's schema and wrote out all the SQL DDL and pipeline configuration files dynamically. Engineers just updated the YAML; the generator produced consistent, validated SQL across all environments."*

**Technical detail:** *"Used Python's Jinja2 templating engine to render SQL templates with table-specific variables. Added schema validation using Pydantic models to catch errors in the YAML before generating files."*

**Impact:** *"What previously took 3–4 hours per table setup became a 5-minute YAML edit and a script run. Across our 30+ table migrations, this recovered over 15 hours per week and virtually eliminated configuration drift between environments."*

---

### Q39 (Medium) — How do you ensure data quality in your pipelines? Walk me through your approach.

**In layers — technical, process, and cultural:**

**1. Prevention at ingestion** — enforce schema at the point of entry. Use Iceberg/Delta schema enforcement so malformed records fail loudly at the Raw layer rather than silently corrupting downstream data.

**2. Validation between layers** — at each Medallion transition, run data quality checks:
- Completeness: null rates on critical fields must be below threshold
- Uniqueness: no duplicate records on primary keys
- Referential integrity: foreign keys must resolve to master data
- Range checks: values within expected business bounds
- Row count consistency: within ±10% of yesterday's run

**3. Automated with Modak Nabu** — validation rules are codified and versioned. Failures generate quality reports and block pipeline promotion.

**4. Monitoring** — dashboards showing data freshness, quality scores, and record counts. Alerts trigger on deviations.

**5. Incident loop** — when quality issues reach production, root cause analysis → rule improvement → prevention for next cycle.

---

### Q40 (Advanced) — A stakeholder says your pipeline is too slow and data arrives 3 hours late. How do you diagnose and fix it?

# How Do You Ensure Data Quality in Your Pipelines?
### Interview Answer Guide — Mayuresh Patil | PwC Data Engineering Associate

---

## Opening Statement

> *"I approach data quality in layers, mirroring the Medallion Architecture I've built. Each layer of the pipeline has its own quality gate — so bad data is caught as early as possible and never silently propagates downstream."*

---

## Layer 1 — Schema Validation at Ingestion (Bronze)

The first line of defence is stopping bad data from entering the lake entirely.

In **eCDP**, I used **Delta Schema Enforcement** so any record with unexpected columns or mismatched data types would fail loudly rather than silently corrupt downstream tables. When schema *did* need to evolve (Oracle and Salesforce changed frequently), I used `mergeSchema` in a controlled, reviewed way — never blindly.

```python
# Strict schema enforcement by default — any mismatch raises AnalysisException
df.write \
    .format("delta") \
    .mode("append") \
    .save("/mnt/bronze/clinical_events")

# Controlled schema evolution only when approved
df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/mnt/bronze/clinical_events")
```

---

## Layer 2 — Data Quality Rules at Silver (The Core Layer)

This is where most of the quality work happens. I enforce:

- **Null checks** on critical business keys (`patient_id`, `visit_date`)
- **Referential integrity** — joining against dimension/lookup tables to catch orphan records
- **Deduplication** using window functions before writing to Silver
- **Range / domain validation** — dates can't be in the future, amounts can't be negative

```python
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window

# ── Null Check ──────────────────────────────────────────────────────────────
null_count = df.filter(col("patient_id").isNull()).count()
if null_count > 0:
    raise ValueError(f"Data quality failure: {null_count} null patient_ids found")

# ── Deduplication ────────────────────────────────────────────────────────────
w = Window.partitionBy("patient_id", "visit_date").orderBy(desc("updated_at"))
df_deduped = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .drop("rn")
)

# ── Domain Validation ────────────────────────────────────────────────────────
df_valid = df_deduped.filter(
    (col("visit_date") <= current_date()) &
    (col("amount") >= 0)
)
```

---

## Layer 3 — Delta Constraints (Database-Level Hard Stop)

For critical tables I add **Delta table constraints** so bad data physically cannot be written — even from ad-hoc notebooks or other teams running queries outside the pipeline.

```sql
-- Enforce valid status values
ALTER TABLE silver.patient_records
ADD CONSTRAINT valid_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'PENDING'));

-- Enforce non-null business key
ALTER TABLE silver.patient_records
ADD CONSTRAINT non_null_patient_id CHECK (patient_id IS NOT NULL);
```

> Any write that violates a constraint raises an error and the transaction is rolled back automatically — guaranteed by Delta's ACID compliance.

---

## Layer 4 — Reconciliation After Load

After every pipeline run I do a **row count reconciliation** between source and target. For the **SAP → Snowflake migration** in the FinOps PoC, I also did **checksum validation** on financial amounts to ensure no rounding or truncation occurred during transformation.

```python
source_count = spark.sql("""
    SELECT COUNT(*) FROM bronze.clinical_events
    WHERE load_date = current_date()
""").collect()[0][0]

target_count = spark.sql("""
    SELECT COUNT(*) FROM silver.patient_records
    WHERE load_date = current_date()
""").collect()[0][0]

if source_count != target_count:
    raise Exception(
        f"Row count mismatch: source={source_count}, target={target_count}"
    )

# Checksum validation for financial data
source_sum = df_source.agg({"amount": "sum"}).collect()[0][0]
target_sum = df_target.agg({"amount": "sum"}).collect()[0][0]

if abs(source_sum - target_sum) > 0.01:
    raise Exception(f"Checksum mismatch: source={source_sum}, target={target_sum}")
```

---

## Layer 5 — Observability & DQ Audit Logging

Quality checks alone aren't enough — you need visibility across runs. I log every pipeline run's quality metrics to a **DQ audit table** in Delta Lake.

```python
from datetime import datetime
from pyspark.sql import Row

dq_record = Row(
    pipeline_name="silver_patient_load",
    run_timestamp=datetime.now(),
    source_count=source_count,
    target_count=target_count,
    null_count=null_count,
    status="PASS" if null_count == 0 and source_count == target_count else "FAIL"
)

dq_log = spark.createDataFrame([dq_record])
dq_log.write.format("delta").mode("append").saveAsTable("audit.dq_run_log")
```

This audit table feeds into:
- **Azure Monitor dashboards** for pipeline health visibility
- **ADF failure handlers** that trigger email / Teams alerts on any `FAIL` status
- **RCA investigations** — historical DQ logs make it easy to pinpoint when and where data degraded

---

## Summary Table

| Layer | Where | Technique | Tool |
|---|---|---|---|
| 1 | Bronze (Ingestion) | Schema enforcement & controlled evolution | Delta Lake |
| 2 | Silver (Transform) | Null checks, dedup, domain validation | PySpark |
| 3 | Silver (Table-level) | Hard constraints — ACID-backed | Delta Constraints |
| 4 | Post-load | Row count & checksum reconciliation | PySpark / Spark SQL |
| 5 | Audit | DQ run log + alerting | Delta + Azure Monitor |

---

## Closing Statement for the Interview

> *"So in summary — I don't treat data quality as a single checkpoint. It's a pipeline-wide discipline: enforce schema at entry, apply business rules at Silver, use Delta constraints as a hard stop, reconcile counts after load, and log everything for observability. In eCDP, this approach protected 300+ downstream reports from bad data despite frequent upstream schema changes from Oracle and Salesforce."*

---

*Tailored for Mayuresh Patil | PwC Data Engineering Associate Interview*

---

### Q41 (Advanced) — How would you handle a situation where a source system sends duplicate records?

**1. Detect first:**
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("patient_id", "event_date").orderBy(col("updated_at").desc())
deduped = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
```

**2. Define the deduplication key carefully** — often it's not just the primary key. For event data, it might be `(entity_id, event_type, event_timestamp)`. Consult business stakeholders before assuming.

**3. Upsert into the target table:**
```sql
MERGE INTO trusted.patients t
USING deduped_source s ON t.patient_id = s.patient_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**4. Track and report** — log the duplicate count per run in a metrics table so trends are visible. A sudden spike in duplicates signals a source system issue that needs escalation.

**5. Quarantine true conflicts** — if two records have the same key but conflicting non-time attributes, route to a conflict table for stewardship review rather than silently picking one.

---

### Q42 (Medium) — How do you communicate technical decisions to non-technical stakeholders?

**Core principle:** Translate from "how it works" to "what it means for you."

**Framework:**
- **Outcome first** — lead with business impact, not technical mechanism. *"This change will make your reports update 3 hours earlier each morning"* lands better than *"we switched from full loads to incremental ETL with watermarking."*
- **Use analogies** — *"Iceberg's time travel is like Google Docs version history — you can see exactly what the data looked like on any date."*
- **Risk in plain language** — *"If we don't fix the compaction issue, query times will double within 3 months as files accumulate."*
- **Decision framing** — present options with trade-offs, not just recommendations: *"Option A delivers in 2 weeks but needs a maintenance window. Option B takes 6 weeks but is zero-downtime."*

**Written communication:** Use one-pagers or decision docs for significant changes. Header: what decision needs to be made. Body: options and trade-offs. Footer: your recommendation and why.

---

### Q43 (Basic) — What is your experience with Salesforce API integration?

Salesforce exposes several APIs: REST API (most flexible), Bulk API 2.0 (designed for large-scale data loads), SOAP API (legacy), and Streaming API (real-time events via PushTopic or Change Data Capture).

**In your ingestion pipelines:** You likely used the Salesforce Bulk API 2.0 to pull records into your Data Lakehouse. The Bulk API 2.0 is specifically optimized for reading/writing millions of records — it creates asynchronous batch jobs that Salesforce processes server-side.

**For outbound sync** (your Lakehouse → Salesforce use case): You use the Salesforce REST API's `upsert` endpoint with a composite request to update records in batches, using the Salesforce External ID field to match Golden Record IDs to Salesforce records.

**Key considerations:**
- **Rate limits** — Salesforce enforces daily API call limits; batch operations minimize calls
- **Authentication** — use OAuth 2.0 JWT Bearer flow for server-to-server (no user interaction required)
- **Error handling** — partial success is possible; Salesforce returns per-record success/failure in the response

---

### Q44 (Medium) — How do you handle schema drift — when a source system changes its schema unexpectedly?

Schema drift is one of the most common operational headaches in data engineering.

**1. Detection:**
```python
expected_cols = {"patient_id", "dob", "name", "region"}
actual_cols = set(df.columns)
new_cols = actual_cols - expected_cols
removed_cols = expected_cols - actual_cols

if removed_cols:
    raise ValueError(f"Required columns missing: {removed_cols}")
if new_cols:
    logger.warning(f"Unexpected new columns: {new_cols}")
```

**2. Safe handling:**
- **New columns** — typically safe to add to the schema (Iceberg schema evolution handles this). Log and notify.
- **Removed required columns** — fail the pipeline and alert. Don't silently write null-filled records.
- **Type changes** — widening (int→long) is safe; narrowing or incompatible type changes fail the pipeline.

**3. Schema registry** — tools like AWS Glue Schema Registry or Confluent Schema Registry enforce schema contracts for streaming data.

**4. Stakeholder communication** — maintain a data contract with source system owners. Breaking changes should come with advance notice and a deprecation period.

---

### Q45 (Advanced) — You need to migrate a 10TB legacy dataset with zero downtime. Walk me through your migration strategy.

**Phase 1 — Parallel run (weeks 1–2)**
- Stand up the new Iceberg system alongside the legacy system
- Begin historical backfill in the background — read legacy data in chunks and write to Iceberg
- Legacy system remains the system of record; no consumer impact

**Phase 2 — Incremental catch-up (weeks 3–4)**
- Once historical data is loaded, switch to incremental mode: process only new/changed records from the legacy system into Iceberg
- Run both systems in parallel and compare outputs — validate row counts, spot-check values, run reconciliation queries

**Phase 3 — Cutover preparation**
- Document the cutover procedure with rollback steps
- Test the cutover on a staging environment
- Communicate the switchover plan to all downstream consumers

**Phase 4 — Cutover (maintenance window)**
- Freeze writes to legacy system
- Process final delta into Iceberg
- Switch read pointer (update connection strings / ADF datasets) to Iceberg
- Validate — run smoke tests on downstream queries
- Keep legacy system in read-only mode for 2 weeks as a rollback safety net

**Phase 5 — Decommission** — after 2–4 weeks of stable operation, archive and decommission the legacy system.

---

### Q46 (Medium) — What is your approach to writing maintainable PySpark code?

**1. Modular pipeline design** — break pipelines into small, single-responsibility functions. Each function does one thing: read, transform, validate, or write. Avoid monolithic notebooks that do everything.

**2. Configuration over hardcoding** — table names, paths, thresholds, and connection strings live in config files (YAML or Databricks widgets), not embedded in code.

**3. Type annotations and docstrings:**
```python
def apply_survivorship(df: DataFrame, priority_sources: list[str]) -> DataFrame:
    """Apply source priority survivorship rules to generate golden records.
    
    Args:
        df: Input DataFrame with source_system and updated_at columns
        priority_sources: Ordered list of source systems (highest priority first)
    Returns:
        DataFrame with one golden record per entity
    """
```

**4. Unit tests** — use `chispa` to assert DataFrame equality in pytest. Mock external dependencies (JDBC, APIs).

**5. Meaningful naming** — `df_raw_patients_oracle` over `df1`. Name DataFrames by what they represent at that stage.

**6. Idempotency** — every pipeline should be safely re-runnable. No "append only" operations without deduplication guards.

---

### Q47 (Advanced) — How would you design a real-time streaming pipeline on Azure for pharmaceutical data?

**Architecture:**
```
Source (IoT / App events)
    → Azure Event Hubs (ingestion buffer, Kafka-compatible)
    → Databricks Structured Streaming (real-time processing)
    → Delta Lake on ADLS (streaming sink with ACID guarantees)
    → Downstream: Power BI Streaming, API consumers, Alerts
```

**Structured Streaming configuration:**
```python
df_stream = (spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
    .select(from_json(col("body").cast("string"), schema).alias("data"))
    .select("data.*")
)

df_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://checkpoints@storage/pipeline-name") \
    .option("mergeSchema", "true") \
    .trigger(processingTime="1 minute") \
    .start("abfss://trusted@storage/events")
```

**Governance considerations:**
- Apply schema validation before writing — reject malformed events to a dead-letter queue in Event Hubs
- Use Unity Catalog for the Delta tables — lineage and access control apply to streaming tables too
- Watermarking for late data: `.withWatermark("event_time", "10 minutes")`

---

### Q48 (Medium) — What is Z-ordering in Delta Lake and when should you use it?

**Z-ordering** is a multi-dimensional clustering technique in Delta Lake that co-locates related data within the same files, based on the values of one or more columns.

**How it works:** Z-ordering uses a space-filling curve (Z-curve) to map multi-dimensional values into a single dimension, then sorts and groups data files by that mapping. Records with similar values in the specified columns end up in the same files.

**Syntax:**
```sql
OPTIMIZE pharma.trusted.patients ZORDER BY (region, trial_id)
```

**When to use it:**
- Columns that are frequently used in `WHERE` filters but are not partition columns
- Columns with high cardinality (too many distinct values to partition by)
- Columns used in join conditions

**Effect:** Queries filtering on Z-ordered columns scan dramatically fewer files (data skipping). Spark reads the per-file column statistics from the transaction log — without Z-ordering, a query might scan 1000 files; with it, 50.

**Trade-off:** OPTIMIZE + ZORDER rewrites files and is compute-intensive. Run as a maintenance job, not in the hot path.

---

### Q49 (Advanced) — How do you approach capacity planning for a Data Lakehouse that processes 10M records daily?

**Storage estimation:**
- Raw data: estimate average row size in Parquet (typically 10–50x compression from raw CSV/JSON). 10M records × 500 bytes/row = ~5GB raw; Parquet compressed ≈ 500MB–1GB/day
- With 3 Medallion layers and 1-year retention: ~1–2TB/year for data storage
- Metadata overhead: Iceberg/Delta metadata typically 1–5% of data volume

**Compute estimation:**
- Profile your Spark jobs: measure CPU utilization, memory usage, and shuffle bytes at current volume
- Scale by the growth rate — if volume grows 20%/year, your cluster sizing should handle 1.5–2x current load
- Use autoscaling clusters in Databricks to handle peak loads (e.g., month-end reporting) without over-provisioning for steady-state

**Cost optimization:**
- Use spot/preemptible instances for ETL workloads (70–90% cost reduction vs. on-demand)
- Separate compute pools by workload type: small always-on cluster for orchestration, large auto-scaling clusters for heavy transforms
- Archive old Iceberg snapshots to cheaper storage tiers (S3 Glacier) after retention period

---

### Q50 (Advanced) — Where do you see Data Engineering going in the next 3–5 years, and how are you preparing?

**Key trends shaping Data Engineering:**

**1. AI-native pipelines** — data engineers will increasingly build pipelines that serve LLM training, fine-tuning, and RAG (Retrieval-Augmented Generation) systems. Feature stores, vector databases, and unstructured data handling will become core skills alongside traditional structured ETL.

**2. Streaming-first** — the gap between batch and streaming will narrow. Tools like Apache Paimon and the Streaming Lakehouse pattern (Flink + Iceberg/Delta) are making real-time data with ACID guarantees accessible.

**3. Declarative data engineering** — SQL-based transformation tools (dbt) and data mesh patterns are shifting engineers toward declaring "what the data should look like" rather than "how to move it."

**4. Data contracts** — formal, versioned agreements between data producers and consumers. Breaking changes must go through a contract review process.

**How you're preparing:** Your current Iceberg expertise positions you well for the open table format consolidation. To extend into AI-ready engineering, the next step is familiarity with vector storage (Pinecone, pgvector, Databricks Vector Search), LLM feature engineering patterns, and unstructured data pipelines — which aligns directly with the "AI-Ready" tagline on your resume.

---

*End of document — 50 questions across 7 domains*
