# 🎯 Interview Questions & Answers — Mayuresh Patil (Data Engineer)
> Based on your resume: PySpark · Apache Iceberg · Lakehouse · Metadata-Driven Ingestion · Azure Databricks · Data Governance

---

## 📌 TABLE OF CONTENTS
1. [Introduction & Background](#1-introduction--background)
2. [PySpark & Apache Spark Internals](#2-pyspark--apache-spark-internals)
3. [Apache Iceberg](#3-apache-iceberg)
4. [Lakehouse Architecture](#4-lakehouse-architecture)
5. [Metadata-Driven Ingestion Framework](#5-metadata-driven-ingestion-framework)
6. [Incremental Processing & Watermarking](#6-incremental-processing--watermarking)
7. [ETL Pipeline Design](#7-etl-pipeline-design)
8. [Data Sources — Oracle, Salesforce, REST APIs](#8-data-sources--oracle-salesforce-rest-apis)
9. [Data Governance — Unity Catalog & Modak Nabu](#9-data-governance--unity-catalog--modak-nabu)
10. [Azure Databricks & Delta Lake](#10-azure-databricks--delta-lake)
11. [Azure Data Factory (ADF)](#11-azure-data-factory-adf)
12. [SQL Optimization](#12-sql-optimization)
13. [Python Scripting & Automation](#13-python-scripting--automation)
14. [Data Quality & Validation](#14-data-quality--validation)
15. [CI/CD & GitHub Actions](#15-cicd--github-actions)
16. [Scenario-Based / System Design Questions](#16-scenario-based--system-design-questions)
17. [Behavioral Questions (HR Round)](#17-behavioral-questions-hr-round)

---

## 1. Introduction & Background

**Q1. Tell me about yourself.**

> **Answer:**
> "I'm Mayuresh, a Data Engineer with around 3.5 years of experience at Cognizant Technology Solutions in Pune. I primarily work on two projects — the e-Clinical Data Platform (eCDP) and FinOps Dataverse. My day-to-day involves building PySpark ETL pipelines, designing Apache Iceberg-based lakehouse architectures across Raw, TL, and ATL layers, and developing metadata-driven ingestion frameworks. I've worked with sources like Oracle, MySQL, PostgreSQL, Salesforce, and Smartsheet APIs. I hold a Databricks Certified Data Engineer Associate certification and an Azure Data Fundamentals certification."

---

**Q2. Walk me through your current project.**

> **Answer:**
> "On eCDP, we're building a clinical data lakehouse. Data is ingested from multiple sources — Oracle, Salesforce API, Smartsheet API, and flat files — into a Raw layer stored as Apache Iceberg tables. From Raw, we apply business transformations in PySpark to move it to the TL (Transformed Layer), and then further aggregate and harmonize it in the ATL (Aggregated/Trusted Layer). We use a metadata-driven framework where pipeline configurations (source, target, load type, watermark column) are stored in config files/tables, so the pipelines are reusable across different datasets."

---

**Q3. Why are you looking for a change?**

> **Answer (suggested):**
> "I've built strong foundations in Spark internals, Iceberg, and lakehouse design at Cognizant. I'm now looking for a role where I can take on more ownership — working on larger-scale platforms, contributing to architecture decisions, and deepening my expertise in areas like streaming, advanced data modeling, or cloud-native data products."

---

## 2. PySpark & Apache Spark Internals

**Q4. Explain the Spark execution model — Driver, Executor, and DAG.**

> **Answer:**
> - **Driver**: The master process. Converts user code into a logical DAG, then into a physical execution plan (stages and tasks). Coordinates job execution.
> - **Executor**: Worker JVM processes on cluster nodes. Each executor has CPU cores (slots) and memory. Tasks run inside executors.
> - **DAG (Directed Acyclic Graph)**: Spark builds a DAG of RDD transformations. It splits the DAG into **stages** at shuffle boundaries. Each stage has **tasks** equal to the number of partitions.
> - **Job → Stage → Task** is the hierarchy. One Spark action triggers one Job.

---

**Q5. What is the difference between a narrow and wide transformation?**

> **Answer:**
> - **Narrow transformation**: Each input partition maps to exactly one output partition. No shuffle needed. Examples: `map()`, `filter()`, `select()`.
> - **Wide transformation**: Input partitions contribute to multiple output partitions. Requires a **shuffle** (data moves across the network). Examples: `groupBy()`, `join()`, `distinct()`, `repartition()`.
> - Shuffles are expensive — they involve disk I/O, network transfer, and serialization.

---

**Q6. What is data skew and how did you handle it in your project?**

> **Answer:**
> Data skew occurs when certain partitions have significantly more data than others, causing some tasks to take much longer than the rest (straggler tasks).
>
> **Detection**: Check the Spark UI → Stages tab → Task metrics (max task time vs median task time).
>
> **Solutions I've used:**
> 1. **Salting**: Add a random salt key to skewed keys, increasing partition count for that key range.
> 2. **Broadcast Join**: If one side of a join is small (< broadcast threshold, default 10MB), broadcast it to avoid shuffle.
> 3. **AQE (Adaptive Query Execution)**: Spark 3.x feature that automatically coalesces small shuffle partitions and handles skew joins at runtime.
> 4. **Repartition on high-cardinality column** to distribute load evenly.

---

**Q7. What is the difference between `repartition()` and `coalesce()`?**

> | Feature | `repartition(n)` | `coalesce(n)` |
> |---|---|---|
> | Shuffle | Full shuffle | No shuffle (merges partitions) |
> | Use case | Increase or redistribute partitions evenly | Reduce partitions (e.g., before writing) |
> | Performance | Expensive | Cheap |
> | Direction | Both increase/decrease | Only decrease |

---

**Q8. What is Spark's lazy evaluation? Why is it important?**

> **Answer:**
> Spark does NOT execute transformations immediately. It builds a logical plan (DAG) and executes only when an **action** is triggered (e.g., `show()`, `count()`, `write()`).
>
> **Benefits:**
> - Spark can **optimize the full plan** before execution (predicate pushdown, column pruning, join reordering).
> - Avoids unnecessary intermediate computations.
> - Enables **pipelining** of transformations within a stage.

---

**Q9. Explain Spark memory management — what are the different memory regions?**

> **Answer (Spark Unified Memory Model):**
> Total JVM Heap → **Reserved Memory** (300MB fixed) → **Usable Memory**
>
> **Usable Memory** is split into:
> - **Spark Memory** (60% default, `spark.memory.fraction`):
>   - **Execution Memory**: Used for shuffles, sorts, aggregations.
>   - **Storage Memory**: Used for caching/persisting RDDs and DataFrames.
>   - These two share a unified pool — execution can borrow from storage (and evict cached blocks if needed).
> - **User Memory** (40%): Used for user-defined data structures, UDFs, metadata.
>
> **Key configs**: `spark.executor.memory`, `spark.memory.fraction`, `spark.memory.storageFraction`

---

**Q10. What is the difference between `cache()` and `persist()`?**

> **Answer:**
> - `cache()` = `persist(StorageLevel.MEMORY_AND_DISK)` — stores in memory, spills to disk if memory is insufficient.
> - `persist()` allows you to choose the storage level explicitly:
>   - `MEMORY_ONLY` — fails if doesn't fit
>   - `MEMORY_AND_DISK` — spills to disk
>   - `DISK_ONLY` — always disk
>   - `MEMORY_ONLY_SER` — serialized (less memory, more CPU)
>
> Use `unpersist()` to release cached data when no longer needed.

---

**Q11. What is AQE (Adaptive Query Execution) in Spark 3?**

> **Answer:**
> AQE dynamically re-optimizes the query plan **at runtime** based on actual shuffle statistics, rather than relying purely on static estimates.
>
> **Three main features:**
> 1. **Coalescing small shuffle partitions**: Merges small post-shuffle partitions automatically.
> 2. **Skew join optimization**: Splits skewed partitions and replicates the smaller side to handle skew.
> 3. **Switching join strategies**: Can switch from sort-merge join to broadcast join if runtime stats show the table is small enough.
>
> Enabled by: `spark.sql.adaptive.enabled = true` (default in Spark 3.2+)

---

## 3. Apache Iceberg

**Q12. What is Apache Iceberg and why is it better than Hive tables?**

> **Answer:**
> Apache Iceberg is an **open table format** for large analytic datasets. It brings ACID transactions, schema evolution, and time travel to data lakes.
>
> **Advantages over Hive:**
> | Feature | Hive | Apache Iceberg |
> |---|---|---|
> | ACID Transactions | Limited (ORC only) | Full ACID |
> | Schema Evolution | Risky | Safe (add/drop/rename columns) |
> | Time Travel | No | Yes (snapshot-based) |
> | Partition Evolution | Requires table rebuild | In-place partition evolution |
> | Hidden Partitioning | No | Yes |
> | File Pruning | Basic | Advanced (metadata-driven) |

---

**Q13. Explain the Iceberg table architecture — metadata, manifest, and data files.**

> **Answer:**
> Iceberg has a **3-layer metadata hierarchy**:
>
> 1. **Metadata File** (`metadata.json`): Top-level file. Contains table schema, partition spec, current snapshot pointer, and list of snapshot history.
> 2. **Manifest List** (`snap-*.avro`): One per snapshot. Lists all manifest files belonging to that snapshot.
> 3. **Manifest File** (`*.avro`): Lists data files (Parquet/ORC/Avro) along with partition values and column-level statistics (min/max, null count).
> 4. **Data Files**: Actual Parquet/ORC files containing the data.
>
> **Read path**: Catalog → Metadata File → Manifest List → Manifest Files → Data Files (pruned by partition/column stats)

---

**Q14. What is hidden partitioning in Iceberg?**

> **Answer:**
> In Hive, partition values are stored as explicit directory names (`/dt=2024-01-01/`) and users must write WHERE clauses matching the exact partition column.
>
> In Iceberg, **hidden partitioning** allows you to define partition transforms (e.g., `days(event_time)`, `bucket(16, user_id)`) without exposing a separate partition column. Iceberg automatically derives the partition value from the column.
>
> **Benefits:**
> - Users query on `event_time`, not on a separate `dt` column.
> - Iceberg handles partition pruning automatically.
> - Changing partition granularity doesn't break existing queries.

---

**Q15. What is partition evolution in Iceberg?**

> **Answer:**
> Iceberg allows you to **change the partition scheme of a table without rewriting existing data**.
>
> Example: A table was initially partitioned by `months(event_date)`. As data grew, you want to switch to `days(event_date)`. With Iceberg partition evolution:
> - Old data stays under the old partition spec.
> - New data written under the new partition spec.
> - Iceberg tracks which snapshot used which partition spec.
> - Queries work correctly across both — Iceberg handles the routing internally.

---

**Q16. How does Time Travel work in Iceberg?**

> **Answer:**
> Iceberg maintains a **snapshot history**. Each write operation (insert, update, delete) creates a new snapshot. Old snapshots are retained (until expiry).
>
> ```sql
> -- Query data as of a specific snapshot
> SELECT * FROM catalog.db.table VERSION AS OF 12345678;
>
> -- Query data as of a specific timestamp
> SELECT * FROM catalog.db.table TIMESTAMP AS OF '2024-01-01 00:00:00';
> ```
>
> Use cases: Auditing, debugging bad writes, regulatory compliance, rollback.

---

**Q17. What is Copy-on-Write (CoW) vs Merge-on-Read (MoR) in Iceberg?**

> **Answer:**
> - **Copy-on-Write (CoW)**: On every update/delete, affected data files are rewritten with the changes applied. Reads are fast (no merge needed), but writes are expensive (full file rewrite).
> - **Merge-on-Read (MoR)**: Updates/deletes write small **delta/delete files**. Reads must merge base files + delete files on the fly. Writes are fast, reads are slightly slower.
>
> **When to use:**
> - CoW: Workloads with heavy reads and infrequent updates.
> - MoR: Workloads with frequent small updates (CDC, near-real-time).

---

**Q18. How do you perform compaction in Iceberg?**

> **Answer:**
> Over time, many small files accumulate (especially with MoR or frequent incremental loads). Compaction rewrites small files into larger, optimally sized files.
>
> ```python
> # Using Spark with Iceberg
> spark.sql("""
>     CALL catalog.system.rewrite_data_files(
>         table => 'db.table_name',
>         strategy => 'binpack',
>         options => map('target-file-size-bytes', '134217728')
>     )
> """)
> ```
>
> Also useful: `rewrite_manifests` to compact manifest files.

---

## 4. Lakehouse Architecture

**Q19. What is a Lakehouse and how is it different from a Data Warehouse and Data Lake?**

> **Answer:**
> | | Data Lake | Data Warehouse | Lakehouse |
> |---|---|---|---|
> | Storage | Raw files (S3/ADLS) | Proprietary storage | Open file formats (Parquet/Iceberg) |
> | Schema | Schema-on-read | Schema-on-write | Schema-on-write (with evolution) |
> | ACID | No | Yes | Yes |
> | Cost | Low | High | Low-Medium |
> | Governance | Weak | Strong | Strong |
> | Use case | ML, raw storage | BI, reporting | Both ML + BI |
>
> Lakehouse = Best of both worlds. It uses open table formats (Iceberg, Delta, Hudi) on cloud storage to bring warehouse-grade features to data lakes.

---

**Q20. Explain your Raw → TL → ATL layer architecture.**

> **Answer:**
> - **Raw Layer**: Ingested as-is from source systems (Oracle, Salesforce, flat files). Stored as Apache Iceberg tables. Full historical data retained. No transformations — only type casting if needed.
> - **TL (Transformed Layer)**: Business logic applied. Deduplication, null handling, joins, data type standardization. Still keeps row-level data.
> - **ATL (Aggregated/Trusted Layer)**: Further aggregations, harmonization using XREF mappings, Golden Record generation. This layer serves analytics and reporting teams.
>
> Each layer is governed by metadata-driven pipelines, so adding a new source doesn't require new code — just new config entries.

---

## 5. Metadata-Driven Ingestion Framework

**Q21. What is a metadata-driven ingestion framework and why did you build one?**

> **Answer:**
> Instead of writing a separate pipeline for every source table, we built a **generic, reusable pipeline** that reads configuration from a metadata store (JSON/YAML config files or a config table in the database).
>
> **Config parameters per source:**
> - Source type (Oracle, API, CSV, etc.)
> - Connection details / secret name
> - Target table name / layer
> - Load type (Full / Incremental)
> - Watermark column and last run timestamp
> - Primary key columns (for deduplication)
> - Partition column
>
> **Benefits:**
> - Adding a new source = adding a new config entry (no code change).
> - Consistent error handling, logging, and auditing across all pipelines.
> - Reduced development time by ~60% for new source onboarding.

---

**Q22. How do you handle full load vs incremental load in your framework?**

> **Answer:**
> The `load_type` parameter in config drives the logic:
>
> **Full Load:**
> - Read entire source table.
> - Truncate (or overwrite) target Iceberg table.
> - Write all records.
>
> **Incremental Load:**
> - Read `last_run_timestamp` from the audit/control table.
> - Apply filter: `WHERE updated_at > last_run_timestamp`
> - Use `MERGE INTO` on the target Iceberg table using primary key.
> - After successful run, update `last_run_timestamp` in the control table.

---

## 6. Incremental Processing & Watermarking

**Q23. How does watermarking work in your pipelines?**

> **Answer:**
> Watermarking tracks the high-water mark of data already processed.
>
> **Implementation:**
> 1. Each source table has a `watermark_column` (e.g., `UPDATED_AT`, `CREATED_DATE`) defined in config.
> 2. A **control table** stores `{source_name, last_watermark_value, last_run_status, last_run_ts}`.
> 3. Before each run, read `last_watermark_value` from the control table.
> 4. Query source: `WHERE watermark_column > last_watermark_value`.
> 5. After successful write, update control table with new `max(watermark_column)` from the fetched data.
>
> **Edge cases handled:**
> - Source doesn't have a reliable update timestamp → Use CDC or full load with hash comparison.
> - Clock skew between source and ingestion system → Add a small overlap buffer (e.g., subtract 5 minutes from the watermark).

---

**Q24. What happens if a pipeline fails mid-run? How do you handle it?**

> **Answer:**
> - The control table's `last_run_status` is set to `RUNNING` at job start. Only updated to `SUCCESS` upon completion.
> - If the job fails, status remains `RUNNING` or is set to `FAILED`.
> - On the next run, the framework detects the failed/running status and re-runs from the **same watermark** (not the new one), ensuring no data loss.
> - Iceberg's ACID transactions ensure partial writes are not visible (atomicity), so re-running is safe.

---

## 7. ETL Pipeline Design

**Q25. Explain the end-to-end flow of one of your PySpark pipelines.**

> **Answer (eCDP Example):**
> 1. **Config Load**: Read source config (source=Oracle, table=CLINICAL_TRIALS, load_type=Incremental, watermark_col=UPDATED_AT).
> 2. **Watermark Fetch**: Query control table for `last_watermark`.
> 3. **Source Extraction**: Use JDBC to connect Oracle. Apply pushdown filter `WHERE UPDATED_AT > last_watermark`. Read into Spark DataFrame.
> 4. **Data Validation**: Apply null checks, type checks via Modak Nabu / custom rules.
> 5. **Transformation**: Cast data types, rename columns to match target schema, add audit columns (`ingestion_ts`, `source_system`).
> 6. **Write to Raw Layer**: MERGE INTO Iceberg table using primary key. Handles insert/update automatically.
> 7. **Audit Update**: Write max(UPDATED_AT) as new watermark to control table.
> 8. **Logging**: Record run status, records processed, duration.

---

**Q26. How do you handle schema evolution in your pipelines?**

> **Answer:**
> - **Iceberg's native schema evolution** allows adding/renaming/dropping columns without rewriting data.
> - In our pipeline, before writing to the target, we compare the incoming DataFrame schema with the Iceberg table schema.
> - If a new column is detected, we use `ALTER TABLE ... ADD COLUMN` (via Spark SQL or Iceberg API).
> - If a column is removed from source, we keep it in the target as NULL (we don't drop columns automatically — requires a change approval).
> - Schema version changes are logged in the audit metadata.

---

## 8. Data Sources — Oracle, Salesforce, REST APIs

**Q27. How do you read data from Oracle using PySpark JDBC?**

> **Answer:**
> ```python
> df = spark.read \
>     .format("jdbc") \
>     .option("url", "jdbc:oracle:thin:@host:port/service") \
>     .option("dbtable", "(SELECT * FROM CLINICAL_TRIALS WHERE UPDATED_AT > '2024-01-01') T") \
>     .option("user", dbutils.secrets.get("scope", "oracle-user")) \
>     .option("password", dbutils.secrets.get("scope", "oracle-pwd")) \
>     .option("driver", "oracle.jdbc.OracleDriver") \
>     .option("numPartitions", 8) \
>     .option("partitionColumn", "TRIAL_ID") \
>     .option("lowerBound", 1) \
>     .option("upperBound", 100000) \
>     .load()
> ```
>
> `numPartitions` + `partitionColumn` + `lowerBound/upperBound` are critical for **parallel reads** from JDBC — otherwise Spark reads everything through a single connection.

---

**Q28. How did you connect to the Salesforce API in your pipeline?**

> **Answer:**
> We used the **Salesforce REST API** with OAuth2 authentication:
> 1. POST to Salesforce token endpoint with `client_id`, `client_secret`, `username`, `password` → Get `access_token`.
> 2. Use access token in header: `Authorization: Bearer <token>`.
> 3. Call Salesforce SOQL query endpoint: `/services/data/v57.0/query?q=SELECT+Id,Name+FROM+Account`.
> 4. Handle pagination via `nextRecordsUrl` in the response.
> 5. Convert JSON response to Spark DataFrame using `spark.createDataFrame()` or `spark.read.json()`.
> 6. Credentials stored in Azure Key Vault, accessed via `dbutils.secrets.get()`.

---

## 9. Data Governance — Unity Catalog & Modak Nabu

**Q29. What is Unity Catalog and what governance features does it provide?**

> **Answer:**
> Unity Catalog is Databricks' **centralized governance layer** for data and AI assets across all Databricks workspaces.
>
> **Features:**
> - **Three-level namespace**: `catalog.schema.table` (e.g., `prod_catalog.clinical.trials`)
> - **Fine-grained access control**: Grant/revoke at catalog, schema, table, column, or row level.
> - **Data lineage**: Automatically tracks column-level lineage across notebooks, jobs, and dashboards.
> - **Audit logs**: Every data access and modification is logged.
> - **Delta Sharing**: Securely share data across organizations without data copy.
> - **Metastore**: Single metastore per region — governs all workspaces attached to it.

---

**Q30. How did Modak Nabu help reduce data issues by 25%?**

> **Answer:**
> Modak Nabu is a **data quality and governance platform**. We integrated it into our pipelines to apply rule-based validation before data moves from Raw to TL layer:
>
> - **Completeness checks**: Non-null validation on mandatory fields (e.g., Patient ID must not be null).
> - **Referential integrity**: Foreign key checks against reference tables.
> - **Format validation**: Date formats, regex patterns for codes.
> - **Range checks**: Numeric columns within expected min/max bounds.
> - **Duplicate detection**: Flag duplicate records based on primary key.
>
> Any record failing validation is **quarantined** (written to a bad-records table) rather than rejected outright. This allowed the business to review and fix bad data. By catching issues at Raw → TL, we prevented downstream ATL data from being corrupted, reducing data quality incidents by ~25%.

---

## 10. Azure Databricks & Delta Lake

**Q31. What is the difference between Apache Iceberg and Delta Lake?**

> **Answer:**
> | Feature | Delta Lake | Apache Iceberg |
> |---|---|---|
> | Creator | Databricks (open-sourced) | Netflix (open-sourced via Apache) |
> | File format | Parquet only | Parquet, ORC, Avro |
> | Catalog | Hive metastore / Unity Catalog | Multiple (Hive, Glue, Nessie, REST) |
> | Schema evolution | Yes | Yes (more advanced) |
> | Partition evolution | No (requires ZORDER, liquid clustering) | Yes (native) |
> | Time travel | Yes | Yes |
> | Multi-engine support | Primarily Spark | Spark, Flink, Trino, Presto, Hive |
>
> In our project we chose Iceberg for better **multi-engine support** and **partition evolution** flexibility.

---

**Q32. What is Z-Ordering in Delta Lake / Databricks?**

> **Answer:**
> Z-Ordering is a **data clustering technique** that co-locates related data in the same set of files, improving query performance through better **data skipping**.
>
> ```sql
> OPTIMIZE delta.`/path/to/table` ZORDER BY (patient_id, event_date);
> ```
>
> - Without Z-Order: Spark reads many files even for a selective query.
> - With Z-Order: Files are arranged so that records with similar `patient_id` and `event_date` are in the same files. Spark can skip most files using column statistics.
>
> Best used for high-cardinality columns frequently used in WHERE clauses.

---

## 11. Azure Data Factory (ADF)

**Q33. What is the role of Azure Data Factory in your architecture?**

> **Answer:**
> We use ADF as the **orchestration layer** — it doesn't do heavy data transformation but orchestrates and triggers Databricks notebooks/jobs.
>
> **Our ADF usage:**
> - **Pipelines**: Trigger Databricks jobs for each source ingestion.
> - **Parameters**: Pass source config name, run date, load type as parameters to Databricks notebooks.
> - **Scheduling**: ADF triggers run on schedule (daily, hourly).
> - **Monitoring**: ADF activity runs give visibility into pipeline success/failure.
> - **Linked Services**: Configured for ADLS Gen2, Azure Key Vault, Databricks workspace.
> - **Control flow**: If/else conditions, For-Each loops to iterate over multiple source configs.

---

**Q34. What is the difference between ADF Copy Activity and Databricks Notebook Activity?**

> **Answer:**
> - **Copy Activity**: Low-code data movement. Best for simple source → sink transfers (CSV to ADLS, SQL DB to Blob). Limited transformation capability. Managed connectors for 90+ sources.
> - **Databricks Notebook Activity**: Triggers a Databricks notebook/job. Used for complex PySpark transformations, business logic, Iceberg writes. Full Spark power available.
>
> In our architecture: ADF Copy Activity handles file landing (e.g., CSV from SharePoint to ADLS), and Databricks Notebook Activity handles all transformation and Iceberg writes.

---

## 12. SQL Optimization

**Q35. How do you optimize a slow SQL query?**

> **Answer:**
> 1. **EXPLAIN / EXPLAIN ANALYZE**: Check the query plan for full table scans, missing index usage, hash joins vs nested loops.
> 2. **Indexes**: Add indexes on columns used in WHERE, JOIN, ORDER BY.
> 3. **Avoid SELECT ***: Only select required columns (column pruning).
> 4. **Filter early**: Apply WHERE conditions as early as possible to reduce row count before joins.
> 5. **Partition pruning**: Ensure WHERE clause includes partition key so Spark/DB skips irrelevant partitions.
> 6. **Join order**: Join smaller tables first to reduce intermediate result size.
> 7. **Avoid functions on indexed columns**: `WHERE YEAR(date_col) = 2024` prevents index use; use `WHERE date_col BETWEEN '2024-01-01' AND '2024-12-31'` instead.
> 8. **CTEs vs subqueries**: CTEs improve readability; in some engines, materialized CTEs improve performance.

---

**Q36. What is the difference between `ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()`?**

> **Answer:**
> All are window functions that assign ranks to rows within a partition.
>
> | Function | Ties handling | Gap after ties |
> |---|---|---|
> | `ROW_NUMBER()` | Unique number even for ties | N/A |
> | `RANK()` | Same rank for ties | Yes (gap) |
> | `DENSE_RANK()` | Same rank for ties | No gap |
>
> Example for scores (100, 100, 90):
> - `ROW_NUMBER()` → 1, 2, 3
> - `RANK()` → 1, 1, 3
> - `DENSE_RANK()` → 1, 1, 2

---

**Q37. Write a SQL query to find the second highest salary.**

> ```sql
> -- Method 1: Using DENSE_RANK
> SELECT salary
> FROM (
>     SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
>     FROM employees
> ) ranked
> WHERE rnk = 2;
>
> -- Method 2: Using subquery
> SELECT MAX(salary)
> FROM employees
> WHERE salary < (SELECT MAX(salary) FROM employees);
> ```

---

## 13. Python Scripting & Automation

**Q38. What Python automation scripts did you build in your project?**

> **Answer:**
> - **DDL Generator**: Script that reads a JSON/YAML config (column names, data types, nullable, partition keys) and auto-generates `CREATE TABLE` DDL statements for Iceberg tables. Reduced manual DDL writing effort significantly.
> - **Config File Generator**: Script to generate pipeline config JSON files from a master Excel/CSV maintained by the business team. This allowed business analysts to onboard new sources without engineer intervention.
> - **XREF Mapping Script**: Python script to apply cross-reference (XREF) mapping tables to harmonize codes across different source systems (e.g., different systems use different codes for the same diagnosis).
> - **Golden Record Generation**: Script implementing deduplication + field-level survivorship rules to produce a single trusted record from multiple source records.

---

**Q39. What is the difference between a list, tuple, set, and dictionary in Python?**

> **Answer:**
> | | List | Tuple | Set | Dictionary |
> |---|---|---|---|---|
> | Ordered | Yes | Yes | No | Yes (Python 3.7+) |
> | Mutable | Yes | No | Yes | Yes |
> | Duplicates | Yes | Yes | No | Keys: No, Values: Yes |
> | Indexable | Yes | Yes | No | By key |
> | Use case | General collection | Fixed data | Unique values | Key-value mapping |

---

## 14. Data Quality & Validation

**Q40. How do you implement data quality checks in PySpark?**

> **Answer:**
> We implement DQ checks at multiple levels:
>
> **1. Completeness checks:**
> ```python
> null_count = df.filter(col("patient_id").isNull()).count()
> if null_count > 0:
>     raise DataQualityException(f"{null_count} records have null patient_id")
> ```
>
> **2. Uniqueness checks:**
> ```python
> dup_count = df.count() - df.dropDuplicates(["patient_id"]).count()
> ```
>
> **3. Referential integrity:**
> ```python
> invalid = df.join(ref_df, on="diagnosis_code", how="left_anti")
> ```
>
> **4. Statistical thresholds**: Row count today vs average of last 7 days (>50% drop triggers alert).
>
> **5. Schema validation**: Compare DataFrame schema vs expected schema before write.

---

## 15. CI/CD & GitHub Actions

**Q41. How do you manage code promotion across Dev → Validation → Production?**

> **Answer:**
> - **Branching strategy**: `feature/*` → `develop` (Dev env) → `release/*` → `main` (Prod).
> - **GitHub Actions workflows**:
>   - On PR to `develop`: Run unit tests (pytest), linting (flake8).
>   - On merge to `develop`: Auto-deploy notebooks to Dev Databricks workspace.
>   - On merge to `main`: Deploy to Production workspace after manual approval gate.
> - **Databricks Asset Bundles (DAB)** or Databricks REST API used for deploying notebooks and jobs.
> - **Secrets management**: Environment-specific secrets in Azure Key Vault (Dev KV, Prod KV). Pipelines reference the correct vault based on environment parameter.
> - **Config promotion**: Separate config files per environment (dev_config.json, prod_config.json).

---

## 16. Scenario-Based / System Design Questions

**Q42. Design a pipeline to ingest 500 tables from Oracle into an Iceberg lakehouse.**

> **Answer:**
> 1. **Metadata-driven approach**: Store all 500 table configs (source table, partition col, watermark col, load type, target table) in a config database/table.
> 2. **Parallel execution**: Group tables by size/priority. Run groups in parallel using Databricks Jobs with multiple tasks or ADF ForEach with parallel degree.
> 3. **JDBC parallel reads**: Use `numPartitions` + `partitionColumn` for large tables to parallelize extraction.
> 4. **Schema auto-discovery**: Auto-generate DDL for target Iceberg tables from source Oracle metadata.
> 5. **Error isolation**: Each table is an independent job. Failure of one doesn't block others.
> 6. **Audit & monitoring**: Central audit table tracking per-table run status, record counts, duration.
> 7. **First run**: Full load for all tables. Subsequent runs: Incremental using watermarks.

---

**Q43. A critical Iceberg table has bad data written by a pipeline. How do you recover?**

> **Answer:**
> 1. **Identify the bad snapshot**: Query `iceberg_snapshots` metadata to find the snapshot where bad data was written.
> 2. **Rollback** to the last known good snapshot:
>    ```sql
>    CALL catalog.system.rollback_to_snapshot('db.table', snapshot_id);
>    ```
> 3. **Or rollback by timestamp**:
>    ```sql
>    CALL catalog.system.rollback_to_timestamp('db.table', TIMESTAMP '2024-01-15 10:00:00');
>    ```
> 4. Fix the root cause in the pipeline (data validation rule, source data issue).
> 5. Re-run the pipeline from the corrected watermark.
> 6. Post-incident: Add a DQ check that would have caught this issue.

---

**Q44. How would you optimize a PySpark job that takes 4 hours to run?**

> **Answer (systematic approach):**
> 1. **Spark UI analysis**: Check for stages with high duration, skewed tasks, excessive shuffle read/write.
> 2. **Data skew**: If certain partition keys dominate → Apply salting or AQE skew handling.
> 3. **Shuffle optimization**: Tune `spark.sql.shuffle.partitions` (default 200 — often too high or too low for your data size).
> 4. **Broadcast joins**: Check if any join partner is small enough to broadcast → eliminates shuffle.
> 5. **Partition strategy**: Ensure input data is well-partitioned. Use `repartition()` before heavy operations.
> 6. **File size optimization**: Small files cause excessive task overhead → Compact with `OPTIMIZE`.
> 7. **Predicate pushdown**: Ensure filters are pushed down to the source (JDBC/Iceberg/Parquet) — check execution plan.
> 8. **Caching**: Cache frequently reused DataFrames to avoid recomputation.
> 9. **Cluster sizing**: Check if executors are memory/CPU bound. Right-size executor memory and cores.

---

## 17. Behavioral Questions (HR Round)

**Q45. Tell me about a challenging situation you faced at work and how you resolved it.**

> **Suggested Answer (based on your profile):**
> "During our eCDP migration project, we were ingesting clinical trial data from Oracle into Iceberg tables. One of the source tables had over 500 million records and the initial JDBC read was timing out and taking 6+ hours.
>
> I analyzed the issue and found that the read was happening through a single JDBC connection with no partitioning. I implemented parallel JDBC reads using `numPartitions`, `partitionColumn` (TRIAL_ID), and `lowerBound/upperBound`. This reduced extraction time to under 45 minutes.
>
> Additionally, the target Iceberg write was creating small files. I implemented periodic `OPTIMIZE` calls via a separate maintenance job. Overall the pipeline went from 6+ hours to under 1 hour."

---

**Q46. Give an example of when you improved a process or introduced something new.**

> **Suggested Answer:**
> "I introduced the metadata-driven ingestion framework in our project. Initially, each source had its own custom PySpark script — about 30 separate notebooks for 30 source tables. Any change (like adding an audit column or changing load type) had to be made in each notebook manually.
>
> I designed a single generic pipeline that reads source configuration from a metadata table. Now adding a new source means adding one row to the config table — no code changes. We onboarded 15 new sources in one sprint without writing a single new notebook. It also standardized our error handling and audit logging across all pipelines."

---

**Q47. Where do you see yourself in 3 years?**

> **Suggested Answer:**
> "In 3 years, I see myself as a Senior Data Engineer or a Lead in the data engineering space — taking ownership of end-to-end platform design, mentoring junior engineers, and contributing to architectural decisions. I want to deepen my expertise in real-time data streaming with Kafka and Flink, and expand into data platform engineering — building self-service data infrastructure. I'm also keen on exploring ML platform engineering as data and ML workflows converge."

---

*Prepared for: Mayuresh Uttam Patil | Data Engineer | April 2026*
*Stack: PySpark · Apache Iceberg · Azure Databricks · Delta Lake · ADF · Unity Catalog*
