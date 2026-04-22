# 🎯 Round 2 Interview Prep — Data Engineer
> Stack: PySpark · Python · AWS EMR · AWS S3 · AutoSys · Azure DevOps · Flat File Ingestion · Data Transformation

---

## 📌 TABLE OF CONTENTS
1. [Project Understanding Questions](#1-project-understanding-questions)
2. [Flat File Ingestion](#2-flat-file-ingestion)
3. [AWS S3](#3-aws-s3)
4. [AWS EMR (Elastic MapReduce)](#4-aws-emr-elastic-mapreduce)
5. [PySpark — Core Concepts](#5-pyspark--core-concepts)
6. [PySpark — Data Transformation](#6-pyspark--data-transformation)
7. [AutoSys — Job Scheduling](#7-autosys--job-scheduling)
8. [Azure DevOps — CI/CD & Pipelines](#8-azure-devops--cicd--pipelines)
9. [Python Scripting](#9-python-scripting)
10. [Data Quality & Validation](#10-data-quality--validation)
11. [Final Table / Target Layer](#11-final-table--target-layer)
12. [Scenario-Based Questions](#12-scenario-based-questions)
13. [Behavioral Questions](#13-behavioral-questions)

---

## 1. Project Understanding Questions

**Q1. Can you walk me through the end-to-end data flow of this project?**

> **Answer:**
> "Based on what I understood from the project:
> 1. **Source files** (flat files / packet files) are received from upstream systems.
> 2. A **framework** ingests these files into the big data environment.
> 3. **Pipelines are built via Azure DevOps** — code is versioned and released through CI/CD.
> 4. All scripts and code are **backed up in S3**.
> 5. **AutoSys schedules** all production jobs — triggers the pipelines at defined intervals.
> 6. **AWS EMR** runs the PySpark jobs in a cluster environment for fast big data processing.
> 7. Source data from S3 is **ingested and transformed** using PySpark business logic.
> 8. The **final output is written to a target table** (backed by an S3 table/view).
> 9. **Forecasters access** the final table for analytics and reporting."

---

**Q2. What is the role of each tool in this architecture?**

> **Answer:**
>
> | Tool | Role |
> |---|---|
> | **Flat Files / S3** | Source data landing zone |
> | **Azure DevOps** | CI/CD — build, release pipelines |
> | **S3** | Code storage + data storage |
> | **AutoSys** | Production job scheduling |
> | **AWS EMR** | Cluster compute for PySpark jobs |
> | **PySpark / Python** | Transformation logic |
> | **Final Table** | Target layer for forecasters |

---

## 2. Flat File Ingestion

**Q3. How do you ingest flat files (CSV/TSV/fixed-width) using PySpark?**

> **Answer:**
> ```python
> # CSV ingestion
> df = spark.read \
>     .option("header", "true") \
>     .option("inferSchema", "true") \
>     .option("delimiter", ",") \
>     .option("nullValue", "NULL") \
>     .option("dateFormat", "yyyy-MM-dd") \
>     .csv("s3://bucket-name/source/landing/file.csv")
>
> # Multiple files from a folder
> df = spark.read.csv("s3://bucket-name/source/landing/*.csv", header=True)
>
> # Fixed-width file (no delimiter)
> df = spark.read.text("s3://bucket-name/source/fixed_width/") \
>     .select(
>         col("value").substr(1, 10).alias("account_id"),
>         col("value").substr(11, 50).alias("account_name"),
>         col("value").substr(61, 15).alias("balance")
>     )
> ```

---

**Q4. What challenges do you face with flat file ingestion and how do you handle them?**

> **Answer:**
>
> | Challenge | Solution |
> |---|---|
> | Missing header | Define schema manually using `StructType` |
> | Inconsistent delimiters | Use `multiLine=True`, custom delimiter option |
> | Null/empty values | `.option("nullValue", "")` + downstream null checks |
> | Encoding issues | `.option("encoding", "UTF-8")` or `ISO-8859-1` |
> | File arrives late | AutoSys dependency jobs — trigger only after file arrival check |
> | Bad/corrupt records | `.option("mode", "PERMISSIVE")` + `_corrupt_record` column |
> | Header row count mismatch | Schema validation before processing |

---

**Q5. How do you define schema manually for flat file ingestion?**

> **Answer:**
> ```python
> from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
>
> schema = StructType([
>     StructField("account_id", StringType(), nullable=False),
>     StructField("account_name", StringType(), nullable=True),
>     StructField("balance", IntegerType(), nullable=True),
>     StructField("transaction_date", DateType(), nullable=True)
> ])
>
> df = spark.read \
>     .schema(schema) \
>     .option("header", "true") \
>     .csv("s3://bucket/source/file.csv")
> ```
>
> **Why define schema manually instead of inferSchema?**
> - `inferSchema=True` does an extra pass over data (expensive).
> - May infer wrong types (e.g., leading zeros in zip codes read as integers).
> - Manual schema ensures type safety and faster reads.

---

**Q6. How do you handle a file ingestion framework for multiple source files?**

> **Answer:**
> Build a **metadata-driven framework**:
> - Config table/file stores: `{source_name, s3_path, delimiter, schema_path, target_table, load_type}`
> - One generic ingestion script reads config and loops through sources.
> - For each source: validate file existence → apply schema → read → validate → write to target.
> - Log each run in an audit table: `{source, file_name, record_count, status, run_ts}`.

---

## 3. AWS S3

**Q7. What is AWS S3 and how is it used in a data pipeline?**

> **Answer:**
> S3 (Simple Storage Service) is AWS's object storage service — it stores files as objects in buckets.
>
> **In this project, S3 serves multiple purposes:**
> - **Landing zone**: Source flat files dropped here by upstream systems.
> - **Code repository backup**: PySpark scripts, config files stored in S3.
> - **Intermediate storage**: Transformed data between pipeline stages.
> - **Final target**: Output Parquet/ORC tables stored in S3, queried via Hive/Athena/Spark.
>
> **S3 path structure (best practice):**
> ```
> s3://project-bucket/
> ├── landing/         ← Raw source files
> ├── raw/             ← Ingested as-is
> ├── transformed/     ← After business logic
> ├── final/           ← Target table
> └── scripts/         ← PySpark code
> ```

---

**Q8. How do you read from and write to S3 in PySpark on EMR?**

> **Answer:**
> ```python
> # Reading from S3
> df = spark.read.parquet("s3://bucket-name/transformed/accounts/")
>
> # Writing to S3 as Parquet (partitioned)
> df.write \
>     .mode("overwrite") \
>     .partitionBy("transaction_date") \
>     .parquet("s3://bucket-name/final/accounts_final/")
>
> # Writing as CSV
> df.write \
>     .mode("append") \
>     .option("header", "true") \
>     .csv("s3://bucket-name/output/report/")
> ```
>
> On EMR, S3 access is configured via **IAM roles** attached to the EMR cluster — no hardcoded credentials needed.

---

**Q9. What is S3 partitioning and why is it important?**

> **Answer:**
> Partitioning organizes data into subdirectories based on a column value:
> ```
> s3://bucket/final/accounts_final/
> ├── transaction_date=2024-01-01/
> │   └── part-00001.parquet
> ├── transaction_date=2024-01-02/
> │   └── part-00001.parquet
> ```
>
> **Benefits:**
> - **Partition pruning**: Query with `WHERE transaction_date = '2024-01-01'` only reads that folder — skips all others.
> - **Parallel processing**: Each partition folder processed independently.
> - **Incremental loads**: Only new partition folders need to be processed.

---

## 4. AWS EMR (Elastic MapReduce)

**Q10. What is AWS EMR and why is it used for big data processing?**

> **Answer:**
> EMR is AWS's **managed big data platform** that runs Apache Spark, Hadoop, Hive, and other frameworks on a scalable cluster of EC2 instances.
>
> **Why EMR for this project:**
> - **On-demand cluster**: Spin up a cluster, process data, terminate — pay only for usage.
> - **Managed Spark**: No manual Spark setup — EMR handles installation and configuration.
> - **S3 native**: Deep integration with S3 — read/write directly without HDFS.
> - **Scalability**: Add/remove nodes dynamically based on load.
> - **Fast processing**: Distributes PySpark jobs across multiple EC2 nodes.

---

**Q11. Explain the EMR cluster architecture.**

> **Answer:**
> ```
> EMR Cluster
> ├── Master Node (1)
> │   ├── Runs YARN ResourceManager
> │   ├── Runs Spark Driver
> │   ├── Manages cluster coordination
> │   └── Hosts NameNode (if HDFS used)
> ├── Core Nodes (N)
> │   ├── Run YARN NodeManager
> │   ├── Run Spark Executors
> │   └── Store data (HDFS / local disk)
> └── Task Nodes (optional, N)
>     ├── Run Executors only
>     ├── No data storage
>     └── Used for burst processing (Spot instances)
> ```
>
> **Cost optimization**: Use **Spot instances** for Task nodes (up to 90% cheaper) — they can be interrupted but task nodes don't store data so interruption is safe.

---

**Q12. How do you submit a PySpark job to EMR?**

> **Answer:**
> **Option 1: EMR Steps (most common)**
> ```bash
> aws emr add-steps \
>     --cluster-id j-XXXXXXXX \
>     --steps Type=Spark,Name="TransformJob",\
>     ActionOnFailure=CONTINUE,\
>     Args=[--deploy-mode,cluster,\
>     s3://bucket/scripts/transform.py,\
>     --input,s3://bucket/raw/,\
>     --output,s3://bucket/final/]
> ```
>
> **Option 2: spark-submit on Master Node**
> ```bash
> spark-submit \
>     --master yarn \
>     --deploy-mode cluster \
>     --executor-memory 16g \
>     --executor-cores 4 \
>     --num-executors 10 \
>     s3://bucket/scripts/transform.py
> ```
>
> **Option 3: Triggered by AutoSys** → AutoSys job runs a shell script → shell script calls `aws emr add-steps`.

---

**Q13. What is the difference between EMR and Databricks?**

> **Answer:**
>
> | Feature | AWS EMR | Databricks |
> |---|---|---|
> | Cloud | AWS native | Multi-cloud (AWS/Azure/GCP) |
> | Management | More manual setup | Fully managed |
> | Cost | Lower (raw EC2) | Higher (managed service premium) |
> | Collaboration | Basic notebooks | Advanced collaborative notebooks |
> | Delta Lake / Iceberg | Manual setup | Native support |
> | Auto-scaling | Yes | Yes (more intelligent) |
> | Setup effort | Higher | Lower |
>
> EMR is preferred when the team wants **more control and lower cost**. Databricks is preferred for **developer productivity and lakehouse features**.

---

## 5. PySpark — Core Concepts

**Q14. What happens when you run a PySpark job on EMR — step by step?**

> **Answer:**
> 1. `spark-submit` sends the job to **YARN ResourceManager** on Master node.
> 2. YARN allocates **containers** on Core/Task nodes for Spark Executors.
> 3. Spark **Driver** starts on Master node — reads the script, builds DAG.
> 4. DAG is split into **stages** at shuffle boundaries.
> 5. Each stage has **tasks** = number of partitions.
> 6. Driver sends tasks to **Executors** on Core/Task nodes.
> 7. Executors read data directly from **S3** (via EMRFS — EMR File System).
> 8. Tasks process partitions and write results back to **S3**.
> 9. On completion, Executors release containers back to YARN.

---

**Q15. What is the difference between `map()`, `flatMap()`, and `filter()` in PySpark?**

> **Answer:**
> ```python
> rdd = sc.parallelize(["Hello World", "PySpark EMR"])
>
> # map() — 1 input → 1 output
> rdd.map(lambda x: x.upper())
> # ["HELLO WORLD", "PYSPARK EMR"]
>
> # flatMap() — 1 input → multiple outputs (flattens)
> rdd.flatMap(lambda x: x.split(" "))
> # ["Hello", "World", "PySpark", "EMR"]
>
> # filter() — keep rows matching condition
> rdd.filter(lambda x: "EMR" in x)
> # ["PySpark EMR"]
> ```

---

**Q16. Explain the difference between `groupBy()` and `window functions`.**

> **Answer:**
> - **`groupBy()`**: Aggregates rows into **one row per group**. Loses row-level detail.
> ```python
> df.groupBy("dept").agg(sum("salary").alias("total_salary"))
> # Result: one row per dept
> ```
>
> - **Window functions**: Aggregates **without collapsing rows** — each row keeps its detail + the aggregate.
> ```python
> from pyspark.sql.window import Window
> from pyspark.sql.functions import sum, rank
>
> w = Window.partitionBy("dept").orderBy("salary")
> df.withColumn("running_total", sum("salary").over(w))
> # Result: all original rows + running_total column
> ```

---

## 6. PySpark — Data Transformation

**Q17. How do you apply business logic transformations in PySpark?**

> **Answer:**
> Common transformations used in production:
>
> ```python
> from pyspark.sql.functions import col, when, upper, trim, to_date, coalesce, lit
>
> df_transformed = df \
>     # Trim whitespace
>     .withColumn("account_name", trim(col("account_name"))) \
>     # Uppercase
>     .withColumn("status", upper(col("status"))) \
>     # Date conversion
>     .withColumn("txn_date", to_date(col("txn_date_str"), "MM/dd/yyyy")) \
>     # Conditional logic
>     .withColumn("risk_flag",
>         when(col("balance") < 0, "HIGH")
>         .when(col("balance") < 1000, "MEDIUM")
>         .otherwise("LOW")) \
>     # Handle nulls
>     .withColumn("region", coalesce(col("region"), lit("UNKNOWN"))) \
>     # Drop unwanted columns
>     .drop("txn_date_str", "internal_id") \
>     # Filter bad records
>     .filter(col("account_id").isNotNull())
> ```

---

**Q18. How do you perform joins in PySpark? What types of joins are supported?**

> **Answer:**
> ```python
> # Inner join
> df_result = df_accounts.join(df_transactions, on="account_id", how="inner")
>
> # Left join
> df_result = df_accounts.join(df_transactions, on="account_id", how="left")
>
> # Multiple join keys
> df_result = df1.join(df2,
>     (df1["account_id"] == df2["acc_id"]) &
>     (df1["date"] == df2["txn_date"]),
>     how="inner")
>
> # Broadcast join (for small lookup tables)
> from pyspark.sql.functions import broadcast
> df_result = df_large.join(broadcast(df_small), on="code", how="left")
> ```
>
> **Join types**: `inner`, `left`, `right`, `outer`/`full`, `left_semi`, `left_anti`, `cross`

---

**Q19. How do you remove duplicates in PySpark?**

> **Answer:**
> ```python
> # Remove exact duplicates across all columns
> df_dedup = df.dropDuplicates()
>
> # Remove duplicates based on specific columns
> df_dedup = df.dropDuplicates(["account_id", "txn_date"])
>
> # Keep latest record per account (using window)
> from pyspark.sql.window import Window
> from pyspark.sql.functions import row_number, desc
>
> w = Window.partitionBy("account_id").orderBy(desc("updated_ts"))
> df_dedup = df \
>     .withColumn("rn", row_number().over(w)) \
>     .filter(col("rn") == 1) \
>     .drop("rn")
> ```

---

**Q20. How do you write the final transformed data to the target table in S3?**

> **Answer:**
> ```python
> # Write as Parquet (most common for big data)
> df_final.write \
>     .mode("overwrite") \           # or "append"
>     .partitionBy("txn_date") \
>     .option("compression", "snappy") \
>     .parquet("s3://bucket/final/target_table/")
>
> # Register as Hive table on EMR (so forecasters can query via HiveQL/SparkSQL)
> spark.sql("""
>     CREATE EXTERNAL TABLE IF NOT EXISTS final_db.target_table
>     USING PARQUET
>     LOCATION 's3://bucket/final/target_table/'
> """)
>
> # Or use saveAsTable
> df_final.write \
>     .mode("overwrite") \
>     .saveAsTable("final_db.target_table")
> ```

---

## 7. AutoSys — Job Scheduling

**Q21. What is AutoSys and how is it used for job scheduling?**

> **Answer:**
> AutoSys is an **enterprise workload automation tool** used to schedule, monitor, and manage production jobs.
>
> **In this project:**
> - Every PySpark job on EMR is scheduled as an **AutoSys job**.
> - Jobs run on defined schedules (daily, hourly, event-driven).
> - AutoSys handles **dependencies** — Job B runs only after Job A completes successfully.
> - Sends **alerts** (email/pager) on job failure.
> - Provides **audit trail** of all job executions.

---

**Q22. What are the key AutoSys job attributes?**

> **Answer:**
> ```
> insert_job: INGEST_FLAT_FILES
> job_type: CMD
> command: /scripts/trigger_emr_job.sh
> machine: prod-scheduler-01
> owner: data_team
>
> /* Schedule */
> run_calendar: weekdays
> start_times: "06:00"
>
> /* Dependencies */
> condition: success(FILE_ARRIVAL_CHECK)
>
> /* Alerts */
> alarm_if_fail: 1
> notification_emailaddress: data-team@company.com
> ```
>
> **Key job types:**
> - `CMD` — Runs a shell command/script
> - `FW` — File Watcher (triggers when a file arrives in a path)
> - `BOX` — Container for grouping related jobs

---

**Q23. What is a File Watcher job in AutoSys? Why is it important for flat file ingestion?**

> **Answer:**
> A **File Watcher (FW) job** monitors a directory and triggers downstream jobs **only when the expected file arrives**.
>
> ```
> insert_job: WATCH_SOURCE_FILE
> job_type: FW
> watch_file: /landing/source_files/accounts_*.csv
> watch_interval: 60           ← Check every 60 seconds
> ```
>
> **Why important for flat file ingestion:**
> - Source files may arrive at unpredictable times.
> - Don't want to run ingestion job on an empty/missing file.
> - File Watcher ensures pipeline starts **only when file is present**.
> - Downstream ingestion job has condition: `success(WATCH_SOURCE_FILE)`.

---

**Q24. What happens if an AutoSys job fails? How do you handle it?**

> **Answer:**
> 1. AutoSys sets job status to `FAILURE`.
> 2. All **downstream dependent jobs** are blocked (won't run).
> 3. **Alert is sent** via email/pager to on-call team.
> 4. On-call engineer investigates logs.
> 5. Fix the issue (reprocess file, fix script, etc.).
> 6. **Force-start** the failed job: `sendevent -E FORCE_STARTJOB -J JOB_NAME`
> 7. Or restart from checkpoint if pipeline supports it.
>
> **AutoSys key commands:**
> ```bash
> autorep -J JOB_NAME          # Check job status
> sendevent -E STARTJOB -J JOB_NAME    # Start a job
> sendevent -E KILLJOB -J JOB_NAME     # Kill a running job
> sendevent -E FORCE_STARTJOB -J JOB_NAME  # Force start
> ```

---

## 8. Azure DevOps — CI/CD & Pipelines

**Q25. What is Azure DevOps and how is it used to build and release pipelines?**

> **Answer:**
> Azure DevOps is Microsoft's **DevOps platform** providing version control, CI/CD pipelines, and project management.
>
> **In this project:**
> - **Repos**: PySpark scripts and config files are version-controlled in Azure Repos (Git).
> - **Build Pipeline**: On code commit → runs unit tests → packages code → uploads to S3.
> - **Release Pipeline**: Promotes code from Dev → QA → Production with approvals.

---

**Q26. Explain the CI/CD flow for deploying a PySpark script to EMR.**

> **Answer:**
> ```
> Developer pushes code
>        ↓
> Azure DevOps Build Pipeline triggers
>        ↓
> Run unit tests (pytest)
>        ↓
> Package scripts (zip or copy .py files)
>        ↓
> Upload scripts to S3 (aws s3 cp script.py s3://bucket/scripts/)
>        ↓
> Release Pipeline — Deploy to Dev EMR
>        ↓
> QA approval gate
>        ↓
> Release Pipeline — Deploy to Production S3
>        ↓
> AutoSys job picks up new script from S3 on next scheduled run
> ```

---

**Q27. What is a YAML pipeline in Azure DevOps?**

> **Answer:**
> ```yaml
> # azure-pipelines.yml
> trigger:
>   branches:
>     include:
>       - main
>
> stages:
>   - stage: Build
>     jobs:
>       - job: Test
>         steps:
>           - script: pip install pytest
>           - script: pytest tests/
>
>   - stage: Deploy
>     dependsOn: Build
>     jobs:
>       - job: UploadToS3
>         steps:
>           - script: |
>               aws s3 cp src/transform.py s3://bucket/scripts/transform.py
>               aws s3 cp src/ingest.py s3://bucket/scripts/ingest.py
> ```

---

## 9. Python Scripting

**Q28. How do you read a config file in Python for a metadata-driven pipeline?**

> **Answer:**
> ```python
> import json
> import boto3
>
> # Read config from S3
> s3 = boto3.client("s3")
> response = s3.get_object(Bucket="bucket-name", Key="config/pipeline_config.json")
> config = json.loads(response["Body"].read())
>
> # Config structure
> # {
> #   "source_name": "accounts",
> #   "s3_path": "s3://bucket/landing/accounts/",
> #   "delimiter": ",",
> #   "target_table": "final_db.accounts_final",
> #   "load_type": "incremental",
> #   "watermark_col": "updated_date"
> # }
>
> source_path = config["s3_path"]
> load_type = config["load_type"]
> ```

---

**Q29. How do you use boto3 to interact with S3 from Python?**

> **Answer:**
> ```python
> import boto3
>
> s3 = boto3.client("s3")
>
> # List files in a bucket/prefix
> response = s3.list_objects_v2(Bucket="my-bucket", Prefix="landing/accounts/")
> files = [obj["Key"] for obj in response.get("Contents", [])]
>
> # Check if file exists
> try:
>     s3.head_object(Bucket="my-bucket", Key="landing/accounts/file.csv")
>     print("File exists")
> except s3.exceptions.ClientError:
>     print("File not found")
>
> # Upload file to S3
> s3.upload_file("local_script.py", "my-bucket", "scripts/local_script.py")
>
> # Download file from S3
> s3.download_file("my-bucket", "landing/file.csv", "/tmp/file.csv")
> ```

---

**Q30. How do you handle exceptions in a Python/PySpark pipeline?**

> **Answer:**
> ```python
> import logging
>
> logging.basicConfig(level=logging.INFO)
> logger = logging.getLogger(__name__)
>
> def run_pipeline(config):
>     try:
>         logger.info(f"Starting pipeline for {config['source_name']}")
>
>         df = spark.read.csv(config["s3_path"], header=True)
>         logger.info(f"Records read: {df.count()}")
>
>         df_transformed = apply_transformations(df)
>
>         df_transformed.write.mode("overwrite").parquet(config["target_path"])
>         logger.info("Pipeline completed successfully")
>         update_audit_table(config["source_name"], "SUCCESS")
>
>     except Exception as e:
>         logger.error(f"Pipeline failed: {str(e)}")
>         update_audit_table(config["source_name"], "FAILED")
>         raise  # Re-raise so AutoSys marks job as FAILURE
> ```

---

## 10. Data Quality & Validation

**Q31. What data quality checks do you apply before writing to the final table?**

> **Answer:**
> ```python
> def validate(df, source_name):
>     errors = []
>
>     # 1. Row count check
>     count = df.count()
>     if count == 0:
>         errors.append("Empty dataframe — no records to process")
>
>     # 2. Null check on key columns
>     null_count = df.filter(col("account_id").isNull()).count()
>     if null_count > 0:
>         errors.append(f"{null_count} records have null account_id")
>
>     # 3. Duplicate check
>     dup_count = count - df.dropDuplicates(["account_id"]).count()
>     if dup_count > 0:
>         errors.append(f"{dup_count} duplicate account_ids found")
>
>     # 4. Date format validation
>     invalid_dates = df.filter(col("txn_date").isNull() &
>                               col("txn_date_str").isNotNull()).count()
>
>     # 5. Volume threshold (today vs yesterday)
>     # if count < yesterday_count * 0.5: alert
>
>     if errors:
>         raise DataQualityException("\n".join(errors))
>
>     return df
> ```

---

## 11. Final Table / Target Layer

**Q32. What is meant by "final table backed by S3 table or view"?**

> **Answer:**
> The **final table** is an **external Hive/Spark table** — the table definition (schema, column names) is stored in the Hive Metastore, but the actual data files live in **S3**.
>
> ```sql
> CREATE EXTERNAL TABLE final_db.forecast_data (
>     account_id STRING,
>     balance DOUBLE,
>     txn_date DATE,
>     risk_flag STRING
> )
> STORED AS PARQUET
> LOCATION 's3://bucket/final/forecast_data/';
> ```
>
> - **Forecasters** query this table using Spark SQL, Hive, Athena, or Presto.
> - Dropping the table does NOT delete the S3 data (external table).
> - A **view** on top of this table can simplify forecasters' queries:
> ```sql
> CREATE VIEW final_db.v_forecast_data AS
>     SELECT * FROM final_db.forecast_data
>     WHERE txn_date >= date_sub(current_date(), 90);
> ```

---

**Q33. How do you do incremental writes to the final table?**

> **Answer:**
> **Option 1 — Partition-based incremental:**
> ```python
> # Only overwrite today's partition, keep historical data
> spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
>
> df_today.write \
>     .mode("overwrite") \
>     .partitionBy("txn_date") \
>     .parquet("s3://bucket/final/forecast_data/")
> ```
>
> **Option 2 — MERGE (if using Delta/Iceberg):**
> ```python
> df_new.createOrReplaceTempView("incoming")
>
> spark.sql("""
>     MERGE INTO final_db.forecast_data AS target
>     USING incoming AS source
>     ON target.account_id = source.account_id
>         AND target.txn_date = source.txn_date
>     WHEN MATCHED THEN UPDATE SET *
>     WHEN NOT MATCHED THEN INSERT *
> """)
> ```

---

## 12. Scenario-Based Questions

**Q34. A flat file arrives with 10% more columns than expected. What do you do?**

> **Answer:**
> 1. Schema validation step at ingestion detects extra columns.
> 2. Log a warning: "Extra columns detected: [col_x, col_y]".
> 3. **Option A (strict)**: Reject the file, alert the source team, wait for corrected file.
> 4. **Option B (flexible)**: Select only expected columns, ignore extras, continue processing.
> ```python
> expected_cols = ["account_id", "balance", "txn_date"]
> df_clean = df.select([c for c in expected_cols if c in df.columns])
> ```
> 5. Notify the data owner regardless — schema changes need to be tracked.

---

**Q35. AutoSys job ran but the source file was empty. How do you handle it?**

> **Answer:**
> 1. At the start of the pipeline, check file record count:
> ```python
> df = spark.read.csv(s3_path, header=True)
> if df.count() == 0:
>     logger.warning("Empty file received — skipping processing")
>     update_audit_table(source, "SKIPPED", record_count=0)
>     sys.exit(0)  # Exit with success code — AutoSys marks SUCCESS
> ```
> 2. Decide: Should this be a warning or a failure?
>    - If empty file is expected sometimes → `SKIPPED` with success exit code.
>    - If empty file means upstream issue → `FAILURE` with alert.
> 3. Set up a separate AutoSys alert for zero-record runs.

---

**Q36. The EMR cluster runs out of memory during transformation. How do you debug and fix it?**

> **Answer:**
> **Debug steps:**
> 1. Check **YARN logs**: `yarn logs -applicationId application_xxxx`
> 2. Check **Spark UI** → Executors tab → GC time, memory used.
> 3. Look for `java.lang.OutOfMemoryError` or `ExecutorLostFailure`.
>
> **Common fixes:**
> | Root Cause | Fix |
> |---|---|
> | Data skew | Salt keys, AQE skew handling |
> | Too many partitions in memory | `spark.sql.shuffle.partitions` tuning |
> | Large broadcast join | Increase `spark.sql.autoBroadcastJoinThreshold` or disable broadcast |
> | `collect()` on large data | Replace with distributed write |
> | Insufficient executor memory | Increase `spark.executor.memory` / scale up EMR cluster |
> | Memory overhead too low | Increase `spark.executor.memoryOverhead` |

---

**Q37. How do you ensure the pipeline is idempotent (safe to re-run)?**

> **Answer:**
> An **idempotent pipeline** produces the same result even if run multiple times.
>
> **Techniques:**
> 1. **Overwrite mode**: `df.write.mode("overwrite")` — re-running replaces previous output.
> 2. **Dynamic partition overwrite**: Only overwrites affected partitions.
> 3. **Watermark control**: Re-run uses the same watermark → same source data processed.
> 4. **Audit table check**: If `last_run_status = SUCCESS` for the same run date → skip or re-run with confirmation.
> 5. **MERGE instead of INSERT**: Avoids duplicate inserts.
>
> **Why it matters**: AutoSys may re-trigger a job due to infrastructure failure — idempotency ensures data isn't duplicated or corrupted.

---

## 13. Behavioral Questions

**Q38. Why are you interested in this role/project?**

> **Suggested Answer:**
> "The tech stack aligns well with my experience — I've been working with PySpark and big data pipelines at Cognizant. I'm particularly excited about the AWS side — EMR, S3 — as my current project is on Azure. This gives me the opportunity to expand my cloud expertise and work on a different big data ecosystem. The flat file ingestion framework also mirrors a metadata-driven ingestion framework I built on my current project, so I'm confident I can contribute quickly."

---

**Q39. Tell me about a time you improved pipeline performance.**

> **Suggested Answer (from your experience):**
> "On eCDP, we had an Oracle ingestion job taking 6+ hours due to single-threaded JDBC reads. I analyzed the issue using Spark UI and identified that the read was happening without partitioning. I implemented parallel JDBC reads using `numPartitions`, `partitionColumn`, `lowerBound`, and `upperBound`. This dropped extraction time from 6 hours to under 45 minutes — roughly a 87% improvement. I also added periodic Iceberg compaction to fix small file issues downstream."

---

**Q40. How do you handle a situation where you don't know something the interviewer asks?**

> **Suggested Answer:**
> "I'm transparent about the boundaries of my knowledge. I'd say — 'I haven't worked with that specific tool hands-on, but based on my understanding of how similar systems work, my initial approach would be X. I'd validate that by reading the documentation and testing in a dev environment.' I believe being honest and showing problem-solving thinking is more valuable than pretending to know everything."

---

## 🔑 Quick Revision Cheat Sheet

```
SOURCE FILES
  └── Flat files (CSV, TSV, fixed-width)
  └── Arrive in S3 landing zone

INGESTION
  └── File Watcher (AutoSys FW job) detects file
  └── Triggers ingestion AutoSys CMD job
  └── PySpark reads from S3 → validates → transforms

COMPUTE
  └── AWS EMR cluster (Master + Core + Task nodes)
  └── PySpark scripts stored in S3
  └── spark-submit via EMR Steps

CI/CD
  └── Code in Azure DevOps Repos
  └── Build Pipeline: test → package → upload to S3
  └── Release Pipeline: Dev → QA → Prod (with approvals)

SCHEDULING
  └── AutoSys schedules all production jobs
  └── File Watcher → Ingestion Job → Transform Job → Final Write

OUTPUT
  └── Final Parquet table in S3
  └── External Hive table registered on top
  └── View created for forecasters
  └── Forecasters query via SparkSQL / Athena / Hive
```

---

*Prepared for: Mayuresh Uttam Patil | Round 2 Interview Prep | April 2026*
*Stack: PySpark · Python · AWS EMR · AWS S3 · AutoSys · Azure DevOps*
