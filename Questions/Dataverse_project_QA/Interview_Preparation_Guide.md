# 📘 Data Engineering Interview Preparation Guide

---

## 👤 Professional Introduction

> **"Hi, I'm a Data Engineer with 3.5 years of hands-on experience building and maintaining scalable data pipelines and ETL/ELT frameworks.**
>
> In my current role, I have worked extensively with **PySpark, Python, and SQL** to design and implement end-to-end data ingestion pipelines — primarily loading data from **AWS S3 (CSV/TSV/JSON files)** into **Apache Iceberg tables** using an incremental and CDC-based approach.
>
> I have built and maintained a **multi-layered data architecture** consisting of:
> - **Raw Layer** — for initial data ingestion with explicit schema definition using StructType/StructField
> - **Transformation Layer (TL)** — for incremental logic, SCD handling via merge commands, and deleted record calculation using left anti joins
> - **Assets Transformation Layer (ATL)** — for complex business transformations and aggregations
> - **Outbound Layer** — for pushing logically created data to downstream systems like **Salesforce, Oracle, and S3 buckets**
>
> I have strong experience **resolving production issues** — including API burst limit breaches, out-of-memory errors in YARN executors, nested JSON flattening using `explode()`, and schema ambiguity problems during pipeline execution. I approach these systematically by analyzing logs, collaborating with seniors, and implementing both short-term fixes and long-term optimizations.
>
> On the data quality side, I have handled schema inference issues, null value handling with `eqNullSafe()`, and ensured accurate change data capture through parameterized Spark configurations.
>
> I'm currently deepening my expertise in **PySpark internals, SQL optimization, and system design for large-scale data platforms**, and I'm excited to bring this experience to a challenging Data Engineering role."**

---

## 🧠 Key Skills

`PySpark` · `Python` · `SQL` · `AWS S3` · `Apache Iceberg` · `ETL/ELT` · `CDC` · `SCD` · `Incremental Ingestion` · `Data Quality` · `YARN` · `JSON Processing` · `Spark Performance Tuning` · `Git` · `Pipeline Architecture`

---

## 📋 Table of Contents

1. [Scenario-Based Interview Questions (1–20)](#scenario-based-questions)
2. [Hands-On Coding Questions](#hands-on-questions)

---

## 🎯 Scenario-Based Interview Questions

---

### Q1. Tell me about a production issue you handled.

**Answer (STAR Method):**

**Situation:** After migrating one of our data ingestion pipelines to production, it ran successfully for a few days, then started failing on alternate days, and eventually failed continuously for an entire week. The pipeline was business-critical, so restoring stability was urgent.

**Task:** I was responsible for identifying the root cause and ensuring production stability.

**Action:**
- I thoroughly analyzed the production log file, but the logs did not clearly indicate the issue.
- I scheduled a call with my senior to perform a deeper investigation.
- After multiple rounds of analysis, we identified that we were hitting the **API burst limit** — however, the log did not explicitly state this.
- I further analyzed the API usage pattern across other pipelines and discovered that **two other pipelines were hitting the same API endpoint at the same time**, causing us to collectively exceed the burst limit.

**Result:**
- As a temporary fix, I **rescheduled my pipeline to run at a different time** when no other pipeline was hitting the same endpoint, ensuring no concurrent API calls were made.
- This resolved the issue temporarily and bought time for a permanent API throttling solution.

---

### Q2. Describe a complex ETL project you worked on.

**Answer:**

One complex ETL project I worked on involved building an **incremental data ingestion pipeline** that loaded CSV files from an **S3 bucket into Apache Iceberg tables**.

I was responsible for designing and implementing the complete logic to ensure accurate **Change Data Capture (CDC)** loaded into Iceberg.

**Key steps in the pipeline:**

- **Checkpoint Logic:** I filtered the latest CSV file from the S3 bucket, created a DataFrame with a defined schema to maintain data quality.
- **Incremental Record Calculation:** To identify incremental records, I subtracted the file_df from the table_df.
- **SCD Handling:** Once the incremental records were identified, we handled change data capture through the **MERGE command**, which inserted records into the Iceberg table.
- **Deleted Record Calculation:** Used **left anti join** to calculate records that were deleted from the source.
- After merging, the data was pushed to the next layer.

**Pipeline Architecture (4 Layers):**

| Layer | Purpose |
|-------|---------|
| Raw Layer | Initial ingestion with explicit schema |
| Transformation Layer (TL) | Incremental logic, SCD via merge, deleted record calculation |
| Assets Transformation Layer (ATL) | Complex business transformations & aggregations |
| Outbound Layer | Push data to Salesforce, Oracle, S3 |

---

### Q3. What specific complexities did you face when ingesting data from an API?

**Answer:**

I faced three main complexity areas:

**i) API Burst Limit:**
- There was a burst limit for **authentication** and another burst limit for the **data object** itself.
- When multiple pipelines hit the same endpoint concurrently, we exceeded the limit.
- **Solution:** Rescheduled pipelines at different time slots and parametrized the burst limit value in Spark config.

**ii) Nested JSON Values:**
- The API returned deeply nested JSON responses.
- **Solution:** Used `explode()` function to flatten the nested JSON structure.
- Also used `explode_outer()` to handle null cases within nested values.

**iii) Column Ambiguity from Nested JSON:**
- After flattening, we had **same column names** appearing multiple times, causing the pipeline to fail due to ambiguity.
- **Solution:** Used **different aliases** for same-named columns to resolve the ambiguity.

---

### Q4. How did you approach flattening nested JSON responses and reducing API calls?

**Answer:**

**Problem:** We had a nested JSON structure from the API and needed to flatten it efficiently while minimizing API calls.

**Approach:**

```python
# Flatten nested JSON using explode
from pyspark.sql.functions import explode, col

df_exploded = df.select("id", explode("items").alias("item"))
df_flat = df_exploded.select("id", "item.*")
```

- For **null-safe flattening**, used `explode_outer()` instead of `explode()` to retain rows with null arrays.
- To **reduce API calls**, implemented **batching** — instead of calling the API per record, we batched multiple IDs in a single request.
- Added **caching** at the DataFrame level for reused data: `df.cache()`

---

### Q5. Have you encountered performance bottlenecks in ETL pipelines? How did you diagnose and optimize them?

**Answer:**

**Yes. The most common bottlenecks I encountered:**

**i) Out of Memory (OOM) Error:**
- **Symptom:** Container killed by YARN — executor exceeded memory limit.
- **Root Cause:** Large DataFrames being joined without partitioning strategy.
- **Solution:**
  - Used `left anti join` with `eqNullSafe()` instead of standard joins.
  - Changed join strategy to broadcast join for smaller tables.
  - Tuned `spark.executor.memory` and `spark.driver.memory` in config.

**ii) Skewed Data in Joins:**
- **Solution:** Used `salting` technique to redistribute skewed keys.

**iii) Schema Inference Issues:**
- When reading CSV files, Spark's `inferSchema=True` was causing incorrect data type inference.
- **Solution:** Defined schema explicitly using `StructType` and `StructField`.

---

### Q6. Tell me about the left anti join problem you faced.

**Answer:**

**Problem:** While calculating deleted records via left anti join, the composite key columns could have **null values** in some cases.

```python
# Problematic join — null != null in standard join
df_deleted = df_a.join(df_b, on="composite_key", how="leftanti")
```

When the left anti join was performed, it was **treating null as deleted records** incorrectly — so it was marking wrong records with the deletion flag.

**Root Cause:** In standard SQL/Spark joins, `null <=> null` evaluates to `false`, so records with null keys were not matching and were incorrectly flagged as deleted.

**Solution:**

```python
from pyspark.sql.functions import col

# Using eqNullSafe to treat null == null as True
condition = col("df1.id").eqNullSafe(col("df2.id"))
df_deleted = df_a.join(df_b, condition, "leftanti")
```

- `eqNullSafe()` treats `null <=> null` as **True**, which correctly identifies actual deletions.
- The value was also **parametrized in Spark config** so it could be adjusted without code changes.

---

### Q7. How did you handle schema issues during the ingestion process?

**Answer:**

**Problem:** When reading CSV files from S3, I was using `inferSchema=True`, which caused multiple issues:

- Spark **wrongly inferred column types** — for example, a decimal column was inferred as string.
- When the inferred schema didn't match downstream expectations, the pipeline failed.
- Different runs could produce **different schemas** causing inconsistency.

**Solution:**

I defined the schema explicitly using `StructType` and `StructField`:

```python
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("created_date", StringType(), True)
])

df = spark.read.option("header", True).schema(schema).csv("s3://bucket/path/")
```

This ensured **consistent, correct schema** across all pipeline runs.

---

### Q8. How did you handle files with different delimiters in S3?

**Answer:**

**Problem:** We had a single codebase reading data from S3, but some files were **CSV** and others were **TSV** (tab-separated).

**Solution:** Added the `-option("delimiter")` parameter dynamically while reading the file:

```python
# For CSV
df_csv = spark.read.option("header", True).option("delimiter", ",").schema(schema).csv(path)

# For TSV
df_tsv = spark.read.option("header", True).option("delimiter", "\t").schema(schema).csv(path)

# Dynamic approach based on file extension
delimiter = "\t" if file_path.endswith(".tsv") else ","
df = spark.read.option("header", True).option("delimiter", delimiter).schema(schema).csv(file_path)
```

---

### Q9. Describe your pipeline architecture in detail (multi-layer design).

**Answer:**

Our pipeline had a **4-layer architecture**:

```
S3 (CSV/JSON) ──► Raw Layer ──► TL Layer ──► ATL Layer ──► Outbound Layer
                  (Ingest)     (Transform)   (Aggregate)   (Salesforce/Oracle/S3)
```

**1. Raw Layer:**
- Source: S3 CSV/TSV/JSON files
- Reads data with explicit schema
- Stores as-is into raw Iceberg tables
- No transformation — just ingestion

**2. Transformation Layer (TL):**
- Source is the Raw Layer
- 3 main components: **DDL file**, **Python (.py) file**, **Merge file**
- DDL: Contains table definitions for both staging and raw tables
- Python file: Contains ingestion script and merge logic
- Merge file: Handles SCD via merge command while merging data from staging to raw table
- Applies incremental logic and calculates deleted records via left anti join

**3. Assets Transformation Layer (ATL):**
- Source is TL Layer tables
- Similar structure and file components as TL
- Applies complex business transformations and aggregations based on requirements

**4. Outbound Layer:**
- Source is ATL tables
- Pushes logically created data to downstream systems: **Salesforce, Oracle, S3 bucket**
- Follows **truncate and load** approach in this layer

---

### Q10. How would you estimate storage for a new data pipeline with 5 million users?

**Answer:**

I would break this into three parts:

**i) Data Volume Estimation:**
- 5 million users × 5 KB per user per day = **25 GB/day**
- Considering 1024 factor: ≈ **250 GB/day**

**ii) Processing Requirements:**
- Determine if we need **full load** or **incremental load**
- Full load: Entire dataset reprocessed every run
- Incremental load: Only changed/new records processed

**iii) Storage and Scalability Planning:**
- Based on one-day consumption of ~250 GB, scale for redundancy (3x replication in cloud storage)
- Storage needed = **250 GB × 3 (redundancy) ≈ 750 GB/day**
- Monthly: ≈ **22.5 TB**
- Factor in compression (Parquet/ORC gives ~70% compression): **~7 TB/month actual**

---

### Q11. How would you estimate API calls for 1 million users per day?

**Answer:**

**Estimation Framework:**

- 1 million users/day
- Assume each user triggers **5 API calls** on average (login + data fetch + updates)
- Total: **5 million API calls/day**
- Per hour: ~208,000 calls/hour
- Per minute: ~3,472 calls/minute
- Per second: ~58 calls/second (RPS)

**Considerations:**
- Peak vs. off-peak traffic (peak can be 3–5x average)
- API **burst limit** — need to configure throttling accordingly
- Use **batching** to reduce call count
- Implement **retry logic with exponential backoff** for failures
- Cache frequently accessed, non-changing data to avoid redundant API calls

---

### Q12. Describe a time you handled multiple tasks simultaneously.

**Answer (STAR):**

**Situation:** During a critical project phase, I was simultaneously handling a production pipeline failure while also being in the middle of developing a new ATL layer transformation.

**Task:** Resolve the production issue without delaying the development deadline.

**Action:**
- Prioritized the production issue first as it was business-critical.
- Documented the exact state of my development work so I could resume without loss.
- Fixed the production issue (API burst limit problem) within the day.
- Communicated timeline impact to my manager and got a 1-day extension.
- Resumed and completed the ATL development on the adjusted timeline.

**Result:** Production was stabilized and the new feature was delivered with only a 1-day delay with full stakeholder alignment.

---

### Q13. Describe a situation where requirements were unclear.

**Answer (STAR):**

**Situation:** I was asked to build a pipeline to handle "deleted records" but the business definition of "deleted" was not clearly specified — was it soft delete, hard delete, or records no longer appearing in the source?

**Task:** Clarify requirements and implement the correct logic.

**Action:**
- Scheduled a meeting with the business analyst and data owner.
- Asked specific questions: "Is a record deleted if it disappears from source? Or is there a flag?"
- Documented the agreed definition: "A record is deleted if it exists in the current Iceberg table but is absent from the latest source file."
- Implemented using **left anti join** based on the agreed definition.

**Result:** Built the correct deletion logic, confirmed by UAT testing with the business team.

---

### Q14. Tell me about a failure or mistake you made and what you learned.

**Answer (STAR):**

**Situation:** Early in a project, I used `inferSchema=True` while reading CSV files to save time, without validating the inferred schema.

**Mistake:** In production, Spark inferred a decimal column as a string type. Downstream aggregations failed silently, producing incorrect results that were only caught during UAT.

**Action:**
- Immediately defined explicit schemas using `StructType`/`StructField` for all CSV readers.
- Added **data validation checks** post-ingestion (null counts, type checks, row count comparisons).
- Documented this as a team best practice.

**Learning:** Never rely on `inferSchema` for production pipelines. Explicit schema definition is non-negotiable for data quality and pipeline reliability.

---

### Q15. How did you ensure data quality when collaborating with business users?

**Answer:**

**Approach:**

- **Schema Validation:** Defined explicit schemas so data type mismatches are caught at ingestion.
- **Null Checks:** Added null count assertions for critical columns post-ingestion.
- **Row Count Validation:** Compared source row count vs. loaded row count.
- **Business Rule Checks:** Worked with business users to document and implement validation rules (e.g., amount > 0, date not in future).
- **Data Profiling:** Ran summary stats (`df.describe()`, `df.summary()`) on key columns and shared with business for sign-off.
- **Reconciliation Reports:** Built automated reconciliation between source and target after each pipeline run.
- Used **`eqNullSafe()`** for null-safe comparisons in join conditions.

---

### Q16. How would you explain a technical issue to a non-technical stakeholder?

**Answer:**

**Example — Explaining API Burst Limit to a Business Manager:**

> "Think of our API like a water tap. There's a limit to how much water can flow through it per minute. Right now, three of our systems are all trying to use the same tap at the same time, so the total flow is exceeding the limit and the tap shuts off temporarily. Our pipeline is one of those three systems. The fix is to stagger the timing — each system uses the tap at a different time so we never exceed the limit. I've already implemented this schedule change, and the pipeline has been running stably for the past two days."

**Key principles when explaining technical issues:**
- Use **analogies** relevant to their world (water tap, traffic lane, etc.)
- Focus on **business impact** first (what failed, what data was affected)
- Explain the **fix** in plain language
- Give a **timeline** for resolution
- Avoid acronyms unless already established

---

### Q17. Why should we hire you? / Why do you want to move from your current company?

**Answer Framework:**

- "I have built a solid foundation in Data Engineering — designing multi-layer pipelines, handling complex production issues, and working with modern tools like PySpark, Iceberg, and CDC-based architectures."
- "I'm looking for a role where I can work at **larger scale**, contribute to more **diverse data systems**, and grow into areas like **data platform engineering and real-time processing**."
- "IBM's reputation in enterprise data solutions and its focus on hybrid cloud and AI-driven data platforms aligns perfectly with where I want to grow."
- "I want to bring my current expertise and continue learning in a more challenging environment."

---

### Q18. Where do you see yourself in 3–5 years in Data Engineering?

**Answer:**

"In the next 3–5 years, I see myself growing into a **Senior Data Engineer or Lead Data Engineer** role, where I'm not just building pipelines but also **architecting data platforms** and mentoring junior engineers.

Specifically, I want to:
- Deepen expertise in **real-time/streaming data** (Kafka, Spark Streaming)
- Gain hands-on experience with **cloud-native data platforms** (Databricks, Snowflake, BigQuery)
- Contribute to **data platform governance** — data lineage, cataloging, and observability
- Eventually move toward a **Data Architecture** track where I'm designing systems that serve multiple business units at scale."

---

### Q19. Have you ever disagreed with your manager? What happened?

**Answer (STAR):**

**Situation:** My manager initially wanted to use `inferSchema=True` for a new pipeline to speed up development timelines.

**My Position:** I disagreed because I had already faced production failures from this approach and knew it would cause downstream issues.

**Action:**
- I requested a brief 15-minute discussion to walk through the risks with a concrete example from our previous failure.
- I came prepared with the before/after comparison showing the incorrect schema inference.
- I proposed an alternative: using a schema registry or pre-defined schema class that would add minimal development time but ensure correctness.

**Result:** My manager agreed with the evidence-based approach. We adopted explicit schema definition as the team standard, and it has prevented several potential production issues since then.

---

### Q20. Have you ever automated a manual process?

**Answer (STAR):**

**Situation:** Our team was manually monitoring pipeline run statuses every morning — someone had to check logs and send a status update email to stakeholders.

**Task:** Automate this monitoring and reporting.

**Action:**
- Built a Python script that queried pipeline run logs and extracted success/failure status.
- Added logic to calculate run duration and row counts.
- Integrated with email notification system to auto-send a daily summary report.
- Scheduled via the existing **Modak pipeline** with a Git repo and Spark config.

**Result:** Saved ~30 minutes of manual effort every morning, and stakeholders now receive automated reports with consistent formatting before they start their workday.

---

## 💻 Hands-On Coding Questions

---

### H1. Write PySpark code to read a CSV file from S3 with an explicit schema.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

spark = SparkSession.builder.appName("S3Ingestion").getOrCreate()

# Define explicit schema — never use inferSchema in production
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("created_date", StringType(), True),
    StructField("status", StringType(), True)
])

df = spark.read \
    .option("header", True) \
    .option("delimiter", ",") \
    .schema(schema) \
    .csv("s3://your-bucket/your-path/file.csv")

df.show(5)
df.printSchema()
```

---

### H2. Write code to calculate incremental (new/updated) records between source and target.

```python
from pyspark.sql.functions import col

# file_df = latest data from S3
# table_df = existing data in Iceberg table

# Find new/updated records — records in file_df not in table_df (by updated_at or hash)
incremental_df = file_df.join(
    table_df.select("id", "updated_at"),
    on="id",
    how="left"
).filter(
    col("file_df.updated_at") > col("table_df.updated_at")
)

# Alternative: using anti join for purely new records
new_records_df = file_df.join(table_df, on="id", how="leftanti")
```

---

### H3. Write code to find deleted records using left anti join with null safety.

```python
from pyspark.sql.functions import col

# Records in table_df that are NOT in file_df = deleted records
# Standard left anti join (WRONG if composite key has nulls)
df_deleted_wrong = table_df.join(file_df, on="id", how="leftanti")

# CORRECT approach using eqNullSafe for composite keys with potential nulls
condition = (
    col("table_df.id").eqNullSafe(col("file_df.id")) &
    col("table_df.region").eqNullSafe(col("file_df.region"))
)

df_deleted = table_df.alias("table_df").join(
    file_df.alias("file_df"),
    condition,
    "leftanti"
).withColumn("is_deleted", lit(True))

df_deleted.show()
```

---

### H4. Write code to flatten nested JSON using explode.

```python
from pyspark.sql.functions import explode, explode_outer, col
from pyspark.sql.types import ArrayType

# Sample nested JSON structure
# {"id": 1, "items": [{"sku": "A1", "qty": 2}, {"sku": "B2", "qty": 5}]}

df = spark.read.json("s3://bucket/nested_data/")

# Use explode (drops rows where array is null or empty)
df_exploded = df.select("id", explode("items").alias("item"))
df_flat = df_exploded.select(
    col("id"),
    col("item.sku"),
    col("item.qty")
)

# Use explode_outer (keeps rows even if array is null)
df_exploded_safe = df.select("id", explode_outer("items").alias("item"))
df_flat_safe = df_exploded_safe.select(
    col("id"),
    col("item.sku"),
    col("item.qty")
)

df_flat.show()
```

---

### H5. Write code to handle column ambiguity after joining/exploding.

```python
from pyspark.sql.functions import col, explode_outer

# Problem: after explode, nested JSON had same column name as parent
# e.g., both parent and nested object have "id" column

df_exploded = df.select(
    col("id").alias("parent_id"),          # rename to avoid ambiguity
    explode_outer("items").alias("item")
)

df_flat = df_exploded.select(
    col("parent_id"),
    col("item.id").alias("item_id"),        # rename nested id
    col("item.name").alias("item_name"),
    col("item.value")
)

df_flat.show()
```

---

### H6. Write an Iceberg MERGE statement for SCD (CDC handling).

```python
# Using Spark SQL for Iceberg MERGE (SCD Type 1 — upsert)

spark.sql("""
    MERGE INTO iceberg_catalog.db.target_table AS target
    USING staging_table AS source
    ON target.id = source.id
    WHEN MATCHED AND source.updated_at > target.updated_at THEN
        UPDATE SET
            target.name = source.name,
            target.amount = source.amount,
            target.updated_at = source.updated_at,
            target.is_deleted = false
    WHEN MATCHED AND source.is_deleted = true THEN
        UPDATE SET target.is_deleted = true
    WHEN NOT MATCHED THEN
        INSERT (id, name, amount, created_at, updated_at, is_deleted)
        VALUES (source.id, source.name, source.amount,
                source.created_at, source.updated_at, false)
""")
```

---

### H7. Write code to handle out-of-memory issues using optimized joins.

```python
from pyspark.sql.functions import broadcast

# Scenario: large_df has millions of rows; small_df has thousands

# BAD: shuffle join (causes OOM for large datasets)
df_joined = large_df.join(small_df, on="id", how="inner")

# GOOD: broadcast join (small table sent to all executors, no shuffle)
df_joined_optimized = large_df.join(broadcast(small_df), on="id", how="inner")

# Also tune Spark memory config
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.driver.memory", "2g")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10MB
```

---

### H8. Write PySpark code to perform data quality checks.

```python
from pyspark.sql.functions import col, count, when, isnan, isnull

def data_quality_check(df, table_name):
    total_rows = df.count()
    print(f"\n{'='*50}")
    print(f"Data Quality Report: {table_name}")
    print(f"Total Rows: {total_rows}")
    print(f"{'='*50}")

    # Null check for each column
    null_counts = df.select([
        count(when(isnull(c) | isnan(c), c)).alias(c)
        for c in df.columns
    ])
    print("\nNull Counts per Column:")
    null_counts.show()

    # Duplicate check
    duplicate_count = total_rows - df.dropDuplicates().count()
    print(f"Duplicate Rows: {duplicate_count}")

    # Row count validation (compare with source)
    return {
        "total_rows": total_rows,
        "duplicate_count": duplicate_count
    }

# Usage
result = data_quality_check(df, "raw_sales_table")
```

---

### H9. Write SQL to find records deleted between two snapshots.

```sql
-- Find records present in yesterday's snapshot but absent in today's snapshot
SELECT 
    yesterday.id,
    yesterday.name,
    yesterday.amount,
    CURRENT_TIMESTAMP AS deleted_at
FROM 
    snapshot_yesterday AS yesterday
LEFT ANTI JOIN 
    snapshot_today AS today
ON 
    yesterday.id = today.id
    AND yesterday.region <=> today.region  -- null-safe comparison in SQL

-- Alternative using NOT EXISTS
SELECT id, name, amount
FROM snapshot_yesterday y
WHERE NOT EXISTS (
    SELECT 1 FROM snapshot_today t
    WHERE y.id = t.id
);
```

---

### H10. Write code to read TSV and CSV files dynamically based on file extension.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_file_dynamic(spark, file_path, schema):
    """
    Dynamically reads CSV or TSV files based on file extension.
    Always uses explicit schema — never inferSchema.
    """
    if file_path.endswith(".tsv"):
        delimiter = "\t"
    elif file_path.endswith(".csv"):
        delimiter = ","
    else:
        raise ValueError(f"Unsupported file format: {file_path}")

    df = spark.read \
        .option("header", True) \
        .option("delimiter", delimiter) \
        .option("nullValue", "") \
        .option("mode", "PERMISSIVE") \
        .schema(schema) \
        .csv(file_path)

    return df

# Usage
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", StringType(), True)
])

df = read_file_dynamic(spark, "s3://bucket/data/file.tsv", schema)
df.show()
```

---

### H11. Write code to implement checkpoint logic for incremental pipeline.

```python
from pyspark.sql.functions import col, max as spark_max
import boto3

def get_latest_file_from_s3(bucket, prefix):
    """Get the most recently modified file from S3."""
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    files = sorted(
        response.get("Contents", []),
        key=lambda x: x["LastModified"],
        reverse=True
    )
    
    if not files:
        raise FileNotFoundError("No files found in S3 path")
    
    latest_file = files[0]["Key"]
    return f"s3://{bucket}/{latest_file}"

def get_last_checkpoint(spark, checkpoint_table):
    """Read last processed timestamp from checkpoint table."""
    try:
        df = spark.read.format("iceberg").load(checkpoint_table)
        last_ts = df.agg(spark_max("processed_at")).collect()[0][0]
        return last_ts
    except Exception:
        return None  # First run — no checkpoint

# Usage
latest_file_path = get_latest_file_from_s3("my-bucket", "data/incoming/")
last_checkpoint = get_last_checkpoint(spark, "db.pipeline_checkpoint")

df = spark.read.schema(schema).csv(latest_file_path)

# Filter only new records since last checkpoint
if last_checkpoint:
    df = df.filter(col("updated_at") > last_checkpoint)

print(f"Processing {df.count()} new records from {latest_file_path}")
```

---

### H12. Write PySpark code to drop duplicates and fill nulls (data cleaning).

```python
from pyspark.sql.functions import col, when, lit

# Drop duplicates based on primary key, keeping latest record
df_deduped = df.orderBy(col("updated_at").desc()) \
               .dropDuplicates(["id"])

# Fill nulls with defaults
df_cleaned = df_deduped \
    .fillna({
        "name": "UNKNOWN",
        "amount": 0.0,
        "status": "ACTIVE",
        "region": "DEFAULT"
    })

# Conditional fill using when/otherwise
df_final = df_cleaned.withColumn(
    "status",
    when(col("status").isNull(), lit("INACTIVE"))
    .otherwise(col("status"))
)

df_final.show()
```

---

## 📌 Quick Reference Cheat Sheet

| Concept | PySpark Code |
|---------|-------------|
| Read CSV with schema | `spark.read.schema(schema).option("header",True).csv(path)` |
| Left Anti Join | `df1.join(df2, on="id", how="leftanti")` |
| Null-safe join | `col("a.id").eqNullSafe(col("b.id"))` |
| Flatten nested JSON | `df.select(explode("col").alias("item"))` |
| Broadcast Join | `df1.join(broadcast(df2), on="id")` |
| Drop duplicates | `df.dropDuplicates(["id"])` |
| Fill nulls | `df.fillna({"col": "default"})` |
| Cache DataFrame | `df.cache()` |
| Iceberg Merge | `spark.sql("MERGE INTO target USING source ON ...")` |
| Repartition | `df.repartition(200, col("id"))` |

---

*Prepared for IBM Data Engineering Interview | April 2026*
