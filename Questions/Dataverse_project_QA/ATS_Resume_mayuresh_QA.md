# Interview Preparation Guide — Mayuresh Patil
### Senior Data Engineer | PySpark · Apache Iceberg · AWS · Medallion Architecture

---

## Table of Contents

1. [PySpark & Spark Internals](#1-pyspark--spark-internals)
2. [Apache Iceberg & Data Lakehouse](#2-apache-iceberg--data-lakehouse)
3. [Medallion Architecture & Pipeline Design](#3-medallion-architecture--pipeline-design)
4. [MDM, Golden Record & XREF Resolution](#4-mdm-golden-record--xref-resolution)
5. [Data Ingestion — Oracle JDBC & Salesforce APIs](#5-data-ingestion--oracle-jdbc--salesforce-apis)
6. [Data Governance — Modak Nabu](#6-data-governance--modak-nabu)
7. [AWS Ecosystem](#7-aws-ecosystem)
8. [Delta Lake & Snowflake](#8-delta-lake--snowflake)
9. [SAP XML Parsing & Hierarchy Flattening](#9-sap-xml-parsing--hierarchy-flattening)
10. [Performance Optimization (Spark)](#10-performance-optimization-spark)
11. [Python Scripting & Automation](#11-python-scripting--automation)
12. [Coding Questions with Solutions](#12-coding-questions-with-solutions)
13. [System Design Questions](#13-system-design-questions)
14. [Behavioral / Scenario-Based Questions](#14-behavioral--scenario-based-questions)

---

## 1. PySpark & Spark Internals

### Q1. Explain the Spark execution model — from job to task.

**Answer:**

When you call an action (e.g., `.count()`, `.write()`), Spark creates a **Job**. Each job is broken into **Stages** separated by shuffle boundaries. Each stage contains **Tasks** — one task per partition.

```
Action → Job → Stages (separated by shuffles) → Tasks (1 per partition)
```

- **Driver**: Orchestrates execution, builds the DAG, schedules tasks.
- **Executor**: Runs tasks on worker nodes; holds data in memory or spills to disk.
- **DAG Scheduler**: Splits the logical plan into stages.
- **Task Scheduler**: Assigns tasks to executors.

**Scenario from your resume:** When processing 10M+ records daily from Oracle into Iceberg, Spark creates one job per ingestion run. Shuffle stages occur when you join XREF tables or repartition for Iceberg write optimization.

---

### Q2. What is the difference between narrow and wide transformations?

**Answer:**

| Type | Definition | Examples | Shuffle? |
|------|-----------|---------|---------|
| Narrow | Each input partition contributes to exactly one output partition | `map`, `filter`, `union` | No |
| Wide | Multiple input partitions contribute to one output partition | `groupBy`, `join`, `distinct`, `repartition` | Yes |

Wide transformations cause **shuffles** — data moves across the network. This is expensive.

**Optimization tip from your work:** When joining XREF tables with patient data, if the XREF table is small, use a **broadcast join** to avoid the shuffle entirely:

```python
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_xref_df), on="patient_id", how="left")
```

---

### Q3. What are the different join strategies in Spark and when do you use each?

**Answer:**

| Strategy | When to use | Hint |
|---------|------------|------|
| Broadcast Hash Join | One table < `spark.sql.autoBroadcastJoinThreshold` (default 10MB) | `broadcast()` |
| Sort Merge Join | Both tables large, join key sortable | Default for large joins |
| Shuffle Hash Join | Medium-size tables | `SHUFFLE_HASH` hint |
| Cartesian Join | No join condition (cross join) | Avoid unless necessary |

```python
# Force broadcast join — used when XREF table is small
from pyspark.sql.functions import broadcast
result = fact_df.join(broadcast(dim_df), "key")

# Force sort merge join
result = df1.join(df2.hint("MERGE"), "key")
```

---

### Q4. Explain lazy evaluation in Spark. Why is it important?

**Answer:**

Spark does **not execute transformations immediately**. It builds a **Directed Acyclic Graph (DAG)** of transformations and only executes when an **action** is called.

```python
# Nothing executes here — just builds the plan
df = spark.read.parquet("s3://bucket/data/")
df_filtered = df.filter(df.status == "ACTIVE")
df_joined = df_filtered.join(xref_df, "patient_id")

# Execution starts HERE
df_joined.write.format("iceberg").save("...")
```

**Why it matters:**
- Spark's Catalyst optimizer can reorder, combine, or eliminate steps before execution.
- Predicate pushdown: filters applied before data is read from disk/S3.
- In your eCDP project, this means Spark pushes `WHERE` clauses into the Iceberg scan, reading only relevant files.

---

### Q5. What is the difference between `cache()` and `persist()`?

**Answer:**

- `cache()` = `persist(StorageLevel.MEMORY_AND_DISK)` — shorthand with default storage level.
- `persist()` allows you to specify the storage level:

```python
from pyspark import StorageLevel

# Cache in memory only (fast, but may OOM on large data)
df.persist(StorageLevel.MEMORY_ONLY)

# Cache in memory; spill to disk if needed (default of cache())
df.persist(StorageLevel.MEMORY_AND_DISK)

# Serialize before storing (lower memory footprint, slower access)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Always unpersist when done
df.unpersist()
```

**Scenario:** In your 10TB Iceberg migration, you'd persist intermediate XREF lookup DataFrames that are reused across multiple pipeline stages to avoid re-reading from S3.

---

## 2. Apache Iceberg & Data Lakehouse

### Q6. What is Apache Iceberg and how is it different from a standard Parquet table on S3?

**Answer:**

Apache Iceberg is an **open table format** for large analytic datasets. It adds a metadata layer on top of data files (Parquet, ORC, Avro) stored on object storage.

**Standard Parquet on S3 (Hive-style):**
- Partitions tracked by directory structure only.
- No ACID — concurrent writes can corrupt data.
- Schema evolution is painful.
- No row-level deletes.

**Apache Iceberg:**
- Full **ACID transactions** (atomic commits).
- **Time travel** — query data as of a past snapshot.
- **Schema evolution** — add/rename/drop columns safely.
- **Partition evolution** — change partitioning without rewriting data.
- **Row-level deletes** using delete files.
- **Hidden partitioning** — users don't need to know partition layout.

```python
# Writing to Iceberg in PySpark
df.writeTo("catalog.db.patient_records") \
  .using("iceberg") \
  .partitionedBy("year", "month") \
  .createOrReplace()

# Time travel — audit trail query
spark.read \
  .option("as-of-timestamp", "2024-01-01 00:00:00") \
  .table("catalog.db.patient_records")

# Snapshot-based time travel
spark.read \
  .option("snapshot-id", "3051729675574597004") \
  .table("catalog.db.patient_records")
```

---

### Q7. Explain Iceberg's metadata architecture — catalog, snapshot, manifest list, manifest file.

**Answer:**

```
Catalog (Glue / Hive Metastore / REST)
  └── Table Metadata (metadata.json)
        └── Snapshot (one per commit)
              └── Manifest List (.avro) — list of manifest files
                    └── Manifest File (.avro) — list of data files with statistics
                          └── Data Files (.parquet on S3)
```

- **Catalog**: Tracks the current metadata file location. Single pointer.
- **Table Metadata JSON**: Contains schema, partition spec, current snapshot ID.
- **Snapshot**: Represents the state of the table after each transaction.
- **Manifest List**: Lists all manifest files for a snapshot.
- **Manifest File**: Lists data files with partition values and column stats (min/max/null counts) — enables **file pruning** at query time.

**Why this matters for audits (eCDP use case):** Each write creates a new snapshot. You can query any past snapshot for regulatory audit trails without maintaining separate audit tables.

---

### Q8. How does Iceberg handle ACID transactions?

**Answer:**

Iceberg uses **optimistic concurrency control**:

1. Reader reads the current snapshot.
2. Writer reads the current snapshot, prepares changes, and attempts to commit a **new snapshot** by atomically swapping the metadata pointer.
3. If two writers conflict, one succeeds and the other retries.

**ACID properties:**
- **Atomicity**: A commit either fully succeeds or is rolled back. No partial writes.
- **Consistency**: The table is always in a valid state.
- **Isolation**: Readers see a consistent snapshot; writers don't block readers.
- **Durability**: Once committed, the snapshot is permanent in object storage.

---

### Q9. What is Iceberg table maintenance and why is it needed?

**Answer:**

Over time, Iceberg accumulates small files and old snapshots. Maintenance tasks:

```python
from pyspark.sql import SparkSession

# 1. Expire old snapshots (keep last 7 days)
spark.sql("""
  CALL catalog.system.expire_snapshots(
    table => 'db.patient_records',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 5
  )
""")

# 2. Compact small files (bin packing)
spark.sql("""
  CALL catalog.system.rewrite_data_files(
    table => 'db.patient_records',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '134217728')  -- 128MB target
  )
""")

# 3. Rewrite manifests (optimize manifest scanning)
spark.sql("""
  CALL catalog.system.rewrite_manifests('db.patient_records')
""")
```

---

## 3. Medallion Architecture & Pipeline Design

### Q10. Explain Medallion Architecture and how you implemented it in eCDP.

**Answer:**

Medallion Architecture organizes data into three progressive quality layers:

```
S3 / Source Systems
     |
     v
[RAW / BRONZE]        — Exact copy of source data, never modified
     |                  Preserves original state; schema-on-read
     v
[TRUSTED / SILVER]    — Cleansed, deduplicated, validated
     |                  Schema enforced, data quality checks applied
     v
[CURATED / GOLD]      — Business-ready aggregations and domain models
                        Optimized for analytics and reporting
```

**In eCDP implementation:**

```python
# RAW layer — land exactly as received
def ingest_to_raw(source_df, table_name):
    source_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("SALESFORCE")) \
        .writeTo(f"catalog.raw.{table_name}") \
        .using("iceberg") \
        .option("write.format.default", "parquet") \
        .append()

# TRUSTED layer — cleanse and validate
def raw_to_trusted(raw_df):
    return raw_df \
        .dropDuplicates(["patient_id", "record_date"]) \
        .filter(col("patient_id").isNotNull()) \
        .withColumn("dob", to_date(col("dob"), "yyyy-MM-dd")) \
        .withColumn("processed_timestamp", current_timestamp())

# CURATED layer — business aggregation
def trusted_to_curated(trusted_df, xref_df):
    return trusted_df \
        .join(broadcast(xref_df), "patient_id") \
        .groupBy("golden_id", "region") \
        .agg(
            count("*").alias("total_records"),
            max("record_date").alias("last_seen_date")
        )
```

---

### Q11. How do you handle full load vs incremental load in your pipelines?

**Answer:**

```python
def run_ingestion(mode: str, table_name: str, source_query: str):
    """
    mode: 'full' or 'incremental'
    """
    if mode == "full":
        df = spark.read.jdbc(
            url=JDBC_URL,
            table=f"({source_query}) t",
            properties=JDBC_PROPS
        )
        # Overwrite entire table
        df.writeTo(f"catalog.raw.{table_name}") \
          .using("iceberg") \
          .overwritePartitions()

    elif mode == "incremental":
        # Get last successful watermark
        watermark = get_last_watermark(table_name)

        df = spark.read.jdbc(
            url=JDBC_URL,
            table=f"({source_query} WHERE updated_at > '{watermark}') t",
            properties=JDBC_PROPS
        )

        # Merge (upsert) into Iceberg
        df.createOrReplaceTempView("incremental_data")
        spark.sql(f"""
            MERGE INTO catalog.raw.{table_name} AS target
            USING incremental_data AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Update watermark
        update_watermark(table_name, df.agg(max("updated_at")).collect()[0][0])
```

---

## 4. MDM, Golden Record & XREF Resolution

### Q12. What is a Golden Record in MDM and how did you build XREF mappings?

**Answer:**

A **Golden Record** is the single, authoritative, de-duplicated version of an entity (e.g., a patient) across multiple source systems.

**The problem:** System A calls the same patient "John Smith, DOB 1985-04-12, ID: SF-001". System B calls them "J. Smith, 12/04/1985, ID: ORA-4482". They're the same person — MDM must link them.

**XREF (Cross-Reference) table** maps each source system ID to a single **Golden ID**:

```
+----------+---------------+------------+
| golden_id | source_system | source_id  |
+----------+---------------+------------+
| GLD-0001  | SALESFORCE    | SF-001     |
| GLD-0001  | ORACLE        | ORA-4482   |
| GLD-0001  | LEGACY_CRM    | CRM-99123  |
+----------+---------------+------------+
```

**Building the XREF in PySpark:**

```python
from pyspark.sql.functions import col, coalesce, sha2, concat_ws, trim, lower

def build_xref(salesforce_df, oracle_df):
    # Normalize match keys across systems
    sf_normalized = salesforce_df.select(
        col("sf_id").alias("source_id"),
        lit("SALESFORCE").alias("source_system"),
        trim(lower(col("first_name"))).alias("first_name"),
        trim(lower(col("last_name"))).alias("last_name"),
        col("dob")
    )

    ora_normalized = oracle_df.select(
        col("ora_id").alias("source_id"),
        lit("ORACLE").alias("source_system"),
        trim(lower(col("first_name"))).alias("first_name"),
        trim(lower(col("last_name"))).alias("last_name"),
        col("dob")
    )

    # Create a deterministic match key (composite hash)
    def add_match_key(df):
        return df.withColumn(
            "match_key",
            sha2(concat_ws("|", col("first_name"), col("last_name"), col("dob").cast("string")), 256)
        )

    sf_keyed = add_match_key(sf_normalized)
    ora_keyed = add_match_key(ora_normalized)

    # Assign golden IDs using match key
    combined = sf_keyed.union(ora_keyed)
    golden_ids = combined.select("match_key") \
        .distinct() \
        .withColumn("golden_id", concat(lit("GLD-"), monotonically_increasing_id()))

    xref = combined.join(golden_ids, "match_key") \
        .select("golden_id", "source_system", "source_id")

    return xref
```

---

### Q13. How do you handle data conflicts when merging records into a Golden Record?

**Answer:**

Define a **survivorship strategy** — rules for which source system's value "wins" when fields conflict.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, coalesce

# Source system priority: ORACLE > SALESFORCE > LEGACY_CRM
SOURCE_PRIORITY = {"ORACLE": 1, "SALESFORCE": 2, "LEGACY_CRM": 3}

def build_golden_record(xref_df, oracle_df, sf_df):
    # Tag each record with source priority
    oracle_tagged = oracle_df.withColumn("priority", lit(1)) \
                             .withColumn("source", lit("ORACLE"))
    sf_tagged = sf_df.withColumn("priority", lit(2)) \
                     .withColumn("source", lit("SALESFORCE"))

    all_records = oracle_tagged.union(sf_tagged)

    # Join with XREF to get golden_id
    with_golden = all_records.join(xref_df, 
                                    (all_records.source == xref_df.source_system) & 
                                    (all_records.id == xref_df.source_id))

    # Pick the highest-priority (lowest priority number) record per field
    window = Window.partitionBy("golden_id").orderBy("priority")

    golden = with_golden \
        .withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .select("golden_id", "first_name", "last_name", "email", "dob", "address")

    return golden
```

---

## 5. Data Ingestion — Oracle JDBC & Salesforce APIs

### Q14. How do you read large Oracle tables via JDBC in PySpark efficiently?

**Answer:**

Naive JDBC reads use a **single connection** and are not parallelized. For large tables, use `numPartitions` with a partition column:

```python
oracle_df = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//oracle-host:1521/servicename") \
    .option("dbtable", "CLINICAL_RECORDS") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "oracle.jdbc.OracleDriver") \
    \
    # Parallel read config — critical for 10M+ records
    .option("partitionColumn", "RECORD_ID") \
    .option("lowerBound", "1") \
    .option("upperBound", "10000000") \
    .option("numPartitions", "50") \  # 50 parallel JDBC connections
    \
    # Performance tuning
    .option("fetchsize", "10000") \   # Rows per network round trip
    .load()
```

**When there's no numeric partition column:**

```python
# Use a custom predicate list
predicates = [
    "MOD(RECORD_ID, 4) = 0",
    "MOD(RECORD_ID, 4) = 1",
    "MOD(RECORD_ID, 4) = 2",
    "MOD(RECORD_ID, 4) = 3"
]

oracle_df = spark.read.jdbc(
    url=JDBC_URL,
    table="CLINICAL_RECORDS",
    predicates=predicates,
    properties=JDBC_PROPS
)
```

---

### Q15. How do you ingest data from Salesforce API and handle pagination?

**Answer:**

Salesforce uses SOQL (Salesforce Object Query Language) and the REST API with pagination via `nextRecordsUrl`.

```python
import requests
import json
from pyspark.sql import SparkSession

def fetch_salesforce_data(instance_url: str, access_token: str, soql: str):
    """Paginate through all Salesforce records."""
    headers = {"Authorization": f"Bearer {access_token}"}
    endpoint = f"{instance_url}/services/data/v57.0/query"
    
    records = []
    url = f"{endpoint}?q={soql}"

    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        records.extend(data["records"])

        # Salesforce gives nextRecordsUrl when there are more pages
        if not data.get("done"):
            url = instance_url + data["nextRecordsUrl"]
        else:
            url = None

    return records

# Convert to Spark DataFrame
records = fetch_salesforce_data(SF_URL, SF_TOKEN, "SELECT Id, Name, DOB__c FROM Patient__c")
schema = infer_schema_from_records(records)  # or define explicitly
df = spark.createDataFrame(records, schema=schema)
```

---

## 6. Data Governance — Modak Nabu

### Q16. What is Modak Nabu and how did you use it for data governance?

**Answer:**

Modak Nabu is a **data quality and pipeline orchestration framework** designed for enterprise data lakehouses. It provides:

- **Validation rules**: Schema checks, null checks, range checks, referential integrity.
- **Dependency management**: Define job dependencies declaratively; Nabu resolves execution order.
- **Retry logic**: Automatic retries with configurable backoff.
- **Audit logging**: Tracks every pipeline run, validation outcome, and data quality score.
- **Data Lineage**: Traces how data flows from source to consumption.

**How you used it in eCDP:**

1. **Programmatic validation rules** — rather than writing ad-hoc checks, you defined rules in Nabu that applied consistently across 300+ pipeline jobs.
2. **Dependency resolution** — downstream Curated layer jobs waited for Trusted layer jobs to complete and pass validation.
3. **Retry workflows** — if an Oracle JDBC read timed out, Nabu retried with exponential backoff, preventing the 15+ hours of downtime.

**Example validation rule (conceptual Nabu config):**

```yaml
validations:
  - name: patient_id_not_null
    type: null_check
    column: patient_id
    threshold: 0.0   # 0% nulls allowed

  - name: dob_range_check
    type: range_check
    column: date_of_birth
    min: "1900-01-01"
    max: "today"

  - name: status_values
    type: value_set_check
    column: status
    allowed_values: ["ACTIVE", "INACTIVE", "PENDING"]
    threshold: 0.99  # 99% must be valid
```

---

## 7. AWS Ecosystem

### Q17. Compare AWS Glue vs EMR vs Lambda for data processing. When do you use each?

**Answer:**

| Service | Best For | Limitations |
|---------|---------|------------|
| **AWS Glue** | Serverless ETL, small-medium data, short jobs | Cold start time, limited Spark config control |
| **AWS EMR** | Large-scale Spark jobs, full cluster control, long-running | Requires cluster management, higher cost for short jobs |
| **AWS Lambda** | Event-driven, lightweight transforms, API triggers | 15-min max, 10GB memory limit, no Spark |

**From your experience:**

```
eCDP (10TB+ data, 10M+ records/day) → EMR: Need full Spark tuning control
LTIMindtree (15+ Lambda functions) → Lambda: Event-driven triggers, lightweight API integrations
Barclays migration → Glue + Athena: Serverless ad-hoc analytics after migration
```

---

### Q18. Explain how you optimized Spark jobs on AWS EMR.

**Answer:**

```python
# Spark configuration tuning for EMR (set in SparkSession or spark-submit)

spark = SparkSession.builder \
    .appName("eCDP_Ingestion") \
    \
    # Executor sizing: (core_count - 1) executors per node, leaving 1 for OS
    .config("spark.executor.instances", "20") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.memoryOverhead", "2g") \  # Off-heap for JVM overhead
    \
    # Driver (avoid OOM on large collect operations)
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "4g") \
    \
    # Shuffle optimization (300+ analytical queries)
    .config("spark.sql.shuffle.partitions", "400") \  # Tune based on data size
    .config("spark.sql.adaptive.enabled", "true") \   # AQE — auto-tunes at runtime
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \  # Handle data skew
    \
    # Iceberg + S3 optimization
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .getOrCreate()
```

---

## 8. Delta Lake & Snowflake

### Q19. How did you migrate SAP data simultaneously to Delta Lake and Snowflake?

**Answer:**

```python
def dual_target_write(transformed_df, table_name: str):
    """
    Write the same DataFrame to both Delta Lake (Databricks) 
    and Snowflake in a single pipeline pass.
    """
    # Cache the transformed data — avoid computing twice
    transformed_df.cache()
    transformed_df.count()  # Materialize the cache

    # Target 1: Delta Lake on Databricks
    transformed_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"dbfs:/finops/{table_name}")

    # Register in Hive metastore
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS finops.{table_name}
        USING DELTA
        LOCATION 'dbfs:/finops/{table_name}'
    """)

    # Target 2: Snowflake
    snowflake_options = {
        "sfURL": SF_ACCOUNT_URL,
        "sfDatabase": "FINOPS_DB",
        "sfSchema": "FINANCIAL",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "DATA_ENGINEER",
        "sfUser": SF_USER,
        "sfPassword": SF_PASSWORD,
        "dbtable": table_name
    }

    transformed_df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**snowflake_options) \
        .mode("overwrite") \
        .save()

    transformed_df.unpersist()
```

---

### Q20. What is the difference between Delta Lake and Apache Iceberg?

**Answer:**

| Feature | Delta Lake | Apache Iceberg |
|---------|-----------|---------------|
| Origin | Databricks | Netflix (Apache) |
| ACID | Yes | Yes |
| Time Travel | Yes | Yes |
| Multi-engine | Limited (improving) | Excellent (Spark, Trino, Flink, etc.) |
| Partition Evolution | No (must rewrite) | Yes |
| Row-level deletes | Copy-on-write or MoR | Copy-on-write or MoR |
| Metadata | Transaction log (JSON) | Manifest files (Avro) |
| Catalog needed | Not strictly | Yes (Hive, Glue, REST) |
| Best with | Databricks | AWS Glue, open ecosystems |

**Your real-world mapping:**
- eCDP → **Iceberg** (AWS S3 + open ecosystem, multi-engine access)
- FinOps PoC → **Delta Lake** (Databricks-native, fast prototyping)

---

## 9. SAP XML Parsing & Hierarchy Flattening

### Q21. How did you flatten nested SAP financial hierarchies from XML into relational tables?

**Answer:**

SAP financial hierarchies are often exported as deeply nested XML:

```xml
<CostCenterHierarchy>
  <Node id="CC_ROOT" name="Global">
    <Node id="CC_AMER" name="Americas">
      <Node id="CC_US" name="United States">
        <CostCenter id="CC_001" name="Engineering" budget="500000"/>
        <CostCenter id="CC_002" name="Marketing" budget="300000"/>
      </Node>
    </Node>
  </Node>
</CostCenterHierarchy>
```

**Flattening approach:**

```python
import xml.etree.ElementTree as ET
from pyspark.sql import Row

def parse_hierarchy(xml_path: str):
    """Recursively flatten SAP hierarchy XML into rows."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    records = []

    def traverse(node, parent_id=None, level=0, path=""):
        node_id = node.get("id")
        node_name = node.get("name")
        current_path = f"{path}/{node_name}" if path else node_name

        records.append(Row(
            node_id=node_id,
            node_name=node_name,
            parent_id=parent_id,
            level=level,
            hierarchy_path=current_path,
            node_type="INTERNAL" if len(node) > 0 else "LEAF",
            budget=float(node.get("budget", 0))
        ))

        for child in node:
            traverse(child, parent_id=node_id, level=level + 1, path=current_path)

    traverse(root)
    return records

# Parallelize with Spark
records = parse_hierarchy("sap_hierarchy.xml")
df = spark.createDataFrame(records)

# Result: flat table with 29 relational records
df.show(truncate=False)
```

**Result schema:**
```
node_id | node_name      | parent_id | level | hierarchy_path        | node_type | budget
CC_US   | United States  | CC_AMER   | 2     | Global/Americas/US    | INTERNAL  | 0
CC_001  | Engineering    | CC_US     | 3     | Global/Americas/US/.. | LEAF      | 500000
```

---

## 10. Performance Optimization (Spark)

### Q22. How did you achieve a 40% improvement in analytical query speed?

**Answer:**

The 40% improvement on 300+ Iceberg queries came from a combination of techniques:

**1. Shuffle partition tuning:**
```python
# Default is 200 — too few for 10M+ records, too many for small data
# Rule of thumb: target 128MB-256MB per partition
estimated_data_size_mb = 50000  # 50GB after filter
target_partition_size_mb = 200
optimal_partitions = estimated_data_size_mb // target_partition_size_mb  # 250

spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))

# Better: use AQE (Adaptive Query Execution) — auto-tunes at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64mb")
```

**2. Broadcast join for XREF tables:**
```python
# XREF table is ~500MB — within broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "600mb")

result = clinical_df.join(broadcast(xref_df), "patient_id")
```

**3. Iceberg file pruning — partition + column stats:**
```python
# Query pushes predicates into Iceberg metadata scan
# Only reads relevant Parquet files — no full table scan
spark.sql("""
    SELECT golden_id, count(*) 
    FROM catalog.curated.patient_records
    WHERE year = 2024 AND month = 3   -- partition pruning
    AND status = 'ACTIVE'              -- column stats pruning (min/max)
    GROUP BY golden_id
""")
```

**4. File compaction before heavy analytics:**
```python
# Compact small files into 128MB target files before batch analytics
spark.sql("""
    CALL catalog.system.rewrite_data_files(
        table => 'db.patient_records',
        where => 'year = 2024'
    )
""")
```

---

### Q23. How do you handle data skew in Spark?

**Answer:**

Data skew occurs when some partitions have far more data than others, causing a few tasks to run much longer.

**Detection:**
```python
# Look at Spark UI — if max task duration >> median duration, you have skew
# Or check partition sizes:
df.groupBy(spark_partition_id()).count().orderBy("count", ascending=False).show()
```

**Solutions:**

```python
# 1. Salting — add a random key to break up skewed partitions
from pyspark.sql.functions import concat, lit, floor, rand

SALT_FACTOR = 10

# On the large (skewed) side: add salt
large_df_salted = large_df.withColumn(
    "salted_key",
    concat(col("patient_id"), lit("_"), (floor(rand() * SALT_FACTOR)).cast("string"))
)

# On the small side: explode to match all salt values
from pyspark.sql.functions import explode, array
small_df_exploded = small_df.withColumn(
    "salt",
    explode(array([lit(i) for i in range(SALT_FACTOR)]))
).withColumn(
    "salted_key",
    concat(col("patient_id"), lit("_"), col("salt").cast("string"))
)

result = large_df_salted.join(small_df_exploded, "salted_key")

# 2. AQE skew join handling (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
```

---

## 11. Python Scripting & Automation

### Q24. How did you auto-generate SQL configurations using Python to save 15 hours/week?

**Answer:**

Manual SQL config generation was error-prone and repetitive. Python templating automated it:

```python
from jinja2 import Template
import yaml
import os

# Config template (Jinja2)
SQL_TEMPLATE = Template("""
-- Auto-generated: {{ table_name }} ingestion config
-- Generated at: {{ generated_at }}

CREATE TABLE IF NOT EXISTS raw.{{ table_name }} (
    {% for col in columns %}
    {{ col.name }} {{ col.type }}{% if not loop.last %},{% endif %}
    
    {% endfor %}
)
USING ICEBERG
PARTITIONED BY ({{ partition_col }})
LOCATION 's3://{{ bucket }}/raw/{{ table_name }}/';

-- Ingestion merge statement
MERGE INTO raw.{{ table_name }} AS target
USING staging.{{ table_name }}_stage AS source
ON target.{{ primary_key }} = source.{{ primary_key }}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
""")

def generate_configs(config_dir: str, output_dir: str):
    """Read YAML table definitions, emit SQL configs."""
    os.makedirs(output_dir, exist_ok=True)

    for yaml_file in os.listdir(config_dir):
        with open(f"{config_dir}/{yaml_file}") as f:
            config = yaml.safe_load(f)

        sql = SQL_TEMPLATE.render(
            table_name=config["table_name"],
            columns=config["columns"],
            partition_col=config["partition_col"],
            primary_key=config["primary_key"],
            bucket=config["s3_bucket"],
            generated_at=datetime.now().isoformat()
        )

        output_path = f"{output_dir}/{config['table_name']}.sql"
        with open(output_path, "w") as f:
            f.write(sql)

        print(f"Generated: {output_path}")

# Run
generate_configs("configs/tables/", "output/sql/")
```

---

## 12. Coding Questions with Solutions

### CQ1. Write a PySpark function to perform an upsert (MERGE) into an Iceberg table.

```python
from pyspark.sql import DataFrame

def iceberg_upsert(
    spark,
    source_df: DataFrame,
    target_table: str,
    merge_keys: list,
    update_cols: list = None
):
    """
    Perform an upsert into an Iceberg table.
    
    Args:
        source_df: New/updated records
        target_table: Full Iceberg table name (catalog.db.table)
        merge_keys: Columns to match on (e.g., ["patient_id"])
        update_cols: Columns to update on match; if None, updates all
    """
    source_df.createOrReplaceTempView("__merge_source__")

    # Build the ON clause
    on_clause = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    # Build the UPDATE SET clause
    if update_cols:
        set_clause = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
        update_stmt = f"WHEN MATCHED THEN UPDATE SET {set_clause}"
    else:
        update_stmt = "WHEN MATCHED THEN UPDATE SET *"

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING __merge_source__ AS source
        ON {on_clause}
        {update_stmt}
        WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)


# Usage
iceberg_upsert(
    spark,
    source_df=incremental_df,
    target_table="catalog.trusted.patient_records",
    merge_keys=["patient_id"],
    update_cols=["name", "email", "status", "updated_at"]
)
```

---

### CQ2. Deduplicate records keeping the latest version per patient.

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, to_timestamp
from pyspark.sql.window import Window

def deduplicate_latest(df: DataFrame, partition_cols: list, order_col: str) -> DataFrame:
    """
    Keep only the most recent record per entity.
    
    Args:
        partition_cols: Columns that identify a unique entity (e.g., ["patient_id"])
        order_col: Timestamp column to determine latest (e.g., "updated_at")
    """
    window = Window \
        .partitionBy([col(c) for c in partition_cols]) \
        .orderBy(col(order_col).desc())  # Most recent first

    return df \
        .withColumn("_rn", row_number().over(window)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")


# Usage
clean_df = deduplicate_latest(
    df=raw_patient_df,
    partition_cols=["patient_id"],
    order_col="record_timestamp"
)
```

---

### CQ3. Write a function to read a CSV/XML/TXT file from S3 dynamically based on file type.

```python
from pyspark.sql import DataFrame

def read_from_s3(spark, s3_path: str, file_type: str, schema=None) -> DataFrame:
    """
    Dynamically read raw files from S3 into a Spark DataFrame.
    
    Args:
        s3_path: Full S3 path (e.g., s3://bucket/prefix/file.csv)
        file_type: 'csv', 'txt', 'xml', 'parquet', 'json'
        schema: Optional StructType schema
    """
    reader = spark.read

    if schema:
        reader = reader.schema(schema)

    if file_type in ("csv", "txt"):
        return reader \
            .option("header", "true") \
            .option("inferSchema", "true" if not schema else "false") \
            .option("delimiter", "," if file_type == "csv" else "|") \
            .option("nullValue", "NULL") \
            .option("emptyValue", "") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .csv(s3_path)

    elif file_type == "xml":
        # Requires spark-xml library
        return reader \
            .format("xml") \
            .option("rowTag", "Record") \  # Adjust per SAP schema
            .load(s3_path)

    elif file_type == "parquet":
        return reader.parquet(s3_path)

    elif file_type == "json":
        return reader \
            .option("multiLine", "true") \
            .json(s3_path)

    else:
        raise ValueError(f"Unsupported file type: {file_type}")


# Usage
clinical_df = read_from_s3(spark, "s3://ecdp-raw/clinical/2024/06/data.csv", "csv")
sap_df = read_from_s3(spark, "s3://finops-raw/sap/hierarchy.xml", "xml")
```

---

### CQ4. Implement a watermark-based incremental load with retry logic.

```python
import time
import logging
from datetime import datetime
from pyspark.sql.functions import max as spark_max

logger = logging.getLogger(__name__)

def incremental_load_with_retry(
    spark,
    source_table: str,
    target_table: str,
    watermark_col: str,
    max_retries: int = 3,
    backoff_seconds: int = 30
):
    """
    Load records newer than the last watermark, with exponential retry.
    """
    attempt = 0
    last_error = None

    while attempt < max_retries:
        try:
            # 1. Get last watermark from audit table
            watermark_df = spark.sql(f"""
                SELECT COALESCE(MAX(last_watermark), '1900-01-01')
                FROM audit.pipeline_watermarks
                WHERE table_name = '{target_table}'
            """)
            watermark = watermark_df.collect()[0][0]
            logger.info(f"Watermark for {target_table}: {watermark}")

            # 2. Read only new/changed records
            source_df = spark.sql(f"""
                SELECT * FROM {source_table}
                WHERE {watermark_col} > '{watermark}'
            """)

            record_count = source_df.count()
            if record_count == 0:
                logger.info("No new records to process.")
                return

            logger.info(f"Processing {record_count} new records.")

            # 3. Write to target (upsert)
            source_df.createOrReplaceTempView("__incremental__")
            spark.sql(f"""
                MERGE INTO {target_table} AS t
                USING __incremental__ AS s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

            # 4. Update watermark
            new_watermark = source_df.agg(spark_max(watermark_col)).collect()[0][0]
            spark.sql(f"""
                MERGE INTO audit.pipeline_watermarks AS t
                USING (SELECT '{target_table}' AS table_name, '{new_watermark}' AS last_watermark) AS s
                ON t.table_name = s.table_name
                WHEN MATCHED THEN UPDATE SET t.last_watermark = s.last_watermark
                WHEN NOT MATCHED THEN INSERT *
            """)

            logger.info(f"Successfully loaded {record_count} records. New watermark: {new_watermark}")
            return  # Success

        except Exception as e:
            attempt += 1
            last_error = e
            wait = backoff_seconds * (2 ** (attempt - 1))  # Exponential backoff
            logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    raise RuntimeError(f"Pipeline failed after {max_retries} attempts. Last error: {last_error}")
```

---

### CQ5. Flatten a nested JSON structure (simulate nested Salesforce API response).

```python
from pyspark.sql.functions import col, explode_outer, from_json
from pyspark.sql.types import *

def flatten_nested_json(df, max_depth=3):
    """
    Recursively flatten all nested struct and array columns.
    """
    def get_nested_cols(schema, prefix=""):
        fields = []
        for field in schema.fields:
            full_name = f"{prefix}.{field.name}" if prefix else field.name
            col_alias = full_name.replace(".", "_")

            if isinstance(field.dataType, StructType):
                # Recurse into struct
                fields.extend(get_nested_cols(field.dataType, full_name))
            elif isinstance(field.dataType, ArrayType):
                # Return as-is (explode separately if needed)
                fields.append((full_name, col_alias, "array"))
            else:
                fields.append((full_name, col_alias, "scalar"))

        return fields

    nested_cols = get_nested_cols(df.schema)
    scalar_exprs = [
        col(c).alias(alias)
        for c, alias, t in nested_cols
        if t == "scalar"
    ]

    return df.select(scalar_exprs)


# Example: Salesforce nested response
sample_data = [({
    "Id": "SF-001",
    "Name": "John Smith",
    "Address": {
        "Street": "123 Main St",
        "City": "Boston",
        "State": "MA"
    },
    "LastModifiedDate": "2024-06-01"
},)]

schema = StructType([
    StructField("Id", StringType()),
    StructField("Name", StringType()),
    StructField("Address", StructType([
        StructField("Street", StringType()),
        StructField("City", StringType()),
        StructField("State", StringType()),
    ])),
    StructField("LastModifiedDate", StringType()),
])

df = spark.createDataFrame(sample_data, schema)
flat_df = flatten_nested_json(df)
flat_df.show()
# Result: Id | Name | Address_Street | Address_City | Address_State | LastModifiedDate
```

---

## 13. System Design Questions

### SD1. Design a real-time + batch data platform for pharmaceutical clinical data (eCDP extension).

**Answer:**

```
┌─────────────────────────────────────────────────────────────┐
│                        SOURCES                              │
│  Oracle DB │ Salesforce API │ CSV/XML Files │ IoT Devices   │
└──────┬─────────────┬──────────────┬──────────────┬──────────┘
       │             │              │              │
       ▼             ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                         │
│  JDBC Spark │ REST API Ingestor │ S3 Drop Zone │ Kafka       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              RAW LAYER (Iceberg on S3)                      │
│          Immutable. Schema-on-read. Audit trail.            │
└──────────────────────────┬──────────────────────────────────┘
                           │ Modak Nabu validates
                           ▼
┌─────────────────────────────────────────────────────────────┐
│            TRUSTED LAYER (Iceberg on S3)                    │
│     Cleansed │ Deduplicated │ XREF resolved │ MDM applied   │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│             CURATED LAYER (Iceberg on S3)                   │
│     Golden Records │ Aggregations │ Business KPIs           │
└──────────────────────────┬──────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         Analytics    Oracle Sync   Flat File Export
       (Athena/Spark)  (outbound)    (CSV/TXT)
```

**Key design decisions:**
- Iceberg for ACID + time travel (regulatory requirement)
- Modak Nabu for governance at every layer boundary
- Medallion architecture for lineage and auditability
- XREF/MDM resolved at Trusted layer — Curated always uses Golden IDs

---

### SD2. Design a FinOps cost optimization pipeline that monitors cloud spend.

**Answer:**

```
AWS Cost Explorer API → Lambda (daily pull) → S3 Raw
                                               ↓
                                    PySpark on Glue/EMR
                                    - Parse billing records
                                    - Join with resource tags
                                    - Compute dept attribution
                                               ↓
                                      Snowflake (FINOPS_DB)
                                    - Historical trends
                                    - Budget vs. actual
                                    - Anomaly detection
                                               ↓
                                      Dashboard (QuickSight)
                                    - Cost by team/project
                                    - Alerting on overruns
```

---

## 14. Behavioral / Scenario-Based Questions

### B1. "Tell me about the 60% delivery acceleration on the FinOps PoC."

**Structure your answer with STAR:**

- **Situation**: Client needed a PoC to validate migrating SAP financial data. Standard estimate was 8 weeks.
- **Task**: Deliver the PoC in 2.5 weeks to meet a business review deadline and secure continued investment.
- **Action**: 
  - Immediately mapped SAP hierarchy structure before writing any code.
  - Built reusable PySpark parsing modules from day one, not as an afterthought.
  - Ran Delta Lake and Snowflake writes in parallel (dual-target) instead of sequentially.
  - Daily syncs with stakeholders to prevent scope creep.
- **Result**: Delivered in 2.5 weeks (60% faster), validated the data model, secured $5,000 in additional revenue.

---

### B2. "How did you prevent 15+ hours of downtime using Nabu retry workflows?"

**Structure:**

- **Situation**: During critical quarterly reporting windows, Spark jobs occasionally failed on transient Oracle connection timeouts, stalling the entire pipeline.
- **Task**: Ensure zero unplanned downtime during reporting windows.
- **Action**: Configured Modak Nabu with retry logic — 3 retries with exponential backoff (30s, 60s, 120s). Set up alerting for failures that exhausted retries. Added health checks before Oracle JDBC reads.
- **Result**: Eliminated manual intervention, prevented 15+ hours of potential cumulative downtime, achieved 99.9% pipeline uptime.

---

### B3. "How do you handle regulatory compliance requirements in a pharmaceutical data platform?"

**Key points to cover:**

1. **Data immutability**: Raw layer in Iceberg is append-only. Source records are never modified.
2. **Audit trail**: Every Iceberg snapshot is timestamped and queryable. Regulators can query "what did this record look like on Date X?"
3. **Data lineage**: Modak Nabu tracks every transformation step — from Oracle source to Curated Golden Record.
4. **Access controls**: Column-level and row-level security enforced at the Curated layer.
5. **Data quality thresholds**: Validation rules with documented pass/fail rates maintained as evidence.
6. **Encryption**: S3 SSE-KMS for data at rest; TLS for data in transit.

---

### B4. "Describe a time you improved a legacy system's performance significantly."

**Answer using the 30% pipeline reduction example:**

- **Legacy state**: SQL scripts run serially, monolithic jobs, no partitioning strategy.
- **Analysis**: Used Spark UI to identify slow stages — shuffle was the bottleneck (too many small partitions post-join).
- **Changes made**:
  - Replaced serial SQL with modular PySpark following Medallion.
  - Tuned shuffle partitions from default 200 → 400 (matched data volume).
  - Enabled AQE for dynamic optimization.
  - Added broadcast joins for XREF lookups.
  - Implemented Iceberg file compaction to reduce small file reads.
- **Outcome**: 30% reduction in end-to-end runtime = significant EMR compute cost savings.

---

*Last updated: May 2026 | Tailored for Mayuresh Patil's profile*
