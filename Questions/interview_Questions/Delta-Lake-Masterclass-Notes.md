# Delta Lake Masterclass — Complete Notes
**Based on: Delta Lake Masterclass | Azure Databricks | PySpark | From Zero-To-Expert**
**Duration: ~6 hours | Platform: Azure Databricks**

---

## TABLE OF CONTENTS

1. [Challenges with Data Lakes](#1-challenges-with-data-lakes)
2. [ACID Properties — Deep Dive](#2-acid-properties--deep-dive)
3. [DML Operations on Delta Tables](#3-dml-operations-on-delta-tables)
4. [The Delta Log — _delta_log Internals](#4-the-delta-log--_delta_log-internals)
5. [How Delta Lake Computes Latest State](#5-how-delta-lake-computes-latest-state)
6. [How the Transaction Log Scales](#6-how-the-transaction-log-scales)
7. [Concurrency Control](#7-concurrency-control)
8. [Time Travel & Versioning](#8-time-travel--versioning)
9. [Schema Validation & Enforcement](#9-schema-validation--enforcement)
10. [Schema Evolution](#10-schema-evolution)
11. [Converting Parquet to Delta](#11-converting-parquet-to-delta)
12. [Managed vs External Tables](#12-managed-vs-external-tables)
13. [Deletion Vectors — CoW vs MoR](#13-deletion-vectors--cow-vs-mor)
14. [Cloning Delta Tables](#14-cloning-delta-tables)
15. [The Small File Problem](#15-the-small-file-problem)
16. [OPTIMIZE & Bin Packing](#16-optimize--bin-packing)
17. [VACUUM Command](#17-vacuum-command)
18. [Z-Ordering](#18-z-ordering)
19. [Liquid Clustering](#19-liquid-clustering)
20. [Interview Q&A — Real World](#20-interview-qa--real-world)

---

## 1. Challenges with Data Lakes

### What is a Data Lake?
A Data Lake is a centralised repository of raw data stored in its native format (Parquet, CSV, JSON, ORC) on cheap object storage like AWS S3, Azure Data Lake Storage Gen2 (ADLS), or Google Cloud Storage.

### Core Problems with Traditional Data Lakes

#### Problem 1 — No ACID Support
- Traditional data lakes have NO transactional guarantees
- If a write job crashes halfway through, you get partial data on disk
- Readers can see incomplete data mid-write (dirty reads)
- No mechanism to roll back a failed operation

#### Problem 2 — No UPDATE, MERGE, DELETE
- Raw Parquet files are **immutable** — you cannot update a row
- To "update" data, you must rewrite the entire file
- No SQL-style `UPDATE SET`, `DELETE WHERE`, or `MERGE INTO` support
- Makes CDC (Change Data Capture) and SCD Type 2 patterns extremely painful

#### Problem 3 — Data Reliability & Quality Issues
- No schema enforcement on write — any schema can land
- A single bad file can break downstream pipelines
- No way to validate data before it's committed
- "Data swamp" problem — data quality degrades over time

#### Problem 4 — Lack of Isolation
- Multiple writers can overwrite each other's data
- No protection against concurrent read-write conflicts
- Last writer wins — data corruption possible

#### Problem 5 — Performance
- Small files accumulate over time (streaming, incremental loads)
- No built-in compaction — thousands of tiny Parquet files
- No statistics or indexing — queries scan ALL files
- Slow query performance even for simple filters

### How Delta Lake Solves These
| Problem | Delta Lake Solution |
|---------|-------------------|
| No ACID | Transaction log (_delta_log) provides ACID guarantees |
| No DML | Full UPDATE, DELETE, MERGE support |
| Data quality | Schema enforcement + schema evolution |
| Isolation | Optimistic Concurrency Control |
| Performance | OPTIMIZE, ZORDER, Liquid Clustering, Deletion Vectors |

---

## 2. ACID Properties — Deep Dive

ACID stands for **Atomicity, Consistency, Isolation, Durability**. These are the four properties that define a reliable transaction.

---

### 2.1 Atomicity

**Definition:** A transaction is treated as a single unit — either ALL operations succeed, or NONE of them happen. There is no partial success.

**Real-world analogy:** Transferring money from Account A to Account B.
- Step 1: Debit $500 from Account A
- Step 2: Credit $500 to Account B
- If Step 2 fails, Step 1 must be rolled back. You can't have money disappear from A without appearing in B.

**Without Atomicity in a Data Lake:**
```
Job writes 1000 files to ADLS...
Files 1-750 written successfully ✅
System crashes ❌
Files 751-1000 never written
Result: Partial data on disk — downstream reads corrupt data
```

**How Delta Lake achieves Atomicity:**
- All file paths are first written to the `_delta_log` as a JSON commit entry
- The commit either fully succeeds (log entry written) or fails completely
- If the job crashes before the log entry is written, the data files exist on disk but are **invisible** to Delta — they are orphan files
- VACUUM later cleans up orphan files
- Result: From Delta's perspective, the transaction never happened

---

### 2.2 Consistency

**Definition:** A transaction brings the database from one valid state to another valid state. All defined rules (schemas, constraints) are always satisfied.

**Real-world analogy:** A bank balance can never go below zero (constraint). Even if multiple transactions try to reduce it, the system must enforce this rule.

**In Delta Lake — Schema Enforcement:**
```python
# Define the expected schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

expected_schema = StructType([
    StructField("patient_id", IntegerType(), nullable=False),
    StructField("name",       StringType(),  nullable=True),
    StructField("age",        IntegerType(),  nullable=True),
])

# Try to write data with a different schema
bad_data = spark.createDataFrame([("Alice", "wrong_type")], ["name", "age"])

bad_data.write.format("delta").mode("append").save("/mnt/delta/patients")
# ❌ AnalysisException: A schema mismatch detected when writing to Delta table
```

**Consistency rules Delta enforces:**
- Column names must match exactly
- Data types must be compatible
- Non-nullable columns cannot receive null values
- New unexpected columns are rejected (unless mergeSchema=true)

---

### 2.3 Isolation

**Definition:** Concurrent transactions execute independently — they don't interfere with each other. A transaction in progress is invisible to other transactions until it commits.

**Isolation levels (from weakest to strongest):**
1. **Read Uncommitted** — can read in-progress (dirty) writes
2. **Read Committed** — only reads committed data
3. **Repeatable Read** — same row reads same value throughout transaction
4. **Serializable** — transactions appear to run sequentially

**Delta Lake provides Serializable isolation by default** (configurable to WriteSerializable for better performance).

**Without isolation — the lost update problem:**
```
Writer A reads employee salary = 50,000
Writer B reads employee salary = 50,000

Writer A adds bonus: 50,000 + 5,000 = 55,000 → writes 55,000
Writer B adds increment: 50,000 + 3,000 = 53,000 → writes 53,000

Final value: 53,000  ← Writer A's bonus is LOST
Correct value should be: 50,000 + 5,000 + 3,000 = 58,000
```

Delta Lake uses **Optimistic Concurrency Control (OCC)** to handle this — covered in Section 7.

---

### 2.4 Durability

**Definition:** Once a transaction is committed, it remains committed even if the system crashes, power fails, or errors occur. Committed data is permanent.

**How Delta Lake achieves Durability:**
- Committed transactions are written to the `_delta_log` before the operation is considered complete
- The `_delta_log` is stored on durable cloud storage (ADLS Gen2, S3, GCS) — not in memory
- Cloud storage itself provides durability guarantees (e.g., Azure Blob Storage: 11 nines of durability = 99.999999999%)
- Even if the Spark cluster crashes immediately after a commit, the log entry persists
- On restart, Delta reads the log and sees the committed state

**Durability guarantee flow:**
```
1. Data files written to ADLS
2. Transaction log entry written to _delta_log/ on ADLS  ← DURABLE
3. Job marks as complete
4. [System crashes here — it doesn't matter]
5. On restart, Delta reads _delta_log → sees committed state
```

---

## 3. DML Operations on Delta Tables

### Creating a Delta Table

```python
# Method 1: Write DataFrame as Delta
df.write \
  .format("delta") \
  .mode("overwrite") \
  .partitionBy("dept") \
  .save("/mnt/delta/employees")

# Method 2: CREATE TABLE SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS employees (
        id     INT,
        name   STRING,
        salary DOUBLE,
        dept   STRING
    )
    USING DELTA
    LOCATION '/mnt/delta/employees'
""")

# Method 3: CTAS (Create Table As Select)
spark.sql("""
    CREATE TABLE employees_copy
    USING DELTA
    AS SELECT * FROM employees WHERE dept = 'Engineering'
""")
```

### INSERT

```python
# PySpark
new_data = spark.createDataFrame([(6, "Frank", 88000, "Engineering")], ["id","name","salary","dept"])
new_data.write.format("delta").mode("append").save("/mnt/delta/employees")

# SQL
spark.sql("""
    INSERT INTO employees VALUES (6, 'Frank', 88000, 'Engineering')
""")

# INSERT OVERWRITE — replaces all existing data
spark.sql("INSERT OVERWRITE employees SELECT * FROM new_employees")
```

### UPDATE

```python
# PySpark using DeltaTable API
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/mnt/delta/employees")
dt.update(
    condition="dept = 'Marketing'",
    set={"salary": "salary * 1.10"}
)

# SQL
spark.sql("""
    UPDATE employees
    SET salary = salary * 1.10
    WHERE dept = 'Marketing'
""")
```

### DELETE

```python
# PySpark
dt.delete(condition="salary < 60000")

# SQL
spark.sql("DELETE FROM employees WHERE salary < 60000")
```

### MERGE (Upsert)

```python
# The most powerful DML — handles INSERT, UPDATE, DELETE in one operation
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/mnt/delta/employees")

target.alias("t").merge(
    source_df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdate(set={
    "name":   "s.name",
    "salary": "s.salary",
    "dept":   "s.dept"
}).whenNotMatchedInsert(values={
    "id":     "s.id",
    "name":   "s.name",
    "salary": "s.salary",
    "dept":   "s.dept"
}).whenNotMatchedBySourceDelete() \
  .execute()

# SQL equivalent
spark.sql("""
    MERGE INTO employees AS t
    USING source_data AS s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
""")
```

---

## 4. The Delta Log — _delta_log Internals

The `_delta_log` is the **heart of Delta Lake**. It is a directory of JSON and Parquet checkpoint files stored alongside your data files that records every change ever made to the table.

### Physical Layout

```
/mnt/delta/employees/
│
├── _delta_log/                        ← Transaction log directory
│   ├── 00000000000000000000.json      ← Version 0 — initial CREATE/INSERT
│   ├── 00000000000000000001.json      ← Version 1 — first UPDATE
│   ├── 00000000000000000002.json      ← Version 2 — DELETE
│   ├── 00000000000000000003.json      ← Version 3 — MERGE
│   ...
│   ├── 00000000000000000010.checkpoint.parquet  ← Checkpoint at version 10
│   └── _last_checkpoint                         ← Points to latest checkpoint
│
├── part-00000-abc123.snappy.parquet   ← Data file (version 0)
├── part-00001-def456.snappy.parquet   ← Data file (version 0)
├── part-00000-ghi789.snappy.parquet   ← New data file (version 1)
└── ...
```

### Anatomy of a JSON Log Entry

Every JSON commit entry contains **Actions**. The key actions are:

#### `add` action — records a new file being added
```json
{
  "add": {
    "path": "part-00000-abc123.snappy.parquet",
    "partitionValues": {"dept": "Engineering"},
    "size": 1048576,
    "modificationTime": 1717200000000,
    "dataChange": true,
    "stats": "{\"numRecords\":1000,\"minValues\":{\"salary\":50000},\"maxValues\":{\"salary\":120000},\"nullCount\":{\"age\":5}}"
  }
}
```

#### `remove` action — marks a file as deleted (logically, not physically)
```json
{
  "remove": {
    "path": "part-00000-abc123.snappy.parquet",
    "deletionTimestamp": 1717200060000,
    "dataChange": true,
    "extendedFileMetadata": true
  }
}
```

#### `metaData` action — records schema and table configuration
```json
{
  "metaData": {
    "id": "abc123-def456",
    "name": "employees",
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\"},{\"name\":\"name\",\"type\":\"string\"}]}",
    "partitionColumns": ["dept"],
    "configuration": {},
    "createdTime": 1717200000000
  }
}
```

#### `commitInfo` action — records who committed and when
```json
{
  "commitInfo": {
    "timestamp": 1717200060000,
    "userId":    "user123",
    "operation": "UPDATE",
    "operationParameters": {"predicate": "dept = 'Marketing'"},
    "readVersion": 2,
    "writeVersion": 3,
    "engineInfo": "Apache-Spark/3.5.0 Delta-Lake/3.0.0"
  }
}
```

### How an UPDATE Works Internally

When you run `UPDATE employees SET salary = salary * 1.10 WHERE dept = 'Marketing'`:

1. Delta reads the current log to find active files containing Marketing records
2. Those files are read into Spark, the update is applied in memory
3. **New Parquet files** are written with the updated records
4. A new JSON log entry is written with:
   - `remove` actions for the OLD files (that had Marketing records)
   - `add` actions for the NEW files (with updated salaries)
5. The old files still exist on disk — they are just no longer referenced by the log
6. Time travel still works because the old log entries point to the old files

```
Before UPDATE (Version 2):
  Active files: [file_A.parquet, file_B.parquet]

After UPDATE (Version 3 log entry):
  remove: file_A.parquet   ← Marketing records were here
  add:    file_C.parquet   ← New file with updated salaries
  
  Active files: [file_B.parquet, file_C.parquet]
  Orphaned: file_A.parquet ← Still on disk, VACUUM will clean later
```

---

## 5. How Delta Lake Computes the Latest State

Delta Lake must reconstruct the "current state" of a table from the log entries. This is called **state reconstruction**.

### The Naive Approach (Replay from version 0)
Read ALL log entries from version 0, replay every add/remove action to know which files are currently active.

**Problem:** After 10,000 commits, replaying all JSON files is extremely slow.

### The Checkpoint Solution

Every **10 commits** (by default), Delta consolidates all log entries into a single **Parquet checkpoint file**. This checkpoint represents the complete table state at that version.

```
Log files:  v0.json → v1.json → ... → v9.json → v10.checkpoint.parquet
                                                    ↑
                                        Contains the full state of the table
                                        as of version 10 in a single Parquet file
```

**State reconstruction algorithm:**
1. Read `_last_checkpoint` to find the most recent checkpoint (e.g., version 10)
2. Load `00000000000000000010.checkpoint.parquet` — this gives full state at v10
3. Replay only the JSON files AFTER the checkpoint (v11.json, v12.json, etc.)
4. Result: current state, with minimal reads

**Example with 1000 commits and checkpoint every 10:**
- Without checkpoints: read 1000 JSON files
- With checkpoints: read 1 checkpoint (v1000) + 0-9 JSON files = max 10 reads

### Column Statistics in Checkpoints

The checkpoint also stores per-file column statistics:
- `numRecords` — row count per file
- `minValues` — minimum value per column per file
- `maxValues` — maximum value per column per file
- `nullCount` — null count per column per file

These statistics enable **Data Skipping** — when you query `WHERE salary > 90000`, Delta reads the checkpoint stats and skips all files where `maxValues.salary < 90000`.

---

## 6. How the Transaction Log Scales

### The Problem at Scale

As a table grows to millions of commits:
- Checkpoint Parquet files can get large (one row per active data file)
- With billions of files, even a single checkpoint can be huge

### Delta Lake's Scaling Solution — Sidecar Files

For very large tables, Delta uses **sidecar files** (introduced in Delta Lake 3.x):
- The checkpoint Parquet file is split into multiple smaller Parquet files
- A `checkpoint.json` acts as a manifest listing all sidecar files
- Enables parallel reads of checkpoint data

### Log Compaction at Scale

Delta also supports **log compaction** for tables with many small JSON log entries:
```sql
-- Trigger a checkpoint manually
DESCRIBE HISTORY delta.`/mnt/delta/employees`  -- see versions
```

```python
# Force a checkpoint
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/mnt/delta/employees")
dt.createOrReplaceTempView("emp")
spark.sql("DESCRIBE HISTORY emp")
```

note -
- Small tables → one checkpoint Parquet file.
  
- Huge tables → checkpoint split into multiple sidecar Parquet files, each capped at a configured size (≈256 MB).

- Manifest ensures Spark reads all sidecars together in parallel.


---

## 7. Concurrency Control

### Why Concurrency Control is Needed

In a distributed environment, multiple jobs may try to write to the same Delta table simultaneously:
- Streaming job + batch job writing to the same table
- Multiple ADF pipelines running in parallel
- Compaction job running while ETL job writes

Without control, these concurrent writes can corrupt data.

---

### 7.1 Pessimistic Concurrency Control

**Philosophy:** "Assume conflict WILL happen — lock the resource before accessing it."

**How it works:**
1. Before reading/writing, acquire a lock on the table
2. Other writers must WAIT until the lock is released
3. Complete the operation
4. Release the lock

**Problems:**
- **Deadlocks** — Writer A locks Table X and waits for Table Y. Writer B locks Table Y and waits for Table X. Both wait forever.
- **Low throughput** — all writers serialize, even if they modify different partitions
- **Lock overhead** — managing locks requires a lock manager service

**Where it's used:** Traditional RDBMS (MySQL, PostgreSQL with `SELECT FOR UPDATE`)

**Delta Lake does NOT use pessimistic locking** for write operations (though some object stores use it for atomic file creation).

---

### 7.2 Optimistic Concurrency Control (OCC)

**Philosophy:** "Assume conflict is RARE — proceed without locking, then verify at commit time."

**Delta Lake's OCC Protocol:**

#### Phase 1: READ
- Read the current table version (e.g., version 5)
- Identify which files your operation will read and modify

#### Phase 2: WRITE (without locking)
- Perform your computation
- Write new data files to storage
- Prepare a new log entry (JSON commit)

#### Phase 3: VALIDATE & COMMIT
- Before committing, check: did any other writer commit between version 5 and now?
- If NO → commit your log entry as version 6 ✅
- If YES → check if the conflict is real:
  - Did the other writer modify files you READ? → **CONFLICT** → retry or fail
  - Did the other writer modify completely different files? → **NO CONFLICT** → commit anyway ✅

#### OCC Conflict Example

```
Initial state: Version 5

Writer A: UPDATE salary WHERE dept='Engineering'  → reads Engineering files
Writer B: UPDATE salary WHERE dept='Marketing'    → reads Marketing files

Both start at Version 5.
Writer B commits first → Version 6 (modified Marketing files)

Writer A tries to commit as Version 7:
  - Check: did Version 6 touch Engineering files? NO
  - No real conflict → Writer A commits as Version 7 ✅

Both succeed! No lock needed.
```

#### When OCC Fails

```
Writer A: UPDATE ALL employees salary += 10%     → reads ALL files
Writer B: UPDATE ALL employees salary += 5%      → reads ALL files

Writer B commits first → Version 6

Writer A tries to commit as Version 7:
  - Check: did Version 6 touch files Writer A read? YES (all files)
  - Real conflict detected → ConcurrentModificationException ❌
  - Writer A must retry from scratch
```

#### Configuring Isolation Level

```python
# Default: WriteSerializable (good balance of performance vs correctness)
spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "my_job")

# Strictest: Serializable (prevents all anomalies)
spark.sql("""
    ALTER TABLE employees
    SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')
""")

# For streaming where slight anomalies acceptable:
spark.sql("""
    ALTER TABLE employees
    SET TBLPROPERTIES ('delta.isolationLevel' = 'WriteSerializable')
""")
```

---

## 8. Time Travel & Versioning

Time travel is one of Delta Lake's most powerful features — the ability to query a table as it existed at any point in the past.

### How Time Travel Works

Every commit creates a new version. The log preserves all `add` and `remove` actions. To query version N, Delta:
1. Reads log entries up to version N
2. Collects all `add` actions with version ≤ N and `remove` actions with version > N
3. Those are the "active" files at version N

**The files are never physically deleted until VACUUM runs** — so time travel is possible as long as old files exist.

### Query by Version Number

```python
# PySpark
df_v0 = spark.read \
    .format("delta") \
    .option("versionAsOf", 0) \
    .load("/mnt/delta/employees")

df_v3 = spark.read \
    .format("delta") \
    .option("versionAsOf", 3) \
    .load("/mnt/delta/employees")

# SQL
spark.sql("SELECT * FROM employees VERSION AS OF 0").show()
spark.sql("SELECT * FROM employees VERSION AS OF 3").show()
```

### Query by Timestamp

```python
# PySpark
df_yesterday = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-15") \
    .load("/mnt/delta/employees")

df_hour_ago = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-15T10:00:00") \
    .load("/mnt/delta/employees")

# SQL
spark.sql("SELECT * FROM employees TIMESTAMP AS OF '2024-01-15'").show()
spark.sql("SELECT * FROM employees TIMESTAMP AS OF '2024-01-15T10:00:00.000Z'").show()
```

### DESCRIBE HISTORY

```sql
DESCRIBE HISTORY employees
```

Returns a table with columns:
- `version` — version number
- `timestamp` — when the commit happened
- `userId` — who made the change
- `operation` — WRITE, UPDATE, DELETE, MERGE, OPTIMIZE, etc.
- `operationParameters` — predicate used, columns affected
- `operationMetrics` — rows affected, files added/removed

### RESTORE — Roll Back to a Previous Version

```sql
-- Restore to version 2
RESTORE TABLE employees TO VERSION AS OF 2

-- Restore to a timestamp
RESTORE TABLE employees TO TIMESTAMP AS OF '2024-01-15T09:00:00'
```

**RESTORE creates a new version** — it doesn't delete existing history. It adds a new commit entry that brings back the state of the specified version.

### Time Travel Limits

Time travel only works as long as the underlying data files exist. Once VACUUM runs and deletes old files, those versions are no longer queryable:
- Default VACUUM retention: **7 days (168 hours)**
- After VACUUM, time travel is limited to the retention period

---

## 9. Schema Validation & Enforcement

### What is Schema Enforcement?

Schema enforcement (also called Schema Validation) means Delta Lake **rejects writes that don't match the table's defined schema**. This is enforced by default — you cannot turn it off.

### What Gets Checked

1. **Column names** — must match exactly (case-sensitive by default)
2. **Data types** — must be compatible (widening allowed, narrowing rejected)
3. **Nullable** — non-nullable columns cannot receive nulls
4. **Extra columns** — new columns in the source are rejected

### Schema Mismatch Examples

```python
# Current table schema: id INT, name STRING, salary DOUBLE

# ❌ Extra column rejected
wrong_df = spark.createDataFrame([(1, "Alice", 90000, "Engineering")],
                                  ["id", "name", "salary", "dept"])  # dept is new!
wrong_df.write.format("delta").mode("append").save("/mnt/delta/employees")
# AnalysisException: A schema mismatch detected when writing to the Delta table

# ❌ Wrong type rejected
wrong_type = spark.createDataFrame([(1, "Alice", "ninety-thousand")],
                                    ["id", "name", "salary"])  # salary as string!
wrong_type.write.format("delta").mode("append").save("/mnt/delta/employees")
# AnalysisException: Failed to merge incompatible data types DoubleType and StringType

# ❌ Missing required column
missing_col = spark.createDataFrame([(1, "Alice")], ["id", "name"])
missing_col.write.format("delta").mode("append").save("/mnt/delta/employees")
# AnalysisException: cannot resolve 'salary' given input columns: [id, name]
```

### Safe Type Widening (Allowed)

```python
# INT → LONG is safe (widening)
# FLOAT → DOUBLE is safe (widening)
# These are implicitly allowed
```

### Schema Enforcement in Streaming

```python
# Schema enforcement works in Structured Streaming too
query = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load("/mnt/landing/") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/employees") \
    .start("/mnt/delta/employees")
# If incoming JSON has unexpected columns → stream fails with AnalysisException
```

---

## 10. Schema Evolution

Schema Evolution is the ability to **automatically update the Delta table's schema** to accommodate new columns or type changes. It must be explicitly enabled — it's NOT on by default.

### Enable Schema Evolution

```python
# Option 1: Per-write option
df_with_new_col.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/mnt/delta/employees")

# Option 2: Spark session config (applies to all writes in session)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Option 3: SQL
spark.sql("""
    ALTER TABLE employees
    SET TBLPROPERTIES ('delta.autoOptimize.autoCompact'='true')
""")
```

### What Schema Evolution Allows

#### Adding new columns
```python
# Source has a new column 'bonus' that didn't exist in Delta table
df_with_bonus = spark.createDataFrame([
    (7, "Grace", 92000, "Engineering", 10000)
], ["id", "name", "salary", "dept", "bonus"])

df_with_bonus.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/mnt/delta/employees")

# Result: Delta table now has columns: id, name, salary, dept, bonus
# Old rows have NULL for bonus
```

#### Merging schemas in MERGE operations
```python
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

target.alias("t").merge(
    source_with_new_cols.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
# New columns from source are automatically added to target schema
```

### What Schema Evolution Does NOT Allow

- **Type narrowing** — DOUBLE → INT is not allowed (would lose precision)
- **Column deletion** — you cannot auto-remove columns via schema evolution
- **Renaming columns** — renaming requires explicit `ALTER TABLE RENAME COLUMN`

### ALTER TABLE for Manual Schema Changes

```sql
-- Add a column
ALTER TABLE employees ADD COLUMN bonus DOUBLE

-- Rename a column (Delta Lake 2.0+)
ALTER TABLE employees RENAME COLUMN salary TO annual_salary

-- Drop a column (Delta Lake 2.0+)
ALTER TABLE employees DROP COLUMN bonus

-- Change column type (widening only)
ALTER TABLE employees ALTER COLUMN id TYPE BIGINT

-- Add column with default value
ALTER TABLE employees ADD COLUMN country STRING DEFAULT 'India'
```

---

## 11. Converting Parquet to Delta

You can convert existing Parquet tables to Delta format **without copying or rewriting data files**.

### In-Place Conversion

```python
# Convert existing Parquet files to Delta
from delta.tables import DeltaTable

DeltaTable.convertToDelta(
    spark,
    "parquet.`/mnt/raw/employees`"
)

# With partitioned Parquet
DeltaTable.convertToDelta(
    spark,
    "parquet.`/mnt/raw/employees`",
    "dept STRING, year INT"
)

# SQL equivalent
spark.sql("""
    CONVERT TO DELTA parquet.`/mnt/raw/employees`
    PARTITIONED BY (dept STRING, year INT)
""")
```
note-

- This creates a _delta_log alongside your Parquet files at s3a://bucket/raw/abc.parquet.

- The dataset is now a Delta table by path, but it’s not known to the metastore.

- You can query it using .load("s3a://bucket/raw/abc.parquet"), but not with spark.sql("SELECT * FROM abc") yet.


**What happens during conversion:**
1. Delta scans all Parquet files and collects statistics
2. Creates the `_delta_log` directory
3. Writes a version 0 checkpoint with all existing files as `add` actions
4. The original Parquet files are NOT moved or copied
5. From now on, the directory behaves as a Delta table

**Time travel starts from conversion** — you cannot time travel to before the conversion point.

---

## 12. Managed vs External Tables

### Managed Tables

- Delta Lake owns both the **metadata** AND the **data files**
- Data is stored in the default warehouse location (e.g., `dbfs:/user/hive/warehouse/`)
- **DROP TABLE deletes both metadata AND data files** ← DANGEROUS

```sql
-- Managed table (no LOCATION specified)
CREATE TABLE employees_managed (
    id     INT,
    name   STRING,
    salary DOUBLE
) USING DELTA

-- Data is stored at: dbfs:/user/hive/warehouse/employees_managed/
-- DROP TABLE employees_managed → deletes everything
```

### External Tables

- Delta Lake owns only the **metadata** (table definition in the metastore)
- Data files live at a **user-specified LOCATION** - S3, ADLSGen2
- **DROP TABLE deletes only the metadata — data files remain safe**

```sql
-- External table (LOCATION specified)
CREATE TABLE employees_external (
    id     INT,
    name   STRING,
    salary DOUBLE
) USING DELTA
LOCATION '/mnt/delta/employees'

-- Data stays at /mnt/delta/employees even after DROP TABLE
-- DROP TABLE employees_external → only removes metastore entry
```

### Which to Use?

| Factor | Managed | External |
|--------|---------|----------|
| Simplicity | ✅ Simpler | More setup |
| Data safety | ❌ Data lost on DROP | ✅ Data survives DROP |
| Multiple catalogs/tools | ❌ Harder | ✅ Same files, multiple tools |
| Production use | ❌ Risky | ✅ Recommended |
| Development/testing | ✅ Convenient | Overkill |

**Best practice:** Always use **External Tables** in production. Data files should outlive any single table definition.

---

## 13. Deletion Vectors — CoW vs MoR

### The Problem with Traditional Updates in Delta Lake

Traditionally, when Delta Lake updates or deletes rows, it uses **Copy-on-Write (CoW)**:
1. Read the entire file that contains the affected rows
2. Apply the change (update/delete) to the rows in memory
3. Write an entirely new Parquet file with the modified data
4. Mark the old file as removed in the log

**Problem with CoW:** Even if only 1 row out of 1 million needs to be updated — you rewrite the entire 1GB file. This is very expensive for frequent small updates.

### Copy-on-Write (CoW) — Detailed

```
File: employees_part1.parquet (1,000,000 rows, 1GB)
UPDATE: only 5 rows need salary updated

CoW process:
1. Read all 1,000,000 rows from the file (1GB network read)
2. Update 5 rows in memory
3. Write 1,000,000 rows to a new file (1GB network write)
4. Mark old file as removed

Total I/O: 2GB for a 5-row update! ❌
```

**When CoW is good:**
- Infrequent updates / deletions
- Read-heavy workloads
- Analytical queries where read performance is critical

### Merge-on-Read (MoR) with Deletion Vectors

**Deletion Vectors (DVs)** — introduced in Delta Lake 2.3 — implement a MoR approach:

Instead of rewriting the entire file, a DV is a small **bitmap file** that records which rows in the base Parquet file are deleted/updated. When reading, Spark applies the DV to filter out deleted rows.

```
File: employees_part1.parquet (1,000,000 rows, 1GB)
UPDATE: only 5 rows need salary updated

DV process:
1. Write a tiny Deletion Vector file marking 5 rows as deleted (just a few KB)
2. Write a new small Parquet file with the 5 updated rows
3. Old file stays but DV tells readers to skip those 5 rows

Total I/O: few KB DV + small update file << 2GB ✅
```

### Enable Deletion Vectors

```python
# Enable at table creation
spark.sql("""
    CREATE TABLE employees (
        id INT, name STRING, salary DOUBLE
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")

# Enable on existing table
spark.sql("""
    ALTER TABLE employees
    SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")
```

### DV Trade-offs

| | Copy-on-Write | Deletion Vectors (MoR) |
|--|--------------|----------------------|
| Write performance | ❌ Slow (rewrites files) | ✅ Fast (tiny DV files) |
| Read performance | ✅ Fast (clean files) | Slightly slower (DV merge) |
| Best for | Read-heavy, infrequent updates | Write-heavy, frequent updates |
| Storage overhead | Rewritten files | Small DV files |
| Compaction needed | No (files are clean) | Yes (periodic rewrite to merge DVs) |

### DV + OPTIMIZE to Reclaim Performance

After accumulating many DVs, run OPTIMIZE to merge them:
```sql
-- This rewrites files and removes deletion vectors
OPTIMIZE employees
```

---

## 14. Cloning Delta Tables

Cloning creates a copy of a Delta table. Delta Lake supports two types: Shallow Clone and Deep Clone.

### 14.1 Shallow Clone

A shallow clone creates a new table that **references the same data files** as the source — it does NOT copy the actual Parquet data files.

```sql
-- Create a shallow clone
CREATE TABLE employees_shallow_clone
SHALLOW CLONE employees
-- or with a specific version
SHALLOW CLONE employees VERSION AS OF 5

-- PySpark
spark.sql("""
    CREATE OR REPLACE TABLE employees_dev
    SHALLOW CLONE employees
""")
```

**What gets created:**
- A new `_delta_log` directory for the clone
- The clone's log points to the SAME data files as the source
- **No data files are copied**

**Behaviour:**
- Reads from clone → reads from source's files (shared)
- Writes to clone → new files are written to the CLONE's location (copy-on-write for the clone)
- Source and clone become independent after any write to the clone
- **Deleting the source CAN break the shallow clone** (files no longer exist)
- Storage cost: near zero (just the new _delta_log)

**Use cases:**
- Creating dev/test environments quickly
- Experimenting on data without affecting production
- Quick table staging

```
Storage: Essentially free (no data copied)
Time to create: Seconds
Independence: Partial (writes diverge, but reads from shared source files)
Source dependency: YES — source files must remain
```

### 14.2 Deep Clone

A deep clone **copies ALL data files AND the transaction log** to a new location.

```sql
-- Create a deep clone
CREATE TABLE employees_deep_clone
DEEP CLONE employees

-- Clone to a specific location
CREATE TABLE employees_backup
DEEP CLONE employees
LOCATION '/mnt/backup/employees'

-- Clone a specific version
CREATE TABLE employees_snapshot
DEEP CLONE employees VERSION AS OF 10
```

**What gets created:**
- A complete copy of all Parquet data files
- A complete copy of the _delta_log (full history)
- Fully independent from the source

**Behaviour:**
- Completely independent of the source after creation
- Dropping or modifying the source has NO effect on the deep clone
- **Incremental sync supported** — run the same DEEP CLONE command again to sync new changes

```sql
-- Incremental sync (only copies new/changed files since last clone)
CREATE OR REPLACE TABLE employees_backup
DEEP CLONE employees
LOCATION '/mnt/backup/employees'
```

**Use cases:**
- Production backups
- Disaster recovery
- Data migration between environments (dev → staging → prod)
- Sharing data with external teams

```
Storage: Full copy (same size as source)
Time to create: Minutes/hours depending on size
Independence: Complete
Source dependency: None
```

Note  - inert/update/merge/delete Operations are synced incrementally. However, Schema changes or changes in Partitioning, 
column changes will trigger a full DEEP CLONE.

### 14.3 CTAS vs Deep Clone

| | CTAS | Deep Clone |
|--|------|-----------|
| History preserved | ❌ No (starts fresh) | ✅ Yes (full history) |
| Time travel available | ❌ Only from creation | ✅ Same as source |
| Schema preserved | ✅ Yes | ✅ Yes |
| Partitioning preserved | ✅ Configurable | ✅ Yes (same layout) |
| Incremental sync | ❌ No | ✅ Yes |
| Use case | New table from data | Backup/migration |

---

## 15. The Small File Problem

### What Causes Small Files?

Small files accumulate when many small writes are made to a Delta table:
- **Streaming jobs** — micro-batches write every 10-60 seconds, each creating small files
- **Frequent incremental loads** — hourly/every-15-min batch loads
- **High partition cardinality** — if partitioned by hour and you write frequently
- **Many ADF copy activities** — each run creates new files

### Why Small Files Hurt Performance

```
Query: SELECT * FROM events WHERE event_date = '2024-01-15'

With 10 files of 100MB: 
  → Open 10 files → 10 S3/ADLS API calls → Read 1GB
  → Fast ✅

With 10,000 files of 100KB:
  → Open 10,000 files → 10,000 S3/ADLS API calls → Read 1GB
  → 10,000 API calls have massive overhead
  → Metadata listing takes minutes
  → Executor overhead for each tiny task
  → Slow ❌
```

**Why each file is expensive:**
1. Object store API call (LIST, GET) has latency
2. Spark creates one task per file — scheduling overhead
3. JVM startup cost per task
4. Parquet file footer reads (even for empty files)

### Root Causes Summary

| Root Cause | Solution |
|------------|---------|
| Streaming micro-batches | Optimize Write, Auto Compaction |
| Incremental batch loads | Manual OPTIMIZE on schedule |
| Over-partitioned tables | Review partition strategy |
| Too many partition columns | Use Liquid Clustering instead |

---

## 16. OPTIMIZE & Bin Packing

### OPTIMIZE Command

OPTIMIZE compacts many small files into fewer, larger files using the **Bin Packing Algorithm**.

```sql
-- Basic OPTIMIZE
OPTIMIZE employees

-- OPTIMIZE a specific partition
OPTIMIZE employees WHERE dept = 'Engineering'

-- OPTIMIZE with ZORDER (covered in Section 18)
OPTIMIZE employees ZORDER BY (salary, dept)
```

```python
# PySpark
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/mnt/delta/employees")
dt.optimize().executeCompaction()
# With ZORDER:
dt.optimize().executeZOrderBy("salary", "dept")
```

### Bin Packing Algorithm

Bin Packing is a classic computer science problem: given N items of various sizes, pack them into as few fixed-size bins as possible.

**Delta's Bin Packing for OPTIMIZE:**
- **Target file size** = 1GB by default (configurable)
- Algorithm groups small files so their combined size approaches the target
- Small files are read, merged, and written as fewer large files

```
Before OPTIMIZE:
File A: 10MB ├─────────────────────────────────────────────────────┐
File B: 15MB │  All grouped together because combined < 1GB target  │ → 1 new 245MB file
File C: 80MB │                                                      │
File D: 140MB└─────────────────────────────────────────────────────┘
File E: 800MB → Already close to target → kept as-is (or slightly merged)

After OPTIMIZE: 2 files instead of 5
```

**Configure target file size:**
```python
spark.conf.set("spark.databricks.delta.optimize.minFileSize", 1073741824)  # 1GB in bytes
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1073741824)  # 1GB
```

### Optimize Write

Optimize Write is a **write-time** optimisation that reduces small files from being created in the first place.

Without Optimize Write:
- Spark creates one file per partition per Spark partition
- With 200 Spark partitions → 200 output files per Delta partition

With Optimize Write:
- Spark dynamically coalesces files before writing
- Produces fewer, larger files from the start

```python
# Enable per-write
df.write \
  .format("delta") \
  .option("optimizeWrite", "true") \
  .mode("append") \
  .save("/mnt/delta/employees")

# Enable for the table (all future writes)
spark.sql("""
    ALTER TABLE employees
    SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# Enable globally for session
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```

### Auto Compaction

Auto Compaction automatically runs a small OPTIMIZE after every write operation — but only on files created in the current write, not the entire table.

```python
# Enable auto compaction
spark.sql("""
    ALTER TABLE employees
    SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')
""")

# Or globally
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

**Auto Compaction vs Manual OPTIMIZE:**

| | Auto Compaction | Manual OPTIMIZE |
|--|----------------|-----------------|
| Scope | Files from current write only | Entire table |
| Timing | After each write, automatically | Scheduled/manual |
| Overhead | Low (runs inline) | High (full scan) |
| Target file size | Smaller (128MB default) | Larger (1GB default) |
| Use case | Reduce small files at write time | Full table compaction |

**Best practice:** Use both — Auto Compaction reduces small files at write time, scheduled OPTIMIZE handles full compaction weekly.

---

## 17. VACUUM Command

### What VACUUM Does

VACUUM physically deletes data files from storage that are **no longer referenced by the Delta transaction log**.

Files become unreferenced when:
- An UPDATE overwrites them (old files are marked `remove` in log)
- A DELETE removes records
- OPTIMIZE compacts them into larger files
- Old time-travel versions beyond retention period

### VACUUM Syntax

```sql
-- Default: delete files older than 7 days (168 hours)
VACUUM employees

-- Custom retention period
VACUUM employees RETAIN 240 HOURS   -- 10 days

-- Dry run: shows what WOULD be deleted without deleting
VACUUM employees DRY RUN

-- Zero retention (DANGEROUS — breaks time travel completely)
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM employees RETAIN 0 HOURS;
```

```python
# PySpark
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/mnt/delta/employees")
dt.vacuum()               # default 168 hours
dt.vacuum(retentionHours=240)  # custom
```

### Why the 7-Day Minimum?

The 7-day (168-hour) minimum protects against this scenario:
1. A long-running query starts reading version 5 of the table (takes 8 hours)
2. Meanwhile, VACUUM runs and deletes files that were only referenced in version 5
3. The running query tries to read those files → FileNotFoundException → query fails

The 7-day buffer ensures that any reasonable running query will complete before VACUUM removes files it might need.

### VACUUM and Time Travel

After VACUUM runs with a 7-day retention:
- Time travel queries going back MORE than 7 days → fail (files deleted)
- Time travel queries within the 7 days → still work (files preserved)
- Log entries older than retention → still in `_delta_log` (VACUUM doesn't touch the log)
- But log entries pointing to deleted files → reading those versions fails

```
Timeline:
Day 0: Version 1 created (file_A.parquet)
Day 5: Version 2 created (file_B.parquet, file_A marked removed)
Day 8: VACUUM runs (retentionHours=168 → deletes files older than 7 days)
       → file_A.parquet deleted (was unreferenced since day 5, > 7 days old)
       → file_B.parquet kept (only 3 days old)

After VACUUM:
  SELECT * FROM employees VERSION AS OF 1  ← ❌ file_A.parquet gone
  SELECT * FROM employees VERSION AS OF 2  ← ✅ file_B.parquet exists
```

### What VACUUM Does NOT Touch

- **The `_delta_log` directory** — never cleaned by VACUUM
- **Active data files** — files currently referenced by any version within retention
- **Files outside the Delta table directory** — VACUUM only looks in the table path

### Schedule VACUUM

```python
# ADF or Databricks job — run weekly
spark.sql("VACUUM employees RETAIN 720 HOURS")  # 30 days retention
```

---

## 18. Z-Ordering

### What is Z-Ordering?

Z-Ordering is a **multi-dimensional data clustering technique** that co-locates related data within the same files by sorting records using a **Z-curve (Morton curve)** mapping.

**Goal:** Minimise the number of files that need to be read for a given query filter.

Z-order is logically sort and repatition.

### The Problem Without Z-Ordering

Imagine a table with 1000 files where each file contains records from random ranges of `salary` and `region`:

```
Query: SELECT * FROM employees WHERE salary > 90000 AND region = 'West'

Without Z-Ordering:
  → Delta checks statistics for all 1000 files
  → Files are randomly distributed → many files have salary > 90000 AND region = 'West'
  → Must scan 850 of 1000 files
  → Slow ❌
```

### How Z-Ordering Works

Z-Ordering maps multiple column values to a single Z-curve value using bit interleaving:

```
salary: 90000 → binary: 10101111110000000000000
region: West  → binary: 10111001...

Z-curve value = interleaved bits from both columns

Records with similar Z-curve values are placed in the same file.
Records with similar salary AND region end up in the same files.
```

**Practical effect:**
```
After Z-Ordering on (salary, region):
  File 1: salary 80k-100k, region West  ← compact, related data together
  File 2: salary 80k-100k, region East
  File 3: salary 60k-80k,  region West
  ...

Query: salary > 90000 AND region = 'West'
  → Only need to scan File 1! (maybe 2-3 files total)
  → 150 files scanned instead of 850
  → Fast ✅
```

### OPTIMIZE with ZORDER

```sql
-- Z-Order by one column
OPTIMIZE employees ZORDER BY (salary)

-- Z-Order by multiple columns
OPTIMIZE employees ZORDER BY (salary, region)

-- Z-Order specific partition
OPTIMIZE employees WHERE dept = 'Engineering' ZORDER BY (salary)
```

```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/mnt/delta/employees")
dt.optimize().executeZOrderBy("salary", "region")
```

### Z-Ordering Limitations

1. **Runs OPTIMIZE (rewrite files)** — expensive, cannot be incremental
2. **Does NOT work well as a partition replacement** — partitioning is still better for columns with few distinct values (like `year`, `month`, `dept`)
3. **Statistics degraded over time** — as new files are added without Z-ordering, effectiveness drops. Must re-run periodically.
4. **Diminishing returns with more columns** — effectiveness decreases as you add more Z-Order columns
5. **Cannot add new columns later without full re-OPTIMIZE**

### Z-Ordering vs Partitioning

| | Partitioning | Z-Ordering |
|--|-------------|-----------|
| Best for | Low cardinality (year, month, region) | High cardinality (user_id, salary) |
| Filter efficiency | Skips entire partitions | Skips files within a partition |
| Implementation | Physical folder structure | Data within files co-located |
| Flexibility | Fixed at table creation | Can change with re-OPTIMIZE |
| Overhead | None at query time | None at query time (done at OPTIMIZE) |

---

## 19. Liquid Clustering

### What is Liquid Clustering?

Liquid Clustering is Delta Lake's **next-generation replacement for both partitioning and Z-Ordering**. Introduced in Delta Lake 3.1, it provides intelligent automatic file clustering without the limitations of traditional approaches.

### Problems It Solves

**Partitioning problems:**
- Cannot change partition columns without rewriting the entire table
- Partition pruning only works if your query filters on the partition column
- High cardinality columns cause millions of tiny partitions

**Z-Ordering problems:**
- Must run full OPTIMIZE to re-Z-Order
- Not incremental — new files are not Z-Ordered
- Doesn't work well when query patterns change over time

### How Liquid Clustering Works

Liquid Clustering uses a **Hilbert curve** (a space-filling curve, similar to Z-curve but with better locality properties) to cluster data.

Key differences from Z-Ordering:
1. **Incremental** — new writes are automatically clustered (no full OPTIMIZE needed)
2. **Flexible** — you can change clustering columns without rewriting data
3. **Self-tuning** — clustering runs progressively in the background

### Liquid Clustering Syntax

```sql
-- Create table with Liquid Clustering
CREATE TABLE employees (
    id     INT,
    name   STRING,
    salary DOUBLE,
    dept   STRING,
    region STRING
)
USING DELTA
CLUSTER BY (salary, region)
-- Note: NO PARTITIONED BY — Liquid Clustering replaces partitioning

-- Enable on existing table
ALTER TABLE employees CLUSTER BY (salary, region)

-- Remove clustering
ALTER TABLE employees CLUSTER BY NONE

-- Check clustering definition
DESCRIBE TABLE employees  -- shows clusteringColumns
```

### Running Clustering

```sql
-- Run clustering (equivalent to OPTIMIZE for clustered tables)
OPTIMIZE employees
-- This automatically applies the Hilbert curve clustering
-- No need to specify ZORDER BY — clustering columns are remembered
```

### Incremental Clustering

After a write, only newly written files are unclustered. Running OPTIMIZE clusters those files without touching already-clustered files:

```
Day 1: 100GB data written → OPTIMIZE clusters it ✅
Day 2: 10GB new data added → new files are unclustered
       OPTIMIZE runs → only clusters the 10GB, not the 100GB
       Time: seconds, not hours ✅
```

### Choosing Liquid Clustering Columns

**Good candidates:**
- Columns frequently used in query filters (`WHERE`, `JOIN ON`)
- High-cardinality columns (user_id, product_id, timestamp)
- Columns you commonly query together

**Not needed for:**
- Low-cardinality columns (year, month) — use partitioning instead (but only if you must partition; Liquid Clustering handles these too)
- Columns never used in filters

```sql
-- Example: If your common queries are:
-- WHERE event_date = '2024-01-15' AND user_id = '12345'
-- CLUSTER BY (event_date, user_id)

-- If cardinality is very low (e.g., only 3 regions):
-- Partitioning might still be slightly better for region
-- But Liquid Clustering is simpler and good enough
```

### Liquid Clustering vs Traditional Approaches

| Feature | Partitioning | Z-Ordering | Liquid Clustering |
|---------|-------------|-----------|-----------------|
| Flexibility | Low (fixed) | Medium (re-OPTIMIZE) | High (ALTER TABLE) |
| Incremental | N/A | ❌ No | ✅ Yes |
| High cardinality | ❌ Bad | ✅ Good | ✅ Good |
| Low cardinality | ✅ Good | Waste | ✅ Good |
| Setup complexity | Medium | Medium | Low |
| Production recommended | Legacy | Legacy | ✅ Modern |

---

## 20. Interview Q&A — Real World

### Fundamentals

---

**Q1. What is Delta Lake and how does it differ from a regular Parquet data lake?**

**A:** Delta Lake is an open-source storage layer that adds ACID transactions, schema enforcement, DML operations (UPDATE/DELETE/MERGE), and time travel on top of existing cloud storage (S3, ADLS, GCS). Regular Parquet on a data lake provides none of these — it's just immutable files with no transactional guarantees.

The key difference is the `_delta_log` transaction log — a sequence of JSON files that record every operation. This log is what gives Delta Lake its reliability. Without it, you're just working with raw Parquet files where:
- Concurrent writes can corrupt data
- Failed writes leave partial data
- There's no history of changes
- You can't update or delete rows

---

**Q2. Explain the _delta_log and why it is the core of Delta Lake.**

**A:** The `_delta_log` is a directory stored alongside the data files that contains one JSON file per transaction. Each JSON file records "actions" — primarily `add` (new file written), `remove` (old file invalidated), `metaData` (schema change), and `commitInfo` (who committed and when).

Delta Lake's "table" is not just the Parquet files — it is the Parquet files PLUS the log. When you query a Delta table, Spark doesn't just list all files in the directory. It reads the log to determine which files belong to the current version of the table.

This separation is powerful:
- You can "update" data by adding new files and marking old ones as removed — without touching the old files
- Time travel works because old `add` actions still point to old files
- Schema evolution is recorded in `metaData` actions
- ACID is guaranteed because each commit is atomic (JSON write succeeds or fails entirely)

Every 10 commits, the log is consolidated into a Parquet **checkpoint** file for performance, so state reconstruction doesn't require replaying all JSON files from version 0.

---

**Q3. How does Delta Lake achieve Atomicity?**

**A:** Atomicity in Delta Lake works through the commit protocol:

1. All data files are written to the table directory first (but not yet "visible" to Delta)
2. A commit entry is written to `_delta_log/0000000000000N.json` listing all `add` actions for the new files
3. Only after the JSON file is successfully written does Delta consider the transaction committed

If the job crashes:
- If it crashes BEFORE the JSON commit: the data files exist on disk but the log has no record of them — they are invisible to all Delta readers. VACUUM will eventually clean them up.
- If it crashes AFTER the JSON commit: the transaction is committed and durable.

This is the "write-ahead log" pattern — the log entry is the definitive record of whether a transaction happened. There is no partial state visible to readers.

---

**Q4. What is the difference between Optimistic and Pessimistic Concurrency Control? Which does Delta Lake use?**

**A:** 

**Pessimistic Concurrency Control** assumes conflicts are frequent. It acquires locks on resources before accessing them, forcing other writers to wait. This prevents all conflicts but reduces throughput and can cause deadlocks.

**Optimistic Concurrency Control (OCC)** assumes conflicts are rare. Writers proceed without locking, and only at commit time do they check whether a conflict occurred. If no conflict, the commit succeeds. If there is a conflict, the transaction retries.

**Delta Lake uses OCC.** At commit time, Delta checks whether the files the writer READ have been modified by another concurrent writer. If yes and the same files were modified, it raises a `ConcurrentModificationException`. If the concurrent writer modified different files (disjoint sets), both commits succeed.

OCC is a good fit for Delta Lake because most parallel workloads write to different partitions and rarely conflict. The optimistic assumption holds in practice.

---

**Q5. What happens when two writers try to write to the same Delta table simultaneously?**

**A:** Delta Lake uses Optimistic Concurrency Control. Both writers proceed independently. When each writer tries to commit:

1. Both read the current version (e.g., v5)
2. Both write their data files
3. Writer A commits first → creates version 6
4. Writer B tries to commit:
   - Checks if version 6's changes conflict with Writer B's reads
   - If Writer A modified files that Writer B also read → `ConcurrentModificationException` → Writer B retries from scratch
   - If Writer A modified completely different files → Writer B commits successfully as version 7

The isolation level determines how aggressively conflicts are detected:
- `WriteSerializable` (default) — allows some non-serializable reads but prevents write-write conflicts
- `Serializable` — strictest, prevents all anomalies but may cause more retries

---

### Time Travel

**Q6. How does time travel work in Delta Lake? What are its limitations?**

**A:** Time travel works because Delta Lake's transaction log preserves the history of every `add` and `remove` action. The data files marked as `remove` are not physically deleted immediately — they stay on disk.

To query version N:
1. Delta reads the log up to version N
2. Collects all `add` actions at version ≤ N
3. Excludes files that had `remove` actions at version ≤ N
4. Those remaining files form the "table" at version N

Query syntax:
```sql
SELECT * FROM employees VERSION AS OF 3
SELECT * FROM employees TIMESTAMP AS OF '2024-01-15'
```

**Limitations:**
1. Once `VACUUM` runs and deletes old files, those versions are no longer queryable
2. Default retention is 7 days — older versions unavailable after VACUUM
3. Time travel reads historical data but cannot write to historical versions (you can RESTORE to create a new version that replicates a historical state)
4. After `CONVERT TO DELTA`, time travel only works from the conversion point forward

---

**Q7. A production pipeline accidentally deleted 50,000 records. How do you recover using Delta Lake?**

**A:**

Step 1 — Find the version before the deletion:
```sql
DESCRIBE HISTORY employees
-- Look for the DELETE operation and find the version just before it
```

Step 2 — Verify the data at that version:
```sql
SELECT COUNT(*) FROM employees VERSION AS OF 12
-- Confirm the count matches expected
```

Step 3 — Restore the table:
```sql
RESTORE TABLE employees TO VERSION AS OF 12
```

Step 4 — Verify the restoration:
```sql
SELECT COUNT(*) FROM employees
-- Should match the pre-delete count
```

RESTORE creates a new version (e.g., version 14) that brings the table back to version 12's state. It doesn't delete version 13 (the bad delete). The full history is preserved.

---

### Schema

**Q8. What is the difference between Schema Enforcement and Schema Evolution in Delta Lake?**

**A:**

**Schema Enforcement** (always on, cannot disable):
- Rejects writes that don't match the table's defined schema
- Protects data quality — bad data never enters the table
- Throws `AnalysisException` if you try to write extra columns, wrong types, etc.

**Schema Evolution** (opt-in, disabled by default):
- Allows the schema to automatically update to accommodate new columns or type changes
- Enabled with `.option("mergeSchema", "true")` or `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")`
- New columns are added to the schema; old rows get NULL for the new column
- Does NOT allow: type narrowing, column deletion, column renaming

Together they form a safety + flexibility balance:
- By default, Delta is strict (enforcement on, evolution off)
- You must explicitly opt into evolution, which is a deliberate choice

---

**Q9. A source system added a new column to its schema and your Delta Lake pipeline is failing. How do you handle it?**

**A:**

Option 1 — Enable schema evolution for the write:
```python
df_new.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/mnt/delta/table")
```
This auto-adds the new column to the Delta schema. Old rows have NULL for the new column.

Option 2 — Explicitly add the column first:
```sql
ALTER TABLE my_table ADD COLUMN new_column STRING
```
Then the write proceeds without needing `mergeSchema`.

Option 3 — Drop the new column from the incoming data (if you don't want it):
```python
incoming_df = incoming_df.drop("unwanted_new_column")
incoming_df.write.format("delta").mode("append").save(...)
```

Option 4 — Enable auto-merge at table level (for streaming pipelines):
```sql
ALTER TABLE my_table SET TBLPROPERTIES ('delta.autoOptimize.mergeSchema' = 'true')
```

The right choice depends on whether the new column is useful. In production pharmaceutical pipelines where data contracts are strict, I'd use Option 3 (explicitly handle it) to avoid accidentally ingesting columns that haven't been approved.

---

### Performance

**Q10. What is the small files problem in Delta Lake and how do you solve it?**

**A:**

The small files problem occurs when a Delta table accumulates many small Parquet files — typically from streaming micro-batches, frequent incremental loads, or high-cardinality partitioning.

**Why it's bad:** Object stores (S3, ADLS) charge per API call. Reading 10,000 files of 100KB takes 10,000 API calls, which is slow and expensive. Each file also creates a Spark task with its own scheduling and JVM overhead.

**Solutions:**

1. **OPTIMIZE** — manually compact small files into larger ones (target 1GB by default)
   ```sql
   OPTIMIZE my_table
   ```

2. **Optimize Write** — prevents small files at write time by coalescing before writing
   ```python
   df.write.format("delta").option("optimizeWrite", "true").save(...)
   ```

3. **Auto Compaction** — automatically runs a limited OPTIMIZE after each write
   ```sql
   ALTER TABLE my_table SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')
   ```

4. **Liquid Clustering** — modern replacement that handles file sizing automatically

5. **Review partition strategy** — over-partitioning (e.g., by `event_timestamp`) creates tiny files per partition. Prefer coarser partitioning (by `date` instead of `timestamp`).

**Recommended combination:** Enable Optimize Write + Auto Compaction for streaming tables. Schedule weekly OPTIMIZE for full compaction.

---

**Q11. When would you use ZORDER vs Liquid Clustering? Which is better?**

**A:**

**Use Z-Ordering when:**
- You're on an older Delta Lake version (< 3.1) that doesn't support Liquid Clustering
- Your table is relatively static (infrequent writes, frequent reads)
- Your query patterns are stable and well-known

**Use Liquid Clustering when:**
- You're on Delta Lake 3.1+ (Databricks Runtime 13.3+)
- Your query patterns change over time (you want to update clustering columns)
- Your table has frequent writes (Z-Order would need expensive full re-OPTIMIZE)
- You want incremental clustering without full rewrites

**Liquid Clustering is the modern recommendation.** It solves the core problem of Z-Ordering (not incremental, can't change columns) while providing the same or better query acceleration. For new tables, always use Liquid Clustering. For existing tables with Z-Ordering, migrate when convenient.

The key operational advantage: with Liquid Clustering, running OPTIMIZE incrementally clusters only new files. With Z-Ordering, every OPTIMIZE rewrites all affected files. At scale this is the difference between minutes and hours.

---

**Q12. Explain Deletion Vectors and when you would enable them.**

**A:**

Deletion Vectors (DVs) are small bitmap files that record which rows within a base Parquet file have been deleted or updated — without rewriting the base file. This implements Merge-on-Read (MoR) semantics for updates and deletes.

**Without DVs (Copy-on-Write):** Any UPDATE or DELETE rewrites entire files. For a 1GB file with 5 changed rows, you read 1GB and write 1GB — expensive.

**With DVs:** The 5 changed rows are recorded in a tiny DV file (a few KB). Readers apply the DV to filter out those rows. Writes are fast. Reads have a small overhead to merge the DV.

**Enable when:**
- Your table has frequent, small updates (CDC patterns, streaming upserts)
- The update volume is a small fraction of total data (few rows per file)
- Write performance is more critical than read performance

**Don't enable (or run OPTIMIZE to merge) when:**
- Most rows in a file have been deleted (DV overhead not worth it — just rewrite the file)
- Read performance is the absolute priority (analytical queries on rarely-updated tables)

**In practice:** Enable DVs for the Raw and Trusted layers where frequent incremental upserts happen. Run OPTIMIZE periodically to merge accumulated DVs into clean base files.

---

**Q13. What is the difference between Shallow Clone and Deep Clone? When would you use each?**

**A:**

**Shallow Clone:** Creates a new table that points to the same data files as the source. No data is physically copied. The clone has its own `_delta_log` but shares source files.
- Storage cost: near zero
- Creation time: seconds
- Independence: partial (writes to clone diverge from source, but reads still need source files)
- Use: dev/test environments, quick experiments, table staging

**Deep Clone:** Copies all data files AND the transaction log to a new location. Completely independent.
- Storage cost: same as source (full copy)
- Creation time: proportional to data size
- Independence: complete (source can be deleted without affecting clone)
- Use: production backups, disaster recovery, data migration, cross-environment sharing

**Real example from my work:** When testing a new Medallion Architecture refactor, I'd Shallow Clone the production Trusted layer to a dev workspace. I can run experiments, the clone is free, and any writes I make don't affect production. But for a quarterly disaster recovery backup before a major migration, I'd Deep Clone to ensure we can restore independently even if the production workspace is unavailable.

---

**Q14. How do you convert an existing Parquet table to Delta and what are the implications?**

**A:**

```python
from delta.tables import DeltaTable
DeltaTable.convertToDelta(spark, "parquet.`/mnt/raw/employees`")
```

Or with partitions:
```sql
CONVERT TO DELTA parquet.`/mnt/raw/employees`
PARTITIONED BY (dept STRING, year INT)
```

**What happens:**
1. Delta scans all Parquet files and collects statistics
2. Creates `_delta_log` with a version 0 checkpoint listing all existing files as `add` actions
3. Original Parquet files are NOT moved, copied, or modified

**Implications:**
1. **No downtime** — conversion happens while data is in place
2. **Time travel starts from conversion** — cannot query history before the conversion
3. **Partition discovery** — if the Parquet table has Hive-style partitions (folders like `dept=Engineering/`), you must specify the partition schema
4. **Existing readers** — tools that can read Parquet directly can still read the files, but Delta features only work through the Delta API
5. **Irreversible** — there's no `CONVERT TO PARQUET` command (though you could remove the _delta_log directory to revert, this is dangerous)

---

**Q15. Design a production-grade Delta Lake pipeline for a pharmaceutical company.**

**A:** Based on my actual experience building pharma pipelines, here's the architecture:

**Layer design (Medallion Architecture):**

```
Raw Layer:
  - External Delta tables at /mnt/raw/
  - Schema enforcement: STRICT (reject anything unexpected)
  - Deletion Vectors: enabled (frequent CDC updates from Oracle)
  - Auto Compaction: enabled (streaming ingestion)
  - Retention: 90 days VACUUM
  - partitionBy: event_date

Trusted Layer:
  - External Delta tables at /mnt/trusted/
  - Schema: enforced + controlled evolution (ALTER TABLE for approved changes)
  - Liquid Clustering: CLUSTER BY (patient_id, event_date)
  - Optimize Write: enabled
  - Retention: 365 days VACUUM
  - Golden Record MERGE runs here

Curated Layer:
  - External Delta tables at /mnt/curated/
  - Schema: strictly managed, version-controlled
  - Liquid Clustering: CLUSTER BY (report_date, region)
  - OPTIMIZE: weekly scheduled job
  - Retention: 7 years (FDA compliance)
  - Read-heavy: Deletion Vectors disabled
```

**Governance:**
```sql
-- All tables use Unity Catalog
CREATE TABLE pharma_prod.trusted.patient_xref
USING DELTA
CLUSTER BY (patient_id, source_system)
LOCATION 'abfss://trusted@storage.dfs.core.windows.net/patient_xref'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
```

**Key decisions:**
1. External tables everywhere — data must survive table drops
2. Deletion Vectors in lower layers (high write volume)
3. Liquid Clustering in all layers (modern, flexible)
4. Long retention (7 years) in curated for FDA 21 CFR Part 11 compliance
5. Time travel used for regulatory audit responses

---

*End of Delta Lake Masterclass Notes*
*Based on: https://www.youtube.com/watch?v=8IjCyvyAPpM*
