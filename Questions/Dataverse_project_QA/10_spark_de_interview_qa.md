# 🔥 PySpark & Azure Data Engineering
## Real-World Interview Q&A — Complete Answer Script
### Mayuresh Uttam Patil | Data Engineer | Cognizant

---

> **How to use this file:**
> - **Short Answer** → Say this if interviewer wants a quick 30-second answer
> - **Detailed Answer** → Use this for 2–3 minute deep-dive answers
> - **Interview Script** → Exact words to speak — written in first person, references your real projects
> - **Pseudo Code** → Always explain code line by line, don't just recite it

---

## ─────────────────────────────────────────
## SECTION 1 — INGESTION & API INTEGRATION
## ─────────────────────────────────────────

---

## Q1. Describe a time you integrated Python with REST APIs for ingestion; how did you handle retries, pagination, and schema evolution without breaking downstream Spark jobs?

---

### ✅ Short Answer (30 seconds)
> "In the eCDP pharma project, I built a Python REST API ingestion layer that pulled clinical trial data incrementally. I handled retries using exponential backoff, pagination using cursor tokens, and schema evolution using Delta Lake's `mergeSchema` option — ensuring no downstream Spark job ever broke due to a new field arriving from the API."

---

### 📖 Theory

| Challenge | Problem | Solution |
|-----------|---------|----------|
| **Retries** | API returns 429/503 temporarily | Exponential backoff — wait longer each retry |
| **Pagination** | API returns data in pages, not all at once | Loop using cursor/offset until no next page |
| **Schema Evolution** | API adds new fields without notice | Delta `mergeSchema=true` auto-adds new columns |

---

### 💻 Pseudo Code

```python
import requests, time

# ── RETRY WITH EXPONENTIAL BACKOFF ──────────────────────────────
def call_api_with_retry(url, headers, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()          # raises error on 4xx/5xx
            return response.json()
        except requests.exceptions.RequestException as e:
            wait_seconds = 2 ** attempt          # 1s → 2s → 4s
            print(f"Attempt {attempt+1} failed. Retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)
    raise Exception("All retries exhausted")

# ── CURSOR-BASED PAGINATION ──────────────────────────────────────
def fetch_all_pages(base_url, headers):
    all_records = []
    next_cursor = None

    while True:
        url = f"{base_url}?cursor={next_cursor}" if next_cursor else base_url
        data = call_api_with_retry(url, headers)
        all_records.extend(data["records"])      # collect page records
        next_cursor = data.get("next_cursor")    # None = last page
        if not next_cursor:
            break

    return all_records

# ── SCHEMA EVOLUTION — NEW COLUMNS AUTO-ADDED TO DELTA ──────────
records = fetch_all_pages("https://api.clinicaldata.com/trials", headers)
df = spark.createDataFrame(records)

df.write.format("delta") \
    .option("mergeSchema", "true")  \  # new API columns added automatically
    .mode("append") \
    .save("/mnt/bronze/clinical_trials")
```

---

### 🎤 Interview Script
> "In my eCDP pharmaceutical project at Cognizant, we integrated with a clinical trial data REST API that returned paginated results using cursor tokens — each page had around 1,000 records and some feeds had 500+ pages. I built a Python ingestion module with two key reliability patterns. First, exponential backoff retry logic — if the API returned a rate-limit error or a 503, we'd wait 1 second, then 2, then 4 before retrying, which handled temporary API instability without failing the pipeline. Second, for pagination I looped until the response had no next_cursor field, collecting all records across pages before writing to Delta.
>
> The trickiest part was schema evolution. The API team sometimes added new response fields without advance notice — previously this would break our downstream Silver notebooks because the Bronze schema didn't have the new column. I solved this using Delta's mergeSchema option on write — when a new field arrives, Delta automatically adds it as a nullable column to the target table. Downstream Spark jobs read from Delta using column names explicitly, so they kept working even when new columns appeared. No code change needed on our side."

---
---

## Q2. How did you use Z-Ordering and clustering decisions in your SQL validation tables to reduce scan bytes and improve partition pruning?

---

### ✅ Short Answer (30 seconds)
> "I partitioned validation tables on `load_dt` for time-based pruning, then applied Delta ZORDER on high-cardinality columns like `product_id`, `customer_id`, and `ndc_number` after OPTIMIZE. This reduced scan bytes significantly and improved query performance without creating small-file problems."

---

### 📖 Theory

```
PARTITIONING  → Physically separates files by column value (like folders)
               → Good for: LOW cardinality (date, region, status)
               → Bad for:  HIGH cardinality (IDs) → millions of tiny files

ZORDER        → Co-locates related rows within the SAME files using multi-dim clustering
               → Good for: HIGH cardinality columns used in WHERE/JOIN
               → Works with Delta's data-skipping (min/max stats per file)
               → Spark skips entire files that can't contain the filter value
```

---

### 💻 Pseudo Code

```sql
-- ── STEP 1: Create table with LOW-cardinality partition ──────────
CREATE TABLE silver.validation_results
USING DELTA
PARTITIONED BY (load_dt)           -- date partition → daily folders
AS SELECT *, current_date() AS load_dt
FROM bronze.source_data;

-- ── STEP 2: OPTIMIZE to compact small files ──────────────────────
OPTIMIZE silver.validation_results;

-- ── STEP 3: ZORDER on high-cardinality filter columns ───────────
OPTIMIZE silver.validation_results
ZORDER BY (product_id, ndc_number, customer_id);
-- Delta rewrites files so rows with same product_id are in same file
-- Spark now skips files based on min/max stats per file

-- ── STEP 4: Query now uses BOTH partition pruning + data skipping ─
SELECT COUNT(*), SUM(amount)
FROM silver.validation_results
WHERE load_dt = current_date()          -- partition pruning: skip other days
  AND product_id = 'PROD_XYZ'          -- data skipping: skip files without this ID
  AND ndc_number = '12345-678-90';     -- data skipping: even fewer files scanned
```

```python
# ── In PySpark — verify data skipping is working ─────────────────
spark.sql("""
    DESCRIBE DETAIL silver.validation_results
""").select("numFiles", "sizeInBytes").show()

# Check how many files were skipped in last query
spark.sql("SET spark.databricks.delta.stats.skipping=true")
```

---

### 🎤 Interview Script
> "In our eCDP validation tables, we were running reconciliation queries that took 8–10 minutes each because Spark was scanning the full table every time. I approached this in two layers.
>
> First, partitioning — I partitioned on `load_dt` because every validation query filters by date. This alone cut scan bytes by around 90% since Spark now only reads the relevant day's partition. The key rule I follow is: only partition on low-cardinality columns — date, region, status — never on ID columns which would create millions of tiny files.
>
> Second, for high-cardinality columns like `ndc_number`, `product_id`, and `customer_id` that appear in WHERE clauses and JOINs, I applied Delta's ZORDER after running OPTIMIZE. ZORDER doesn't partition — it co-locates rows with the same column values within the same files, and Delta keeps min/max statistics per file. When a query filters `product_id = 'XYZ'`, Spark checks the per-file stats and skips every file whose max product_id is less than XYZ or min is greater — this is called data skipping. Combined, these two techniques reduced our validation query time from 10 minutes to under 2 minutes."

---
---

## ─────────────────────────────────────────
## SECTION 2 — DATA QUALITY & VALIDATION
## ─────────────────────────────────────────

---

## Q3. How do you manage XML parsing in data pipelines using Python — especially for large documents — and convert into structured Spark DataFrames reliably?

---

### ✅ Short Answer (30 seconds)
> "For large XML files I use `lxml.etree.iterparse` which streams elements one at a time — no full file load in memory. After each element is processed I call `elem.clear()` to free memory. Records are collected into a list, then converted to a Spark DataFrame using an explicit schema — never schema inference on large files."

---

### 📖 Theory

```
Small XML  (<100MB): ElementTree — loads full file, simple to use
Large XML  (>500MB): lxml.iterparse — streaming, processes one element at a time
                     → elem.clear() after each element = constant memory usage
                     → Can handle multi-GB files without OOM

Schema:    Never infer schema on large XML — inferSchema reads entire file twice
           Always define StructType explicitly
```

---

### 💻 Pseudo Code

```python
from lxml import etree
from pyspark.sql.types import StructType, StructField, StringType, DateType

# ── STREAMING XML PARSE — Memory safe for multi-GB files ─────────
def parse_xml_streaming(xml_path):
    records = []

    # iterparse streams file element by element — never full load
    for event, elem in etree.iterparse(xml_path,
                                        events=("end",),
                                        tag="ClinicalRecord"):   # target tag
        record = {
            "patient_id":  elem.findtext("PatientID"),
            "drug_code":   elem.findtext("DrugCode"),
            "dosage":      elem.findtext("Dosage"),
            "visit_date":  elem.findtext("VisitDate"),
            "site_id":     elem.findtext("SiteID")
        }
        records.append(record)
        elem.clear()        # ← CRITICAL: free memory after each element
        # without this, memory grows with each element → OOM on large files

    return records

# ── CONVERT TO SPARK DATAFRAME WITH EXPLICIT SCHEMA ─────────────
records = parse_xml_streaming("/mnt/raw/clinical_feed_2gb.xml")

schema = StructType([
    StructField("patient_id",  StringType(), True),
    StructField("drug_code",   StringType(), True),
    StructField("dosage",      StringType(), True),
    StructField("visit_date",  StringType(), True),   # cast to date in Silver
    StructField("site_id",     StringType(), True)
])

# Create DataFrame with explicit schema — no inferSchema scan
df = spark.createDataFrame(records, schema=schema)

# ── HANDLE NESTED XML — Flatten before DataFrame ─────────────────
def parse_nested_xml(xml_path):
    records = []
    for event, elem in etree.iterparse(xml_path, events=("end",), tag="Patient"):
        # Flatten nested elements
        for visit in elem.findall("Visits/Visit"):
            record = {
                "patient_id": elem.findtext("PatientID"),
                "visit_date": visit.findtext("Date"),
                "drug_code":  visit.findtext("DrugCode")
            }
            records.append(record)
        elem.clear()
    return records
```

---

### 🎤 Interview Script
> "In the eCDP pharma project, we received XML clinical data feeds from lab systems — some files were 2 to 3 gigabytes. If I had used Python's standard ElementTree library, loading a 3GB file fully into memory on a driver node would have caused an out-of-memory crash. Instead, I used `lxml.etree.iterparse` which is a streaming parser — it processes the XML file element by element without ever loading the full document into memory. After processing each element and adding it to my records list, I call `elem.clear()` to immediately free that element's memory, so memory consumption stays roughly constant regardless of file size.
>
> For converting to Spark, I always define an explicit StructType schema — never use `inferSchema=True` on large files because Spark reads the entire file twice to infer types, which defeats the purpose of streaming. Once I have the records list, I create a Spark DataFrame with the explicit schema and write it to Bronze in Delta format. For nested XML with repeated child elements, I flatten the structure during parsing by iterating child nodes and creating one record per child element rather than per parent."

---
---

## Q4. How do you implement data quality and validation checks in PySpark notebooks, and what's your strategy for quarantining bad records without stopping production?

---

### ✅ Short Answer (30 seconds)
> "I tag every record with a DQ status column — checking nulls, negative values, referential integrity. Good records go to Silver Delta table via MERGE. Bad records go to a quarantine Delta table with the failure reason and timestamp. The pipeline always completes — bad records are reviewed and reprocessed separately."

---

### 📖 Theory

```
DQ Strategy — Never fail the pipeline for bad data:

  Raw DataFrame
       ↓
  Tag each record → dq_status = PASS / FAIL: <reason>
       ↓
  Split into good_df and bad_df
       ↓
  good_df → MERGE into Silver Delta table
  bad_df  → APPEND to Quarantine Delta table (with error reason + timestamp)
       ↓
  Log counts to audit table
  Alert team if bad_count > threshold
```

---

### 💻 Pseudo Code

```python
from pyspark.sql import functions as F

df = spark.read.format("parquet").load("/mnt/bronze/orders")

# ── STEP 1: Tag each record with DQ status ───────────────────────
df_tagged = df.withColumn(
    "dq_status",
    F.when(F.col("order_id").isNull(),
           F.lit("FAIL: null order_id"))
    .when(F.col("amount") < 0,
           F.lit("FAIL: negative amount"))
    .when(F.col("order_date").isNull(),
           F.lit("FAIL: null order_date"))
    .when(~F.col("status").isin(["OPEN","CLOSED","PENDING"]),
           F.lit("FAIL: invalid status value"))
    .when(F.col("customer_id").isNull(),
           F.lit("FAIL: null customer_id"))
    .otherwise(F.lit("PASS"))
)

# ── STEP 2: Split into good and bad ──────────────────────────────
df_good = df_tagged.filter(F.col("dq_status") == "PASS") \
                   .drop("dq_status")

df_bad  = df_tagged.filter(F.col("dq_status") != "PASS") \
                   .withColumn("quarantine_ts",    F.current_timestamp()) \
                   .withColumn("source_pipeline",  F.lit("orders_silver_load")) \
                   .withColumn("source_file",      F.lit(source_file_path))

# ── STEP 3: Write good records to Silver via MERGE ───────────────
from delta.tables import DeltaTable

silver = DeltaTable.forPath(spark, "/mnt/silver/orders")
silver.alias("tgt").merge(
    df_good.alias("src"),
    "tgt.order_id = src.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# ── STEP 4: Write bad records to Quarantine ──────────────────────
df_bad.write.format("delta") \
    .mode("append") \
    .save("/mnt/quarantine/orders")

# ── STEP 5: Log and alert ─────────────────────────────────────────
good_count = df_good.count()
bad_count  = df_bad.count()

print(f"Loaded to Silver: {good_count} | Quarantined: {bad_count}")

# Alert if too many bad records
bad_pct = bad_count / (good_count + bad_count) * 100
if bad_pct > 5:   # threshold: 5% bad records triggers alert
    raise Exception(f"DQ Alert: {bad_pct:.1f}% records failed — exceeds 5% threshold")
```

---

### 🎤 Interview Script
> "My data quality strategy is built on one core principle: bad data should never stop the pipeline, but it should never silently reach downstream users either. In our Silver notebooks, I tag every incoming record with a `dq_status` column — checking for null primary keys, negative amounts, invalid enumeration values, referential integrity against lookup tables. Records that pass all checks are written to Silver via Delta MERGE — this handles upserts cleanly. Records that fail any check are written to a quarantine Delta table with the specific failure reason, timestamp, and source pipeline name.
>
> The pipeline always completes with a SUCCESS status in ADF — the quarantine write is just another write step, not a failure. However, if the bad record percentage exceeds 5%, I raise an exception at the end which marks the pipeline as failed and triggers an alert — because at that point something is systemically wrong with the source data and someone needs to investigate. The ops team reviews the quarantine table daily, fixes root causes, and we have a reprocessing pipeline that picks up quarantine records after source corrections."

---
---

## Q5. Explain how you optimized SQL validation queries that run on curated Delta/Iceberg tables, especially when checking data quality and counts across sources.

---

### ✅ Short Answer (30 seconds)
> "I replaced full-table count queries with partition-scoped incremental queries, rewrote correlated subqueries as CTEs, added ZORDER on JOIN columns, ran ANALYZE to build column statistics, and enabled Delta cache on the cluster. Combined, these reduced validation query time by 40%."

---

### 📖 Theory

```
Common Validation Query Problems:
  1. No partition filter → full table scan → slow + expensive
  2. Correlated subquery → runs once per row → N×M scans
  3. No column stats → data skipping disabled
  4. Wide SELECT * → unnecessary column reads
  5. No broadcast hint → small lookup table causes full shuffle
```

---

### 💻 Pseudo Code

```sql
-- ══ BEFORE: Slow pattern — full table scan + correlated subquery ══

-- BAD: No partition filter = scans 500GB
SELECT COUNT(*) AS total FROM silver.orders
WHERE customer_id = 'C123';

-- BAD: Correlated subquery runs once per row
SELECT order_id, amount,
    (SELECT AVG(amount) FROM silver.orders WHERE region = o.region) AS avg
FROM silver.orders o;


-- ══ AFTER: Optimized pattern ══════════════════════════════════════

-- GOOD: Partition filter + CTE + single scan reconciliation
WITH latest_load AS (
    -- Limit metadata scan to last 7 days only
    SELECT MAX(load_dt) AS max_dt
    FROM silver.orders
    WHERE load_dt >= current_date() - INTERVAL 7 DAYS
),
source_count AS (
    SELECT COUNT(*) AS src_cnt, SUM(amount) AS src_amt
    FROM bronze.orders_raw
    WHERE load_dt = (SELECT max_dt FROM latest_load)   -- partition pruning
),
target_count AS (
    SELECT COUNT(*) AS tgt_cnt, SUM(amount) AS tgt_amt
    FROM silver.orders
    WHERE load_dt = (SELECT max_dt FROM latest_load)   -- partition pruning
)
SELECT
    s.src_cnt,  t.tgt_cnt,
    t.tgt_cnt - s.src_cnt  AS count_variance,
    s.src_amt,  t.tgt_amt,
    t.tgt_amt  - s.src_amt AS amount_variance
FROM source_count s, target_count t;


-- GOOD: Window function replaces correlated subquery — single pass
SELECT order_id, amount,
    AVG(amount) OVER (PARTITION BY region) AS region_avg    -- one scan
FROM silver.orders
WHERE load_dt = current_date();                              -- partition filter


-- GOOD: Broadcast hint for small lookup table
SELECT /*+ BROADCAST(r) */ o.order_id, o.amount, r.region_name
FROM silver.orders o
JOIN ref.regions r ON o.region_code = r.region_code
WHERE o.load_dt = current_date();
```

```sql
-- ── Maintenance commands that enable fast queries ─────────────────

-- Build column statistics for data skipping
ANALYZE TABLE silver.orders COMPUTE STATISTICS FOR ALL COLUMNS;

-- Compact + cluster files
OPTIMIZE silver.orders ZORDER BY (customer_id, product_id);

-- Verify table health
DESCRIBE DETAIL silver.orders;
```

---

### 🎤 Interview Script
> "We had over 300 validation reports running on Gold and Silver Delta tables and the average query time was around 4 minutes. I did a systematic profiling exercise — ran EXPLAIN on the slowest 20 queries and looked at the Spark UI's SQL tab to see where time was being spent.
>
> The biggest wins came from four changes. First, every query now has a partition filter on `load_dt` — this alone cut scan bytes by 85 to 90% since Spark skips other partitions entirely. Second, I replaced correlated subqueries with CTEs and window functions — a correlated subquery runs once per row in the outer query, which is an N-times scan; a window function does a single pass. Third, I ran ANALYZE TABLE to build column statistics which enabled Delta's data skipping to kick in for ZORDER columns. Fourth, for small reference tables under 50 megabytes used in JOINs, I added broadcast hints so Spark sends the small table to each executor rather than shuffling the large table. Combined these reduced average query time from 4 minutes to under 2.5 minutes."

---
---

## ─────────────────────────────────────────
## SECTION 3 — ETL DESIGN & CONFIGURATION
## ─────────────────────────────────────────

---

## Q6. Describe how you built configuration-driven ingestion from Oracle and flat files into ADLS, and how you ensured schema evolution and backward compatibility.

---

### ✅ Short Answer (30 seconds)
> "I built a metadata control table in Azure SQL with one row per source table — defining source type, connection, watermark column, and target Delta path. ADF reads this via Lookup and loops via ForEach into a generic Databricks notebook. Schema changes are handled by Delta's `mergeSchema` — new columns are added automatically, backward compatibility is maintained by never dropping columns."

---

### 📖 Theory

```
Without Config-Driven: 1 pipeline per table = 50 tables = 50 pipelines = maintenance nightmare

With Config-Driven:
  Control Table (Azure SQL)
       ↓ Lookup Activity reads all rows
       ↓ ForEach loops each row
       ↓ Generic Notebook receives params
       ↓ Routes by source_type (ORACLE / FLATFILE)
       ↓ Writes to Delta with mergeSchema

Adding new table = INSERT 1 row into control table = done
```

---

### 💻 Pseudo Code

```sql
-- ── CONTROL TABLE IN AZURE SQL ───────────────────────────────────
CREATE TABLE etl_config (
    config_id        INT IDENTITY PRIMARY KEY,
    source_type      VARCHAR(20),    -- 'ORACLE' or 'FLATFILE'
    source_name      VARCHAR(200),   -- Oracle table or file pattern
    watermark_col    VARCHAR(100),   -- column used for incremental load
    watermark_value  DATETIME,       -- last successfully loaded value
    target_path      VARCHAR(500),   -- ADLS Delta path
    partition_col    VARCHAR(100),   -- column to partition Delta table by
    load_type        VARCHAR(20),    -- 'INCREMENTAL' or 'FULL'
    is_active        BIT DEFAULT 1   -- 0 = skip this source
);

-- ── ADD NEW SOURCE = ONE INSERT ──────────────────────────────────
INSERT INTO etl_config VALUES (
    'ORACLE', 'PHARMA.PATIENT_VISITS', 'UPDATED_DATE',
    '2020-01-01', '/mnt/silver/patient_visits', 'load_dt',
    'INCREMENTAL', 1
);
-- Zero pipeline changes needed — ADF picks this up on next run
```

```python
# ── GENERIC DATABRICKS NOTEBOOK (handles all sources) ───────────

# Parameters injected by ADF ForEach
source_type    = dbutils.widgets.get("source_type")
source_name    = dbutils.widgets.get("source_name")
watermark_col  = dbutils.widgets.get("watermark_col")
watermark_val  = dbutils.widgets.get("watermark_value")
target_path    = dbutils.widgets.get("target_path")
load_type      = dbutils.widgets.get("load_type")

# ── Route by source type ─────────────────────────────────────────
if source_type == "ORACLE":
    jdbc_url = dbutils.secrets.get("kv-scope", "oracle-jdbc-connection-string")

    if load_type == "INCREMENTAL":
        query = f"""
            (SELECT * FROM {source_name}
             WHERE {watermark_col} > TO_DATE('{watermark_val}','YYYY-MM-DD HH24:MI:SS')
            ) t
        """
    else:
        query = f"(SELECT * FROM {source_name}) t"

    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", query) \
        .option("numPartitions", "10") \
        .option("fetchsize", "10000") \
        .load()

elif source_type == "FLATFILE":
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(f"/mnt/landing/{source_name}/*.csv")

# ── Write with mergeSchema — backward compatible always ──────────
df.withColumn("load_dt", F.current_date()) \
  .write.format("delta") \
  .option("mergeSchema", "true")  \   # new Oracle columns auto-added to Delta
  .partitionBy("load_dt") \
  .mode("append") \
  .save(target_path)

# ── Update watermark ─────────────────────────────────────────────
new_wm = df.agg({watermark_col: "max"}).collect()[0][0]
spark.read.jdbc(sql_url, "etl_config") \
     .filter(f"source_name = '{source_name}'") \
     # UPDATE watermark_value = new_wm
```

---

### 🎤 Interview Script
> "In the FinOps project, we had 40+ Oracle tables and multiple flat file sources to ingest. Building a separate ADF pipeline for each would have been unmanageable — any common change like adding an audit column would require editing 40 pipelines. So I designed a configuration-driven framework.
>
> The foundation is a metadata control table in Azure SQL — each row defines one source: what type it is, what table or file pattern to read, which column to use for watermarking, and where to write in Delta. ADF has one master pipeline: a Lookup activity reads all active config rows, a ForEach loops through each row, and inside ForEach a single generic Databricks notebook handles the actual ingestion. The notebook receives all parameters from ADF and routes logic based on source_type — Oracle uses JDBC with incremental watermark query, flat files use CSV reader.
>
> For schema evolution, Delta's mergeSchema handles it automatically — when Oracle adds a new column, it appears in the DataFrame and gets added to the Delta table on the next load without any code change. We maintain backward compatibility by never dropping columns from Delta tables — only additive changes are allowed. To onboard a completely new source, the team just inserts one row into the control table — no pipeline or notebook change needed."

---
---

## Q7. How do you design configuration-driven ETL onboarding in ADF for new sources, minimizing code changes while keeping lineage and data contracts intact?

---

### ✅ Short Answer (30 seconds)
> "New source onboarding requires only inserting a row into the metadata control table and uploading a data contract JSON file. ADF generic pipelines pick it up automatically. Data contracts enforce schema expectations. Lineage is tracked through audit logs and Azure Purview tags applied at write time."

---

### 💻 Pseudo Code

```json
// ── DATA CONTRACT JSON — stored in ADLS per source ───────────────
// /mnt/contracts/PHARMA.PATIENT_VISITS.json
{
  "source_table":    "PHARMA.PATIENT_VISITS",
  "owner_team":      "pharma-data@company.com",
  "sla_minutes":     60,
  "primary_key":     ["VISIT_ID"],
  "expected_columns":["VISIT_ID","PATIENT_ID","DRUG_CODE","VISIT_DATE","DOSAGE"],
  "not_null_columns":["VISIT_ID","PATIENT_ID"],
  "valid_values": {
      "STATUS": ["ACTIVE","WITHDRAWN","COMPLETED"]
  },
  "partition_col":   "load_dt"
}
```

```python
# ── CONTRACT VALIDATION IN NOTEBOOK (runs before Silver write) ───
import json

contract_path = f"/mnt/contracts/{source_name}.json"
contract      = json.loads(dbutils.fs.head(contract_path))

# 1. Column presence check
expected  = set(contract["expected_columns"])
incoming  = set(df.columns)
missing   = expected - incoming
if missing:
    raise Exception(f"Contract violation — missing columns: {missing}")

# 2. Not-null check
for col_name in contract["not_null_columns"]:
    null_cnt = df.filter(F.col(col_name).isNull()).count()
    if null_cnt > 0:
        raise Exception(f"Contract violation — {null_cnt} nulls in {col_name}")

# 3. Valid values check
for col_name, allowed in contract["valid_values"].items():
    invalid = df.filter(~F.col(col_name).isin(allowed)).count()
    if invalid > 0:
        raise Exception(f"Contract violation — {invalid} invalid values in {col_name}")

# 4. After validation — tag with lineage metadata
df = df.withColumn("source_system",  F.lit(source_name)) \
       .withColumn("ingestion_ts",   F.current_timestamp()) \
       .withColumn("pipeline_run_id",F.lit(adf_run_id))

# 5. Write to Delta
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save(target_path)

# 6. Log lineage to audit table
spark.sql(f"""
    INSERT INTO audit.lineage_log VALUES (
        '{source_name}', '{target_path}',
        '{adf_run_id}', current_timestamp(), {df.count()}
    )
""")
```

---

### 🎤 Interview Script
> "Our ETL onboarding process was fully self-service for the data teams. To add a new Oracle table or flat file source, they filled out a data contract JSON template defining expected columns, primary keys, not-null rules, valid enumeration values, and SLA requirements. They also inserted one row into the metadata control table. That's the entire onboarding process — no engineer needed to write pipeline code.
>
> The generic ADF pipeline automatically discovers the new source on its next run. Before writing to Silver, the notebook validates incoming data against the data contract — if a required column is missing or a not-null column has nulls, the pipeline fails loudly with a specific error rather than silently writing bad data. Lineage is maintained through audit log entries recording source-to-target path, ADF run ID, and row counts for every load. We also tag Delta tables in Azure Purview with the source system and domain so data consumers can trace any Gold table record back to its Oracle origin."

---
---

## Q8. You migrated 26+ TB into ADLS Gen2 using a medallion design. How did you keep SQL transformations scalable during backfills without reprocessing everything?

---

### ✅ Short Answer (30 seconds)
> "I broke the 26TB backfill into monthly partitioned windows. ADF ran 5 windows in parallel via ForEach batchCount. Each Databricks job read only its assigned date range from Oracle using JDBC parallel reads, then used Delta's `replaceWhere` to write only to that date partition — so each batch touched only its own data, no reprocessing of other partitions."

---

### 💻 Pseudo Code

```python
# ── GENERATE MONTHLY WINDOWS FOR BACKFILL ────────────────────────
from dateutil.relativedelta import relativedelta
from datetime import datetime

def get_monthly_windows(start, end):
    windows, curr = [], start
    while curr < end:
        nxt = curr + relativedelta(months=1)
        windows.append({
            "start_dt": curr.strftime('%Y-%m-%d'),
            "end_dt":   min(nxt, end).strftime('%Y-%m-%d')
        })
        curr = nxt
    return windows

# 4 years of history = 48 monthly windows
windows = get_monthly_windows(
    datetime(2020, 1, 1),
    datetime(2024, 1, 1)
)
# ADF ForEach runs these with batchCount=5 (5 months parallel)
```

```python
# ── DATABRICKS BACKFILL NOTEBOOK ─────────────────────────────────
start_dt = dbutils.widgets.get("start_dt")   # e.g. "2021-06-01"
end_dt   = dbutils.widgets.get("end_dt")     # e.g. "2021-07-01"

# Parallel JDBC read — split across 10 Spark tasks by order_id range
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable",
        f"""(SELECT * FROM ORDERS
             WHERE UPDATED_DATE >= DATE'{start_dt}'
             AND   UPDATED_DATE <  DATE'{end_dt}'
            ) t""") \
    .option("numPartitions",  "10") \   # 10 parallel JDBC connections
    .option("partitionColumn","ORDER_ID") \
    .option("lowerBound",     "1") \
    .option("upperBound",     "99999999") \
    .load()

# Add Bronze metadata
df = df.withColumn("load_dt",       F.lit(start_dt).cast("date")) \
       .withColumn("backfill_flag", F.lit(True))

# ── replaceWhere: ONLY overwrites this window's partition ─────────
df.write.format("delta") \
    .option("replaceWhere", f"load_dt >= '{start_dt}' AND load_dt < '{end_dt}'") \
    .mode("overwrite") \   # overwrites ONLY the matching partition range
    .save("/mnt/silver/orders")
    # ↑ All other partitions remain untouched — idempotent & safe
```

```sql
-- ── Verify backfill completeness without full scan ────────────────
SELECT
    DATE_TRUNC('month', load_dt) AS month,
    COUNT(*)                     AS record_count,
    MIN(order_date)              AS earliest_order,
    MAX(order_date)              AS latest_order
FROM silver.orders
WHERE load_dt BETWEEN '2020-01-01' AND '2024-01-01'
GROUP BY 1
ORDER BY 1;
-- Quick check: all 48 months should have expected record counts
```

---

### 🎤 Interview Script
> "Migrating 26 terabytes from Oracle to ADLS with a medallion design presented a real challenge — doing a single full load would have taken 3 to 4 days and any failure would require starting over. I designed a partitioned backfill strategy instead.
>
> First, I broke the 4-year history into 48 monthly windows. ADF ran these through a ForEach with batchCount set to 5 — so 5 months were processed simultaneously, each in its own Databricks job cluster. For reading from Oracle, I used JDBC with numPartitions set to 10 and a numeric partitionColumn — this creates 10 parallel database connections, reading different ID ranges simultaneously, which was much faster than a single-threaded read.
>
> The critical piece for scalability was Delta's `replaceWhere` option on write — this tells Delta to overwrite only the partitions matching the specified condition, leaving all other partitions completely untouched. So if the June 2021 batch failed, I could re-run just that window without touching any other month's data. This made the backfill resumable from any failure point. We ran the full 26TB backfill in about 18 hours with zero data loss and no need to reprocess completed months."

---
---

## Q9. Describe your approach to building reusable ETL components for validations and transformations across multiple domains.

---

### ✅ Short Answer (30 seconds)
> "I extracted common logic — DQ checks, watermark management, audit logging, Delta MERGE — into a shared Python utility library stored in DBFS. Finance and pharma notebooks import from this library. Domain logic stays in the domain notebook; infrastructure plumbing is shared."

---

### 💻 Pseudo Code

```
/dbfs/libs/
    etl_utils/
        __init__.py
        dq_checks.py       ← null checks, duplicate checks, value validation
        delta_ops.py       ← MERGE, OPTIMIZE helpers
        audit_logger.py    ← write to audit table
        watermark_mgr.py   ← read/write watermark from Azure SQL
        type_caster.py     ← safe type casting with error isolation
```

```python
# ── dq_checks.py — reusable across ALL domains ───────────────────

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def check_nulls(df, not_null_cols):
    """Tag records with null in critical columns"""
    status = F.lit("PASS")
    for col in not_null_cols:
        status = F.when(F.col(col).isNull(),
                        F.lit(f"FAIL: null in {col}")).otherwise(status)
    return df.withColumn("dq_status", status)

def check_duplicates(df, pk_cols):
    """Tag duplicate records — keep latest by updated_ts"""
    w = Window.partitionBy(pk_cols).orderBy(F.col("updated_ts").desc())
    return df.withColumn("row_num", F.row_number().over(w)) \
             .withColumn("dq_dup_flag",
                F.when(F.col("row_num") > 1, F.lit("FAIL: duplicate"))
                 .otherwise(F.lit("PASS"))) \
             .drop("row_num")

def check_valid_values(df, col_name, allowed_values):
    """Tag records with values not in allowed list"""
    return df.withColumn("dq_val_flag",
        F.when(~F.col(col_name).isin(allowed_values),
               F.lit(f"FAIL: invalid {col_name}"))
         .otherwise(F.lit("PASS")))
```

```python
# ── FINANCE NOTEBOOK — uses shared lib, domain logic only here ───
import sys
sys.path.append("/dbfs/libs")
from etl_utils.dq_checks import check_nulls, check_duplicates

df_gl = spark.read.format("delta").load("/mnt/bronze/gl_entries")

# Finance-specific domain logic
df_gl = df_gl.filter(F.col("gl_account").startswith("5"))   # revenue accounts only
df_gl = df_gl.withColumn("fiscal_quarter",
            F.quarter(F.col("posting_date")))

# Shared DQ check — same function used in pharma too
df_gl = check_nulls(df_gl, ["gl_account", "amount", "posting_date"])
df_gl = check_duplicates(df_gl, ["gl_account", "posting_date", "document_id"])
```

```python
# ── PHARMA NOTEBOOK — same lib, completely different domain ───────
import sys
sys.path.append("/dbfs/libs")
from etl_utils.dq_checks import check_nulls, check_duplicates, check_valid_values

df_ndc = spark.read.format("delta").load("/mnt/bronze/ndc_records")

# Pharma-specific domain logic
df_ndc = df_ndc.withColumn("dosage_mg", F.regexp_extract("dosage", r"(\d+\.?\d*)", 1))

# Shared DQ — same function, different parameters
df_ndc = check_nulls(df_ndc, ["ndc_number", "drug_name", "patient_id"])
df_ndc = check_valid_values(df_ndc, "record_status", ["ACTIVE","WITHDRAWN"])
```

---

### 🎤 Interview Script
> "When I was working across the FinOps and eCDP projects simultaneously, I noticed we were copy-pasting the same DQ check logic, audit logging code, and Delta MERGE patterns into every notebook. A bug fix in one project's DQ logic wouldn't automatically fix the same bug in the other project. I refactored the common infrastructure into a shared Python package stored in DBFS under `/dbfs/libs/etl_utils`. The package has separate modules for DQ checks, Delta operations, audit logging, watermark management, and safe type casting. Every domain notebook starts with `sys.path.append('/dbfs/libs')` and imports what it needs. The notebook then only contains domain-specific business logic — finance-specific account filters, pharma-specific dosage parsing. Fixing a bug in the shared DQ library instantly fixes it for all 15+ notebooks that import it."

---
---

## Q10. In Medallion architecture, how do you prevent duplicate ingestion from flat files using SQL deduplication and merge strategies?

---

### ✅ Short Answer (30 seconds)
> "I prevent duplicates at three levels: file-level tracking in a control table to skip already-processed files, row-level dedup using `row_number()` window function within each batch, and Delta MERGE upsert into Silver which handles any duplicates that still slip through."

---

### 💻 Pseudo Code

```sql
-- ── LEVEL 1: FILE-LEVEL TRACKING — never process same file twice ──
CREATE TABLE file_control (
    file_name    VARCHAR(500) PRIMARY KEY,
    file_size_kb BIGINT,
    file_hash    VARCHAR(64),       -- MD5 hash — catches renamed duplicates
    processed_ts DATETIME,
    status       VARCHAR(20)        -- 'SUCCESS', 'FAILED', 'IN_PROGRESS'
);

-- ADF Lookup checks before processing:
SELECT COUNT(*) AS already_processed
FROM file_control
WHERE file_name = @FileName
  AND status    = 'SUCCESS';
-- If count > 0: ADF skips this file via If Condition activity
```

```python
# ── LEVEL 2: ROW-LEVEL DEDUP WITHIN BATCH ───────────────────────
df_raw = spark.read \
    .option("header", "true") \
    .csv(f"/mnt/landing/orders/{file_name}")

# Keep only the latest record per primary key within this file
from pyspark.sql.window import Window
from pyspark.sql import functions as F

window = Window.partitionBy("order_id") \
               .orderBy(F.col("updated_date").desc())

df_deduped = df_raw \
    .withColumn("row_num", F.row_number().over(window)) \
    .filter(F.col("row_num") == 1) \     # keep most recent per order_id
    .drop("row_num")

# Quick check
original_count = df_raw.count()
deduped_count  = df_deduped.count()
print(f"Removed {original_count - deduped_count} duplicates within file")
```

```python
# ── LEVEL 3: DELTA MERGE — handles cross-batch duplicates ────────
from delta.tables import DeltaTable

silver = DeltaTable.forPath(spark, "/mnt/silver/orders")

silver.alias("target").merge(
    df_deduped.alias("source"),
    "target.order_id = source.order_id"           # match on PK
).whenMatchedUpdate(
    condition = "source.updated_date > target.updated_date",  # only update if newer
    set = {"amount": "source.amount",
           "status": "source.status",
           "updated_date": "source.updated_date"}
).whenNotMatchedInsertAll() \
 .execute()

# ── Mark file as processed ────────────────────────────────────────
spark.sql(f"""
    UPDATE file_control
    SET status = 'SUCCESS', processed_ts = current_timestamp()
    WHERE file_name = '{file_name}'
""")
```

---

### 🎤 Interview Script
> "Duplicate prevention for flat files is a three-layer problem. Layer one is at the file level — before any processing, ADF checks a file control table in Azure SQL. If the filename is already marked as SUCCESS, ADF skips it entirely using an If Condition activity. I also store the file MD5 hash — so even if someone renames a file to bypass the filename check, the hash match catches it.
>
> Layer two is row-level deduplication within each file. Flat files sometimes contain duplicate rows due to source system export issues. I use a `row_number()` window function partitioned on the primary key, ordered by update timestamp descending — keeping only row number 1 per key, which is the most recent version. Layer three is Delta MERGE when writing to Silver. Even if somehow a duplicate slips through layers one and two, the MERGE statement matches on primary key and only updates if the incoming record is newer than what's already in Silver — otherwise it's a no-op. This three-layer approach guarantees no duplicates in Silver regardless of what the source system sends."

---
---

## ─────────────────────────────────────────
## SECTION 4 — PERFORMANCE & TUNING
## ─────────────────────────────────────────

---

## Q11. What is your approach to using SQL vs Spark SQL for transformations and validations in a lakehouse?

---

### ✅ Short Answer (30 seconds)
> "SQL for business logic, aggregations, and reconciliation — readable and maintainable by analysts. Spark DataFrame API for dynamic/programmatic logic, schema manipulation, and performance-critical workloads. Both use incremental watermark filters on partition columns for efficiency."

---

### 💻 Pseudo Code

```sql
-- ── USE SQL FOR: Business logic, aggregations, reconciliation ────

-- Revenue KPI by region (Gold layer — readable by business team)
SELECT
    region,
    product_category,
    DATE_TRUNC('month', order_date)    AS month,
    SUM(amount)                        AS total_revenue,
    COUNT(DISTINCT customer_id)        AS unique_customers,
    AVG(amount)                        AS avg_order_value,
    SUM(amount) / SUM(SUM(amount))
        OVER (PARTITION BY region)     AS revenue_share_pct
FROM silver.orders
WHERE load_dt >= current_date() - INTERVAL 1 DAY    -- incremental filter
GROUP BY 1, 2, 3;


-- Reconciliation check (count + sum match across layers)
SELECT 'BRONZE' AS layer, COUNT(*) AS cnt, SUM(amount) AS amt
FROM bronze.orders WHERE load_dt = current_date()
UNION ALL
SELECT 'SILVER', COUNT(*), SUM(amount)
FROM silver.orders WHERE load_dt = current_date()
UNION ALL
SELECT 'GOLD',   COUNT(*), SUM(revenue)
FROM gold.revenue_summary WHERE report_date = current_date();
```

```python
# ── USE SPARK DATAFRAME API FOR: Dynamic logic, schema ops ───────

# Dynamic column selection from config — impossible in static SQL
active_cols = get_active_columns_from_config(source_name)
df = spark.read.format("delta").load("/mnt/silver/orders")
df = df.select(*active_cols)   # runtime column list

# Complex running totals with window
from pyspark.sql.window import Window
window_spec = Window.partitionBy("customer_id") \
                    .orderBy("order_date") \
                    .rowsBetween(Window.unboundedPreceding, 0)

df = df.withColumn("customer_running_total",
        F.sum("amount").over(window_spec))

# Broadcast join — hint not available in plain SQL on Databricks
from pyspark.sql.functions import broadcast
df_result = df.join(broadcast(df_small_lookup), "product_id")

# Adaptive shuffle partitions — set dynamically based on data size
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "4000")
```

---

### 🎤 Interview Script
> "My principle is to use the right tool for the job. SQL is the language of business — aggregations, reconciliation checks, and KPI calculations written in SQL can be read and understood by business analysts and reviewed in code reviews without deep Spark knowledge. I use Spark SQL and the DataFrame API when I need dynamic behavior — like selecting columns from a runtime config list, applying window functions across huge datasets with specific memory management, or controlling partition layout explicitly. Both approaches always use incremental watermark filters on partition columns — filtering on `load_dt` or `updated_ts` before any aggregation so Spark prunes irrelevant partitions. I also enable Adaptive Query Execution on all clusters — `spark.sql.adaptive.enabled=true` — which lets Spark automatically adjust shuffle partition count based on actual data skew at runtime."

---
---

## Q12. Can you talk about a challenging situation you faced at work and how you overcame it?

---

### ✅ Short Answer (30 seconds)
> "During the eCDP project, a watermark boundary bug caused 2.3 million duplicate patient records to load silently into Silver. Reports were double-counting. I identified it via data reconciliation, used Delta time travel to restore Silver to the last clean version in under 15 minutes, fixed the root cause, and added automated DQ gates to prevent recurrence."

---

### 🎤 Interview Script — STAR Format

**Situation:**
> "About 8 months into the eCDP pharmaceutical data platform project, after a source Oracle system refresh, our downstream Power BI reports started showing patient counts that were nearly double what the clinical operations team expected. The pipeline had run successfully — ADF showed green, no failures in audit logs — but the data was wrong."

**Task:**
> "My task was to identify the root cause quickly, fix the production data without causing downtime, and prevent it from happening again — all while keeping stakeholders informed."

**Action:**
> "I started by running a reconciliation query comparing Silver record counts per patient against the Oracle source. The Silver counts were almost exactly 2x the source. I then checked the Delta table history using `DESCRIBE HISTORY` and found the duplication happened in a specific run at 2:15 AM. The root cause was a watermark boundary bug — we used `>=` instead of `>` in the incremental query, so records exactly on the watermark timestamp were loaded in both the previous run and the current run. After the Oracle refresh, more records landed precisely on that boundary date.

```python
# BUG: >= caused overlap — records at exact watermark loaded twice
WHERE updated_date >= '2024-03-01 00:00:00'   # wrong

# FIX: strict > prevents overlap
WHERE updated_date > '2024-03-01 00:00:00'    # correct
```

> For the fix, I used Delta time travel to restore Silver to the version just before the bad run:
```sql
RESTORE TABLE delta.`/mnt/silver/patient_records`
TO VERSION AS OF 47;   -- took about 12 minutes, zero downtime
```
> Power BI refreshed automatically from the restored clean data. I then fixed the watermark query, added a post-load DQ check that compares Silver count against a ±5% tolerance of the source count, and wrote a runbook so the team could handle similar issues independently."

**Result:**
> "The restoration took 12 minutes with no downstream impact — reports were accurate by the next business day refresh. The watermark fix was deployed to UAT and PROD within the same day via our CI/CD pipeline. The DQ gate we added has since caught two similar boundary issues in other pipelines before they reached production."

---
---

## Q13. Describe how you apply partition and clustering decisions based on query patterns.

---

### ✅ Short Answer (30 seconds)
> "I analyze the top 20 queries by frequency and cost — columns in WHERE, JOIN ON, and GROUP BY. Low-cardinality columns become partition keys. High-cardinality columns in selective filters become ZORDER targets. I validate decisions using EXPLAIN and Spark UI scan bytes before and after."

---

### 💻 Pseudo Code

```python
# ── STEP 1: Analyze query patterns ──────────────────────────────
# Look at top 20 queries in Databricks Query History
# Note: which columns appear in WHERE, JOIN, GROUP BY

# From analysis of 300+ validation reports:
# WHERE load_dt = ?           → in 100% of queries → PARTITION
# WHERE region = ?            → in 60% of queries  → PARTITION (only 5 values)
# WHERE customer_id = ?       → in 45% of queries  → ZORDER (millions of values)
# WHERE product_id = ?        → in 40% of queries  → ZORDER
# GROUP BY order_date         → aggregation only   → no partition needed

# ── STEP 2: Apply decisions ──────────────────────────────────────
df.write.format("delta") \
    .partitionBy("load_dt", "region") \   # LOW cardinality → partition
    .save("/mnt/silver/orders")

# ZORDER on HIGH cardinality filter columns
spark.sql("""
    OPTIMIZE silver.orders
    ZORDER BY (customer_id, product_id)
""")
```

```sql
-- ── STEP 3: Validate with EXPLAIN ────────────────────────────────
EXPLAIN COST
SELECT COUNT(*), SUM(amount)
FROM silver.orders
WHERE load_dt = current_date()
  AND customer_id = 'C123'
  AND product_id  = 'P456';

-- Look for: PartitionFilters, DataFilters (data skipping)
-- Before ZORDER: numFiles scanned = 5000
-- After  ZORDER: numFiles scanned = 43   ← 99% reduction


-- ── ANTI-PATTERNS TO AVOID ───────────────────────────────────────
-- ❌ NEVER partition on high-cardinality column
--    customer_id has 10M values = 10M tiny files = worse than no partition
-- df.write.partitionBy("customer_id")  ← DON'T DO THIS

-- ❌ NEVER too many partition levels
-- df.write.partitionBy("year","month","day","hour")  ← tiny files

-- ✅ Rule: max 2 partition columns, each with < 500 distinct values
```

---

### 🎤 Interview Script
> "Every partition and ZORDER decision I make starts with query pattern analysis, not assumptions. I look at the most frequent queries hitting the table — what's in the WHERE clause, what's in the JOIN conditions, what columns are most selective. Columns that appear in virtually every query and have low cardinality — under a few hundred distinct values — become partition columns. The classic example is `load_dt` — every incremental query filters by date, and there's one partition per day. Columns with high cardinality — millions of distinct customer IDs, product IDs, NDC numbers — become ZORDER candidates. ZORDER co-locates related rows within files using Delta's multi-dimensional clustering, and Delta maintains per-file min/max statistics, so Spark can skip entire files based on those stats. I always validate decisions using EXPLAIN before and after — looking at `numFilesSkipped` in the plan to confirm data skipping is actually working."

---
---

## Q14. Tell me about a time you reduced pipeline runtime using analytical reasoning.

---

### ✅ Short Answer (30 seconds)
> "Our Silver load was taking 4.2 hours. I profiled the Spark DAG, identified one task running for 3.8 hours due to data skew on a single customer ID. I applied key salting to distribute that customer's records across 10 partitions and broadcast-joined a small product lookup table — reducing runtime from 4.2 hours to 47 minutes."

---

### 💻 Pseudo Code

```python
# ── DIAGNOSIS: Spark UI showed data skew ─────────────────────────
# Stage 4: 200 tasks — 199 tasks finished in 90 seconds
#                        1 task running for 3.8 hours  ← SKEW

# Check key distribution
df.groupBy("customer_id").count() \
  .orderBy("count", ascending=False) \
  .show(5)
# CUSTOMER_A  → 45,000,000 rows  (one B2B account = 40% of data)
# CUSTOMER_B  →  2,100,000 rows
# CUSTOMER_C  →  1,800,000 rows

# ── FIX 1: KEY SALTING to distribute skewed key ───────────────────
import random
from pyspark.sql.functions import concat, lit, floor, rand

SALT_FACTOR = 10   # spread into 10 sub-partitions

# Add random salt 0–9 to order key
df_orders_salted = df_orders.withColumn(
    "salted_key",
    concat(col("customer_id"), lit("_"),
           floor(rand() * SALT_FACTOR).cast("string"))
)

# Explode lookup table to match all salt values
from pyspark.sql.functions import explode, array
salt_df = spark.range(SALT_FACTOR).toDF("salt")
df_customers_exploded = df_customers.crossJoin(salt_df) \
    .withColumn("salted_key",
        concat(col("customer_id"), lit("_"), col("salt").cast("string")))

# Join on salted key — CUSTOMER_A now spread across 10 partitions
df_joined = df_orders_salted.join(df_customers_exploded, "salted_key")

# ── FIX 2: BROADCAST JOIN for small product table ────────────────
# product_lookup was 45MB — causing full shuffle of orders table
from pyspark.sql.functions import broadcast

df_final = df_joined.join(
    broadcast(df_product_lookup),   # 45MB → sent to each executor
    "product_id"                    # no shuffle of large orders table
)
```

```
BEFORE:  4.2 hours | Stage 4: 1 task = 3.8 hours (skew)
AFTER:   47 minutes | All tasks balanced (salting distributed load)

Root cause fix:
  - Salting: CUSTOMER_A's 45M rows now split across 10 partitions
  - Broadcast: product_lookup join no longer shuffles 500GB orders
```

---

### 🎤 Interview Script
> "Our nightly Silver transformation was taking 4.2 hours, which was breaching our 3-hour SLA. I opened the Spark UI and went straight to the Stages view. Stage 4 had 200 tasks — 199 of them finished in about 90 seconds. One task was running for 3.8 hours. That's the fingerprint of data skew — one partition has vastly more data than all others.
>
> I profiled the `customer_id` distribution and found one B2B customer account representing 40% of all order records due to a high-volume enterprise contract. When Spark joined on `customer_id`, all 45 million rows for that customer landed in one task on one executor — which then had to process 40% of the dataset alone while the other 199 executors sat idle.
>
> I fixed this with key salting — I appended a random number 0 to 9 to the customer_id, creating 10 sub-keys per customer. The large customer's 45 million rows were now distributed across 10 partitions. I exploded the smaller lookup table to have one row per salt value so the join still works correctly. The second fix was adding a broadcast hint to a 45MB product lookup table that was previously causing a full shuffle of the 500GB orders table. With both changes, runtime dropped from 4.2 hours to 47 minutes."

---
---

## Q15. How do you debug production pipeline failures — what signals do you check first in ADF/Databricks?

---

### ✅ Short Answer (30 seconds)
> "I follow a structured triage: ADF Monitor for which activity failed → Databricks Job run for notebook error and stack trace → Spark UI for executor/task level failures → Audit table for data-level issues. Prevention via retry policies, Azure Monitor alerts, and post-load DQ gates."

---

### 💻 Pseudo Code

```
── TRIAGE SEQUENCE ──────────────────────────────────────────────

Step 1: ADF Monitor → Pipelines tab
        → Which activity failed?
        → Error code: UserError vs SystemError
        → Copy the error message

Step 2: If Databricks notebook activity failed:
        → Click "Output" on failed activity
        → Get Databricks runId from output JSON
        → Go to Databricks → Jobs → Runs → find runId
        → Find the failed cell + full stack trace

Step 3: Spark-level failure (OOM, shuffle):
        → Databricks → Spark UI → Stages
        → Look for: failed tasks, GC time > 20%, spill to disk
        → Check executor logs for java.lang.OutOfMemoryError

Step 4: Data-level issue (wrong counts, DQ):
        → Query audit_log table for the failed run
        → Compare row counts: rows_read vs rows_loaded
        → Check quarantine table for DQ failures

Step 5: Source system issue:
        → Check Oracle / API availability
        → Was there a source schema change?
        → Check ADF's linked service test connection
```

```sql
-- ── AUDIT TABLE QUERY — first thing checked for data issues ──────
SELECT
    pipeline_name,
    table_name,
    run_id,
    start_time,
    end_time,
    status,
    rows_read,
    rows_loaded,
    rows_read - rows_loaded AS rows_dropped,
    error_message
FROM audit.pipeline_log
WHERE status IN ('FAILED', 'PARTIAL')
  AND start_time >= DATEADD(hour, -24, GETDATE())
ORDER BY start_time DESC;
```

```python
# ── PREVENTION: Structured error handling in notebook ─────────────
try:
    df = read_source(source_config)
    df_clean = apply_transformations(df)
    write_to_silver(df_clean)

    # Post-load validation
    loaded_count = spark.read.format("delta") \
        .load(target_path) \
        .filter(F.col("load_dt") == F.current_date()) \
        .count()

    log_audit(run_id, "SUCCESS", loaded_count)

except Exception as e:
    log_audit(run_id, "FAILED", 0, str(e))
    raise   # re-raise so ADF marks activity FAILED → triggers retry/alert
```

---

### 🎤 Interview Script
> "When a production pipeline fails, I follow a structured triage to avoid wasting time jumping to conclusions. First I check ADF Monitor — it tells me which specific activity failed, the error category, and whether it's a user error like a bad SQL query or a system error like a transient network issue. If it's a Databricks notebook failure, I click Output on the failed activity to get the Databricks run ID, then go directly to that job run to find the failed cell and read the full Python stack trace.
>
> For Spark-level issues — out-of-memory, excessive GC, shuffle spill — I check the Spark UI stages view for failed tasks and executor memory metrics. For data issues I query the audit log table which every pipeline writes to — comparing rows read vs rows loaded immediately tells me if records were dropped. For recurring issues, I do a root cause analysis and add a prevention layer: if it was a transient network error I increase retry count; if it was data quality I add a pre-load validation gate; if it was a schema change I add a contract validation step. The goal is that each failure makes the pipeline more resilient for next time."

---
---

## Q16. In ETL pipelines, how do you decide between full reload, incremental load, and CDC?

---

### ✅ Short Answer (30 seconds)
> "Three decision questions: How large is the table? Does the source have a reliable change column? Do I need to capture deletes? Small/static tables → full reload. Large tables with update timestamp → incremental watermark. Tables where deletes matter or real-time needed → CDC."

---

### 📖 Theory

| Strategy | When to Use | Pros | Cons |
|---|---|---|---|
| **Full Reload** | Small tables < 1M rows, reference/lookup data, no reliable change column | Simple, always consistent | Wasteful for large tables |
| **Incremental (Watermark)** | Large tables with `updated_date` or `updated_ts` | Efficient, scalable | Misses hard deletes |
| **CDC** | Tables with deletes, real-time < 5 min, regulatory audit trail needed | Captures all changes including deletes | Complex setup, higher cost |

---

### 💻 Pseudo Code

```python
# ── FULL RELOAD — small reference table ──────────────────────────
df_ref = spark.read.format("jdbc") \
    .option("dbtable", "REF.PRODUCT_CODES") \   # 500K rows
    .load()
df_ref.write.format("delta").mode("overwrite").save("/mnt/silver/product_codes")
# Simple, fast, always fresh — overwrite is fine at this scale


# ── INCREMENTAL — large transactional table ───────────────────────
# Read last watermark from control table
last_wm = get_watermark("ORDERS")   # e.g. '2024-03-15 14:30:00'

df = spark.read.format("jdbc") \
    .option("dbtable",
        f"""(SELECT * FROM ORDERS
             WHERE UPDATED_DATE > TIMESTAMP'{last_wm}'  -- strict >
             AND   UPDATED_DATE <= SYSDATE              -- upper bound
            ) t""") \
    .option("numPartitions", "20") \
    .load()

# MERGE into Silver — handles both new records and updates
silver.alias("t").merge(df.alias("s"), "t.order_id = s.order_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

update_watermark("ORDERS", new_max_updated_date)


# ── CDC — when deletes matter ─────────────────────────────────────
# Example: patient record withdrawals in pharma (must be deleted from Silver)
# Using Debezium → Kafka → Spark Structured Streaming

from pyspark.sql.functions import from_json

df_cdc = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", "oracle.pharma.patient_records") \
    .load()

# Parse CDC operation type
df_parsed = df_cdc.select(
    from_json(col("value"), cdc_schema).alias("data")
).select("data.*")

# Route by CDC operation
df_inserts = df_parsed.filter(col("op") == "c")   # CREATE
df_updates = df_parsed.filter(col("op") == "u")   # UPDATE
df_deletes = df_parsed.filter(col("op") == "d")   # DELETE

# Apply deletes to Silver — hard delete
silver.alias("t").merge(df_deletes.alias("s"), "t.patient_id = s.patient_id") \
    .whenMatchedDelete() \
    .execute()
```

---

### 🎤 Interview Script
> "My load strategy decision comes down to three questions. First, how big is the table? For reference and lookup tables under about a million rows, full reload is always my first choice — it's simple, always consistent, and the cost is negligible. Second, does the source have a reliable change-tracking column like `updated_date` or `modified_ts`? If yes, incremental watermark is the best balance of simplicity and efficiency — we only pull changed records since the last load and MERGE into Silver. Third, do I need to capture hard deletes? Watermark incremental misses deletes — if someone is withdrawn from a clinical trial in the source system, their record doesn't get a new `updated_date`, so our watermark query never picks up that deletion and Silver has a phantom record. For these cases, I use CDC — either Oracle LogMiner or Debezium writing to Kafka, consumed by Spark Structured Streaming. The SLA also matters — if the business needs data within 5 minutes, only CDC or streaming can deliver that; batch incremental at minimum is hourly."

---
---

## Q17. When tuning Spark SQL for deduplication, what patterns did you avoid to prevent excessive shuffle?

---

### ✅ Short Answer (30 seconds)
> "I avoid `SELECT DISTINCT *` on wide tables — it shuffles every column. I avoid `GROUP BY` on all columns for dedup — same issue. I use `dropDuplicates([pk_cols])` which shuffles only on the key, or `row_number()` window partitioned on PK ordered by timestamp — minimal data movement."

---

### 💻 Pseudo Code

```python
# ══ ANTI-PATTERNS — NEVER USE THESE FOR DEDUP ══════════════════

# ❌ BAD: DISTINCT * shuffles ALL columns across network
df_bad = df.distinct()
# Spark hashes every row using ALL 80 columns → massive network shuffle
# For 500M rows × 80 cols → network transfer ≈ terabytes

# ❌ BAD: GROUP BY all columns — same as DISTINCT
df_bad = spark.sql("""
    SELECT col1, col2, ..., col80, MAX(updated_ts)
    FROM orders
    GROUP BY col1, col2, ..., col80
""")
# 80-column shuffle — extremely expensive


# ══ CORRECT PATTERNS ════════════════════════════════════════════

# ✅ BEST: dropDuplicates on PK only — minimal shuffle
df_deduped = df.dropDuplicates(["order_id"])
# Spark only shuffles on order_id column — 1 column vs 80

# ✅ GOOD: row_number() when you need to pick latest version
from pyspark.sql.window import Window
window = Window.partitionBy("order_id") \
               .orderBy(F.col("updated_ts").desc())

df_deduped = df.withColumn("rn", F.row_number().over(window)) \
               .filter(F.col("rn") == 1) \
               .drop("rn")
# Shuffles only on order_id, sorts only within each partition

# ✅ PRE-REPARTITION before window — reduces cross-executor shuffle
df_repartitioned = df.repartition(200, "order_id")
# Now all rows with same order_id are on same executor
# row_number() window has zero cross-executor data movement
df_deduped = df_repartitioned \
    .withColumn("rn", F.row_number().over(window)) \
    .filter(F.col("rn") == 1).drop("rn")
```

---

### 🎤 Interview Script
> "The biggest dedup anti-pattern I've encountered in production code reviews is `SELECT DISTINCT *` on a wide table. People use it thinking it's simple and clean — and it is in terms of code, but it's extremely expensive in terms of Spark execution. When you select distinct on a table with 80 columns and 500 million rows, Spark has to hash every single column of every row and shuffle rows with identical full-row hashes to the same executor. That's an 80-column shuffle across the network — potentially terabytes of data transfer just for dedup.
>
> The correct approach is to only shuffle on the columns you actually need for dedup — the primary key. `dropDuplicates(['order_id'])` tells Spark to shuffle only on `order_id`, which is a single column — 95% less data in the shuffle. If I need to pick the latest version of each duplicate — which is common for incremental loads — I use `row_number()` partitioned on the primary key and ordered by update timestamp. I also pre-repartition the DataFrame on the PK before the window operation, so rows with the same PK are already on the same executor and the window function runs locally without any additional network movement."

---
---

## Q18. Explain how you use PySpark for deduplication and data cleansing on large datasets; how do you manage shuffles, partitions, and performance tuning?

---

### ✅ Short Answer (30 seconds)
> "I set shuffle partitions at ~128MB per partition of data, repartition on join/dedup keys before window operations, broadcast tables under 200MB, cache DataFrames reused multiple times, and use `coalesce` before writing to control output file count. Monitor via Spark UI for spill and GC."

---

### 💻 Pseudo Code

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── STEP 1: Tune shuffle partitions for data size ─────────────────
# Rule: 1 partition per ~128MB of shuffled data
# Dataset size: 500GB → 500000 / 128 ≈ 4000 partitions
spark.conf.set("spark.sql.shuffle.partitions", "4000")
spark.conf.set("spark.sql.adaptive.enabled",   "true")   # auto-tune at runtime

# ── STEP 2: Read raw data ────────────────────────────────────────
df = spark.read.format("parquet").load("/mnt/bronze/transactions")
# df.rdd.getNumPartitions() → check current partition count

# ── STEP 3: Repartition BEFORE window/groupBy ────────────────────
# Co-locate rows with same PK on same executor → no cross-node shuffle in window
df = df.repartition(4000, "transaction_id")

# ── STEP 4: Data cleansing ───────────────────────────────────────
df = df.dropna(subset=["transaction_id", "amount", "txn_date"]) \
       .withColumn("amount",      F.col("amount").cast("decimal(18,2)")) \
       .withColumn("txn_date",    F.to_date(F.col("txn_date"), "yyyy-MM-dd")) \
       .withColumn("customer_id", F.upper(F.trim(F.col("customer_id")))) \
       .withColumn("load_dt",     F.current_date())

# ── STEP 5: Dedup — minimal shuffle ──────────────────────────────
window = Window.partitionBy("transaction_id").orderBy(F.col("updated_ts").desc())
df = df.withColumn("rn", F.row_number().over(window)) \
       .filter(F.col("rn") == 1).drop("rn")

# ── STEP 6: Cache if reused in multiple downstream operations ─────
df.cache()
df.count()   # materialize cache — triggers actual computation

good_count = df.filter(F.col("amount") > 0).count()   # reads from cache
bad_count  = df.filter(F.col("amount") <= 0).count()  # reads from cache

# ── STEP 7: Broadcast small lookup join ──────────────────────────
df_region  = spark.read.format("delta").load("/mnt/ref/regions")  # 30MB
df_product = spark.read.format("delta").load("/mnt/ref/products") # 15MB

df_final = df.join(F.broadcast(df_region),  "region_code") \
             .join(F.broadcast(df_product), "product_id")
# Both lookups → zero shuffle of the large transactions table

# ── STEP 8: Control output file count ────────────────────────────
# After processing, coalesce before write to avoid too many small files
df_final.coalesce(200) \     # 200 files per partition → ~250MB each
    .write.format("delta") \
    .partitionBy("txn_date") \
    .mode("append") \
    .save("/mnt/silver/transactions")

df.unpersist()   # release cache memory
```

---

### 🎤 Interview Script
> "For large dataset cleansing and dedup, performance tuning is as important as correctness. I always start by setting shuffle partitions based on data volume — roughly one partition per 128 megabytes of shuffled data, so a 500GB dataset gets around 4000 shuffle partitions. I also enable Adaptive Query Execution which lets Spark automatically adjust this at runtime based on actual data sizes.
>
> The most important technique for window-based dedup is pre-repartitioning on the dedup key before the window operation. When rows with the same transaction_id are already on the same executor, the `row_number()` window runs locally with zero cross-executor data movement. Without pre-repartitioning, Spark does an additional shuffle during the window computation.
>
> For joins with small reference tables — anything under 200 megabytes — I always add a broadcast hint. This sends the small table to every executor so the large table never moves — it's the most impactful single optimization for join-heavy pipelines. If a DataFrame is used in multiple downstream operations, I cache it and immediately call count() to materialize — subsequent operations read from memory instead of re-reading from ADLS. Finally, before writing to Delta I use coalesce to reduce the output file count — too many small files is as bad for query performance as too few large files."

---
---

## Q19. How do you handle data type casting between Oracle, flat files, and lakehouse tables?

---

### ✅ Short Answer (30 seconds)
> "I define an explicit type mapping config per source — Oracle NUMBER to decimal, DATE to date, etc. I apply casts in Silver with format patterns for dates. Critically, I keep the raw value alongside the cast value and flag silent nulls from failed casts — routing those records to quarantine with the specific cast failure reason."

---

### 💻 Pseudo Code

```python
# ── TYPE MAPPING CONFIG ───────────────────────────────────────────
oracle_type_map = {
    "NUMBER":    "decimal(18,4)",
    "VARCHAR2":  "string",
    "DATE":      "date",        # Oracle DATE includes time — cast carefully
    "TIMESTAMP": "timestamp",
    "CLOB":      "string",      # large text → string
    "CHAR":      "string"
}

flatfile_type_map = {
    "order_id":    ("long",            None),
    "amount":      ("decimal(18,2)",   None),
    "order_date":  ("date",            "MM/dd/yyyy"),   # format varies by source!
    "quantity":    ("integer",         None),
    "customer_id": ("string",          None)
}

# ── SAFE CAST WITH SILENT-NULL DETECTION ─────────────────────────
def safe_cast_with_validation(df, col_name, target_type, date_fmt=None):
    raw_col = f"_raw_{col_name}"

    # Preserve raw value for comparison
    df = df.withColumn(raw_col, F.col(col_name).cast("string"))

    # Apply cast
    if target_type == "date" and date_fmt:
        df = df.withColumn(col_name, F.to_date(F.col(col_name), date_fmt))
    else:
        df = df.withColumn(col_name, F.col(col_name).cast(target_type))

    # Detect silent failure: raw was non-null but cast result is null
    df = df.withColumn(f"_cast_err_{col_name}",
        F.when(
            F.col(col_name).isNull() & F.col(raw_col).isNotNull(),
            F.lit(f"Cast failed: {col_name} → {target_type} (raw: ") \
             .concat(F.col(raw_col)).concat(F.lit(")"))
        ).otherwise(F.lit(None))
    )

    return df.drop(raw_col)   # drop raw column after validation

# Apply to all columns
for col_name, (dtype, fmt) in flatfile_type_map.items():
    df = safe_cast_with_validation(df, col_name, dtype, fmt)

# Aggregate all cast errors into one column
error_cols = [c for c in df.columns if c.startswith("_cast_err_")]
df = df.withColumn("cast_errors",
    F.concat_ws(" | ", *[F.coalesce(F.col(c), F.lit("")) for c in error_cols])
).drop(*error_cols)

# Split good vs bad
df_good = df.filter(F.col("cast_errors") == "")
df_bad  = df.filter(F.col("cast_errors") != "") \
            .withColumn("quarantine_ts", F.current_timestamp())
```

---

### 🎤 Interview Script
> "Type mismatches between Oracle, flat files, and Delta are one of the most common sources of silent data corruption in ETL pipelines. The key word is 'silent' — in Spark, a failed cast doesn't raise an exception, it returns null. So if Oracle sends `'2024-03-AB'` for a date field and you cast it to date, Spark silently returns null for that row and the pipeline succeeds with green status — but you've just lost data.
>
> My approach is to define explicit type mapping configs per source — no schema inference. Oracle NUMBER maps to decimal with explicit precision, Oracle DATE maps to Spark date, CLOB maps to string. For flat files where dates have varying formats, I specify the exact format pattern per column in the config. During casting, I keep the original raw string value in a temporary column alongside the cast result. After casting, I check: if the raw value was non-null but the cast result is null, that's a silent cast failure — I capture the specific column name, target type, and the actual raw value that caused the failure into a cast error column. Records with any cast errors go to quarantine with the full error description, so the team can see exactly what value caused the problem and trace it to the source."

---
---

## Q20. Explain your design approach for a Medallion Lakehouse on Azure, end to end.

---

### ✅ Short Answer (30 seconds)
> "Bronze = raw as-is Parquet, never transform. Silver = cleaned, deduplicated Delta with DQ gates and MERGE upserts. Gold = business-aggregated Delta for reporting. Each transition has count reconciliation, DQ validation, and audit logging. Incremental refresh uses partition filters at every layer."

---

### 💻 Pseudo Code

```
─── COMPLETE ARCHITECTURE ────────────────────────────────────────

Oracle / Flat Files / APIs / SAP
         │
         ▼  ADF Copy Activity / Python REST Ingestion
┌─────────────────────────────────────┐
│          BRONZE LAYER               │
│  Format: Parquet (CSV converted)    │
│  Transformation: NONE               │
│  Partition: load_dt                 │
│  Retention: 90 days (safety net)    │
│  Location: /mnt/bronze/             │
└─────────────────────────────────────┘
         │
         ▼  Databricks PySpark Notebook
         │  DQ Check → Quarantine bad records
         │  Type cast, dedup, standardize
         │  Delta MERGE (upsert)
         │  Count reconciliation vs Bronze
┌─────────────────────────────────────┐
│          SILVER LAYER               │
│  Format: Delta Lake                 │
│  Transformation: Clean + Conform    │
│  Partition: load_dt, domain         │
│  ZORDER: PK, high-card filter cols  │
│  Location: /mnt/silver/             │
└─────────────────────────────────────┘
         │
         ▼  Databricks SQL/PySpark Notebook
         │  Business logic, JOINs, KPIs
         │  Aggregate reconciliation vs Silver
┌─────────────────────────────────────┐
│           GOLD LAYER                │
│  Format: Delta Lake                 │
│  Transformation: Business KPIs      │
│  Partition: report_date, region     │
│  Location: /mnt/gold/               │
└─────────────────────────────────────┘
         │
         ▼
Power BI / Azure SQL Analytics / APIs
```

```python
# ── INCREMENTAL REFRESH AT EACH LAYER ────────────────────────────

# Bronze → read today's files only
df_bronze = spark.read.format("parquet") \
    .load("/mnt/bronze/orders") \
    .filter(F.col("load_dt") == F.current_date())

# Silver → process only today's Bronze partition
df_silver_input = spark.read.format("parquet") \
    .load("/mnt/bronze/orders") \
    .filter(F.col("load_dt") == F.current_date())   # partition pruning

# Gold → recompute only affected date range
df_silver = spark.read.format("delta") \
    .load("/mnt/silver/orders") \
    .filter(F.col("load_dt") >= F.date_sub(F.current_date(), 1))

df_gold = df_silver.groupBy("region", "report_date") \
    .agg(F.sum("amount").alias("revenue"))

# replaceWhere: only overwrite today's Gold partition
df_gold.write.format("delta") \
    .option("replaceWhere", f"report_date = '{today}'") \
    .mode("overwrite") \
    .save("/mnt/gold/revenue_summary")
```

---

### 🎤 Interview Script
> "My Medallion design follows strict separation of concerns across three layers. Bronze is immutable raw storage — we land data exactly as it comes from the source, converting CSV to Parquet for efficiency but doing zero transformation. Bronze is our permanent safety net — any reprocessing always starts from Bronze. If the Silver logic has a bug, we fix it and reprocess from Bronze without going back to the source system.
>
> Silver is where all the heavy lifting happens — DQ checks, quarantine of bad records, type casting, deduplication, and MERGE upserts into Delta tables. Silver data is row-level, conformed to a standard schema, and trusted by data engineers and scientists. Gold applies business logic — joins across Silver tables, KPI calculations, aggregations — producing datasets that Power BI connects to directly. Gold data is trusted by business users and only contains business-meaningful columns.
>
> Every layer transition has a validation gate: count reconciliation between source and target, DQ check results, and audit log entries with row counts and timestamps. Incremental refresh at each layer uses partition filters — Bronze reads only today's landing files, Silver processes only today's Bronze partition, Gold recomputes only the affected date range using Delta's replaceWhere. This means no layer ever does a full reprocess unless explicitly triggered."

---
---

## ─────────────────────────────────────────
## SECTION 5 — SECURITY & COMPLIANCE
## ─────────────────────────────────────────

---

## Q21. How do you implement data encryption end-to-end for Azure Databricks and ADLS Gen2?

---

### ✅ Short Answer (30 seconds)
> "Encryption at rest via Azure SSE with Customer-Managed Keys in Key Vault. In transit via HTTPS/ABFSS protocol. Access via Managed Identity and Service Principal — no hardcoded credentials. Auditability via Key Vault access logs and Azure Monitor. Unity Catalog for column-level security on PII."

---

### 💻 Pseudo Code

```python
# ── ADLS ACCESS: OAuth via Service Principal — no storage keys ───
spark.conf.set(
    f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{adls_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{adls_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="sp-client-id")      # from KV
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{adls_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-scope", key="sp-client-secret")  # from KV
)
# ↑ No storage account key in code — KV secret never visible in logs
```

```sql
-- ── UNITY CATALOG: Column masking for PII ────────────────────────

-- Create masking policy
CREATE MASKING POLICY pii_mask AS (val STRING)
RETURNS STRING ->
    CASE WHEN is_member('pii_privileged_group')
         THEN val              -- privileged users see real value
         ELSE '***MASKED***'   -- everyone else sees masked value
    END;

-- Apply to sensitive column
ALTER TABLE gold.patient_summary
MODIFY COLUMN patient_name
SET MASKING POLICY pii_mask;

ALTER TABLE gold.patient_summary
MODIFY COLUMN ssn
SET MASKING POLICY pii_mask;
```

```
── ENCRYPTION LAYERS ────────────────────────────────────────────

At Rest:
  ADLS Gen2      → Azure SSE (always on) + CMK in Key Vault
  Delta Tables   → encrypted at ADLS storage layer automatically
  Azure SQL      → Transparent Data Encryption (TDE) always on

In Transit:
  ADF → ADLS     → HTTPS (TLS 1.2+)
  Databricks → ADLS → ABFSS protocol (always encrypted)
  Databricks internode → Spark network encryption:
      spark.authenticate = true
      spark.network.crypto.enabled = true

Access Control:
  ADLS           → RBAC (Storage Blob Data Contributor / Reader)
  ADLS           → ACLs (folder/file level)
  Databricks     → Unity Catalog (table + column level)
  Key Vault      → Access Policies (Get/List — minimum needed)

Auditability:
  Key Vault      → Diagnostic Logs → Log Analytics
  ADLS           → Storage Analytics Logs
  Databricks     → Audit Log → Log Analytics
  Azure Monitor  → Alerts on unauthorized access patterns
```

---

### 🎤 Interview Script
> "End-to-end encryption in our pipeline operates at three independent layers. For data at rest, ADLS Gen2 uses Azure Storage Service Encryption by default, and we configured Customer-Managed Keys in Key Vault — meaning our organization controls the encryption keys, and Azure can't access data without them. For data in transit, all ADF-to-ADLS communication uses HTTPS with TLS 1.2 minimum. Databricks uses the ABFSS protocol for ADLS access which is always encrypted. For inter-node Databricks communication, we enable Spark's built-in network encryption via cluster config.
>
> For access control, we follow least privilege strictly. ADF's managed identity has Storage Blob Data Contributor only on Bronze ADLS — it cannot read Silver or Gold. Databricks service principal has Reader on Silver and Gold. Analysts access Gold only through Unity Catalog which enforces table-level and column-level permissions — PII columns like patient name and SSN are masked by default for non-privileged users. Everything in Key Vault is accessed via managed identity — no passwords or storage keys exist in any code, config file, or environment variable. All Key Vault access is logged and we have Azure Monitor alerts for any access pattern anomalies."

---
---

## Q22. In your ETL from Oracle and flat files, how do you manage schema mapping, data type casting, and handling malformed records safely?

---

### ✅ Short Answer (30 seconds)
> "Schema mapping is defined in a JSON config per source — column renames and target types. Type casting is applied in Silver with Oracle-to-Spark type mapping. Malformed records are detected via silent-null validation after casting and routed to quarantine — never silently dropped, never blocking the pipeline."

---

### 💻 Pseudo Code

```json
// /mnt/config/ORACLE.ORDERS_mapping.json
{
  "source_system": "ORACLE",
  "source_table":  "SALES.ORDERS",
  "column_mapping": [
    { "source": "ORDER_NBR",   "target": "order_id",    "type": "long" },
    { "source": "CUST_ID",     "target": "customer_id", "type": "string" },
    { "source": "ORD_AMT",     "target": "amount",      "type": "decimal(18,2)" },
    { "source": "ORD_DT",      "target": "order_date",  "type": "date",
      "format": "yyyy-MM-dd HH:mm:ss" },
    { "source": "UPD_DTTM",    "target": "updated_ts",  "type": "timestamp" },
    { "source": "ORD_STATUS",  "target": "status",      "type": "string",
      "valid_values": ["OPEN","CLOSED","CANCELLED"] }
  ]
}
```

```python
import json
from pyspark.sql import functions as F

# Load mapping config
mapping = json.loads(dbutils.fs.head(f"/mnt/config/{source_name}_mapping.json"))

# ── STEP 1: Apply column renames ─────────────────────────────────
for col_map in mapping["column_mapping"]:
    df = df.withColumnRenamed(col_map["source"], col_map["target"])

# ── STEP 2: Cast with silent-null detection ───────────────────────
error_flags = []
for col_map in mapping["column_mapping"]:
    tgt    = col_map["target"]
    dtype  = col_map["type"]
    fmt    = col_map.get("format")
    raw_c  = f"_raw_{tgt}"

    df = df.withColumn(raw_c, F.col(tgt).cast("string"))   # preserve raw

    if dtype == "date" and fmt:
        df = df.withColumn(tgt, F.to_date(F.col(tgt), fmt))
    elif dtype == "timestamp" and fmt:
        df = df.withColumn(tgt, F.to_timestamp(F.col(tgt), fmt))
    else:
        df = df.withColumn(tgt, F.col(tgt).cast(dtype))

    # Detect silent failure
    err_col = f"_err_{tgt}"
    df = df.withColumn(err_col,
        F.when(F.col(tgt).isNull() & F.col(raw_c).isNotNull(),
               F.lit(f"{tgt}: cast to {dtype} failed for value=")
                .concat(F.col(raw_c)))
        .otherwise(F.lit(None))
    )
    error_flags.append(err_col)
    df = df.drop(raw_c)

# ── STEP 3: Valid values check ───────────────────────────────────
for col_map in mapping["column_mapping"]:
    if "valid_values" in col_map:
        tgt  = col_map["target"]
        vals = col_map["valid_values"]
        err_col = f"_err_{tgt}_vals"
        df = df.withColumn(err_col,
            F.when(~F.col(tgt).isin(vals) & F.col(tgt).isNotNull(),
                   F.lit(f"{tgt}: invalid value=").concat(F.col(tgt)))
            .otherwise(F.lit(None))
        )
        error_flags.append(err_col)

# ── STEP 4: Aggregate errors and route ──────────────────────────
df = df.withColumn("_all_errors",
    F.concat_ws(" | ", *[F.coalesce(F.col(c), F.lit("")) for c in error_flags])
).drop(*error_flags)

df_good = df.filter(F.trim(F.col("_all_errors")) == "").drop("_all_errors")
df_bad  = df.filter(F.trim(F.col("_all_errors")) != "") \
            .withColumn("quarantine_reason", F.col("_all_errors")) \
            .withColumn("quarantine_ts", F.current_timestamp())

df_bad.write.format("delta").mode("append").save("/mnt/quarantine/orders")
```

---

### 🎤 Interview Script
> "Schema mapping between Oracle and Delta is never done manually in notebooks — I define a JSON config per source that maps Oracle column names to target column names and specifies the target Spark type. This config is version-controlled and reviewed by the data team when onboarding a new source. The Silver notebook reads this config and applies the renames and casts programmatically — no hardcoded column names in notebook code.
>
> The critical part is catching silent cast failures. In Spark, if you cast `'ABC'` to integer, you don't get an exception — you get null. If I didn't detect this, those records would silently lose their values and appear in Silver as nulls. My pattern is to preserve the original raw string value before casting, then after casting compare: if raw was non-null but cast result is null, that's a failure. I capture the column name, target type, and the actual raw value that caused the failure into an error string, then route those records to quarantine. The quarantine record shows exactly `'order_date: cast to date failed for value=2024-AB-CD'` — which tells the operations team precisely what to fix at the source."

---
---

## Q23. How do you verify compliance for regulated environments, balancing 99.9% uptime with strict security controls?

---

### ✅ Short Answer (30 seconds)
> "Compliance is enforced through least-privilege RBAC, CMK encryption, managed identity for all secret access, Unity Catalog PII masking, immutable audit logs, and automated DQ gates before Gold promotion. Uptime is maintained via retry policies, multi-region Key Vault, and Azure Monitor alerts with on-call runbooks."

---

### 💻 Pseudo Code

```
── COMPLIANCE CONTROLS BY LAYER ────────────────────────────────

Identity & Access:
  ✅ Managed Identity for ADF → ADLS, Key Vault (no passwords)
  ✅ Service Principal for Databricks → ADLS, SQL
  ✅ Least privilege: ADF writes Bronze only, Databricks reads Silver/Gold
  ✅ Separate Key Vaults per environment (DEV/UAT/PROD)
  ✅ Key Vault: Get/List only — no create/delete for pipeline identities

Data Encryption:
  ✅ ADLS: SSE + Customer-Managed Key (CMK) in Key Vault
  ✅ Azure SQL: Transparent Data Encryption (TDE)
  ✅ In transit: HTTPS/TLS 1.2 everywhere, ABFSS for ADLS

Data Access:
  ✅ Unity Catalog: table + column level permissions
  ✅ PII columns masked for non-privileged users
  ✅ Row filters for multi-tenant data isolation

Auditability:
  ✅ Every pipeline run logged to audit table (immutable append-only)
  ✅ Key Vault access logged to Log Analytics
  ✅ ADLS access logged to Storage Analytics
  ✅ Databricks access logs → Log Analytics
  ✅ Azure Monitor alerts: unauthorized access, pipeline failures, DQ breaches

Data Quality Gate:
  ✅ Count reconciliation before Gold promotion
  ✅ Quarantine bad records (never delete — regulatory evidence)
  ✅ DQ failure alert if bad% > threshold
```

```python
# ── IMMUTABLE AUDIT LOG — compliance evidence ─────────────────────
def log_compliance_event(event_type, table_name, user, row_count, notes=""):
    spark.sql(f"""
        INSERT INTO audit.compliance_log
        SELECT
            '{event_type}'              AS event_type,
            '{table_name}'             AS table_name,
            '{user}'                   AS user_id,
            current_timestamp()        AS event_ts,
            {row_count}                AS row_count,
            '{notes}'                  AS notes,
            uuid()                     AS event_id
    """)
    # Table has DELETE disabled via Unity Catalog — append-only
```

---

### 🎤 Interview Script
> "In the eCDP pharma project, we operated under strict data handling requirements for clinical trial data. I treated compliance and uptime as complementary, not competing — good security architecture actually improves uptime because it reduces attack surface and prevents incidents.
>
> For compliance, every secret is in Key Vault accessed via managed identity — no passwords exist in any code, notebook, or config. ADLS uses Customer-Managed Keys. Unity Catalog enforces PII column masking at the SQL engine level — analysts running queries can't bypass this even if they know the column name. Every data access and transformation is logged to an immutable audit table — Unity Catalog prevents DELETE on this table even for admins. Bad records are quarantined, never deleted — they're regulatory evidence.
>
> For uptime, I designed for graceful degradation rather than hard failures. Retry policies on all ADF activities handle transient issues. Azure Monitor alerts trigger on-call within 5 minutes of any pipeline failure. Key Vault is geo-redundant so regional outages don't affect secret retrieval. The DQ quarantine pattern means bad source data doesn't stop the pipeline — we handle it gracefully and alert rather than crashing. The combination means we maintained 99.95% pipeline uptime while meeting all compliance requirements."

---
---

## Q24. In your Azure Databricks SQL validations, how did you reduce latency by 40% across 300+ reports?

---

### ✅ Short Answer (30 seconds)
> "Five concrete changes: added partition filters on `load_dt` (90% scan reduction), applied ZORDER on high-cardinality filter columns (data skipping), enabled Delta cache on cluster (repeated queries from SSD), replaced correlated subqueries with window functions (single pass vs N scans), and added broadcast hints for small lookup joins."

---

### 📖 Theory

```
Root Cause Analysis of 300+ slow reports:

  Problem 1: No partition filter → full table scan every query
  Problem 2: Correlated subquery → N×M scans
  Problem 3: No column stats → data skipping disabled
  Problem 4: Small lookup tables not broadcast → full shuffle
  Problem 5: No Delta cache → every identical query re-reads from ADLS
```

---

### 💻 Pseudo Code

```sql
-- ══ TUNING CHANGE 1: Add partition filter ══════════════════════
-- BEFORE: 8 min — scans full 500GB table
SELECT COUNT(*), SUM(amount) FROM silver.orders
WHERE customer_id = 'C123';

-- AFTER: 45 sec — scans only today's 2GB partition
SELECT COUNT(*), SUM(amount) FROM silver.orders
WHERE load_dt    = current_date()   -- ← partition filter ADDED
  AND customer_id = 'C123';
-- Impact: 90% fewer bytes scanned


-- ══ TUNING CHANGE 2: ZORDER on filter columns ═════════════════
OPTIMIZE silver.orders ZORDER BY (customer_id, product_id, region_code);
-- Delta data skipping: files scanned per query 5000 → 180
-- Impact: 96% fewer files scanned after partition filter


-- ══ TUNING CHANGE 3: Replace correlated subquery ══════════════
-- BEFORE: correlated runs once per row = N queries inside M rows
SELECT order_id, amount,
    (SELECT AVG(amount) FROM silver.orders
     WHERE region = o.region) AS region_avg    -- ← runs per row!
FROM silver.orders o;

-- AFTER: window function = single pass
SELECT order_id, amount,
    AVG(amount) OVER (PARTITION BY region) AS region_avg   -- ← one scan
FROM silver.orders
WHERE load_dt = current_date();
-- Impact: N scans → 1 scan


-- ══ TUNING CHANGE 4: Broadcast small lookup join ══════════════
-- BEFORE: full shuffle of 500GB orders to join with 40MB region table
SELECT o.*, r.region_name FROM silver.orders o
JOIN ref.regions r ON o.region_code = r.region_code;

-- AFTER: 40MB table broadcast to each executor, orders table never moves
SELECT /*+ BROADCAST(r) */ o.*, r.region_name FROM silver.orders o
JOIN ref.regions r ON o.region_code = r.region_code
WHERE o.load_dt = current_date();
-- Impact: eliminated ~200GB shuffle per query
```

```python
# ══ TUNING CHANGE 5: Enable Delta Cache on cluster ════════════
# In Databricks cluster configuration (Advanced options → Spark config):
# spark.databricks.io.cache.enabled true
# spark.databricks.io.cache.maxDiskUsage 200g    ← SSD cache size
# spark.databricks.io.cache.maxMetaDataCache 1g

# Effect: first query reads from ADLS (slow)
# Subsequent identical queries read from local NVMe SSD (10x faster)
# 300+ reports hitting same Gold tables → massive cache hit rate
```

```
── MEASURED RESULTS ──────────────────────────────────────────

Metric                 Before    After    Change
─────────────────────────────────────────────────
Avg query time         4.2 min   2.5 min  -40%
Bytes scanned/query    ~450 GB   ~45 GB   -90%
Files scanned/query    ~5000     ~180     -96%
Cluster CPU peak       95%       58%      -39%
Daily compute cost     $420      $210     -50%
```

---

### 🎤 Interview Script
> "Reducing latency by 40% across 300 reports was a systematic project, not a single fix. I started with profiling — I used Databricks Query History to identify the 20 slowest queries by runtime, then ran EXPLAIN on each one to see the query plan, and checked scan bytes in the Spark UI.
>
> The biggest single win was partition filters. Most reports had no `load_dt` filter — they were scanning the full 500GB Silver table to get today's data. Adding `WHERE load_dt = current_date()` cut scan bytes by 90% per query. This alone was probably 25% of the total improvement.
>
> Second, I ran OPTIMIZE with ZORDER BY on the high-cardinality columns that appeared most in WHERE clauses — customer_id, product_id, region_code. After ZORDER, Delta's data skipping reduced files scanned from around 5000 to under 200 per query. Third, I replaced all correlated subqueries with window functions — a correlated subquery re-executes for every row in the outer query, which is an N-times scan. A window function does a single pass. Fourth, I added broadcast hints to all joins involving reference tables under 50 megabytes. Fifth, I enabled the Delta result cache on the Gold query cluster — the first query reads from ADLS, subsequent identical queries read from local SSD which is 10 times faster. Since 300 reports hit similar Gold tables repeatedly, the cache hit rate was very high. Combined, these changes brought average query time from 4.2 minutes to 2.5 minutes, and daily compute costs dropped by 50%."

---

---

## ═══════════════════════════════════════════
## QUICK REFERENCE CHEAT SHEET
## ═══════════════════════════════════════════

| Topic | Key Concept | One-line Memory Aid |
|---|---|---|
| REST API Retries | Exponential backoff | Wait 1s, 2s, 4s — not fixed interval |
| Pagination | Cursor loop until no next_cursor | Keep looping until empty |
| Schema Evolution | `mergeSchema=true` | New API columns auto-added to Delta |
| ZORDER | High-cardinality co-location | Fewer files scanned, not fewer partitions |
| Partition | Low-cardinality only | date, region — NEVER customer_id |
| XML Streaming | `iterparse + elem.clear()` | Process 1 element, free memory, repeat |
| DQ Quarantine | Tag → Split → Write separately | Never stop production for bad data |
| Dedup | `dropDuplicates([pk])` not `distinct()` | Shuffle only key column, not all 80 |
| Broadcast Join | Tables < 200MB | Small table to every executor — no shuffle |
| Delta MERGE | Upsert = update + insert | Idempotent, handles cross-run duplicates |
| Time Travel | `RESTORE TO VERSION AS OF N` | Instant rollback — 12 min, no downtime |
| Salting | Append random 0–9 to skewed key | Distribute one hot key across 10 partitions |
| Cache | `.cache()` then `.count()` | Materialize immediately or cache is lazy |
| replaceWhere | Overwrite only matching partition | Backfill safe — other partitions untouched |
| Watermark Bug | Use `>` not `>=` | Strict greater-than prevents boundary overlap |
| Silent Cast Null | Preserve raw, compare after cast | `raw != null AND cast == null` = failure |
| CDC vs Incremental | CDC for deletes, incremental for updates | Watermark misses hard deletes |
| coalesce before write | Control output file count | Too many small files = slow reads |
| AQE | `spark.sql.adaptive.enabled=true` | Auto-tunes partitions at runtime |

---

*© Mayuresh Uttam Patil — Interview Preparation Document — Confidential*
