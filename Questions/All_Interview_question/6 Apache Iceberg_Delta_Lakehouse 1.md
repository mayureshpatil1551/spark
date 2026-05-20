# 🧊 Apache Iceberg / Delta / Lakehouse — Interview Answers
> **Format:** Short · Simple · Clear | Mayuresh Patil | AbbVie / Cognizant

---

## 📚 THEORY QUESTIONS

---

### Q1. Delta vs Iceberg vs Hudi

**One-liner:**
> All three add ACID transactions to data lakes on top of Parquet files. They differ in how they manage metadata and which ecosystems they support best.

---

**What to say:**

*"All three solve the same problem — raw Parquet on S3 has no transactions, no schema enforcement, no versioning. They add a metadata layer on top to fix that.*

*Delta Lake was built by Databricks. It uses a transaction log — a folder of JSON files tracking every commit. Tightly integrated with Spark and Databricks. Best choice if your entire stack is Databricks.*

*Apache Iceberg was built at Netflix, donated to Apache. Most vendor-neutral — works with Spark, Trino, Flink, Snowflake all reading the same table. Its metadata is a tree of files — snapshots → manifests → data files — which makes large-scale queries much faster. Also has hidden partitioning and partition evolution without data rewrites. This is what we used at AbbVie.*

*Apache Hudi was built at Uber. Optimized for high-frequency CDC and upserts. Has two modes — Copy-on-Write for read-heavy, Merge-on-Read for write-heavy workloads. Best for millions of upserts per hour.*"

---

**Comparison table:**

| Feature | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| Built by | Databricks | Netflix / Apache | Uber |
| Multi-engine support | Limited | ✅ Best | Good |
| Hidden partitioning | ❌ No | ✅ Yes | ❌ No |
| Partition evolution | Requires rewrite | ✅ No rewrite | Limited |
| Best for | Databricks stack | Multi-engine lake | High-freq CDC |
| Metadata approach | JSON transaction log | Tree: snapshot → manifest | Timeline + index |

**My choice — AbbVie:**
> *"We chose Iceberg because we needed Spark for ingestion AND Trino for ad-hoc queries on the same tables. Delta didn't support Trino well at the time. Iceberg's hidden partitioning also let us change partition strategy on 20 TB tables without rewriting data."*

---

### Q2. How Does Iceberg Handle Metadata?

**One-liner:**
> Iceberg uses a 4-level metadata tree — current metadata file → snapshot → manifest list → manifest files — where each level stores stats that let Spark skip irrelevant files without scanning everything.

---

**What to say:**

*"Think of it as a funnel. At the top is a single metadata JSON file — the entry point to the table. It points to the current snapshot.*

*The snapshot represents the table at one point in time. Every commit creates a new snapshot — that's how time travel works.*

*The snapshot points to a manifest list — an Avro file listing all manifests for that snapshot, with partition-level min/max stats for each.*

*Each manifest file tracks a subset of data files — Parquet files — with column-level min/max and null stats per file.*

*When a query runs with a WHERE filter, Spark reads the manifest list, skips manifests whose partition stats don't match, reads relevant manifests, skips data files whose column stats don't match — and opens only the files that can contain the answer.*

*On a 20 TB table, a typical filtered query might read 2-3 GB instead of 20 TB — because most files are skipped using metadata stats alone.*"

---

**Visual:**
```
iceberg table/
└── metadata/
    ├── v1.metadata.json        ← entry point (updated atomically per commit)
    │    └── → snapshot-001
    ├── snapshot-001.avro       ← point-in-time state (time travel)
    │    └── → manifest-list-001
    ├── manifest-list-001.avro  ← lists manifests + partition stats
    │    ├── manifest-A: [region=US, date 2024-01 to 2024-03]
    │    └── manifest-B: [region=EU, date 2024-01 to 2024-03]
    └── manifest-A.avro         ← lists data files + column min/max stats
         ├── part-001.parquet   [patient_id: P001..P500]
         └── part-002.parquet   [patient_id: P501..P999]

Query: WHERE region='US' AND patient_id='P750'
  → Skip manifest-B (EU only)
  → Skip part-001 (P001-P500, no P750)
  → Read only part-002 ✅  (1 file out of 100+)
```

---

### Q3. Table Formats vs File Formats

**One-liner:**
> A file format defines how data is encoded inside one file (bytes, columns, compression). A table format manages a collection of files as a logical table — adding transactions, schema, versioning, and partitioning on top.

---

**What to say:**

*"These are two different layers and you need both.*

*A file format — Parquet, ORC, Avro, CSV — defines the bytes inside one file. Parquet is columnar — groups values by column, compresses per column, stores min/max per row group. ORC is similar, older, more common in Hive. Avro is row-oriented — better for streaming writes. CSV is plain text — no compression, no stats.*

*A table format — Iceberg, Delta, Hudi — manages a collection of files as a table. It adds: atomic writes, schema enforcement, partition tracking, file-level statistics for skipping, and versioning for time travel. It sits on top of the file format.*

*An Iceberg table on Parquet = Parquet's columnar compression and row-group stats PLUS Iceberg's ACID transactions, partition pruning, and time travel. They work together.*"

---

**Summary:**
```
File Format  →  how bytes are stored in ONE file
               Parquet (columnar) | ORC | Avro | CSV

Table Format →  how a COLLECTION of files is managed as a table
               Iceberg | Delta | Hudi

Stack at AbbVie:
  Iceberg (table format) + Parquet (file format) + ZSTD (compression)
  Iceberg gives: ACID, schema, partitioning, time travel
  Parquet gives: columnar reads, compression, row-group stats
  ZSTD gives:   40-50% better compression than Snappy on structured data
```

---

### Q4. Iceberg Partitioning & Hidden Partitions

**One-liner:**
> Iceberg separates the partition logic from the schema — you define a transform on a column (like `days(event_ts)` or `bucket(100, patient_id)`), Iceberg computes partition values automatically during writes and applies pruning during reads, without adding extra columns to your table.

---

**What to say:**

*"In Hive or Delta, to partition by date you add a `load_date` column to your schema, populate it manually on every write, and filter on it in every query. The partition is part of the visible schema.*

*In Iceberg, you declare a transform — `days(event_ts)` — and Iceberg computes the partition from the existing `event_ts` column. No extra column. No manual population. You query with WHERE event_ts >= '2024-01-01' and Iceberg automatically prunes. That's hidden partitioning.*

*The transforms available:*
- `days(ts)` — partition by date extracted from timestamp
- `months(ts)`, `years(ts)`, `hours(ts)` — other time granularities
- `bucket(N, col)` — N fixed hash buckets — for high-cardinality columns like patient_id without creating millions of folders
- `truncate(col, L)` — first L characters of a string

*The other big advantage is partition evolution. In Delta/Hive, changing partition strategy means rewriting all your data — weeks of compute for 20 TB. In Iceberg, partition evolution is a metadata-only operation. New writes use the new spec, old files keep the old spec, readers handle both transparently. No data rewrite.*"

---

**Code:**
```python
# Create table with hidden partitions — NO extra columns added to schema
spark.sql("""
    CREATE TABLE iceberg.silver.patient_events (
        patient_id   STRING,
        event_ts     TIMESTAMP,   -- partition source, stays as-is in schema
        region       STRING,
        event_type   STRING
    ) USING iceberg
    PARTITIONED BY (
        days(event_ts),           -- extracts date automatically
        bucket(4, region)         -- 4 fixed buckets for region
    )
""")

# Write — Iceberg computes partition values automatically
df.writeTo("iceberg.silver.patient_events").append()

# Read — pruning happens automatically on the source column
spark.sql("""
    SELECT * FROM iceberg.silver.patient_events
    WHERE event_ts >= '2024-01-01'   -- maps to days(event_ts) partition pruning
      AND region = 'US'              -- maps to bucket(4,'US') = bucket 2
""")
# Opens only Jan+ files in bucket 2 — no full scan ✅

# Partition evolution — no data rewrite needed
spark.sql("""
    ALTER TABLE iceberg.silver.patient_events
    ADD PARTITION FIELD months(event_ts)  -- switch new writes to monthly
""")
# Old files: daily partitions | New files: monthly | Readers handle both ✅
```

---

---

## 🎯 SCENARIO QUESTIONS

---

### Scenario 1. How Did You Migrate 20 TB to Iceberg? What Were the Challenges?

**One-liner:**
> We migrated in phases — parallel write first, then consumer cutover, then decommission. Key was making each partition migration idempotent so failures could be retried safely.

---

**What to say:**

*"We had existing Hive tables — raw Parquet files partitioned by load_date on ADLS. We needed to move to Iceberg for ACID guarantees, hidden partitioning, and Trino support.*

*We did it in 3 phases to avoid downtime. First, we ran new ingestion writing to BOTH Hive and Iceberg in parallel. Validated that Iceberg output matched Hive row-by-row. No consumers moved yet — zero risk.*

*Second phase — moved Spark jobs to read from Iceberg. Validated query results matched. Hive stayed as fallback.*

*Third phase — stopped dual-writing, decommissioned Hive tables, reclaimed storage.*

*For the actual data migration, we wrote a PySpark job that migrated one date partition at a time — idempotent, so if it failed mid-way, the next retry was safe. We ran 20 parallel Databricks jobs across date ranges to complete in 3 days instead of weeks.*"

---

**Challenges and fixes:**

| Challenge | Root Cause | Fix |
|---|---|---|
| Migration too slow | Sequential partition-by-partition | Parallelized — 20 Databricks jobs across date ranges |
| Checksum mismatch on 12 partitions | Hive had duplicates — Iceberg dedup removed them | Explicit dedup step during migration + documented |
| Hive Metastore overloaded | 500K+ files overwhelmed MySQL-backed HMS | Moved Iceberg catalog to Nessie — HMS only for legacy |
| Old queries breaking post-cutover | Queries used `WHERE load_date=...` (Hive column, not needed in Iceberg) | Updated queries to `WHERE event_ts >= ...` + added compat views |
| Partition strategy needed to change | Old: by load_date / New: by days(event_ts) | Iceberg partition evolution — metadata-only, no rewrite |

---

**Key code pattern — idempotent partition migration:**

```python
def migrate_partition(date_str, source_table, target_table):
    df = spark.sql(f"""
        SELECT * FROM {source_table}
        WHERE load_date = '{date_str}'
    """)

    row_count = df.count()
    if row_count == 0:
        return 0

    # Repartition for optimal Iceberg file sizes (~512 MB each)
    partitions = max(1, row_count // 500_000)

    df.repartition(partitions, col("region")) \
      .writeTo(target_table) \
      .overwritePartitions()  # idempotent — safe to re-run ✅

    return row_count


def validate_partition(date_str, hive_table, iceberg_table):
    hive_count    = spark.sql(f"SELECT COUNT(*) FROM {hive_table} WHERE load_date='{date_str}'").collect()[0][0]
    iceberg_count = spark.sql(f"SELECT COUNT(*) FROM {iceberg_table} WHERE event_ts >= '{date_str}' AND event_ts < date_add('{date_str}', 1)").collect()[0][0]
    match = hive_count == iceberg_count
    print(f"{'✅' if match else '❌'} {date_str}: Hive={hive_count} | Iceberg={iceberg_count}")
    return match
```

---

### Scenario 2. How Did You Ensure Schema Consistency Across Layers?

**One-liner:**
> Bronze absorbs all changes (mergeSchema=true). Silver validates strictly — pipeline fails loudly on unexpected changes, which acts as an alert. Gold is strictest. Schema changes follow a controlled process: alert → review → ALTER TABLE → backfill → resume.

---

**What to say:**

*"The core idea is — each layer has a different tolerance for schema change, and that tolerance decreases as you go downstream.*

*Bronze is a raw archive. We used mergeSchema=true — if Veeva API added a new field, Bronze absorbed it silently. No human action needed. We want complete source data.*

*Silver is the trust boundary. We had strict schema validation — if something unexpected appeared, the pipeline failed with a clear error. That failure was intentional — it acted as an alert to a human to review whether the change was safe to propagate downstream.*

*Gold is strictest. Changes here must be coordinated with BI consumers — because Tableau and Power BI dashboards have hardcoded column references. A rename at Gold breaks every report.*

*For schema change process: When Veeva added a new field — pipeline failed at Silver boundary → we got an alert → reviewed impact → PR to update schema definition in Git → ALTER TABLE on Silver → backfill historical NULL for new column → resume pipeline. Controlled, documented, auditable.*"

---

**Layer-by-layer rules:**

```
BRONZE  → mergeSchema = true
          Accept new columns silently
          Purpose: preserve raw source data completely

SILVER  → mergeSchema = false  (Iceberg default — strict)
          Reject unexpected columns → pipeline FAILS loudly → alert triggered
          Purpose: protect downstream from silent drift

GOLD    → mergeSchema = false  (strictest)
          Coordinate with BI team before any change
          Purpose: dashboards break on unexpected column changes
```

---

**Schema change handling:**

```python
# Bronze write — absorb everything
df_raw.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \      # ← new columns absorbed silently
    .save("/mnt/bronze/veeva_patients/")


# Silver write — strict enforcement (default, no option needed)
try:
    df_clean.writeTo("iceberg.silver.patient_master").append()

except Exception as e:
    if "schema" in str(e).lower():
        # Intentional fail — send alert and stop
        send_alert(f"Schema mismatch at Silver — review before resuming: {e}")
    raise   # Re-raise — never swallow schema errors


# Schema change process when new field approved:
# Step 1: Add column to Iceberg Silver
spark.sql("""
    ALTER TABLE iceberg.silver.patient_master
    ADD COLUMN preferred_language STRING
""")
# Old rows: preferred_language = NULL (Iceberg handles automatically)
# New rows: populated from source

# Step 2: Update conform logic to map Bronze field → Silver column
# Step 3: Resume pipeline — now passes schema validation ✅
```

---

**Schema validation function (lightweight version):**

```python
def validate_schema(df, expected_cols: dict, table_name: str):
    """
    expected_cols = {"patient_id": "string", "territory_id": "string", ...}
    Raises clear exception with field-level diff.
    """
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    missing   = set(expected_cols) - set(actual)
    type_diff = {
        col: f"expected {expected_cols[col]}, got {actual[col]}"
        for col in expected_cols
        if col in actual and actual[col] != expected_cols[col]
    }

    issues = []
    if missing:     issues.append(f"MISSING: {missing}")
    if type_diff:   issues.append(f"TYPE MISMATCH: {type_diff}")

    if issues:
        raise Exception(f"Schema failed for {table_name}: {' | '.join(issues)}")
    print(f"✅ Schema OK: {table_name}")


# Usage at every Bronze → Silver boundary
EXPECTED_SILVER_SCHEMA = {
    "patient_id":   "string",
    "territory_id": "string",
    "region":       "string",
    "status":       "string",
    "record_hash":  "string"
}

validate_schema(df_incoming, EXPECTED_SILVER_SCHEMA, "silver.patient_master")
```

---

## 📊 Quick Reference

### Choose Your Table Format
```
Entire stack is Databricks?           → Delta Lake
Need Spark + Trino + Flink + Snowflake → Apache Iceberg ✅ (AbbVie choice)
High-frequency CDC (millions/hour)?   → Apache Hudi
```

### Iceberg Partition Transforms
```
days(event_ts)        → daily partition    ← most common
months(event_ts)      → monthly
hours(event_ts)       → streaming / hourly
bucket(N, patient_id) → N fixed buckets    ← high cardinality keys
identity(region)      → categorical        ← low cardinality
```

### Schema Rules by Layer
```
Bronze  → mergeSchema=true   absorb changes
Silver  → strict             fail loudly on change → alert
Gold    → strictest          coordinate with BI consumers
```

### File Format vs Table Format
```
File Format   = bytes inside one file    (Parquet, ORC, Avro)
Table Format  = manages collection of files as table (Iceberg, Delta, Hudi)
Stack         = Table Format + File Format + Compression
AbbVie stack  = Iceberg      + Parquet    + ZSTD
```

---

*Mayuresh Uttam Patil | Data Engineer | AbbVie / Cognizant*
