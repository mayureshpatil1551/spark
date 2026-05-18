# 🛡️ Data Governance & Quality + Apache Iceberg / Lakehouse
> **Format:** Speak-ready Scripts + Scenario Code | Real-World Context
> **Target Role:** Data Engineer | Mayuresh Patil | AbbVie / Cognizant

---

## 🎤 HOW TO USE THIS GUIDE
> Each theory answer has **3 parts:**
> - 📌 **Opening line** — say this first, confidently
> - 🗣️ **Script** — what to say (natural, not robotic)
> - ✅ **Closing hook** — tie it to your real experience

---

# PART A — DATA GOVERNANCE & QUALITY

---

## 📚 SECTION 1 — THEORY QUESTIONS

---

### Q1. What Are Common DQ Checks?

---

📌 **Opening line:**
> *"Data quality checks are programmatic rules that validate data before it moves between layers. The most common categories are completeness checks, validity checks, consistency checks, referential integrity checks, and freshness checks — each catching a different class of data problem."*

---

🗣️ **Script to say:**

*"Let me walk through each category with what it actually catches in practice.*

*First — **Null / Completeness checks**. These are your most basic but most important checks. Mandatory fields like patient_id or territory_id must never be null — if they are, downstream joins silently drop rows or produce wrong aggregations. You check for nulls on every primary key, every foreign key, and every field that drives business logic.*

*Second — **Referential integrity checks**. This asks: does every foreign key in your fact table have a matching record in the dimension table? If an event references territory_id T999 and T999 doesn't exist in your territories table, that event is an orphan. It won't join. It'll disappear silently from all reports. You catch this by doing a LEFT ANTI JOIN — find all rows in the fact that have no match in the dimension.*

*Third — **Range and domain checks**. Business rules encoded as constraints. Age must be between 0 and 120. Status must be in the set ACTIVE, INACTIVE, PENDING. Revenue must be non-negative. Visit duration can't be negative minutes. These seem obvious but API responses and database exports violate them more often than you'd expect.*

*Fourth — **Uniqueness checks**. Primary keys must be unique. Composite keys must be unique together. Duplicates in a patient master table, for example, will cause fan-out in joins — counts doubling silently.*

*Fifth — **Freshness checks**. This one is often overlooked. You verify that new data actually arrived. If your pipeline ran successfully but the source system had an outage and sent zero rows — that looks like success from the pipeline perspective, but your Silver table now has a data gap. A freshness check says: the maximum event_date in this table must be within the last 2 hours.*

*Sixth — **Schema checks**. Column count, column names, data types — do they match what you expect? A source system changing a column type from integer to string breaks your entire downstream pipeline silently or loudly depending on your type handling.*

*The key architecture decision is: where do you apply each check? Null and schema checks go at Bronze — catch issues before they propagate. Business rule and referential checks go at Silver — the data is cleansed and typed, ready for validation against dimensions. Freshness and aggregate checks go at Gold — ensuring the final layer is complete and accurate before BI tools consume it.*"

---

✅ **Closing hook:**
> *"At AbbVie, I implemented DQ checks at every layer. Bronze had schema checks and null checks on primary keys. Silver had referential integrity checks against territory and diagnosis dimensions, range checks on numeric fields, and uniqueness checks before the MERGE INTO. Any failed records were routed to a quarantine Iceberg table — iceberg.dq.failed_records — with the failing rule name attached. We reviewed that table weekly and fed patterns back to source system owners."*

---

**DQ Check Reference with PySpark code:**

```python
from pyspark.sql.functions import col, count, countDistinct, max as spark_max
from pyspark.sql.functions import current_timestamp, datediff, lit, when
from datetime import datetime, timedelta

class DQChecker:
    def __init__(self, df, table_name: str, spark):
        self.df         = df
        self.table_name = table_name
        self.spark      = spark
        self.results    = []

    # ── 1. NULL / COMPLETENESS ─────────────────────────────────────────
    def check_not_null(self, columns: list):
        for col_name in columns:
            null_count = self.df.filter(col(col_name).isNull()).count()
            self.results.append({
                "check": "NOT_NULL",
                "column": col_name,
                "failed_rows": null_count,
                "passed": null_count == 0
            })
        return self

    # ── 2. UNIQUENESS ─────────────────────────────────────────────────
    def check_unique(self, columns: list):
        total    = self.df.count()
        distinct = self.df.select(columns).distinct().count()
        dups     = total - distinct
        self.results.append({
            "check":       "UNIQUENESS",
            "column":      str(columns),
            "failed_rows": dups,
            "passed":      dups == 0
        })
        return self

    # ── 3. DOMAIN / ALLOWED VALUES ────────────────────────────────────
    def check_allowed_values(self, col_name: str, allowed: list):
        failed = self.df.filter(~col(col_name).isin(allowed) & col(col_name).isNotNull()).count()
        self.results.append({
            "check":       "ALLOWED_VALUES",
            "column":      col_name,
            "failed_rows": failed,
            "passed":      failed == 0
        })
        return self

    # ── 4. RANGE CHECK ────────────────────────────────────────────────
    def check_range(self, col_name: str, min_val, max_val):
        failed = self.df.filter(
            (col(col_name) < min_val) | (col(col_name) > max_val)
        ).count()
        self.results.append({
            "check":       "RANGE",
            "column":      col_name,
            "failed_rows": failed,
            "passed":      failed == 0
        })
        return self

    # ── 5. REFERENTIAL INTEGRITY ──────────────────────────────────────
    def check_referential_integrity(self, fk_col: str, ref_df, ref_col: str):
        orphans = self.df.join(ref_df, self.df[fk_col] == ref_df[ref_col], "left_anti").count()
        self.results.append({
            "check":       "REFERENTIAL_INTEGRITY",
            "column":      fk_col,
            "failed_rows": orphans,
            "passed":      orphans == 0
        })
        return self

    # ── 6. FRESHNESS CHECK ────────────────────────────────────────────
    def check_freshness(self, ts_col: str, max_age_hours: int = 25):
        max_ts    = self.df.agg(spark_max(col(ts_col))).collect()[0][0]
        age_hours = (datetime.now() - max_ts).total_seconds() / 3600 if max_ts else 9999
        self.results.append({
            "check":       "FRESHNESS",
            "column":      ts_col,
            "failed_rows": 0 if age_hours <= max_age_hours else 1,
            "passed":      age_hours <= max_age_hours,
            "detail":      f"Latest data: {max_ts}, Age: {age_hours:.1f}h"
        })
        return self

    # ── Run and route results ─────────────────────────────────────────
    def run(self, fail_on_error: bool = False):
        failed = [r for r in self.results if not r["passed"]]
        passed = [r for r in self.results if r["passed"]]

        print(f"\n{'='*60}")
        print(f"DQ Report: {self.table_name}")
        print(f"  Passed: {len(passed)} | Failed: {len(failed)}")
        for r in failed:
            print(f"  ❌ {r['check']} on {r['column']}: {r['failed_rows']} failed rows")
        for r in passed:
            print(f"  ✅ {r['check']} on {r['column']}")

        if failed and fail_on_error:
            raise Exception(f"DQ failed: {len(failed)} checks failed on {self.table_name}")

        return {"passed": passed, "failed": failed}


# ── Usage at Silver ingestion ─────────────────────────────────────────
territory_df = spark.read.format("iceberg").load("iceberg.silver.territories")

dq = DQChecker(df_incoming, "silver.patient_events", spark)
results = (
    dq
    .check_not_null(["patient_id", "territory_id", "event_date"])
    .check_unique(["event_id"])
    .check_allowed_values("status", ["ACTIVE", "INACTIVE", "PENDING"])
    .check_range("duration_minutes", 0, 480)
    .check_referential_integrity("territory_id", territory_df, "territory_id")
    .check_freshness("event_date", max_age_hours=25)
    .run(fail_on_error=False)
)

# Quarantine failed records
if results["failed"]:
    df_incoming \
        .filter(col("patient_id").isNull() | col("event_id").isNull()) \
        .withColumn("_dq_rule",     lit("NULL_PRIMARY_KEY")) \
        .withColumn("_dq_check_ts", current_timestamp()) \
        .writeTo("iceberg.dq.failed_records").append()
```

---

### Q2. What is Data Lineage? Why is it Important?

---

📌 **Opening line:**
> *"Data lineage is the complete map of where data comes from, how it has been transformed at each step, and where it ultimately ends up — a full audit trail from source to consumption that lets you answer 'how was this number calculated?' for any data point in your system."*

---

🗣️ **Script to say:**

*"Think about a scenario every data engineer eventually faces. A business analyst looks at a dashboard and says 'this patient count looks wrong — it's 10% lower than last week.' Without lineage, you're manually tracing through 6 notebooks, 3 ADF pipelines, and 4 Iceberg tables trying to figure out where those patients went. With lineage, you can immediately see the full path — Veeva API → Bronze raw → Silver patient_master → Gold territory_summary — and identify which transformation step dropped them.*

*Lineage has two forms. **Technical lineage** tracks table-to-table and column-to-column transformations — which columns from Bronze were used to derive Silver columns, which Silver tables feed into Gold aggregates. **Business lineage** maps technical assets to business terms — 'patient_count in the Tableau dashboard = COUNT(DISTINCT patient_id) in gold.territory_summary, derived from silver.patient_master, originally from Veeva CRM.'*

*Why it's critical in three contexts:*

*First — **debugging and root cause analysis**. A number changed. Without lineage you're guessing. With lineage you follow the chain upstream and find the broken step in minutes.*

*Second — **compliance and audit**. HIPAA requires you to know exactly what happened to patient data — where it came from, how it was transformed, who accessed it, whether it was masked. GDPR has a 'right to erasure' requirement — when a patient requests deletion, lineage tells you every table, every partition, every downstream aggregate that contains their data.*

*Third — **impact analysis**. A source system team says 'we're changing the patient_id format from numeric to alphanumeric next month.' Without lineage you have no idea how many downstream tables, notebooks, and dashboards that breaks. With lineage you immediately see the impact — 12 tables, 5 notebooks, 3 dashboards — and can plan the migration.*

*In modern data platforms, lineage is tracked by tools like Apache Atlas, Azure Purview, or Databricks Unity Catalog. But even without dedicated tooling, you can implement lightweight lineage by tagging every record with its source system, source path, and transformation timestamp — which is what I did at AbbVie with the `_source_system`, `_ingest_ts`, and `_source_path` audit columns on every Bronze table.*"

---

✅ **Closing hook:**
> *"At AbbVie, we used Modak Nabu for data lineage tracking — it captured source-to-target mappings for our Iceberg pipelines. Beyond that tool, every table had audit columns: source_system, source_path, ingest_ts, batch_id. When an analyst questioned a number in a Power BI dashboard, I could trace it back to the exact Veeva API call, the exact Bronze file, and the exact MERGE INTO operation that updated that record."*

---

```
Lineage Example — AbbVie Patient Count:

Source: Veeva CRM API → veeva/patients_2024-03-14.json (S3)
           ↓ [ADF Copy Activity — 08:00:15]
Bronze:  iceberg.bronze.veeva_patients_raw
         [_source_system=VEEVA, _ingest_ts=2024-03-14T08:00:15, _batch_id=adf-run-12345]
           ↓ [PySpark notebook silver_transform.py — 08:05:30]
Silver:  iceberg.silver.patient_master
         [SCD2 MERGE — patient_sk=abc123, effective_from=2024-03-14T08:05:30]
           ↓ [PySpark notebook gold_aggregate.py — 08:15:00]
Gold:    iceberg.gold.patient_territory_summary
         [territory_id=T001, region=US, total_patients=1247]
           ↓ [Power BI DirectQuery]
Dashboard: Patient Count by Territory — "US T001: 1,247"

Lineage answers:
  "Why did the count change?"  → trace to silver MERGE — found duplicate key issue
  "Who can see this data?"     → ADLS RBAC + Purview access log
  "Was this patient masked?"   → trace Bronze → silver PII masking step → confirmed
```

---

### Q3. What is GDPR? Data Masking? Encryption?

---

📌 **Opening line:**
> *"GDPR is the EU regulation that gives individuals control over their personal data — and puts legal obligations on any organization processing it. In data engineering, GDPR compliance means implementing technical controls: data masking to de-identify sensitive fields, encryption to protect data at rest and in transit, access controls, and the ability to delete individual data on request."*

---

🗣️ **Script to say:**

*"Let me break each piece down practically.*

***GDPR** — General Data Protection Regulation. For a pharmaceutical company like AbbVie with EU patient data, the key obligations that affect data engineering are: you must have a lawful basis for processing personal data; you must be able to respond to a Subject Access Request — show a person all data you hold on them; and you must honour the Right to Erasure — delete all data about a person on request. That last one is technically hard when data is spread across Bronze, Silver, Gold Iceberg tables, and possibly archival storage.*

***Data Masking** is about removing or obfuscating personally identifiable information so the data remains useful for analytics but cannot identify individuals. There are two types. Static masking permanently replaces the value — you store age_group '35-44' instead of the actual date of birth. This is what I used at AbbVie for the analytical layers. Dynamic masking shows different values to different users based on their role — the actual DOB is stored but analysts see '35-44' while compliance officers see the real value. Dynamic masking is usually handled by the query engine layer — Databricks Unity Catalog supports column-level masking policies.*

***Encryption** has two contexts. Encryption at rest means the files on storage are encrypted — in ADLS Gen2 this is automatic with Microsoft-managed keys, or you can use customer-managed keys in Key Vault for tighter control. Encryption in transit means data moving over the network is encrypted with TLS — HTTPS for API calls, SSL for JDBC connections, the abfss protocol for ADLS uses TLS. Both are table stakes for any cloud data platform.*

*For the Right to Erasure specifically — in Iceberg and Delta, you handle this with a targeted DELETE statement that removes that individual's records, followed by compaction to rewrite the affected files. Because Delta/Iceberg have transaction logs, you also need to expire old snapshots and vacuum to physically remove the data from storage. Without vacuuming, the record still exists in old snapshot files.*"

---

✅ **Closing hook:**
> *"At AbbVie, HIPAA was the equivalent of GDPR for US healthcare data. We masked PII at the Bronze-to-Silver boundary — DOB became age_group, full name was never stored, only patient_id from the source. Encryption at rest was handled by ADLS Gen2 with customer-managed keys in Key Vault. For right-to-erasure, we had a documented process using Iceberg DELETE + compaction + snapshot expiry."*

---

```python
# ── Data Masking at Silver transform ──────────────────────────────────
from pyspark.sql.functions import (
    col, when, floor, datediff, current_date,
    sha2, lit, regexp_replace
)

def apply_pii_masking(df):
    """
    Apply PII masking at Bronze → Silver boundary.
    HIPAA / GDPR: remove or generalize all identifying fields.
    """
    return df \
        .withColumn("age_group",
            when(col("age") < 18,  lit("<18"))
            .when(col("age") < 25, lit("18-24"))
            .when(col("age") < 35, lit("25-34"))
            .when(col("age") < 45, lit("35-44"))
            .when(col("age") < 55, lit("45-54"))
            .when(col("age") < 65, lit("55-64"))
            .otherwise(lit("65+"))
        ) \
        .withColumn("patient_token",          # Pseudonymization — reversible with key
            sha2(col("patient_id"), 256)
        ) \
        .drop("date_of_birth") \              # Remove — store age_group instead
        .drop("full_name") \                  # Remove — never needed for analytics
        .drop("phone_number") \               # Remove
        .drop("email") \                      # Remove
        .withColumn("zip_code",               # Generalize — keep only first 3 digits
            col("zip_code").substr(1, 3)
        )


# ── Right to Erasure (GDPR Article 17) ────────────────────────────────
def erase_patient(patient_id: str, tables: list):
    """
    Physically remove all records for a patient across all Iceberg tables.
    Must be followed by compaction + snapshot expiry to remove from files.
    """
    for table in tables:
        # Step 1: Logical delete
        spark.sql(f"""
            DELETE FROM {table}
            WHERE patient_id = '{patient_id}'
               OR patient_token = '{sha2_python(patient_id)}'
        """)
        print(f"  Deleted from {table}")

        # Step 2: Compaction — rewrites affected files without the deleted rows
        spark.sql(f"""
            CALL iceberg.system.rewrite_data_files(
                table => '{table}',
                where => 'patient_id IS NULL'   -- rewrite files that had this patient
            )
        """)

        # Step 3: Expire snapshots — remove old versions that still have the data
        spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{datetime.now().isoformat()}',
                retain_last => 1
            )
        """)
        print(f"  Snapshots expired for {table}")

    print(f"✅ Patient {patient_id} erased from {len(tables)} tables")


# Usage
erase_patient("P001234", [
    "iceberg.bronze.veeva_patients_raw",
    "iceberg.silver.patient_master",
    "iceberg.gold.patient_territory_summary"
])
```

---

### Q4. Schema Validation vs Schema Evolution

---

📌 **Opening line:**
> *"Schema validation rejects data that doesn't match the expected structure — it's a protective gate. Schema evolution allows the structure to change over time — it's a controlled adaptation. The key is knowing where in your pipeline to apply each, and they serve opposite purposes."*

---

🗣️ **Script to say:**

*"Schema validation is essentially a contract enforcement. You define: this table has these columns, these types, these not-null constraints. When incoming data violates that contract — a column is missing, a type is wrong, an unexpected column appears — the write is rejected with an exception. This is the default behaviour in Delta Lake and something you explicitly configure in Iceberg. The purpose is protection — preventing bad or unexpected data from silently corrupting your downstream tables.*

*Schema evolution is the opposite intent — it allows the schema to change safely when source systems change. When Veeva API adds a new field to its response payload, instead of the pipeline failing, the new column is added to the Bronze table automatically. Old rows have null for that column. This is what `mergeSchema=true` does in Delta and what `ADD COLUMN` does in Iceberg.*

*The architectural decision is: which layer gets which treatment? I think of it as a funnel. Bronze is the widest — accept everything, use schema evolution. If the source adds a column, absorb it. If the source changes a type, attempt to cast or land as string and fix later. Bronze is a raw archive, correctness is secondary to completeness.*

*Silver is where the contract tightens. Strict schema validation. If something unexpected appears, I want the pipeline to fail loudly so a human reviews it — because a schema change in Silver could break downstream Gold tables and dashboards. The failure at Silver is intentional — it's an alert mechanism.*

*Gold is strictest of all. Schema changes here must be planned and coordinated with BI consumers — because Tableau and Power BI data sources have hardcoded column references. A column rename at Gold breaks every dashboard that references it.*

*There's also a middle path — schema-on-read. Instead of enforcing schema at write time, you enforce it at query time. This is what you get with raw Parquet or CSV on a data lake without a table format. The problem is the error surfaces much later — when an analyst runs a report — instead of immediately at ingestion. Table formats like Iceberg and Delta moved the industry back toward schema-on-write precisely because late error detection is expensive.*"

---

✅ **Closing hook:**
> *"At AbbVie, Veeva API response schema changed twice in 18 months — new fields added, one field renamed. Bronze had mergeSchema enabled, so those changes were absorbed automatically. Silver had strict schema validation — when Veeva renamed a field, Silver rejected the incoming data and the pipeline failed with a schema mismatch alert. That alert triggered a deliberate migration: update Silver schema, backfill old records with the new column mapping, then resume. Controlled evolution rather than silent drift."*

---

```python
# ── Bronze: Schema Evolution ON (absorb source changes) ───────────────
df_raw.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \    # ← new columns absorbed automatically
    .save("/mnt/bronze/veeva_patients/")


# ── Silver: Schema Validation ON (reject unexpected changes) ──────────
try:
    df_clean.write \
        .format("delta") \
        .mode("append") \
        .save("/mnt/silver/patient_master/")
        # ← no mergeSchema — will FAIL if schema doesn't match ✅
except Exception as e:
    if "schema mismatch" in str(e).lower() or "AnalysisException" in str(type(e)):
        # Alert the team — this is intentional behaviour, not a bug
        send_alert(f"Schema mismatch at Silver: {e}")
        raise  # Re-raise — don't swallow, let the pipeline fail visibly
    raise


# ── Iceberg: explicit schema management ────────────────────────────────
# Safe to add — backward compatible (old files read with NULL for new col)
spark.sql("""
    ALTER TABLE iceberg.silver.patient_master
    ADD COLUMN preferred_language STRING AFTER region
""")

# Renaming — requires migration (not safe to do silently)
spark.sql("""
    ALTER TABLE iceberg.silver.patient_master
    RENAME COLUMN old_territory_code TO territory_id
""")
# After rename: all downstream SELECT old_territory_code → fails ← intentional alert

# Type widening — safe in Iceberg (int → long, float → double)
spark.sql("""
    ALTER TABLE iceberg.silver.patient_events
    ALTER COLUMN duration_minutes TYPE BIGINT
""")
```

---

## 🎯 SECTION 2 — SCENARIO QUESTIONS

---

### Scenario 1. How to Apply DQ Checks in Bronze → Silver Layer?

**Full production pattern — route, quarantine, alert, and resume:**

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws,
    upper, trim, when, row_number, count
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Silver-DQ-Pipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# ════════════════════════════════════════════════════════════════════════
# STEP 1 — Read from Bronze (yesterday's incremental batch)
# ════════════════════════════════════════════════════════════════════════
load_date = dbutils.widgets.get("load_date")  # "2024-03-14"

df_bronze = spark.read.format("iceberg") \
    .load("iceberg.bronze.veeva_patients_raw") \
    .filter(col("_ingest_ts").cast("date") == load_date)

total_incoming = df_bronze.count()
print(f"📥 Incoming from Bronze: {total_incoming} rows")

# ════════════════════════════════════════════════════════════════════════
# STEP 2 — Apply DQ rules with ROUTING (good vs bad)
# ════════════════════════════════════════════════════════════════════════

# Load reference data for integrity checks (small → broadcast)
from pyspark.sql.functions import broadcast
territories_ref = spark.read.format("iceberg").load("iceberg.silver.territories") \
                       .select("territory_id")

# ── Rule 1: NULL primary key ──────────────────────────────────────────
null_pk = df_bronze.filter(col("patient_id").isNull()) \
    .withColumn("_dq_rule", lit("NULL_PATIENT_ID")) \
    .withColumn("_dq_severity", lit("CRITICAL"))

# ── Rule 2: NULL mandatory foreign key ────────────────────────────────
null_fk = df_bronze.filter(
    col("patient_id").isNotNull() &  # exclude already-caught nulls
    col("territory_id").isNull()
).withColumn("_dq_rule", lit("NULL_TERRITORY_ID")) \
 .withColumn("_dq_severity", lit("CRITICAL"))

# ── Rule 3: Referential integrity — territory must exist ──────────────
orphan_territory = df_bronze.filter(col("patient_id").isNotNull()) \
    .join(broadcast(territories_ref), on="territory_id", how="left_anti") \
    .withColumn("_dq_rule", lit("UNKNOWN_TERRITORY_ID")) \
    .withColumn("_dq_severity", lit("HIGH"))

# ── Rule 4: Domain check — status must be known value ─────────────────
valid_statuses = ["ACTIVE", "INACTIVE", "PENDING", "TRANSFERRED"]
invalid_status = df_bronze.filter(
    col("patient_id").isNotNull() &
    col("territory_id").isNotNull() &
    ~upper(trim(col("status"))).isin(valid_statuses)
).withColumn("_dq_rule", lit("INVALID_STATUS")) \
 .withColumn("_dq_severity", lit("MEDIUM"))

# ── Rule 5: Range check — age must be 0-120 ───────────────────────────
invalid_age = df_bronze.filter(
    col("patient_id").isNotNull() &
    col("age").isNotNull() &
    ((col("age") < 0) | (col("age") > 120))
).withColumn("_dq_rule", lit("AGE_OUT_OF_RANGE")) \
 .withColumn("_dq_severity", lit("MEDIUM"))

# ════════════════════════════════════════════════════════════════════════
# STEP 3 — Collect all failed records → quarantine table
# ════════════════════════════════════════════════════════════════════════
all_failed_ids = (
    null_pk.select("patient_id")
    .union(null_fk.select("patient_id"))
    .union(orphan_territory.select("patient_id"))
    .union(invalid_status.select("patient_id"))
    .union(invalid_age.select("patient_id"))
    .distinct()
)

failed_count = all_failed_ids.count()

# Quarantine ALL failed records (with DQ rule tags)
all_failed_records = null_pk \
    .unionByName(null_fk, allowMissingColumns=True) \
    .unionByName(orphan_territory, allowMissingColumns=True) \
    .unionByName(invalid_status, allowMissingColumns=True) \
    .unionByName(invalid_age, allowMissingColumns=True) \
    .withColumn("_dq_check_ts", current_timestamp()) \
    .withColumn("_source_table", lit("iceberg.bronze.veeva_patients_raw")) \
    .withColumn("_load_date", lit(load_date))

all_failed_records.writeTo("iceberg.dq.failed_records") \
    .option("merge-schema", "true") \
    .append()

print(f"⚠️  Quarantined: {failed_count} rows → iceberg.dq.failed_records")

# ════════════════════════════════════════════════════════════════════════
# STEP 4 — Pass only clean records to Silver transforms
# ════════════════════════════════════════════════════════════════════════
df_clean = df_bronze.join(all_failed_ids, on="patient_id", how="left_anti")

clean_count = df_clean.count()
pass_rate   = (clean_count / total_incoming * 100) if total_incoming > 0 else 0
print(f"✅ Clean records: {clean_count} ({pass_rate:.1f}% pass rate)")

# Alert if pass rate is suspiciously low (might indicate a systemic issue)
if pass_rate < 90.0:
    raise Exception(
        f"DQ pass rate {pass_rate:.1f}% is below 90% threshold for {load_date}. "
        f"Check iceberg.dq.failed_records. Pipeline halted."
    )

# ════════════════════════════════════════════════════════════════════════
# STEP 5 — Silver transforms on clean records only
# ════════════════════════════════════════════════════════════════════════
df_silver = df_clean \
    .withColumn("status", upper(trim(col("status")))) \
    .withColumn("region", upper(trim(col("region")))) \
    .withColumn("record_hash",
        sha2(concat_ws("||", col("patient_id"), col("territory_id"), col("status")), 256)
    ) \
    .withColumn("_silver_ts", current_timestamp()) \
    .drop("_ingest_ts", "_source_path", "_batch_id")

# Deduplicate — keep latest per patient_id within this batch
w = Window.partitionBy("patient_id").orderBy(col("source_ts").desc())
df_silver = df_silver.withColumn("_rn", row_number().over(w)) \
    .filter(col("_rn") == 1).drop("_rn")

# MERGE INTO Silver
df_silver.createOrReplaceTempView("silver_incoming")
spark.sql("""
    MERGE INTO iceberg.silver.patient_master AS tgt
    USING silver_incoming AS src
    ON tgt.patient_id = src.patient_id
    WHEN MATCHED AND tgt.record_hash != src.record_hash
        THEN UPDATE SET *
    WHEN NOT MATCHED
        THEN INSERT *
""")

# ════════════════════════════════════════════════════════════════════════
# STEP 6 — Log DQ metrics to monitoring table
# ════════════════════════════════════════════════════════════════════════
metrics = spark.createDataFrame([{
    "pipeline":         "veeva_bronze_to_silver",
    "load_date":        load_date,
    "total_incoming":   total_incoming,
    "total_quarantined":failed_count,
    "total_passed":     clean_count,
    "pass_rate_pct":    round(pass_rate, 2),
    "run_ts":           datetime.now().isoformat()
}])
metrics.writeTo("iceberg.monitoring.dq_metrics").append()

print(f"📊 DQ Metrics logged | Pass rate: {pass_rate:.1f}%")
```

---

### Scenario 2. If CDC Ingestion Duplicates Records, How Do You Fix It?

**Context:** Change Data Capture (CDC) from Oracle using log-based replication. Duplicate records are appearing in Iceberg Silver.

---

**Understand why CDC duplicates happen:**

```
Root causes of CDC duplicates:
─────────────────────────────────────────────────────────────────
1. Pipeline retried after partial failure
   → Batch A committed to Bronze but then crashed before updating watermark
   → Next run reprocesses same batch → doubles the rows in Silver

2. CDC source emits duplicate events
   → Oracle LogMiner occasionally re-emits events on reader restart
   → Two INSERT events for the same row in the CDC log

3. Micro-batch overlap
   → Window [T1, T2] and [T2, T3] both include T2 (boundary row)
   → Row with updated_at exactly at boundary appears twice

4. Multiple CDC streams for same source table
   → Two ADF pipelines accidentally configured to same source table
   → Both write to Bronze → Silver double-counts

5. Late-arriving data with same key
   → Record arrives in two different daily batches
   → Both batches write to Silver without dedup
```

---

**Fix 1: Idempotent MERGE INTO (prevents duplicates from mattering)**

```python
# The MERGE INTO pattern is inherently idempotent:
# Running the same source data twice produces the same Silver result
# → New records inserted once (WHEN NOT MATCHED)
# → Same records update to same values (hash match → no-op)
# → No net duplicate if source has duplicates within the batch

df_cdc.createOrReplaceTempView("cdc_incoming")

spark.sql("""
    MERGE INTO iceberg.silver.patient_master AS tgt
    USING (
        -- Deduplicate within incoming batch BEFORE merging
        -- Keep latest CDC operation per patient_id
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY patient_id
                       ORDER BY cdc_ts DESC,
                                CASE cdc_operation
                                    WHEN 'DELETE' THEN 1
                                    WHEN 'UPDATE' THEN 2
                                    WHEN 'INSERT' THEN 3
                                END ASC
                   ) AS rn
            FROM cdc_incoming
        )
        WHERE rn = 1
    ) AS src
    ON tgt.patient_id = src.patient_id
    WHEN MATCHED AND src.cdc_operation = 'DELETE'
        THEN UPDATE SET is_deleted = true, updated_at = src.cdc_ts
    WHEN MATCHED AND tgt.record_hash != src.record_hash
        THEN UPDATE SET *
    WHEN NOT MATCHED AND src.cdc_operation != 'DELETE'
        THEN INSERT *
""")
```

---

**Fix 2: Detect and remove existing duplicates in Silver**

```python
from pyspark.sql.functions import row_number, col, count
from pyspark.sql.window import Window

# ── Step 1: Detect if duplicates exist ────────────────────────────────
df_silver = spark.read.format("iceberg").load("iceberg.silver.patient_master")

dup_check = df_silver.groupBy("patient_id") \
    .agg(count("*").alias("cnt")) \
    .filter(col("cnt") > 1)

dup_count = dup_check.count()
print(f"Duplicate patient_ids found: {dup_count}")

if dup_count == 0:
    print("✅ No duplicates — nothing to fix")
else:
    # ── Step 2: Identify which rows to keep ───────────────────────────
    # Strategy: keep the row with the latest updated_at per patient_id
    w = Window.partitionBy("patient_id").orderBy(
        col("updated_at").desc(),
        col("_ingest_ts").desc()     # tiebreaker
    )
    df_deduped = df_silver \
        .withColumn("_rn", row_number().over(w)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")

    print(f"Before dedup: {df_silver.count()} rows")
    print(f"After dedup:  {df_deduped.count()} rows")
    print(f"Removed:      {df_silver.count() - df_deduped.count()} duplicates")

    # ── Step 3: Overwrite the table with deduped data ─────────────────
    # Use overwritePartitions to atomically replace affected partitions
    df_deduped.writeTo("iceberg.silver.patient_master") \
        .overwritePartitions()

    # ── Step 4: Compaction — merge small files created by overwrite ────
    spark.sql("""
        CALL iceberg.system.rewrite_data_files(
            table => 'iceberg.silver.patient_master',
            strategy => 'sort',
            sort_order => 'patient_id ASC, updated_at DESC'
        )
    """)
    print("✅ Deduplication complete and table compacted")
```

---

**Fix 3: Prevent CDC duplicates at Bronze ingestion level**

```python
# Bronze should be append-only — but we can track which batches were written
# Using a CDC event_id or LSN (Log Sequence Number) to detect re-processing

from pyspark.sql.functions import col, max as spark_max

# Read high-water mark from Bronze control table
last_lsn = spark.read.format("iceberg") \
    .load("iceberg.control.cdc_watermarks") \
    .filter(col("table_name") == "patient_master") \
    .agg(spark_max("last_lsn")).collect()[0][0] or 0

print(f"Last processed LSN: {last_lsn}")

# Only process CDC events with LSN > last processed
df_new_cdc = df_cdc_source.filter(col("_lsn") > last_lsn)

# Write to Bronze
df_new_cdc.writeTo("iceberg.bronze.patient_master_cdc").append()

# Update watermark (ONLY after successful Bronze write)
new_lsn = df_new_cdc.agg(spark_max("_lsn")).collect()[0][0]
spark.sql(f"""
    MERGE INTO iceberg.control.cdc_watermarks AS tgt
    USING (SELECT 'patient_master' AS table_name, {new_lsn} AS last_lsn) AS src
    ON tgt.table_name = src.table_name
    WHEN MATCHED THEN UPDATE SET last_lsn = src.last_lsn
    WHEN NOT MATCHED THEN INSERT *
""")
# If Bronze write fails → watermark NOT updated → next run reprocesses
# But MERGE INTO is idempotent → reprocessing is safe ✅
```

---

**Full root cause → fix mapping:**

```
Symptom: Duplicates in Silver after CDC ingestion

Step 1: Identify where duplicates entered
  SELECT patient_id, COUNT(*) AS cnt
  FROM iceberg.silver.patient_master
  GROUP BY patient_id HAVING cnt > 1
  LIMIT 100;

Step 2: Trace back to Bronze
  SELECT * FROM iceberg.bronze.patient_master_cdc
  WHERE patient_id IN ('P001', 'P002')
  ORDER BY _ingest_ts;
  -- If duplicates are in Bronze too → CDC source / pipeline retry issue
  -- If only in Silver → MERGE logic issue

Step 3: Root cause:
  CDC source re-emitting?    → Add LSN-based watermark at Bronze
  Pipeline retry?            → MERGE is idempotent → safe to retry
  Boundary overlap?          → Use BETWEEN exclusive on upper bound
  Multiple pipelines?        → Audit ADF pipelines for same source
  Missing dedup in MERGE?    → Add ROW_NUMBER() dedup in USING clause

Step 4: Fix Silver now:
  → ROW_NUMBER() dedup + overwritePartitions (shown above)

Step 5: Prevent recurrence:
  → LSN-based watermark at Bronze ingestion
  → ROW_NUMBER() dedup in MERGE USING clause
  → DQ uniqueness check before every Silver write
  → Alert if pass_rate drops (sudden duplicates = data volume spike = flag)
```

---

---

# PART B — APACHE ICEBERG / DELTA / LAKEHOUSE

---

## 📚 SECTION 1 — THEORY QUESTIONS

---

### Q1. Delta vs Iceberg vs Hudi

---

📌 **Opening line:**
> *"Delta, Iceberg, and Hudi are all open table formats that add ACID transactions, schema enforcement, and time travel to data lake storage. The difference is in their metadata architecture, ecosystem support, and which trade-offs they optimise for — and Iceberg is the most vendor-neutral and metadata-efficient of the three at scale."*

---

🗣️ **Script to say:**

*"Let me frame this correctly first. All three solve the same core problem — raw Parquet files on S3 or ADLS have no transactional guarantees, no schema enforcement, no versioning. These three formats add a metadata layer on top of Parquet to give you those guarantees. The question is how they do it and what they do better.*

***Delta Lake** was built by Databricks and is tightly integrated with the Spark and Databricks ecosystem. It uses a transaction log — the `_delta_log` folder — which is a sequence of JSON files and periodic Parquet checkpoints that record every commit. Delta's strength is its deep Databricks integration, mature streaming support with Structured Streaming, and features like OPTIMIZE and VACUUM are well-polished. The limitation is that the transaction log can become a bottleneck at very large scale — when you have a table with millions of files, scanning the transaction log to reconstruct the current state is expensive. Also, Delta is most performant within the Databricks ecosystem — outside of it, support was limited until the open Delta format announcement.*

***Apache Iceberg** was originally built at Netflix and donated to the Apache Foundation. It's the most vendor-neutral — supported by Spark, Flink, Trino, Hive, Dremio, StarRocks, and Snowflake. Iceberg's metadata architecture is fundamentally different — it uses a tree of metadata files: a current metadata pointer, snapshot files, manifest lists, and manifest files that each track a subset of data files. This hierarchical structure means large-scale metadata operations — like listing files in a partition — are O(1) rather than O(n). Iceberg also has hidden partitioning — the partition specification is separate from the schema, so you can change partition strategy without rewriting data. This is where I spent most of my time at AbbVie.*

***Apache Hudi** (Hadoop Upserts Deletes and Incrementals) was built at Uber and is optimised for CDC and streaming upsert workloads. It has two storage types — Copy-On-Write for read-heavy workloads and Merge-On-Read for write-heavy. Hudi's timeline service is very efficient for high-frequency upsert patterns. Its weakness compared to Iceberg is community size and ecosystem breadth.*

*In terms of choosing: if you're entirely on Databricks and want the tightest integration, Delta is the default. If you need multi-engine access — Spark AND Trino AND Flink AND Snowflake all reading the same table — Iceberg is the clear choice. If you have extremely high-frequency CDC with millions of upserts per hour, Hudi's Merge-On-Read might win. At AbbVie we chose Iceberg specifically for multi-engine flexibility — we needed Spark for ingestion and Trino for ad-hoc analytics.*"

---

✅ **Closing hook:**
> *"At AbbVie, the decision for Iceberg over Delta came down to three things: multi-engine support for Trino queries, the hidden partitioning feature that let us change partition strategy on 20 TB tables without data rewrites, and the hierarchical metadata structure that handled our large file counts efficiently. These were specific requirements that Iceberg addressed better than Delta at the time."*

---

**Comparison table:**

| Feature | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| **Origin** | Databricks | Netflix / Apache | Uber |
| **Metadata** | Transaction log (JSON + Parquet) | Tree: metadata → snapshots → manifests | Timeline + index |
| **Multi-engine** | Limited (improving) | ✅ Best (Spark, Trino, Flink, Snowflake) | Good |
| **Hidden partitioning** | ❌ No | ✅ Yes | ❌ No |
| **Partition evolution** | Requires rewrite | ✅ No rewrite needed | Limited |
| **CDC / Upserts** | Good (MERGE INTO) | Good (MERGE INTO) | ✅ Best (built for it) |
| **Streaming** | ✅ Excellent (Databricks) | Good | ✅ Excellent |
| **Scale metadata** | Degrades at very large scale | ✅ O(1) partition listing | Good |
| **Ecosystem** | Databricks-centric | ✅ Most neutral | Hadoop-centric |
| **Best for** | Databricks platform | Multi-engine lake | High-freq CDC |

---

### Q2. How Does Iceberg Handle Metadata?

---

📌 **Opening line:**
> *"Iceberg uses a four-level hierarchical metadata tree — a current metadata pointer, snapshot files, manifest lists, and manifest files — where each level tracks a subset of the information, so operations like listing files in a specific partition are O(1) rather than scanning everything."*

---

🗣️ **Script to say:**

*"This hierarchy is the core technical advantage of Iceberg over alternatives — understanding it tells you why Iceberg performs better at scale.*

*At the top is the **current metadata file** — a single JSON file that is the entry point to the entire table. It contains the table schema, partition spec, sort order, and a pointer to the current snapshot. This file is updated atomically on every commit using a compare-and-swap operation on object storage — that's how Iceberg achieves optimistic concurrency control.*

*The **snapshot** represents a complete point-in-time state of the table. Every commit creates a new snapshot. Each snapshot has a pointer to a manifest list.*

*The **manifest list** is an Avro file that lists all the manifest files for that snapshot, along with partition-level statistics for each manifest — min/max values for each partition field. This is critical: when Spark reads a query with a WHERE clause on a partition column, it reads the manifest list, checks the partition stats for each manifest, and skips any manifest that cannot possibly contain matching rows. This is how partition pruning works in Iceberg.*

*The **manifest files** are Avro files that each track a subset of data files (Parquet, ORC, Avro). Each manifest entry stores the data file path plus column-level statistics — min, max, null count for each column. When Spark evaluates a filter predicate, it reads the manifest file stats and skips data files whose min-max range doesn't overlap with the filter value. This is data file skipping.*

*The result is: a query that filters on a partition column reads the manifest list (maybe a few KB), skips most manifests, reads only relevant manifests, skips most data files via column stats, and only opens the files that might match. On a 20 TB table, this can mean scanning 50 MB instead of 20 TB.*

*A key operational detail: Iceberg does not require a central metadata server. All this metadata lives as files on the same object storage as the data. There's no external catalog service required for reads — though you do need a catalog (like AWS Glue, Hive Metastore, or Nessie) to register the current metadata pointer.*"

---

✅ **Closing hook:**
> *"At AbbVie with 20 TB Iceberg tables, this metadata hierarchy was critical. When an analyst queried 'US region, last 30 days', Iceberg read the manifest list, found only 30 manifests with US region data, read those manifests, and used patient_id min/max stats to skip files. A query that would have scanned 20 TB was reading 2-3 GB in actual data. The metadata file reads are negligible — kilobytes."*

---

```
Iceberg Metadata Tree:

table/
├── metadata/
│   ├── v1.metadata.json          ← current metadata pointer
│   │   └── schema, partition spec, → snapshot ref
│   ├── snap-001.avro             ← Snapshot (what files exist at this version)
│   │   └── → manifest-list-001.avro
│   ├── manifest-list-001.avro   ← Manifest List (with partition stats per manifest)
│   │   ├── manifest-001.avro    [region=US,  date: 2024-01-01..2024-01-31]
│   │   ├── manifest-002.avro    [region=EU,  date: 2024-01-01..2024-01-31]
│   │   └── manifest-003.avro    [region=US,  date: 2024-02-01..2024-02-28]
│   └── manifest-001.avro        ← Manifest File (tracks data files + column stats)
│       ├── data/region=US/part-001.parquet  [patient_id: P001..P499]
│       ├── data/region=US/part-002.parquet  [patient_id: P500..P999]
│       └── data/region=US/part-003.parquet  [patient_id: P1000..P1499]
└── data/
    ├── region=US/part-001.parquet
    └── region=EU/part-001.parquet

Query: WHERE region='US' AND event_date>='2024-01-01' AND patient_id='P750'

Step 1: Read manifest list  → skip manifest-002 (EU only), skip manifest-003 (Feb only)
Step 2: Read manifest-001   → skip part-001 (P001-P499), skip part-003 (P1000+)
Step 3: Open only part-002  → scan for P750
Result: 1 file opened out of 6 total ✅
```

---

### Q3. Table Formats vs File Formats

---

📌 **Opening line:**
> *"A file format defines how data is physically encoded in a single file — the binary layout, compression, column organisation. A table format defines how a collection of files is managed as a logical table — it adds transactions, versioning, schema enforcement, and metadata. They are different layers and you need both."*

---

🗣️ **Script to say:**

*"The confusion between these two comes up a lot, so let me be precise about what each level does.*

*A **file format** — Parquet, ORC, Avro, CSV — defines the bytes inside one file. Parquet is columnar — it groups column values together, applies column-level compression, stores min/max statistics per row group. ORC is similar, also columnar, historically more optimised for Hive. Avro is row-oriented — better for write-heavy streaming. CSV is human-readable, no structure, no compression benefits. The choice of file format affects: how fast you can read a subset of columns, how well data compresses, whether you have built-in statistics.*

*A **table format** — Iceberg, Delta Lake, Hudi — operates at a higher level. It manages a collection of files as a logical table. It adds: a transaction log so writes are atomic, a schema registry so the table always knows its column types, metadata that tracks which files belong to the current version of the table, partition information so queries can skip irrelevant files, and versioning so you can query historical snapshots.*

*The key insight is that table formats sit on top of file formats. An Iceberg table on Parquet files gives you: Parquet's columnar compression and row-group statistics (file format benefits) PLUS Iceberg's ACID transactions, schema evolution, partition pruning, and time travel (table format benefits). They are complementary.*

*You can also change file formats independently of the table format — an Iceberg table can use Parquet for most data and ORC for specific partitions. Similarly, you can use Parquet without a table format — just raw Parquet files — but then you lose all the table format benefits: no transactions, no schema enforcement, no pruning.*

*In practice today for large-scale data engineering: Parquet is the dominant file format (columnar, compressed, Spark-native), and Iceberg or Delta is the table format on top.*"

---

✅ **Closing hook:**
> *"At AbbVie, every Iceberg table used Parquet as the file format with ZSTD compression. Parquet gave us columnar reads — queries that only needed patient_id and region read a fraction of the file bytes. ZSTD gave us 40-50% better compression than Snappy on our structured healthcare data. Iceberg sat on top and gave us the transactional guarantees, partition pruning, and schema management."*

---

```
Layers in an Iceberg table on ADLS:

TABLE FORMAT LAYER (Iceberg):
  - Transactions: atomic commits via metadata swap
  - Schema:       enforced, versioned, evolvable
  - Partitioning: hidden partitions, evolvable spec
  - Metadata:     manifest list → manifests → data file stats
  - Time travel:  query any snapshot
  - Concurrency:  optimistic locking on metadata file
           ↕
FILE FORMAT LAYER (Parquet):
  - Physical layout: columnar (column values grouped together)
  - Compression:     ZSTD per column (3-5x compression vs raw)
  - Row groups:      128 MB chunks, each with min/max stats
  - Column chunks:   dictionary encoding for low-cardinality cols
  - Predicate push-down: row group stats used for skipping

Common combinations:
  Iceberg + Parquet + ZSTD    ← Most common (our AbbVie stack)
  Delta   + Parquet + Snappy  ← Databricks default
  Hudi    + Parquet + ZSTD    ← Uber / CDC workloads
  Raw     + Parquet           ← No table format (data lake anti-pattern)
  Hive    + ORC + ZLIB        ← Legacy Hadoop setups
```

---

### Q4. Explain Iceberg Partitioning & Hidden Partitions

---

📌 **Opening line:**
> *"Iceberg separates the partition specification from the physical file layout. You declare a partition transform on a column — like days(event_ts) or bucket(100, patient_id) — and Iceberg automatically computes the partition value during writes and applies partition pruning during reads, without adding extra columns to your schema or changing your queries."*

---

🗣️ **Script to say:**

*"Let me contrast this with how Delta and Hive partitioning works to show why hidden partitioning is an improvement.*

*In Delta or Hive, if you want to partition by date, you have to: add a load_date column to your schema, populate it on every write, remember to include it in your write statement, and filter on it in every query to get partition pruning. The partition column is part of the visible schema — it shows up in SELECT star, it takes up storage, and if you named it inconsistently across tables it causes confusion.*

*In Iceberg, you define a **partition transform** on a column. For example, `days(event_ts)` tells Iceberg: compute the day from the event_ts column, use that as the partition. The column `event_ts` already exists in your schema. Iceberg computes the partition value automatically during write. Your schema doesn't gain an extra column. Your queries filter on `event_ts` naturally — WHERE event_ts >= '2024-01-01' — and Iceberg automatically applies partition pruning without you writing WHERE load_date = '2024-01-01'.*

*There are several built-in partition transforms. `years(ts)`, `months(ts)`, `days(ts)`, `hours(ts)` extract time components. `bucket(N, col)` hashes the column into N fixed buckets — useful for high-cardinality columns like patient_id without creating millions of partitions. `truncate(col, L)` takes the first L characters — useful for string prefixes. And you can combine multiple transforms — partition by `days(event_ts)` and `bucket(10, region)` together.*

*The other major advantage is **partition evolution**. In Delta/Hive, changing how a table is partitioned means rewriting every file — weeks of compute for a 20 TB table. In Iceberg, partition evolution is a metadata-only operation. You update the partition spec, and new writes use the new spec while old files keep the old spec. Iceberg readers handle both transparently. No data rewrite.*"

---

✅ **Closing hook:**
> *"At AbbVie, hidden partitioning was one of the key reasons we chose Iceberg. Our Bronze tables used `days(ingest_ts)` — no extra date column, no ETL to populate it. Our Silver patient_events table used `days(event_date), bucket(4, region)` — clean schema, automatic partition pruning. And when we needed to change the partition granularity from daily to monthly on a mature table, it was an ALTER TABLE metadata operation — not a 20 TB rewrite."*

---

```python
# ── Iceberg Partition Transforms ───────────────────────────────────────

spark.sql("""
    CREATE TABLE iceberg.silver.patient_events (
        patient_id   STRING,
        event_ts     TIMESTAMP,      -- source of partition — NO extra column needed
        territory_id STRING,
        region       STRING,
        event_type   STRING,
        revenue      DECIMAL(18,2)
    ) USING iceberg
    PARTITIONED BY (
        days(event_ts),              -- extracts date from timestamp automatically
        bucket(4, region)            -- 4 fixed buckets: hash(region) % 4
    )
    TBLPROPERTIES (
        'write.target-file-size-bytes' = '536870912',
        'write.sort-order' = 'patient_id ASC, event_ts ASC'
    )
""")

# ── Write — no need to specify partition values ────────────────────────
df.writeTo("iceberg.silver.patient_events").append()
# Iceberg computes: partition = (days(event_ts), bucket(4, region)) per row automatically ✅

# ── Read — partition pruning works on the SOURCE column ───────────────
spark.sql("""
    SELECT * FROM iceberg.silver.patient_events
    WHERE event_ts >= '2024-01-01'         -- Iceberg maps this to partition days(event_ts)
      AND region = 'US'                    -- Iceberg maps this to bucket(4, 'US') = 2
""")
# Only reads: partition=2024-01-01 through today, bucket=2 ✅
# No need to write: WHERE load_date = '2024-01-01' AND region_bucket = 2

# ── Partition Evolution — change spec without rewriting data ───────────
spark.sql("""
    ALTER TABLE iceberg.silver.patient_events
    ADD PARTITION FIELD months(event_ts)    -- switch new writes to monthly granularity
""")
# Old files: partitioned by days(event_ts)
# New files: partitioned by months(event_ts)
# Readers handle both transparently ✅ — no 20 TB rewrite needed

# ── Other useful transforms ────────────────────────────────────────────
"""
years(ts)           → partition by year
months(ts)          → partition by year-month
days(ts)            → partition by date           ← most common
hours(ts)           → partition by hour           ← streaming / high-frequency
bucket(N, col)      → N fixed hash buckets        ← for high-cardinality cols
truncate(col, L)    → first L chars/digits        ← string prefix partitioning
"""
```

---

## 🎯 SECTION 2 — SCENARIO QUESTIONS

---

### Scenario 1. How Did You Migrate 20 TB to Iceberg? What Were the Challenges?

**Context:** Migrating AbbVie's existing Hive/Parquet data lake (20+ TB) to Apache Iceberg tables on S3/ADLS.

---

**Migration Architecture:**

```
BEFORE:                                AFTER:
────────────────────────────────       ────────────────────────────────────────
Raw Parquet files on ADLS              Apache Iceberg tables on ADLS
Hive Metastore (external tables)       Iceberg catalog (Hive Metastore + Nessie)
No transactions                        ACID transactions ✅
Schema-on-read                         Schema-on-write + evolution ✅
No partition evolution                 Hidden partitioning + evolution ✅
No time travel                         Snapshot-based time travel ✅
Manual compaction (MSCK REPAIR)        System procedures (rewrite_data_files) ✅
```

---

**Migration Approach — Phased, Zero Downtime:**

```
Phase 1: Parallel Write (Week 1-2)
  New ingestion writes to BOTH Hive AND Iceberg
  Validate Iceberg output matches Hive output
  No consumers moved yet — zero risk

Phase 2: Consumer Migration (Week 3-4)
  Move Spark jobs to read from Iceberg
  Validate query results match
  Keep Hive tables as fallback

Phase 3: Cutover (Week 5)
  Stop dual-writing (Iceberg only)
  Decommission Hive tables
  Reclaim storage

Phase 4: Optimisation (Week 6+)
  Apply Iceberg-specific features:
  - Hidden partitioning
  - Sort orders
  - File compaction
  - Snapshot management
```

---

**Migration PySpark Code:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, count, sum as spark_sum, hash as spark_hash
)

spark = SparkSession.builder \
    .appName("Hive-to-Iceberg-Migration") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .getOrCreate()

# ── Step 1: Create Iceberg table with optimised schema ─────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.silver.patient_events (
        patient_id   STRING,
        territory_id STRING,
        region       STRING,
        event_type   STRING,
        event_ts     TIMESTAMP,
        duration_min INT,
        revenue      DECIMAL(18,2),
        source_sys   STRING
    ) USING iceberg
    PARTITIONED BY (days(event_ts), region)
    TBLPROPERTIES (
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'write.target-file-size-bytes'    = '536870912',
        'write.sort-order'                = 'patient_id ASC, event_ts ASC',
        'history.expire.max-snapshot-age-ms' = '604800000



Claude is AI and can make mistakes. Please double-check responses.



