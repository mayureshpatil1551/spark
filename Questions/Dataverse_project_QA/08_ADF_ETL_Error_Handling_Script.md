# ADF Complex ETL — Interview Script
### Error Handling | Dependencies | Recovery | Real Scenario

---

## QUESTION 1
### "How do you design and implement pipelines in ADF to handle complex ETL workflows with dependencies?"

---

**YOUR ANSWER — SAY THIS:**

"In ECDP, we had a complex ETL workflow with 25 plus tables flowing from Oracle into Delta Lake through three layers — Bronze, Silver, and Gold. Each layer had strict dependencies — Silver could not start until Bronze completed successfully, and Gold could not start until all Silver tables were ready.

Here is how I designed it:"

---

### THE PIPELINE ARCHITECTURE

```
MASTER PIPELINE
│
├── Activity 1: Lookup (read control table)
│
├── Activity 2: ForEach (loop each table config)
│   │
│   └── CHILD PIPELINE (called per table)
│       ├── Copy Activity      → Bronze write
│       │     ↓ (On Success)
│       ├── Notebook Activity  → Silver transform
│       │     ↓ (On Success)
│       ├── Notebook Activity  → Gold aggregation
│       │     ↓ (On Success)
│       └── SP Activity        → Update control table SUCCESS
│             ↓ (On Failure — anywhere above)
│           Web Activity       → Send alert + log error
```

---

### CONTROL TABLE (Azure SQL)

```sql
CREATE TABLE pipeline_control (
    config_id       INT IDENTITY PRIMARY KEY,
    source_table    VARCHAR(100),
    target_path     VARCHAR(200),
    watermark_col   VARCHAR(50),
    last_watermark  DATETIME,
    load_type       VARCHAR(20),   -- FULL / INCREMENTAL
    last_run_status VARCHAR(20),   -- SUCCESS / FAILED / RUNNING
    last_run_time   DATETIME,
    is_active       BIT DEFAULT 1
);
```

---

### MASTER PIPELINE — 3 ACTIVITIES

**Activity 1 — Lookup reads control table:**
```sql
SELECT * FROM pipeline_control
WHERE is_active = 1
AND last_run_status != 'RUNNING'
```

**Activity 2 — ForEach loops each row:**
```json
{
  "type": "ForEach",
  "typeProperties": {
    "isSequential": false,
    "batchCount": 5,
    "items": "@activity('LookupConfig').output.value",
    "activities": [
      {
        "type": "ExecutePipeline",
        "pipeline": { "referenceName": "child_pipeline" },
        "parameters": {
          "source_table":   "@item().source_table",
          "target_path":    "@item().target_path",
          "watermark":      "@item().last_watermark",
          "load_type":      "@item().load_type"
        }
      }
    ]
  }
}
```

> `isSequential: false` + `batchCount: 5` = 5 tables run in parallel

---

### CHILD PIPELINE — ACTIVITY DEPENDENCIES

```
[Copy Activity — Bronze]
        |
   On Success ──────────────────────────────┐
        |                                   |
[Notebook — Silver Transform]          On Failure
        |                                   |
   On Success                    [SP Activity — Log Error]
        |                                   |
[Notebook — Gold Aggregation]      [Web Activity — Alert]
        |
   On Success
        |
[SP Activity — Update Watermark]
```

**ADF dependency condition codes:**
- `Succeeded` — run only if previous activity passed
- `Failed` — run only if previous activity failed
- `Skipped` — run if previous was skipped
- `Completed` — run regardless (success or fail)

---

## QUESTION 2
### "Break down the steps for error handling and recovery"

---

**YOUR ANSWER — SAY THIS:**

"I implemented error handling in four layers:"

---

### LAYER 1 — Activity Level Retry (Transient Errors)

```json
{
  "policy": {
    "retry": 3,
    "retryIntervalInSeconds": 30,
    "secureOutput": false
  }
}
```

*"This handles temporary issues — network blip, source system briefly unavailable. Three retries, 30 seconds apart."*

---

### LAYER 2 — Failure Branch (On Failure Path)

Every critical activity has a failure branch connected to two things:

**Step 1 — Log error to audit table:**
```sql
-- Stored Procedure called On Failure
INSERT INTO pipeline_audit (
    pipeline_name,
    run_id,
    source_table,
    activity_name,
    status,
    error_message,
    failed_at
)
VALUES (
    '@{pipeline().Pipeline}',
    '@{pipeline().RunId}',
    '@{pipeline().parameters.source_table}',
    '@{activity('CopyBronze').Error.message}',
    'FAILED',
    '@{activity('CopyBronze').Error.message}',
    '@{utcnow()}'
)
```

**Step 2 — Send Alert via Web Activity:**
```json
{
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://logic-app-url/triggers/manual/invoke",
    "method": "POST",
    "body": {
      "pipeline":  "@{pipeline().Pipeline}",
      "table":     "@{pipeline().parameters.source_table}",
      "error":     "@{activity('CopyBronze').Error.message}",
      "run_id":    "@{pipeline().RunId}",
      "timestamp": "@{utcnow()}"
    }
  }
}
```

---

### LAYER 3 — Watermark Protection (Idempotency)

```sql
-- Watermark updates ONLY after full success
-- Never at the start of the run

UPDATE pipeline_control
SET last_watermark  = '@{activity('CopyBronze').output.dataRead}',
    last_run_status = 'SUCCESS',
    last_run_time   = GETDATE()
WHERE source_table = '@{pipeline().parameters.source_table}'
```

*"If the pipeline fails at Silver, the watermark stays at the last successful point. On retry, Bronze re-reads the same data — no data is skipped, no duplicates because we use MERGE not INSERT."*

---

### LAYER 4 — Pipeline State Table (Skip Already Done)

```sql
CREATE TABLE pipeline_state (
    batch_id     VARCHAR(50),
    source_table VARCHAR(100),
    stage        VARCHAR(20),   -- BRONZE / SILVER / GOLD
    status       VARCHAR(20),   -- SUCCESS / FAILED / RUNNING
    rows_written BIGINT,
    run_time     DATETIME
);
```

**Databricks notebook checks this at start:**
```python
def is_already_done(batch_id, table_name, stage):
    result = spark.sql(f"""
        SELECT status FROM pipeline_state
        WHERE batch_id    = '{batch_id}'
        AND   source_table = '{table_name}'
        AND   stage        = '{stage}'
        AND   status       = 'SUCCESS'
    """)
    return result.count() > 0

# If Silver already succeeded — skip it on retry
if is_already_done(batch_id, table_name, "SILVER"):
    print(f"SILVER already completed for {table_name} — skipping")
    dbutils.notebook.exit("SKIPPED")
```

*"This means if Gold fails, on retry only Gold reruns. Bronze and Silver are skipped because they already have SUCCESS status. Saves hours of unnecessary reprocessing."*

---

## QUESTION 3
### "Share a real scenario where this helped maintain reliability"

---

**YOUR ANSWER — SAY THIS STORY:**

*"Yes — this happened in production in ECDP.*

*We had an end-to-end pipeline with five stages: Oracle extract, Bronze write, Silver transform, Gold aggregation, and Snowflake export. The pipeline ran every night at 2 AM.*

*One Monday morning, the business team flagged that the Gold reports were missing data for the weekend. I checked ADF Monitor — the pipeline showed as Succeeded. But the audit table showed zero rows written to Gold for Saturday and Sunday.*

---

**ROOT CAUSE INVESTIGATION:**

```
Step 1 — Checked ADF Monitor
         Pipeline status = Succeeded ← misleading

Step 2 — Checked audit table
         Bronze  rows = 1,200,000  ✓
         Silver  rows = 1,200,000  ✓
         Gold    rows = 0          ✗ ← problem here

Step 3 — Checked Databricks job run logs
         Gold notebook ran but wrote 0 rows

Step 4 — Checked Gold notebook code
         Found filter: WHERE fiscal_year = 2024
         Saturday date = 2024-12-28 → correct
         But partition column had NULL fiscal_year
         due to a date parsing bug introduced Friday
```

**THE BUG:**
```python
# Incorrect — returned NULL for Dec dates
df = df.withColumn("fiscal_year",
    col("posting_date").substr(1, 4).cast(IntegerType())
)

# The substr was returning None for a specific SAP date format
# that appeared only in weekend data
```

**THE FIX:**
```python
# Correct — handles all date formats
from pyspark.sql.functions import year, to_date, coalesce

df = df.withColumn("fiscal_year",
    year(
        coalesce(
            to_date(col("posting_date"), "yyyyMMdd"),
            to_date(col("posting_date"), "yyyy-MM-dd")
        )
    )
)
```

---

**RECOVERY — Using Pipeline State Table:**

```python
# Since Bronze and Silver had SUCCESS status
# Only Gold needed to rerun

# Step 1 — Reset Gold status for affected batch IDs
UPDATE pipeline_state
SET status = 'FAILED'
WHERE stage = 'GOLD'
AND run_time BETWEEN '2024-12-28' AND '2024-12-29'

# Step 2 — Trigger manual rerun in ADF
# Bronze and Silver auto-skipped (status = SUCCESS)
# Gold reran with fixed code
# Total recovery time: 25 minutes
```

**WITHOUT the pipeline state table:**
Full rerun from Bronze would have taken 4 hours.
With the state table: Gold-only rerun took 25 minutes.

---

**WHAT I ADDED AFTER THIS:**

```python
# Gold now validates row count before writing
gold_count = df_gold.count()

if gold_count == 0:
    raise ValueError(
        f"Gold produced 0 rows for batch {batch_id} "
        f"— possible upstream data issue. Pipeline halted."
    )

# This means ADF now correctly shows FAILED
# instead of Succeeded with 0 rows
```

*"After this fix, a zero-row Gold output raises an exception — ADF marks it as Failed, the alert fires, and the on-call engineer is notified immediately instead of business users discovering it Monday morning."*

---

## KEY LINES TO REMEMBER

| What | How |
|------|-----|
| Dependencies | On Success / On Failure paths in ADF activity connections |
| Parallel tables | ForEach with isSequential=false, batchCount=5 |
| Transient errors | Retry policy — 3 retries, 30 sec interval |
| Error logging | Stored Procedure on Failure path → audit table |
| Alerting | Web Activity → Logic App → email/Teams |
| Idempotency | Watermark updates only on Success, MERGE not INSERT |
| Skip completed stages | Pipeline state table checked in Databricks notebook |
| Zero row validation | df.count() == 0 raises exception before write |

---

## CLOSING LINE — SAY THIS TO END YOUR ANSWER

*"The combination of retry policies at activity level, failure branches for logging and alerting, watermark protection for safe reruns, and a pipeline state table for stage-level checkpointing gave us a 99.2% pipeline success rate over three months in production — and when failures did happen, average recovery time dropped from 4 hours to under 30 minutes."*

---

*Interview script — ECDP project — ADF ETL Error Handling*
