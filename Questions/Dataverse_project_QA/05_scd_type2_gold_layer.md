# SCD Type 2 — Silver to Gold (Delta Lake)

## What SCD Type 2 Means Here

Instead of overwriting a row when an attribute changes (SCD Type 1), you **keep history**:
- The old row is marked "expired" (closed)
- A new row is inserted as "current"

This requires extra columns on the Gold dimension table:

| Column | Purpose |
|---|---|
| `surrogate_key` | Unique key per version of the record (e.g. UUID or hash) |
| `customer_id` (business key) | The natural/source key |
| `effective_start_date` | When this version became active |
| `effective_end_date` | When this version stopped being active (NULL = current) |
| `is_current` | Boolean flag — `true` for the active row |
| `_hash` | Hash of tracked attributes, used to detect changes |

---

## 1. Gold Table Schema (one-time setup)

```sql
CREATE TABLE gold.sales.customer_dim (
    surrogate_key        STRING,
    customer_id          STRING,
    customer_name        STRING,
    region               STRING,
    segment              STRING,
    effective_start_date TIMESTAMP,
    effective_end_date   TIMESTAMP,
    is_current           BOOLEAN,
    _hash                STRING
)
USING DELTA;
```

`surrogate_key` can be `uuid()` or a deterministic hash of `customer_id + effective_start_date`.

---

## 2. Notebook: silver_to_gold (SCD2 version)

```python
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws, expr
)

silver_table = dbutils.widgets.get("silver_table")
gold_table = dbutils.widgets.get("gold_table")

# Columns whose changes should trigger a new version
tracked_cols = ["customer_name", "region", "segment"]

# ---- Step 1: Read Silver and compute a change-hash ----
df_silver = (
    spark.table(silver_table)
    .select("customer_id", *tracked_cols)
    .withColumn("_hash", sha2(concat_ws("||", *tracked_cols), 256))
    .dropDuplicates(["customer_id"])
)

df_silver.createOrReplaceTempView("silver_updates")

# ---- Step 2: First-time load ----
if not spark.catalog.tableExists(gold_table):
    (df_silver
        .withColumn("surrogate_key", sha2(concat_ws("||", "customer_id", "_hash"), 256))
        .withColumn("effective_start_date", current_timestamp())
        .withColumn("effective_end_date", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .write.format("delta").saveAsTable(gold_table)
    )

else:
    # ---- Step 3: Identify changed / new records ----
    # Compare incoming hash vs current hash in Gold (only current rows)
    df_changes = spark.sql(f"""
        SELECT s.*
        FROM silver_updates s
        LEFT JOIN (
            SELECT customer_id, _hash
            FROM {gold_table}
            WHERE is_current = true
        ) g
        ON s.customer_id = g.customer_id AND s._hash = g._hash
        WHERE g.customer_id IS NULL
    """)

    df_changes.createOrReplaceTempView("changed_records")

    if df_changes.count() > 0:
        # ---- Step 4a: Expire old current rows for changed customer_ids ----
        spark.sql(f"""
            MERGE INTO {gold_table} AS target
            USING changed_records AS source
            ON target.customer_id = source.customer_id AND target.is_current = true
            WHEN MATCHED THEN UPDATE SET
                target.effective_end_date = current_timestamp(),
                target.is_current = false
        """)

        # ---- Step 4b: Insert new current rows ----
        df_new_versions = (df_changes
            .withColumn("surrogate_key", sha2(concat_ws("||", "customer_id", "_hash", expr("current_timestamp()")), 256))
            .withColumn("effective_start_date", current_timestamp())
            .withColumn("effective_end_date", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
        )

        df_new_versions.write.format("delta").mode("append").saveAsTable(gold_table)

        print(f"SCD2 applied: {df_changes.count()} record(s) versioned.")
    else:
        print("No changes detected — nothing to version.")
```

---

## 3. How the Logic Works, Step by Step

1. **Hash the tracked columns** (`_hash`) for every incoming Silver record. This single column tells you instantly "did anything I care about change?" — no need to compare column-by-column.
2. **Compare against the current Gold rows only** (`is_current = true`). If `customer_id` doesn't exist in Gold, or the hash differs → it's a new/changed record.
3. **Expire the old version**: set `effective_end_date = now()` and `is_current = false` on the existing current row — its history is preserved, not deleted.
4. **Insert the new version**: a fresh row with a new `surrogate_key`, `effective_start_date = now()`, `effective_end_date = NULL`, `is_current = true`.

Net result: querying `WHERE is_current = true` gives you the latest snapshot (like SCD1), but the full table preserves every historical version with valid date ranges — useful for "as-of" reporting (e.g., "what region was this customer in on 2025-03-01?").

---

## 4. Querying Historical Data

```sql
-- Current state of all customers
SELECT * FROM gold.sales.customer_dim WHERE is_current = true;

-- What did customer C123 look like on a specific date?
SELECT * FROM gold.sales.customer_dim
WHERE customer_id = 'C123'
  AND '2025-03-01' BETWEEN effective_start_date AND COALESCE(effective_end_date, '9999-12-31');
```

---

## 5. Notes / Gotchas

- **New customers** (not in Gold at all) flow through the same `changed_records` logic automatically — the LEFT JOIN returns NULL for `g.customer_id`, so the MERGE's `WHEN MATCHED` simply finds nothing to expire, and the insert step adds them as new current rows.
- **Deletes from source**: SCD2 as written doesn't handle hard deletes from Silver. If a record disappears from Silver, decide whether to mark it expired (soft-delete) — this needs an extra anti-join check (`Gold current` minus `Silver`).
- **Performance**: for large dimensions, `Z-ORDER BY (customer_id)` and periodically `OPTIMIZE` the Gold table to keep MERGE/joins fast.
- **Tracked columns choice matters**: only include columns in `tracked_cols` that actually represent "a meaningful business change." If you include something like a last-modified timestamp from source, every row will appear "changed" every run.
