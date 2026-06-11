# CSV Incremental Load — Step by Step (Beginner Friendly)

---

## Big Picture First

```
CSV File Arrives on ADLS
        │
   ADF Pipeline Wakes Up
        │
   "Is this file new?" → YES
        │
   Copy to Bronze (raw Parquet)
        │
   Databricks Notebook runs
        │
   Clean data → Silver (Delta)
        │
   Aggregate → Gold (Delta)
```

---

## Step 0 — Key Vault Setup (Before Everything)

**What is Key Vault?**
Think of it as a **password locker**. You never hardcode passwords in ADF. Instead ADF asks Key Vault "hey give me the storage password" and Key Vault hands it over securely.

**What secrets you store in Key Vault:**

| Secret Name | What It Stores |
|---|---|
| `adls-storage-account-key` | ADLS Gen2 storage access key |
| `oracle-jdbc-connection-string` | Oracle DB connection (if needed) |
| `databricks-pat-token` | Databricks Personal Access Token |
| `sql-control-db-password` | Password for watermark control table |

**How ADF connects to Key Vault:**
- In ADF → Manage → Linked Services → **Azure Key Vault**
- ADF's **Managed Identity** is given `Get` permission on Key Vault secrets
- No password needed — Azure trusts ADF automatically

---

## Step 1 — Global Parameters in ADF

**What are Global Parameters?**
Values that are the **same across all pipelines**. Set once, use everywhere.

Go to: ADF Studio → Manage → Global Parameters

| Parameter Name | Type | Example Value |
|---|---|---|
| `gp_bronze_container_path` | String | `abfss://bronze@yourstorage.dfs.core.windows.net/csv/` |
| `gp_silver_container_path` | String | `abfss://silver@yourstorage.dfs.core.windows.net/` |
| `gp_gold_container_path` | String | `abfss://gold@yourstorage.dfs.core.windows.net/` |
| `gp_environment` | String | `prod` |
| `gp_databricks_workspace_url` | String | `https://adb-xxxx.azuredatabricks.net` |

---

## Step 2 — Pipeline Parameters

**What are Pipeline Parameters?**
Values that **change per pipeline run** — like which entity/table you are loading.

| Parameter Name | Type | Example Value |
|---|---|---|
| `p_entity_name` | String | `patient_data` |
| `p_source_folder_path` | String | `raw/csv/patient_data/` |
| `p_load_date` | String | `@formatDateTime(utcNow(), 'yyyy-MM-dd')` |
| `p_watermark_column` | String | `file_last_modified` |

---

## Step 3 — The ADF Pipeline Activities (One by One)

### Activity 1 — Get Last Watermark (Lookup Activity)

**What it does:** Checks "what was the last time we loaded this file?" so we only pick new files.

**Activity Type:** `Lookup`

**Settings:**
- Source: Azure SQL Table (your control/watermark table)
- Query:
```sql
SELECT last_loaded_timestamp 
FROM dbo.watermark_control 
WHERE entity_name = '@{pipeline().parameters.p_entity_name}'
```

**Output:** `@activity('GetLastWatermark').output.firstRow.last_loaded_timestamp`

You save this value — let's call it `last_wm` — and use it in the next step.

---

### Activity 2 — Get Metadata (GetMetadata Activity)

**What it does:** Lists all files in the source folder and checks their `lastModified` timestamp.

**Activity Type:** `GetMetadata`

**Settings:**
- Dataset: Your ADLS Gen2 source folder dataset
- Field list: `childItems`, `lastModified`
- Folder path: `@pipeline().parameters.p_source_folder_path`

**Output:** A list of files with their last modified timestamps.

---

### Activity 3 — Filter New Files (Filter Activity)

**What it does:** From the file list, keep **only files modified after the last watermark**.

**Activity Type:** `Filter`

**Settings:**
- Items: `@activity('GetMetadata').output.childItems`
- Condition:
```
@greater(
  activity('GetMetadata').output.lastModified,
  activity('GetLastWatermark').output.firstRow.last_loaded_timestamp
)
```

**Output:** Only new/changed files pass through.

---

### Activity 4 — ForEach Loop (ForEach Activity)

**What it does:** For each new file found, run the copy activity.

**Activity Type:** `ForEach`

**Settings:**
- Items: `@activity('FilterNewFiles').output.Value`
- Sequential: `false` (run in parallel, faster)
- Batch count: `5` (max 5 files at a time)

---

### Activity 5 — Copy to Bronze (Copy Activity — inside ForEach)

**What it does:** Reads the CSV file and writes it as **Parquet** into the Bronze layer.

**Activity Type:** `Copy Data`

**Source Settings:**
- Linked Service: ADLS Gen2 (uses Key Vault secret `adls-storage-account-key`)
- File format: `DelimitedText (CSV)`
- File path: `@item().name` (current file from ForEach)
- First row as header: `true`

**Sink Settings:**
- Linked Service: Same ADLS Gen2
- File format: `Parquet`
- Folder path:
```
@concat(
  pipeline().globalParameters.gp_bronze_container_path,
  pipeline().parameters.p_entity_name,
  '/load_date=',
  pipeline().parameters.p_load_date
)
```
- Write behavior: `Append`

**Result:** File lands at:
```
bronze/csv/patient_data/load_date=2026-06-11/file.parquet
```

---

### Activity 6 — Trigger Databricks Notebook — Bronze to Silver (Databricks Notebook Activity)

**What it does:** Runs your PySpark notebook to clean and merge data into Silver.

**Activity Type:** `Databricks Notebook`

**Linked Service:** Azure Databricks
- Authentication: Using `databricks-pat-token` from Key Vault

**Settings:**
- Notebook path: `/pipelines/silver/process_csv_silver`
- Base parameters passed to notebook:

| Parameter | Value |
|---|---|
| `entity_name` | `@pipeline().parameters.p_entity_name` |
| `bronze_path` | `@concat(pipeline().globalParameters.gp_bronze_container_path, pipeline().parameters.p_entity_name, '/load_date=', pipeline().parameters.p_load_date)` |
| `silver_path` | `@pipeline().globalParameters.gp_silver_container_path` |
| `load_date` | `@pipeline().parameters.p_load_date` |

---

### Activity 7 — Trigger Databricks Notebook — Silver to Gold (Databricks Notebook Activity)

**Same as above but runs a different notebook.**

- Notebook path: `/pipelines/gold/process_csv_gold`
- Parameters: `entity_name`, `silver_path`, `gold_path`

---

### Activity 8 — Update Watermark (Stored Procedure Activity)

**What it does:** After successful load, updates the control table with the new timestamp so next run knows where to start.

**Activity Type:** `Stored Procedure`

**Linked Service:** Azure SQL (watermark control DB)

**Stored Procedure:** `usp_update_watermark`

**Parameters:**

| Name | Value |
|---|---|
| `entity_name` | `@pipeline().parameters.p_entity_name` |
| `new_timestamp` | `@utcNow()` |

---

## Step 4 — What Happens Inside Databricks (Silver Notebook)

```python
from pyspark.sql.functions import col, current_date, current_timestamp, lower, year, month
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Parameters passed from ADF
entity_name = dbutils.widgets.get("entity_name")
bronze_path  = dbutils.widgets.get("bronze_path")
silver_path  = dbutils.widgets.get("silver_path")

# Read from Bronze
df_new = spark.read.parquet(bronze_path)

# Clean + Transform
# 1. Deduplicate using window (latest record per patient_id)
w = Window.partitionBy("patient_id").orderBy(col("updated_at").desc())
df_clean = df_new.withColumn("rn", F.row_number().over(w)) \
                 .filter(col("rn") == 1) \
                 .drop("rn")

# 2. Drop null patient_id
df_clean = df_clean.dropna(subset=["patient_id"])

# 3. Standardize formats
df_clean = df_clean.withColumn("patient_id", col("patient_id").cast("string")) \
                   .withColumn("name", lower(col("name")))

# 4. Derived columns
df_clean = df_clean.withColumn("year_of_birth", year(col("dob"))) \
                   .withColumn("month_of_birth", month(col("dob")))

# 5. Fill defaults
df_clean = df_clean.fillna({"gender": "Unknown", "age": 0})

# 6. Audit columns
df_clean = df_clean.withColumn("load_date", current_date()) \
                   .withColumn("etl_insert_ts", current_timestamp())

# --- Optimizations ---
# Partition pruning: repartition by patient_id to balance skew
df_clean = df_clean.repartition("patient_id")

# Cache if reused multiple times
df_clean.cache()

# Enable AQE + skew join handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewedPartitionFactor", "10")
spark.conf.set("spark.sql.adaptive.skewedPartitionThresholdInBytes", str(256*1024*1024))  # 256MB

# MERGE into Silver Delta Table (Upsert)
silver_table = DeltaTable.forName(spark, f"silver.{entity_name}")

silver_table.alias("target").merge(
    df_clean.alias("source"),
    "target.patient_id = source.patient_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Optional: Optimize Delta table for read performance
spark.sql(f"OPTIMIZE silver.{entity_name} ZORDER BY (patient_id)")
```

---

## Full Pipeline Flow Summary

```
[Lookup] Get Last Watermark
        │
[GetMetadata] List files in source folder
        │
[Filter] Keep only files newer than watermark
        │
[ForEach] Loop over new files
        │
    [Copy] CSV → Parquet → Bronze/entity/load_date=today/
        │
[Databricks Notebook] Bronze → Silver (MERGE/Upsert)
        │
[Databricks Notebook] Silver → Gold (Aggregation)
        │
[Stored Procedure] Update Watermark timestamp
```

---

## Where Key Vault Fits In Each Step

| Step | Secret Used | Why |
|---|---|---|
| Copy Activity | `adls-storage-account-key` | To read source CSV and write Bronze Parquet |
| Databricks Activity | `databricks-pat-token` | To authenticate and trigger notebook run |
| Watermark Update | `sql-control-db-password` | To connect to Azure SQL and update control table |

---

Want me to now draw out the **watermark control table schema**, or show you the **ADF pipeline JSON** for this entire flow so you can import it directly?

I'll create both files for you — the watermark control table SQL schema and the full ADF pipeline JSON.

Here's what's in each file and how they connect:

---

## File 1 — `PL_CSV_Incremental_Load.json`
The full ADF pipeline you can import directly into ADF Studio → Author → Pipelines → Import from JSON.

**11 Activities in order:**

| # | Activity Name | Type | What It Does |
|---|---|---|---|
| 1 | `ACT_LKP_GetLastWatermark` | Lookup | Calls stored proc to get last load timestamp |
| 2 | `ACT_SET_LastWatermark` | SetVariable | Saves timestamp into pipeline variable |
| 3 | `ACT_SET_BronzeOutputPath` | SetVariable | Builds the Bronze folder path with today's date |
| 4 | `ACT_META_ListSourceFiles` | GetMetadata | Lists all files in source folder |
| 5 | `ACT_FILTER_NewFiles` | Filter | Keeps only files newer than watermark |
| 6 | `ACT_IF_FilesExist` | IfCondition | Skip run if no new files found |
| 7 | `ACT_FOREACH_ProcessFiles` | ForEach | Loops over new files (5 in parallel) |
| 8 | `ACT_COPY_CSVtoBronze` | Copy | CSV → Parquet → Bronze (inside ForEach) |
| 9 | `ACT_NOTEBOOK_BronzeToSilver` | Databricks | PySpark MERGE into Silver Delta |
| 10 | `ACT_NOTEBOOK_SilverToGold` | Databricks | Aggregations into Gold Delta |
| 11 | `ACT_SP_UpdateWatermark` | Stored Proc | Saves new watermark on success |

---

## File 2 — `watermark_control_table.sql`
Run this on your Azure SQL Control DB. It creates:

- `dbo.watermark_control` — one row per entity, tracks last load time
- `dbo.load_history` — full audit trail of every pipeline run
- `usp_get_watermark` — called by ADF Lookup at start
- `usp_update_watermark` — called by ADF at end on success
- `usp_mark_load_failed` — called by ADF failure branch

---

**Next steps to use these:**
1. Run the SQL file on your Azure SQL DB first
2. Import the JSON into ADF
3. Update the `globalParameters` at top of JSON with your actual storage account name, Key Vault URL, and Databricks workspace URL
4. Create the two Silver/Gold Databricks notebooks at the paths specified

## watermark_control_table.sql

```sql
-- ============================================================
-- WATERMARK CONTROL TABLE SCHEMA
-- Purpose : Tracks last successful load timestamp per entity
-- Database : Azure SQL (Control DB)
-- ============================================================

-- -------------------------------------------------------
-- 1. Create Schema
-- -------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dbo;

-- -------------------------------------------------------
-- 2. Main Watermark Control Table
-- -------------------------------------------------------
CREATE TABLE dbo.watermark_control (
    watermark_id          INT IDENTITY(1,1) PRIMARY KEY,  -- Auto increment PK
    entity_name           VARCHAR(100)  NOT NULL,          -- e.g. 'patient_data', 'claims_data'
    source_type           VARCHAR(20)   NOT NULL,          -- 'CSV' or 'ORACLE'
    source_folder_path    VARCHAR(500)  NULL,              -- Source folder path (for CSV)
    source_table_name     VARCHAR(200)  NULL,              -- Source table name (for Oracle)
    watermark_column      VARCHAR(100)  NOT NULL,          -- Column used for incremental filter e.g. 'file_last_modified'
    last_loaded_timestamp DATETIME2     NOT NULL           -- Last successful load time
                          DEFAULT '1900-01-01 00:00:00',  -- Default = load everything on first run
    last_loaded_rowcount  BIGINT        NULL,              -- How many rows were loaded last run
    pipeline_run_id       VARCHAR(100)  NULL,              -- ADF pipeline run ID for traceability
    load_status           VARCHAR(20)   NOT NULL           -- 'SUCCESS', 'FAILED', 'IN_PROGRESS'
                          DEFAULT 'SUCCESS',
    created_date          DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    updated_date          DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    created_by            VARCHAR(100)  NOT NULL DEFAULT 'ADF_PIPELINE',
    updated_by            VARCHAR(100)  NOT NULL DEFAULT 'ADF_PIPELINE'
);

-- -------------------------------------------------------
-- 3. Index for fast lookup by entity_name
-- -------------------------------------------------------
CREATE UNIQUE INDEX UX_watermark_entity
    ON dbo.watermark_control (entity_name, source_type);

-- -------------------------------------------------------
-- 4. Seed Initial Records (one row per entity)
--    First run will pick up ALL data since timestamp = 1900
-- -------------------------------------------------------
INSERT INTO dbo.watermark_control (
    entity_name,
    source_type,
    source_folder_path,
    source_table_name,
    watermark_column,
    last_loaded_timestamp,
    load_status
)
VALUES
-- CSV Entities
('patient_data',    'CSV',    'raw/csv/patient_data/',    NULL,                   'file_last_modified', '1900-01-01 00:00:00', 'SUCCESS'),
('claims_data',     'CSV',    'raw/csv/claims_data/',     NULL,                   'file_last_modified', '1900-01-01 00:00:00', 'SUCCESS'),
('site_data',       'CSV',    'raw/csv/site_data/',       NULL,                   'file_last_modified', '1900-01-01 00:00:00', 'SUCCESS'),

-- Oracle Entities
('subject_visits',  'ORACLE', NULL,                       'CLIN.SUBJECT_VISITS',  'UPDATED_AT',         '1900-01-01 00:00:00', 'SUCCESS'),
('adverse_events',  'ORACLE', NULL,                       'CLIN.ADVERSE_EVENTS',  'UPDATED_AT',         '1900-01-01 00:00:00', 'SUCCESS');


-- -------------------------------------------------------
-- 5. Load History Table (audit trail of every run)
-- -------------------------------------------------------
CREATE TABLE dbo.load_history (
    history_id            INT IDENTITY(1,1) PRIMARY KEY,
    entity_name           VARCHAR(100)  NOT NULL,
    source_type           VARCHAR(20)   NOT NULL,
    pipeline_run_id       VARCHAR(100)  NOT NULL,          -- ADF Run ID
    pipeline_name         VARCHAR(200)  NULL,
    load_start_time       DATETIME2     NOT NULL,
    load_end_time         DATETIME2     NULL,
    watermark_from        DATETIME2     NOT NULL,          -- What watermark we started from
    watermark_to          DATETIME2     NULL,              -- What watermark we ended at
    rows_read             BIGINT        NULL,
    rows_written          BIGINT        NULL,
    files_processed       INT           NULL,              -- For CSV: how many files
    bronze_path           VARCHAR(500)  NULL,              -- Where Bronze data landed
    load_status           VARCHAR(20)   NOT NULL DEFAULT 'IN_PROGRESS',  -- 'SUCCESS','FAILED','IN_PROGRESS'
    error_message         VARCHAR(MAX)  NULL,              -- Error details if FAILED
    created_date          DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX IX_load_history_entity
    ON dbo.load_history (entity_name, load_start_time DESC);


-- -------------------------------------------------------
-- 6. Stored Procedure: Get Current Watermark
--    Called by ADF Lookup Activity at START of pipeline
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_get_watermark
    @entity_name  VARCHAR(100),
    @source_type  VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        entity_name,
        source_type,
        watermark_column,
        last_loaded_timestamp,
        source_folder_path,
        source_table_name
    FROM dbo.watermark_control
    WHERE entity_name = @entity_name
      AND source_type = @source_type;
END;
GO


-- -------------------------------------------------------
-- 7. Stored Procedure: Update Watermark After Success
--    Called by ADF Stored Procedure Activity at END of pipeline
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_update_watermark
    @entity_name        VARCHAR(100),
    @source_type        VARCHAR(20),
    @new_timestamp      DATETIME2,
    @pipeline_run_id    VARCHAR(100),
    @rows_loaded        BIGINT       = NULL,
    @files_processed    INT          = NULL,
    @bronze_path        VARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Update main watermark table
    UPDATE dbo.watermark_control
    SET
        last_loaded_timestamp = @new_timestamp,
        last_loaded_rowcount  = @rows_loaded,
        pipeline_run_id       = @pipeline_run_id,
        load_status           = 'SUCCESS',
        updated_date          = GETUTCDATE(),
        updated_by            = 'ADF_PIPELINE'
    WHERE entity_name = @entity_name
      AND source_type  = @source_type;

    -- Insert audit record into history
    INSERT INTO dbo.load_history (
        entity_name, source_type, pipeline_run_id,
        load_start_time, load_end_time,
        watermark_to, rows_written, files_processed,
        bronze_path, load_status
    )
    VALUES (
        @entity_name, @source_type, @pipeline_run_id,
        GETUTCDATE(), GETUTCDATE(),
        @new_timestamp, @rows_loaded, @files_processed,
        @bronze_path, 'SUCCESS'
    );
END;
GO


-- -------------------------------------------------------
-- 8. Stored Procedure: Mark Load as FAILED
--    Called in ADF failure branch
-- -------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_mark_load_failed
    @entity_name      VARCHAR(100),
    @source_type      VARCHAR(20),
    @pipeline_run_id  VARCHAR(100),
    @error_message    VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.watermark_control
    SET
        load_status  = 'FAILED',
        pipeline_run_id = @pipeline_run_id,
        updated_date = GETUTCDATE()
    WHERE entity_name = @entity_name
      AND source_type  = @source_type;

    INSERT INTO dbo.load_history (
        entity_name, source_type, pipeline_run_id,
        load_start_time, load_end_time,
        watermark_to, load_status, error_message
    )
    VALUES (
        @entity_name, @source_type, @pipeline_run_id,
        GETUTCDATE(), GETUTCDATE(),
        NULL, 'FAILED', @error_message
    );
END;
GO

-- -------------------------------------------------------
-- 9. Useful Queries for Monitoring
-- -------------------------------------------------------

-- Check current watermark status for all entities
SELECT
    entity_name,
    source_type,
    last_loaded_timestamp,
    last_loaded_rowcount,
    load_status,
    DATEDIFF(HOUR, last_loaded_timestamp, GETUTCDATE()) AS hours_since_last_load
FROM dbo.watermark_control
ORDER BY entity_name;

-- Check last 10 runs for a specific entity
SELECT TOP 10 *
FROM dbo.load_history
WHERE entity_name = 'patient_data'
ORDER BY load_start_time DESC;

-- Check all failed loads
SELECT *
FROM dbo.load_history
WHERE load_status = 'FAILED'
ORDER BY load_start_time DESC;

```

---

## PL_CSV_Incremental_Load.json

```json
{
  "name": "PL_CSV_Incremental_Load",
  "properties": {
    "description": "End-to-end incremental load pipeline: CSV Source → Bronze (Parquet) → Silver (Delta) → Gold (Delta)",

    "globalParameters": {
      "gp_bronze_container_path": {
        "type": "string",
        "value": "abfss://bronze@yourstorage.dfs.core.windows.net/csv/"
      },
      "gp_silver_container_path": {
        "type": "string",
        "value": "abfss://silver@yourstorage.dfs.core.windows.net/"
      },
      "gp_gold_container_path": {
        "type": "string",
        "value": "abfss://gold@yourstorage.dfs.core.windows.net/"
      },
      "gp_databricks_workspace_url": {
        "type": "string",
        "value": "https://adb-XXXXXXXXXX.azuredatabricks.net"
      },
      "gp_environment": {
        "type": "string",
        "value": "prod"
      },
      "gp_control_db_server": {
        "type": "string",
        "value": "your-azure-sql-server.database.windows.net"
      },
      "gp_control_db_name": {
        "type": "string",
        "value": "ControlDB"
      }
    },

    "parameters": {
      "p_entity_name": {
        "type": "string",
        "defaultValue": "patient_data",
        "description": "Name of the entity/table being loaded e.g. patient_data"
      },
      "p_source_folder_path": {
        "type": "string",
        "defaultValue": "raw/csv/patient_data/",
        "description": "Source folder path in ADLS Gen2 where CSV files arrive"
      },
      "p_source_type": {
        "type": "string",
        "defaultValue": "CSV"
      },
      "p_load_date": {
        "type": "string",
        "defaultValue": "@formatDateTime(utcNow(), 'yyyy-MM-dd')",
        "description": "Partition date for Bronze landing"
      },
      "p_silver_notebook_path": {
        "type": "string",
        "defaultValue": "/pipelines/silver/process_csv_silver",
        "description": "Databricks notebook path for Silver processing"
      },
      "p_gold_notebook_path": {
        "type": "string",
        "defaultValue": "/pipelines/gold/process_csv_gold",
        "description": "Databricks notebook path for Gold processing"
      }
    },

    "variables": {
      "v_last_watermark": {
        "type": "String",
        "defaultValue": "1900-01-01 00:00:00"
      },
      "v_bronze_output_path": {
        "type": "String",
        "defaultValue": ""
      },
      "v_files_processed_count": {
        "type": "Integer",
        "defaultValue": 0
      }
    },

    "activities": [

      {
        "name": "ACT_LKP_GetLastWatermark",
        "description": "STEP 1: Lookup Activity — Reads last successful load timestamp from control table",
        "type": "Lookup",
        "dependsOn": [],
        "policy": {
          "timeout": "0.00:05:00",
          "retry": 2,
          "retryIntervalInSeconds": 30
        },
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderStoredProcedureName": "dbo.usp_get_watermark",
            "storedProcedureParameters": {
              "entity_name": {
                "type": "String",
                "value": "@pipeline().parameters.p_entity_name"
              },
              "source_type": {
                "type": "String",
                "value": "@pipeline().parameters.p_source_type"
              }
            }
          },
          "dataset": {
            "referenceName": "DS_AzureSQL_ControlDB",
            "type": "DatasetReference"
          },
          "firstRowOnly": true
        }
      },

      {
        "name": "ACT_SET_LastWatermark",
        "description": "STEP 2: Set Variable — Saves the watermark value into a pipeline variable for reuse",
        "type": "SetVariable",
        "dependsOn": [
          {
            "activity": "ACT_LKP_GetLastWatermark",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "variableName": "v_last_watermark",
          "value": "@activity('ACT_LKP_GetLastWatermark').output.firstRow.last_loaded_timestamp"
        }
      },

      {
        "name": "ACT_SET_BronzeOutputPath",
        "description": "STEP 3: Set Variable — Builds the Bronze output folder path with load_date partition",
        "type": "SetVariable",
        "dependsOn": [
          {
            "activity": "ACT_SET_LastWatermark",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "variableName": "v_bronze_output_path",
          "value": "@concat(pipeline().globalParameters.gp_bronze_container_path, pipeline().parameters.p_entity_name, '/load_date=', formatDateTime(utcNow(), 'yyyy-MM-dd'))"
        }
      },

      {
        "name": "ACT_META_ListSourceFiles",
        "description": "STEP 4: GetMetadata Activity — Lists all files in the source CSV folder with their lastModified timestamps",
        "type": "GetMetadata",
        "dependsOn": [
          {
            "activity": "ACT_SET_BronzeOutputPath",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "policy": {
          "timeout": "0.00:10:00",
          "retry": 2,
          "retryIntervalInSeconds": 30
        },
        "typeProperties": {
          "dataset": {
            "referenceName": "DS_ADLS_CSV_Source_Folder",
            "type": "DatasetReference",
            "parameters": {
              "folder_path": "@pipeline().parameters.p_source_folder_path"
            }
          },
          "fieldList": [
            "childItems",
            "lastModified",
            "itemType"
          ],
          "storeSettings": {
            "type": "AzureBlobFSReadSettings",
            "recursive": false,
            "enablePartitionDiscovery": false
          }
        }
      },

      {
        "name": "ACT_FILTER_NewFiles",
        "description": "STEP 5: Filter Activity — Keeps only files where lastModified > last watermark timestamp",
        "type": "Filter",
        "dependsOn": [
          {
            "activity": "ACT_META_ListSourceFiles",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "items": {
            "value": "@activity('ACT_META_ListSourceFiles').output.childItems",
            "type": "Expression"
          },
          "condition": {
            "value": "@and(equals(item().type, 'File'), greater(activity('ACT_META_ListSourceFiles').output.lastModified, variables('v_last_watermark')))",
            "type": "Expression"
          }
        }
      },

      {
        "name": "ACT_IF_FilesExist",
        "description": "STEP 6: IfCondition — Only proceed if there are new files to process. Avoids empty runs.",
        "type": "IfCondition",
        "dependsOn": [
          {
            "activity": "ACT_FILTER_NewFiles",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "expression": {
            "value": "@greater(length(activity('ACT_FILTER_NewFiles').output.Value), 0)",
            "type": "Expression"
          },

          "ifTrueActivities": [

            {
              "name": "ACT_FOREACH_ProcessFiles",
              "description": "STEP 7: ForEach Activity — Loop over each new CSV file and copy to Bronze",
              "type": "ForEach",
              "dependsOn": [],
              "typeProperties": {
                "items": {
                  "value": "@activity('ACT_FILTER_NewFiles').output.Value",
                  "type": "Expression"
                },
                "isSequential": false,
                "batchCount": 5,
                "activities": [

                  {
                    "name": "ACT_COPY_CSVtoBronze",
                    "description": "STEP 8 (inside ForEach): Copy Activity — Reads CSV from source, writes as Parquet to Bronze layer",
                    "type": "Copy",
                    "dependsOn": [],
                    "policy": {
                      "timeout": "0.01:00:00",
                      "retry": 3,
                      "retryIntervalInSeconds": 60
                    },
                    "typeProperties": {
                      "source": {
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                          "type": "AzureBlobFSReadSettings",
                          "recursive": false
                        },
                        "formatSettings": {
                          "type": "DelimitedTextReadSettings",
                          "skipLineCount": 0
                        }
                      },
                      "sink": {
                        "type": "ParquetSink",
                        "storeSettings": {
                          "type": "AzureBlobFSWriteSettings",
                          "copyBehavior": "MergeFiles"
                        },
                        "formatSettings": {
                          "type": "ParquetWriteSettings"
                        }
                      },
                      "enableStaging": false,
                      "translator": {
                        "type": "TabularTranslator",
                        "typeConversion": true,
                        "typeConversionSettings": {
                          "allowDataTruncation": true,
                          "treatBooleanAsNumber": false
                        }
                      }
                    },
                    "inputs": [
                      {
                        "referenceName": "DS_ADLS_CSV_Source_File",
                        "type": "DatasetReference",
                        "parameters": {
                          "folder_path": "@pipeline().parameters.p_source_folder_path",
                          "file_name": "@item().name"
                        }
                      }
                    ],
                    "outputs": [
                      {
                        "referenceName": "DS_ADLS_Bronze_Parquet",
                        "type": "DatasetReference",
                        "parameters": {
                          "bronze_folder_path": "@variables('v_bronze_output_path')",
                          "file_name": "@concat(replace(item().name, '.csv', ''), '_', formatDateTime(utcNow(), 'HHmmss'), '.parquet')"
                        }
                      }
                    ]
                  }
                ]
              }
            },

            {
              "name": "ACT_NOTEBOOK_BronzeToSilver",
              "description": "STEP 9: Databricks Notebook Activity — Reads Bronze Parquet, cleans + merges into Silver Delta Table",
              "type": "DatabricksNotebook",
              "dependsOn": [
                {
                  "activity": "ACT_FOREACH_ProcessFiles",
                  "dependencyConditions": ["Succeeded"]
                }
              ],
              "policy": {
                "timeout": "0.02:00:00",
                "retry": 1,
                "retryIntervalInSeconds": 60
              },
              "typeProperties": {
                "notebookPath": "@pipeline().parameters.p_silver_notebook_path",
                "baseParameters": {
                  "entity_name": {
                    "value": "@pipeline().parameters.p_entity_name",
                    "type": "Expression"
                  },
                  "bronze_path": {
                    "value": "@variables('v_bronze_output_path')",
                    "type": "Expression"
                  },
                  "silver_path": {
                    "value": "@pipeline().globalParameters.gp_silver_container_path",
                    "type": "Expression"
                  },
                  "load_date": {
                    "value": "@formatDateTime(utcNow(), 'yyyy-MM-dd')",
                    "type": "Expression"
                  },
                  "pipeline_run_id": {
                    "value": "@pipeline().RunId",
                    "type": "Expression"
                  },
                  "environment": {
                    "value": "@pipeline().globalParameters.gp_environment",
                    "type": "Expression"
                  }
                }
              },
              "linkedServiceName": {
                "referenceName": "LS_AzureDatabricks",
                "type": "LinkedServiceReference"
              }
            },

            {
              "name": "ACT_NOTEBOOK_SilverToGold",
              "description": "STEP 10: Databricks Notebook Activity — Reads Silver Delta, runs aggregations, writes Gold Delta Table",
              "type": "DatabricksNotebook",
              "dependsOn": [
                {
                  "activity": "ACT_NOTEBOOK_BronzeToSilver",
                  "dependencyConditions": ["Succeeded"]
                }
              ],
              "policy": {
                "timeout": "0.02:00:00",
                "retry": 1,
                "retryIntervalInSeconds": 60
              },
              "typeProperties": {
                "notebookPath": "@pipeline().parameters.p_gold_notebook_path",
                "baseParameters": {
                  "entity_name": {
                    "value": "@pipeline().parameters.p_entity_name",
                    "type": "Expression"
                  },
                  "silver_path": {
                    "value": "@pipeline().globalParameters.gp_silver_container_path",
                    "type": "Expression"
                  },
                  "gold_path": {
                    "value": "@pipeline().globalParameters.gp_gold_container_path",
                    "type": "Expression"
                  },
                  "load_date": {
                    "value": "@formatDateTime(utcNow(), 'yyyy-MM-dd')",
                    "type": "Expression"
                  },
                  "pipeline_run_id": {
                    "value": "@pipeline().RunId",
                    "type": "Expression"
                  }
                }
              },
              "linkedServiceName": {
                "referenceName": "LS_AzureDatabricks",
                "type": "LinkedServiceReference"
              }
            },

            {
              "name": "ACT_SP_UpdateWatermark",
              "description": "STEP 11: Stored Procedure Activity — Updates control table with new watermark after successful load",
              "type": "SqlServerStoredProcedure",
              "dependsOn": [
                {
                  "activity": "ACT_NOTEBOOK_SilverToGold",
                  "dependencyConditions": ["Succeeded"]
                }
              ],
              "policy": {
                "timeout": "0.00:05:00",
                "retry": 2,
                "retryIntervalInSeconds": 30
              },
              "typeProperties": {
                "storedProcedureName": "dbo.usp_update_watermark",
                "storedProcedureParameters": {
                  "entity_name": {
                    "value": "@pipeline().parameters.p_entity_name",
                    "type": "String"
                  },
                  "source_type": {
                    "value": "@pipeline().parameters.p_source_type",
                    "type": "String"
                  },
                  "new_timestamp": {
                    "value": "@utcNow()",
                    "type": "DateTime"
                  },
                  "pipeline_run_id": {
                    "value": "@pipeline().RunId",
                    "type": "String"
                  },
                  "files_processed": {
                    "value": "@length(activity('ACT_FILTER_NewFiles').output.Value)",
                    "type": "Int32"
                  },
                  "bronze_path": {
                    "value": "@variables('v_bronze_output_path')",
                    "type": "String"
                  }
                }
              },
              "linkedServiceName": {
                "referenceName": "DS_AzureSQL_ControlDB",
                "type": "LinkedServiceReference"
              }
            },

            {
              "name": "ACT_SP_MarkFailed_Silver",
              "description": "FAILURE BRANCH: If Silver or Gold notebook fails — mark load as FAILED in control table",
              "type": "SqlServerStoredProcedure",
              "dependsOn": [
                {
                  "activity": "ACT_NOTEBOOK_BronzeToSilver",
                  "dependencyConditions": ["Failed"]
                },
                {
                  "activity": "ACT_NOTEBOOK_SilverToGold",
                  "dependencyConditions": ["Failed"]
                }
              ],
              "typeProperties": {
                "storedProcedureName": "dbo.usp_mark_load_failed",
                "storedProcedureParameters": {
                  "entity_name": {
                    "value": "@pipeline().parameters.p_entity_name",
                    "type": "String"
                  },
                  "source_type": {
                    "value": "@pipeline().parameters.p_source_type",
                    "type": "String"
                  },
                  "pipeline_run_id": {
                    "value": "@pipeline().RunId",
                    "type": "String"
                  },
                  "error_message": {
                    "value": "@concat('Pipeline failed at: ', activity('ACT_NOTEBOOK_BronzeToSilver').Error?.message)",
                    "type": "String"
                  }
                }
              },
              "linkedServiceName": {
                "referenceName": "DS_AzureSQL_ControlDB",
                "type": "LinkedServiceReference"
              }
            }

          ],

          "ifFalseActivities": [
            {
              "name": "ACT_LOG_NoNewFiles",
              "description": "No new files found — log and exit gracefully. Watermark is NOT updated.",
              "type": "Wait",
              "dependsOn": [],
              "typeProperties": {
                "waitTimeInSeconds": 1
              }
            }
          ]
        }
      }

    ],

    "linkedServices": [
      {
        "name": "LS_AzureKeyVault",
        "description": "Key Vault — Central secret store. All passwords/tokens come from here.",
        "type": "AzureKeyVault",
        "typeProperties": {
          "baseUrl": "https://your-keyvault-name.vault.azure.net/"
        }
      },
      {
        "name": "LS_ADLS_Gen2",
        "description": "ADLS Gen2 — Source CSV and Bronze/Silver/Gold storage",
        "type": "AzureBlobFS",
        "typeProperties": {
          "url": "https://yourstorage.dfs.core.windows.net",
          "accountKey": {
            "type": "AzureKeyVaultSecret",
            "store": {
              "referenceName": "LS_AzureKeyVault",
              "type": "LinkedServiceReference"
            },
            "secretName": "adls-storage-account-key"
          }
        }
      },
      {
        "name": "LS_AzureDatabricks",
        "description": "Databricks — Runs Silver and Gold transformation notebooks",
        "type": "AzureDatabricks",
        "typeProperties": {
          "domain": "@pipeline().globalParameters.gp_databricks_workspace_url",
          "accessToken": {
            "type": "AzureKeyVaultSecret",
            "store": {
              "referenceName": "LS_AzureKeyVault",
              "type": "LinkedServiceReference"
            },
            "secretName": "databricks-pat-token"
          },
          "existingClusterId": "YOUR_CLUSTER_ID_HERE"
        }
      },
      {
        "name": "LS_AzureSQL_ControlDB",
        "description": "Azure SQL — Watermark control table and load history",
        "type": "AzureSqlDatabase",
        "typeProperties": {
          "connectionString": {
            "type": "AzureKeyVaultSecret",
            "store": {
              "referenceName": "LS_AzureKeyVault",
              "type": "LinkedServiceReference"
            },
            "secretName": "sql-control-db-connection-string"
          }
        }
      }
    ],

    "datasets": [
      {
        "name": "DS_ADLS_CSV_Source_Folder",
        "description": "Points to the folder where source CSV files land",
        "type": "DelimitedText",
        "linkedServiceName": {
          "referenceName": "LS_ADLS_Gen2",
          "type": "LinkedServiceReference"
        },
        "parameters": {
          "folder_path": { "type": "string" }
        },
        "typeProperties": {
          "location": {
            "type": "AzureBlobFSLocation",
            "folderPath": "@dataset().folder_path",
            "fileSystem": "source"
          },
          "columnDelimiter": ",",
          "firstRowAsHeader": true,
          "compressionCodec": "none"
        }
      },
      {
        "name": "DS_ADLS_CSV_Source_File",
        "description": "Points to a specific CSV file (used inside ForEach loop)",
        "type": "DelimitedText",
        "linkedServiceName": {
          "referenceName": "LS_ADLS_Gen2",
          "type": "LinkedServiceReference"
        },
        "parameters": {
          "folder_path": { "type": "string" },
          "file_name":   { "type": "string" }
        },
        "typeProperties": {
          "location": {
            "type": "AzureBlobFSLocation",
            "folderPath": "@dataset().folder_path",
            "fileName":   "@dataset().file_name",
            "fileSystem": "source"
          },
          "columnDelimiter": ",",
          "firstRowAsHeader": true
        }
      },
      {
        "name": "DS_ADLS_Bronze_Parquet",
        "description": "Bronze landing zone — output Parquet dataset",
        "type": "Parquet",
        "linkedServiceName": {
          "referenceName": "LS_ADLS_Gen2",
          "type": "LinkedServiceReference"
        },
        "parameters": {
          "bronze_folder_path": { "type": "string" },
          "file_name":          { "type": "string" }
        },
        "typeProperties": {
          "location": {
            "type": "AzureBlobFSLocation",
            "folderPath": "@dataset().bronze_folder_path",
            "fileName":   "@dataset().file_name",
            "fileSystem": "bronze"
          },
          "compressionCodec": "snappy"
        }
      },
      {
        "name": "DS_AzureSQL_ControlDB",
        "description": "Azure SQL dataset pointing to watermark control table",
        "type": "AzureSqlTable",
        "linkedServiceName": {
          "referenceName": "LS_AzureSQL_ControlDB",
          "type": "LinkedServiceReference"
        },
        "typeProperties": {
          "schema": "dbo",
          "table":  "watermark_control"
        }
      }
    ]
  }
}
```
