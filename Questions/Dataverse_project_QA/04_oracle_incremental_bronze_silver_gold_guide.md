# Oracle → Bronze → Silver → Gold: Incremental Load Pipeline (End-to-End Guide)

## 0. Architecture Overview

```
Oracle DB ──(ADF Copy, Incremental)──> Bronze (ADLS Gen2, data packet files - Parquet/Delta)
Bronze ──(Databricks Notebook)──> Silver (Delta Table, Unity Catalog)
Silver ──(Databricks Notebook)──> Gold (Delta Table, Unity Catalog)
```

Orchestrated end-to-end by a single **master ADF pipeline** that:
1. Pulls incremental data from Oracle into Bronze packet files
2. Triggers a Databricks notebook (Bronze → Silver)
3. Triggers a Databricks notebook (Silver → Gold)

All credentials (Oracle DB, Databricks PAT) are stored in **Azure Key Vault**, referenced via ADF Linked Services and Databricks Secret Scopes — never hardcoded.

---

## 1. Pre-requisites Setup

### 1.1 Azure Key Vault
Create secrets in Key Vault:
- `oracle-db-username`
- `oracle-db-password`
- `databricks-pat-token`

Give your ADF's **Managed Identity** "Get/List" access policy on the Key Vault (under Access Policies, or RBAC: "Key Vault Secrets User").

### 1.2 Control / Watermark Table
You need a small table (in Oracle, or a SQL DB/Delta table) to track incremental progress:

```sql
CREATE TABLE watermark_control (
    table_name      VARCHAR2(100),
    watermark_col   VARCHAR2(100),
    last_loaded_val VARCHAR2(100)
);

-- One row per source table
INSERT INTO watermark_control VALUES ('CUSTOMER_MASTER', 'LAST_UPDATED_DTTM', '2020-01-01 00:00:00');
```

This table is the heart of "how do I know what's new since last run".

---

## 2. ADF Setup — Linked Services

A Linked Service = "connection info" to a system.

### 2.1 Azure Key Vault Linked Service
- Type: Azure Key Vault
- Authentication: Managed Identity (ADF's system-assigned identity)
- Name: `LS_AzureKeyVault`

### 2.2 Oracle Linked Service
- Type: Oracle
- Connect via: Integration Runtime (Self-Hosted IR if Oracle is on-prem/private network, Azure IR if reachable publicly)
- Connection string fields: Host, Port, Service Name
- **Username/Password**: Set as "Azure Key Vault" → select `LS_AzureKeyVault`, secret name = `oracle-db-username` / `oracle-db-password`
- Name: `LS_OracleDB`

### 2.3 ADLS Gen2 Linked Service (Bronze landing zone)
- Type: Azure Data Lake Storage Gen2
- Authentication: Managed Identity or Account Key (Managed Identity preferred)
- Name: `LS_ADLS_Bronze`

### 2.4 Azure Databricks Linked Service
- Type: Azure Databricks
- Select existing interactive cluster OR configure a Job Cluster (recommended for cost — spins up only during the run)
- Authentication: Access Token → from Azure Key Vault → secret name `databricks-pat-token`
- Name: `LS_Databricks`

---

## 3. ADF Global Parameters

These are environment-wide constants (Factory level → Manage → Global Parameters):

| Name | Type | Example Value |
|---|---|---|
| `gp_bronze_container` | String | `bronze` |
| `gp_storage_account` | String | `mydatalakeacct` |
| `gp_environment` | String | `dev` / `prod` |
| `gp_databricks_notebook_path_b2s` | String | `/Repos/prod/notebooks/bronze_to_silver` |
| `gp_databricks_notebook_path_s2g` | String | `/Repos/prod/notebooks/silver_to_gold` |

Global parameters can be referenced in any pipeline as `pipeline().globalParameters.gp_xxx`.

---

## 4. ADF Datasets

### 4.1 Oracle Source Dataset (`DS_Oracle_Source`)
- Linked Service: `LS_OracleDB`
- Type: Generic Oracle table dataset
- Parameterize: `p_schema_name`, `p_table_name` (so one dataset works for all tables)
- Leave "Table" field empty in UI — you'll override with a dynamic query in Copy Activity

### 4.2 Bronze Sink Dataset (`DS_Bronze_Parquet`)
- Linked Service: `LS_ADLS_Bronze`
- Format: Parquet (or Delta, but Parquet "packet files" is common for Bronze)
- Path pattern (parameterized):
```
@concat('bronze/', dataset().p_table_name, '/year=', dataset().p_year, '/month=', dataset().p_month, '/day=', dataset().p_day, '/')
```
- File name: auto-generated or `@concat(dataset().p_table_name, '_', utcnow(), '.parquet')`
- Add dataset parameters: `p_table_name`, `p_year`, `p_month`, `p_day`

### 4.3 Watermark Control Dataset (`DS_Watermark_Control`)
- Points to your `watermark_control` table (in Oracle or wherever you keep it)
- Used by Lookup activities

---

## 5. Master Pipeline Parameters

At pipeline level, define:

| Parameter | Type | Example |
|---|---|---|
| `pTableName` | String | `CUSTOMER_MASTER` |
| `pSchemaName` | String | `SALES` |
| `pWatermarkColumn` | String | `LAST_UPDATED_DTTM` |
| `pPrimaryKeyColumn` | String | `CUSTOMER_ID` |
| `pSilverTableName` | String | `silver.sales.customer_master` |
| `pGoldTableName` | String | `gold.sales.customer_dim` |

This lets ONE pipeline be reused for multiple tables by just changing parameter values per trigger/run (or use a "Lookup → ForEach" loop over a list of table configs for full multi-table automation — covered in section 9).

---

## 6. Pipeline: Oracle → Bronze (Incremental via ADF)

**Pipeline name:** `PL_Oracle_to_Bronze_Incremental`

### Step 1 — Lookup: Get Old Watermark
- Activity: **Lookup**
- Source: `DS_Watermark_Control`
- Query:
```sql
SELECT last_loaded_val 
FROM watermark_control 
WHERE table_name = '@{pipeline().parameters.pTableName}'
```
- First row only: ✅ checked
- Output reference: `@activity('Lookup_OldWatermark').output.firstRow.LAST_LOADED_VAL`

### Step 2 — Lookup: Get New Watermark (current max value from source)
- Activity: **Lookup**
- Source: `DS_Oracle_Source` (dynamic query)
- Query:
```sql
SELECT MAX(@{pipeline().parameters.pWatermarkColumn}) AS NEW_WATERMARK 
FROM @{pipeline().parameters.pSchemaName}.@{pipeline().parameters.pTableName}
```
- Output reference: `@activity('Lookup_NewWatermark').output.firstRow.NEW_WATERMARK`

### Step 3 — Copy Activity: Incremental Extract → Bronze
- Activity: **Copy Data**
- Source: `DS_Oracle_Source`
  - Use **Query** option (not table):
```sql
SELECT * FROM @{pipeline().parameters.pSchemaName}.@{pipeline().parameters.pTableName}
WHERE @{pipeline().parameters.pWatermarkColumn} > TO_TIMESTAMP('@{activity('Lookup_OldWatermark').output.firstRow.LAST_LOADED_VAL}','YYYY-MM-DD HH24:MI:SS')
AND @{pipeline().parameters.pWatermarkColumn} <= TO_TIMESTAMP('@{activity('Lookup_NewWatermark').output.firstRow.NEW_WATERMARK}','YYYY-MM-DD HH24:MI:SS')
```
- Sink: `DS_Bronze_Parquet`
  - `p_table_name` = `@pipeline().parameters.pTableName`
  - `p_year/month/day` = derived from `@utcnow()` using formatDateTime, e.g. `@formatDateTime(utcnow(),'yyyy')`
- This produces "packet files" — one Parquet file per run, partitioned by date folders.

### Step 4 — If Condition: Rows Found?
- Check `@greater(activity('Copy_Oracle_To_Bronze').output.rowsCopied, 0)`
- If true → proceed to update watermark and trigger downstream
- If false → skip downstream (no new data), end pipeline gracefully

### Step 5 — Stored Procedure / Script Activity: Update Watermark
- Activity: **Script** (or Stored Procedure if using a proper RDBMS)
```sql
UPDATE watermark_control 
SET last_loaded_val = '@{activity('Lookup_NewWatermark').output.firstRow.NEW_WATERMARK}'
WHERE table_name = '@{pipeline().parameters.pTableName}'
```
- This only runs after successful Copy — ensures watermark never advances if the copy fails (set "On Success" dependency from Copy activity).

### Step 6 — Databricks Notebook Activity: Bronze → Silver
- Activity: **Notebook** (Databricks)
- Linked Service: `LS_Databricks`
- Notebook path: `@pipeline().globalParameters.gp_databricks_notebook_path_b2s`
- Base Parameters (passed into notebook as `dbutils.widgets`):
  - `table_name` = `@pipeline().parameters.pTableName`
  - `load_date` = `@formatDateTime(utcnow(),'yyyy-MM-dd')`
  - `silver_table` = `@pipeline().parameters.pSilverTableName`
  - `primary_key` = `@pipeline().parameters.pPrimaryKeyColumn`
- Dependency: runs **On Success** of Step 5

### Step 7 — Databricks Notebook Activity: Silver → Gold
- Activity: **Notebook** (Databricks)
- Notebook path: `@pipeline().globalParameters.gp_databricks_notebook_path_s2g`
- Base Parameters:
  - `silver_table` = `@pipeline().parameters.pSilverTableName`
  - `gold_table` = `@pipeline().parameters.pGoldTableName`
  - `load_date` = `@formatDateTime(utcnow(),'yyyy-MM-dd')`
- Dependency: runs **On Success** of Step 6

### Error Handling
- Add a parallel branch from Copy/Notebook activities with "On Failure" dependency → Web Activity (Teams/Email alert) or Send Email activity using Logic App.

---

## 7. Databricks Notebook: Bronze → Silver (Delta)

**Notebook: `bronze_to_silver`**

```python
# Widgets (parameters passed from ADF)
table_name = dbutils.widgets.get("table_name")
load_date = dbutils.widgets.get("load_date")
silver_table = dbutils.widgets.get("silver_table")
primary_key = dbutils.widgets.get("primary_key")

# Step 1: Read new Bronze packet files for this load date
bronze_path = f"abfss://bronze@<storage_account>.dfs.core.windows.net/{table_name}/year={load_date[:4]}/month={load_date[5:7]}/day={load_date[8:10]}/"

df_bronze = spark.read.parquet(bronze_path)

# Step 2: Basic cleansing / standardization
from pyspark.sql.functions import current_timestamp, lit

df_clean = (df_bronze
    .dropDuplicates([primary_key])           # dedup within the batch
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_system", lit("ORACLE"))
)

# Step 3: Create Silver table if not exists (first run only)
if not spark.catalog.tableExists(silver_table):
    df_clean.write.format("delta").saveAsTable(silver_table)
else:
    # Step 4: MERGE (upsert) into Silver Delta table
    df_clean.createOrReplaceTempView("staging_updates")
    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING staging_updates AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Bronze -> Silver completed for {table_name}, rows: {df_clean.count()}")
```

Key points:
- **MERGE INTO** is what makes Silver a proper deduplicated, up-to-date "current state" table from raw incremental packets.
- `_ingested_at` and `_source_system` are audit/lineage columns — good practice for traceability.

---

## 8. Databricks Notebook: Silver → Gold (Delta)

**Notebook: `silver_to_gold`**

```python
silver_table = dbutils.widgets.get("silver_table")
gold_table = dbutils.widgets.get("gold_table")
load_date = dbutils.widgets.get("load_date")

# Step 1: Read from Silver
df_silver = spark.table(silver_table)

# Step 2: Business transformations (example: aggregation / dimension build)
df_gold = (df_silver
    .select("customer_id", "customer_name", "region", "segment", "_ingested_at")
    .distinct()
)

# Step 3: Create or MERGE into Gold
if not spark.catalog.tableExists(gold_table):
    df_gold.write.format("delta").saveAsTable(gold_table)
else:
    df_gold.createOrReplaceTempView("gold_updates")
    spark.sql(f"""
        MERGE INTO {gold_table} AS target
        USING gold_updates AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Silver -> Gold completed, rows: {df_gold.count()}")
```

Gold layer typically contains business-curated, reporting-ready tables (dimensions, facts, aggregates) — apply whatever transformation/aggregation logic your business needs here.

---

## 9. Scaling to Multiple Tables (Optional but Recommended)

Instead of one pipeline per table, create a **config table** (Delta or SQL) listing all source tables + their watermark columns + primary keys + target table names. Then:

1. ADF Lookup activity reads this config table → returns array of table configs
2. **ForEach activity** (set to sequential or batch count for parallelism) iterates over each config
3. Inside ForEach → call an **"Execute Pipeline"** activity passing each table's config as parameters into `PL_Oracle_to_Bronze_Incremental`

This makes the framework metadata-driven — adding a new table = adding one row to the config table, no pipeline changes.

---

## 10. Scheduling & Triggers

- Create a **Tumbling Window** or **Schedule Trigger** on the master pipeline (e.g., daily at 2 AM)
- Tumbling window is preferable for incremental loads since it naturally provides `WindowStart`/`WindowEnd` system variables you can use instead of (or alongside) the watermark table

---

## 11. Quick Checklist (Build Order)

1. ✅ Create Key Vault + secrets, grant ADF Managed Identity access
2. ✅ Create `watermark_control` table, seed initial values
3. ✅ Create 4 Linked Services (Key Vault, Oracle, ADLS Gen2, Databricks)
4. ✅ Create Global Parameters
5. ✅ Create Datasets (Oracle source, Bronze sink, Watermark control)
6. ✅ Build `PL_Oracle_to_Bronze_Incremental` pipeline (Lookups → Copy → If → Script → Notebooks)
7. ✅ Write `bronze_to_silver` Databricks notebook (MERGE into Delta)
8. ✅ Write `silver_to_gold` Databricks notebook (MERGE into Delta)
9. ✅ Test end-to-end with one table, then scale via config-driven ForEach
10. ✅ Add Schedule/Tumbling Window trigger + failure alerting
