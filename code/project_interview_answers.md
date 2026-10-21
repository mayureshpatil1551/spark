# Project-Based Interview Answers (eCDP and FinOps Dataverse)

**How to use this file**
- Everything marked **[VERIFIED]** comes from project facts you have told me before.
- Everything marked **⚠️ FILL IN** is something I do not know. Replace it with your real detail, or use the honest fallback script. Do not state numbers or tools you did not actually use.
- Answer structure used throughout (your preferred 3 parts): **what I did → why → what breaks without it.**

Contents
1. Explain your project
2. What optimization did you do (in detail)
3. Biggest challenge you faced
4. Where did you use repartition / coalesce
5. Have you used a UDF
6. How did you manage Unity Catalog
7. What is the CI/CD process
8. Quick answer sheet for the project questions

---

## 1. Explain your project

### Project A: eCDP (e-Clinical Data Platform), July 2023 to early 2025 [VERIFIED]

**30-second version:**
"eCDP is a clinical and financial data platform for a pharma client. We ingested data from sources like Oracle and CSV files using Azure Data Factory, processed it on Azure Databricks with Delta Lake on ADLS Gen2, and organized it with a Medallion architecture: Bronze, Silver, Gold."

**Detailed walkthrough (follow this order):**

1. **Ingestion (ADF).**
   - Metadata-driven batch ingestion: a **control table** drives which sources/tables to load, with **parameterized datasets and Linked Services** so one pipeline serves many sources.
   - Oracle loads were **incremental**, driven by a **watermark control table** (only pull rows newer than the last stored watermark).
   - File sources used a **CSV archive pattern**: Get Metadata → ForEach → Copy → Delete (move processed files to archive).
2. **Bronze.**
   - **Append-only Delta** on ADLS Gen2, path-based (no metastore registration). Keeps raw history for replay and audit.
3. **Silver.**
   - **Unity Catalog external Delta tables.**
   - Deduplicate with `row_number()` before the **MERGE**, so each key appears once in the source batch.
4. **Gold.**
   - **Unity Catalog external Delta tables**, written with **`replaceWhere`** to overwrite only the affected slice.
5. **Security.**
   - **Azure Key Vault** accessed via **Managed Identity** from ADF, and via a **Secret Scope** from Databricks. No credentials in code.
6. **MDM / Golden Record** work was part of the platform. ⚠️ FILL IN: your exact role in it (matching rules, survivorship, which entities).

**Why this design:**
- Bronze append-only gives an immutable raw layer, so reprocessing never needs a source re-pull.
- Metadata-driven pipelines avoid building one pipeline per table.
- Watermarks avoid full reloads of large Oracle tables.
- Unity Catalog on Silver/Gold gives governance (permissions, lineage) where consumers read.

**What breaks without it (pick one or two):**
- Without dedup before MERGE, the MERGE fails or produces wrong results when several source rows match one target row.
- Without watermarks, every run is a full load: slow, expensive, and a larger source-system load.

⚠️ FILL IN (be ready for these follow-ups): number of source systems/tables, data volume per day, run frequency, team size, your specific ownership (which layers/pipelines you built).

### Project B: FinOps Dataverse, early 2025 to present [VERIFIED]

**30-second version:**
"FinOps Dataverse is a financial data migration platform for a pharmaceutical client. I work on migrating **20 TB+ of data to Apache Iceberg on AWS S3** using PySpark, with orchestration through **Modak Nabu**."

**Be ready to explain:**
- **Why Iceberg:** open table format with ACID transactions, schema evolution, hidden partitioning, time travel, and engine independence (not locked to one vendor).
- **Why S3:** cheap, durable object storage that decouples storage from compute.
- **Your part:** ⚠️ FILL IN: for example, which ingestion/migration jobs you wrote, how you validated the migration (row counts, checksums, schema checks), how you handled the load in batches.
- **Source APIs you have worked with (from your stack):** Salesforce, Smartsheet, Veeva REST APIs, Oracle JDBC. ⚠️ FILL IN which of these belong to which project.

---

## 2. What optimization did you do (in detail)

### Optimizations that match your verified project design

#### 2.1 Incremental loading with a watermark (Oracle → Bronze)
- **What I did:** Stored the last successful load timestamp/key in a watermark control table. Each run queried Oracle only for rows beyond the watermark, then updated the watermark after a successful load.
- **Why:** Avoids reading entire large tables each run, which cuts runtime, JDBC load on the source, and cost.
- **What breaks without it:** Full loads on every run, long windows, and risk of source-system impact.
- **Detail to remember:** update the watermark only after the load succeeds, so a failed run is safely re-run without losing data.

#### 2.2 Deduplication with `row_number()` before MERGE (Silver)
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

w = Window.partitionBy("business_key").orderBy(col("updated_at").desc())
latest = (bronze_df
          .withColumn("rn", row_number().over(w))
          .filter("rn = 1")
          .drop("rn"))

# then MERGE `latest` into the Silver Delta table
```
- **Why:** MERGE requires that each target row matches at most one source row. Duplicates in the batch cause an error ("multiple source rows matched") or incorrect updates. Deduping first also shrinks the data being merged.
- **What breaks without it:** failed MERGEs or duplicate/incorrect Silver records.

#### 2.3 `replaceWhere` for Gold
```python
(gold_df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", "load_date = '2026-01-15'")
    .save(gold_path))
```
- **Why:** Rewrites only the affected slice instead of the whole table. Faster, cheaper, and the operation is atomic via the Delta log.
- **What breaks without it:** full-table overwrite on each run (slow, more files rewritten) or duplicate rows if appended on re-runs.
- **Note:** the rows in the DataFrame must all satisfy the `replaceWhere` predicate, otherwise Delta raises an error.

#### 2.4 Metadata-driven, parameterized pipelines
- **What I did:** One generic ADF pipeline driven by a control table, instead of one pipeline per table.
- **Why:** Less duplicated code, easier to onboard a new source (add a row, not a pipeline), simpler maintenance.

### General Spark optimizations: use ONLY if you actually did them
Tick these off honestly before the interview. If you did not do one, do not mention it as your own work.

| Technique | One-line explanation | Did I do this? |
|---|---|---|
| Broadcast join | Broadcast the small side to avoid shuffling the big table | ☐ |
| Partition pruning / filters pushed early | Filter on partition columns so Spark reads less | ☐ |
| Fixing small files (`OPTIMIZE`, compaction, coalesce before write) | Fewer, larger files read faster | ☐ |
| Handling data skew (salting, AQE skew join) | Spread hot keys across tasks | ☐ |
| Caching reused DataFrames | Avoid recomputing the same plan | ☐ |
| Avoiding UDFs, using built-in functions | Built-ins are optimized by Catalyst and run in the JVM | ☐ |
| Selecting only needed columns, columnar formats (Parquet/Delta) | Less I/O | ☐ |
| Tuning `spark.sql.shuffle.partitions` / enabling AQE | Right-size shuffle partitions | ☐ |
| Iceberg: compaction, partition spec design, snapshot expiry | Keep metadata and files healthy for the 20 TB migration | ☐ |

### Template for the "in detail" answer (fill with your real optimization)
1. **Problem:** "The job was taking ⚠️ ___ and ⚠️ ___ (spilling / skew / small files / full loads)."
2. **How I found it:** Spark UI (Stages tab: skewed task durations, spill; SQL tab: join type), job run times.
3. **What I changed:** ⚠️ ___
4. **Result:** ⚠️ ___ (give a measured before/after only if you really have it)
5. **Why it works / what breaks without it:** ⚠️ ___

**Honest fallback if you have no measured numbers:**
"I did not capture an exact before/after benchmark, but the change moved us from full loads to incremental loads, so each run processed only new data."

---

## 3. Biggest challenge you faced

I do not know your real "biggest challenge", so below are **candidates built from your verified project design**. Pick the one that is genuinely yours, and keep the details you can defend.

### Candidate 1: Duplicate/conflicting records breaking Silver MERGE
- **Situation:** Incremental loads from Oracle could bring multiple versions of the same business key in one batch.
- **Action:** Added `row_number()` ordered by the latest update timestamp and merged only the latest version per key.
- **Result:** MERGE became deterministic and stopped failing on duplicate matches. ⚠️ FILL IN any measurable outcome.
- **What breaks without it:** failed pipelines or wrong Silver data.

### Candidate 2: Safe re-runs and recovery of incremental loads
- **Situation:** A run failing midway could leave the watermark and the data out of sync.
- **Action:** Update the watermark only after a successful write, and make Gold writes idempotent with `replaceWhere`.
- **Result:** Failed runs can simply be re-run without duplicates or gaps.
- **What breaks without it:** duplicated or missing data after failures.

### Candidate 3: Project transition (eCDP ending)
- **Situation:** eCDP was winding down and the team had to hand over or move on.
- **Action:** ⚠️ FILL IN: documentation, knowledge transfer, moving to FinOps Dataverse.
- **Result:** ⚠️ FILL IN
- Use this only if the interviewer asks about a non-technical challenge.

### Candidate 4: 20 TB+ migration to Iceberg (FinOps Dataverse)
- ⚠️ FILL IN the real difficulty (for example volume, validation, schema differences, performance). I do not have details of what went wrong or how you solved it, so I will not write them for you.

**Format to speak it:** Situation → what I did → why that choice → result → what I would do differently.

---

## 4. Where did you use repartition / coalesce

⚠️ I do not have a record of where you used these in your projects. Use this order:

1. **Say the concept first** (see `spark_concepts_interview_answers.md`, section 8).
2. **Then give the honest project answer.** Pick the version that is true:

**If you did use them (examples that fit your pipeline; use only what is true):**
- "Before writing to Delta/Iceberg, I used `coalesce(n)` to reduce the number of output files and avoid the small-files problem."
- "Before a join or write by key, I used `repartition(n, key)` so the data was evenly distributed."
- "For the `replaceWhere` write to Gold, I repartitioned by the partition column so each partition was written by fewer tasks." (only if true)

**If you did not use them directly:**
"In my projects Databricks features like adaptive query execution and optimized writes handled most partition sizing automatically, so I rarely needed manual repartitioning. I understand when I would use it: `coalesce` to cut output files cheaply, `repartition` to fix skew or partition by a join key."

---

## 5. Have you used a UDF

⚠️ I do not know whether you used UDFs. Answer truthfully with one of these.

**Concept answer (always safe):**
- A UDF is custom Python/Scala logic applied to column values.
- **Python UDFs are slower** than built-in functions because rows are serialized between the JVM and the Python worker, and Catalyst cannot optimize inside them.
- **Prefer built-in functions** (`when`, `regexp_replace`, `to_date`, etc.) wherever possible.
- If needed, prefer **pandas (vectorized) UDFs**, which use Arrow and process batches.

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

@udf(returnType=StringType())
def clean_code(x):
    return x.strip().upper() if x else None

@pandas_udf("string")
def clean_code_vec(s: pd.Series) -> pd.Series:
    return s.str.strip().str.upper()
```

**If you have used one:** say what it did (for example standardizing a code or parsing a field), why a built-in function could not do it, and whether you tried replacing it with a built-in or pandas UDF. ⚠️ FILL IN the real use case.

**If you have not:**
"I avoid UDFs where possible and use built-in Spark functions, since they are optimized by Catalyst. I know the trade-off: Python UDFs add serialization overhead, so I would use a pandas UDF if custom logic were unavoidable."

---

## 6. How did you manage Unity Catalog

### What is verified
- **Silver and Gold** in eCDP are **Unity Catalog external Delta tables**. **Bronze** is path-based on ADLS Gen2 without metastore registration. [VERIFIED]
- Secrets: Key Vault via Managed Identity (ADF) and Secret Scope (Databricks). [VERIFIED]

### Concepts to explain
**Hierarchy:** Metastore → Catalog → Schema → Table/View/Volume.

**External tables vs managed tables:**
- **Managed:** Unity Catalog controls the storage location; dropping the table deletes data.
- **External:** data stays in your own ADLS path; dropping the table removes only metadata. Used when data must live in a specific storage location or be shared with other engines.

**How external access is governed:**
1. **Storage credential:** the identity (for example an Azure managed identity through the Databricks access connector) that can access the storage.
2. **External location:** maps an ADLS path to a storage credential.
3. **Permissions:** grants on catalog/schema/table.

```sql
-- Example grants (illustrative)
GRANT USE CATALOG ON CATALOG ecdp TO `data_consumers`;
GRANT USE SCHEMA ON SCHEMA ecdp.silver TO `data_consumers`;
GRANT SELECT ON TABLE ecdp.silver.patients TO `data_consumers`;

CREATE TABLE ecdp.silver.patients
USING DELTA
LOCATION 'abfss://silver@<storageaccount>.dfs.core.windows.net/patients';
```
(Names above are examples, not your real objects.)

**Benefits you can state:** centralized access control, column/table lineage, audit logs, one governance layer across workspaces.

### Honest framing of "managed"
⚠️ FILL IN which of these you personally did: created catalogs/schemas, registered external tables, set grants, set up external locations, used lineage. If you only registered Silver/Gold tables and used permissions someone else set up, say:
"I registered and worked with the Silver and Gold external Delta tables in Unity Catalog, and used it for governed access and lineage. The metastore and storage credential setup was handled by the platform/admin team."
(Use that only if it is accurate.)

---

## 7. What is the CI/CD process

### Concept answer
**CI (Continuous Integration):** developers commit to Git; automated checks run (linting, unit tests, validation) so that problems are caught early.
**CD (Continuous Delivery/Deployment):** the validated artifact is automatically promoted through environments (Dev → Test/UAT → Prod), with approvals before production.

### Typical flow for ADF + Databricks (matches your tool stack)
1. **Source control:** ADF connected to a Git repo (Azure DevOps or GitHub). Developers work in **feature branches** and merge via **pull request** into the collaboration branch (main).
2. **Publish:** publishing from the ADF collaboration branch generates **ARM templates** in the `adf_publish` branch (or use the ADF npm package for automated validation/export).
3. **Release pipeline (Azure DevOps or GitHub Actions):**
   - Deploy ARM templates to the target environment's ADF.
   - Use **parameter files per environment** (linked service connection strings, Key Vault names, storage accounts).
   - Pre-deployment step: stop active triggers; post-deployment: restart them and remove deleted resources.
4. **Databricks code:** notebooks/repos synced from Git using **Databricks Repos**; promote the same commit to higher environments (for example by pipeline jobs that update the Repo to a tag/branch).
5. **Secrets:** Key Vault references and secret scopes so no credentials appear in code.
6. **Approvals and gates:** manual approval before Prod.

### Honest framing
In your memory I have both a real project context (CI/CD via Azure DevOps with ARM template deployment) and a hands-on practice build (ADF Git integration, ARM deployment, GitHub Actions, Databricks Repos). ⚠️ FILL IN which parts you did on the real project and which you built in your own practice environment. If part of it is from practice, say:
"On the project we deployed through Azure DevOps with ARM templates. I also built the full flow myself, including GitHub Actions and Databricks Repos, in a hands-on setup to understand environment promotion end to end."

---

## 8. Quick answer sheet (30-second versions)

| Question | Short answer |
|---|---|
| Explain your project | eCDP: pharma clinical/financial data platform on ADF + Databricks + Delta + ADLS with Bronze/Silver/Gold. FinOps Dataverse: 20 TB+ migration to Iceberg on S3 with PySpark and Modak Nabu. |
| Optimization | Watermark-based incremental loads, dedup before MERGE, `replaceWhere` for Gold, metadata-driven pipelines. Add measured results only if real. |
| Biggest challenge | Pick one true candidate from section 3. |
| repartition/coalesce usage | Concept first, then only what you truly did (section 4). |
| UDF | Prefer built-ins; pandas UDF if needed; state real use only if true (section 5). |
| Unity Catalog | Silver/Gold as external Delta tables; explain hierarchy, storage credential, external location, grants (section 6). |
| CI/CD | Git → PR → ARM templates → release pipeline with per-environment parameters → approvals (section 7). |
