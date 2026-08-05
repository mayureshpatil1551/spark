# Interview Preparation — Line-by-Line Q&A (L1 & L2)

**How to use this:** Every question is tied to a specific line in your resume. L1 = screening/HR-technical round (concept check, "have you actually done this"). L2 = deep-dive technical/panel round (design decisions, trade-offs, failure scenarios). Answer using: **what you did → why you did it that way → what breaks without it.**

---

## 1. Professional Summary

> "Azure Data Engineer with 3+ years of experience... improving pipeline runtimes by up to 75%, cutting query latency by 40%, sustaining 99.9% data uptime..."

**L1**
- Walk me through your 3 years of experience in one minute.
- What's the difference between the two projects on your resume (FinOps Dataverse vs eCDP)?
- Which cloud have you worked on more — Azure or AWS?
- What does "Lakehouse" mean, and why not just use a data warehouse?

**L2**
- Where does the 75% runtime improvement come from — which pipeline, and what was the bottleneck before?
  *(Ground this in your eCDP 4-hour → 58-minute story — that's ~76% — be ready to explain the partition tuning that caused it.)*
- The 40% query latency cut — was that Z-Ordering, clustering, file compaction, or a mix? What would you check first if latency regressed again next month?
- 99.9% uptime — how is that measured (pipeline success rate? SLA hours met?) and what caused the 0.1% failures?
- If you had to pick ONE project to go deep on for 30 minutes, which one and why?

---

## 2. Technical Skills Block

**L1**
- Rank these by how much production experience you have: Databricks, ADF, Iceberg, Delta Lake.
- What's the difference between Delta Lake and Apache Iceberg? Why would a project pick one over the other?
- What is Unity Catalog and have you configured it yourself or just used it?
- Difference between ADLS Gen2 and Amazon S3 from an engineering standpoint?

**L2**
- You've listed both Delta Lake (FinOps) and Iceberg (eCDP) — what specific feature of Iceberg made it the right choice for a 20TB pharma migration, versus Delta?
- Explain Z-Ordering vs partition tuning — when do you use one over the other, and what's the failure mode of using Z-Ordering on the wrong column?
- You list "Pagination" under Data Engineering skills — where did you actually implement pagination? (Likely REST API ingestion — be ready with the specific API and page-size/rate-limit logic.)
- Walk through how Azure Key Vault + Managed Identity works end-to-end for a Databricks notebook pulling a secret.

---

## 3. FinOps Dataverse — Financial Data Modernization & Lakehouse Migration (Mar 2025–Present)

### 3.1 "Led the design of a Medallion Lakehouse to modernize SAP financial reporting, migrating legacy on-prem data to a governed Azure platform"

**L1**
- What does "led the design" mean here — were you the sole designer or part of a team?
- What is Medallion architecture and why is it suited to financial reporting?
- What does "governed" platform mean in this context?

**L2**
- SAP data has strict referential and audit requirements — how did Medallion (Bronze/Silver/Gold) preserve auditability from source to report?
- On-prem → Azure migration: what was the actual transfer mechanism (ADF Copy Activity with Self-hosted IR? Data Factory managed VNet? something else)? What breaks if the on-prem gateway goes down mid-load?
- Scenario: Finance says a number in a report doesn't match the SAP source system. How do you trace it back through Bronze → Silver → Gold to find where it diverged?

### 3.2 "Architected a Medallion Lakehouse using Azure Databricks and Delta Lake, migrating 26+ TB of financial data to ADLS Gen2"

**L1**
- Why Delta Lake specifically instead of plain Parquet in ADLS?
- What's the folder/container structure you used in ADLS Gen2 for Bronze/Silver/Gold?

**L2**
- 26TB — was this a one-time historical backfill, ongoing incremental, or both? How did you sequence it to avoid saturating throughput/cost?
- What Delta Lake features (ACID transactions, time travel, schema enforcement) did you actually rely on, and can you give a real incident where one of them saved you?
- Scenario: A Silver table load fails halfway through writing 2TB of data. What does Delta's transaction log guarantee here, and how do you recover without duplicating records?

### 3.3 "Designed end-to-end ETL pipelines in Azure Data Factory to ingest data from Oracle and flat-file sources into ADLS Gen2, using configuration-driven frameworks that cut onboarding time and improved operational efficiency by 50%"

**L1**
- What ADF components did you use to connect to Oracle? (Linked Service, Integration Runtime type)
- What does "configuration-driven framework" mean — metadata tables? Parameterized pipelines?

**L2**
- Walk through your metadata-driven ingestion framework in detail: where do source config/schema/watermark values live, and how does a single generic pipeline read that config to onboard a brand-new Oracle table without new pipeline development?
- How exactly did this cut onboarding time by 50% — what was the "before" process (a new pipeline per source?) and what's the "after" (add a config row)?
- Scenario: A new flat-file source shows up with a different delimiter and an extra column your framework doesn't expect. How does your config-driven design handle that without breaking other sources?
- What Integration Runtime did you use for Oracle (on-prem/self-hosted vs Azure IR) and why?

### 3.4 "Developed PySpark notebooks in Azure Databricks to perform data cleansing, transformation, deduplication and business rule implementation across large-scale financial datasets"

**L1**
- Give one real example of a "business rule" you implemented in PySpark.
- How do you deduplicate records in PySpark — which function/window logic?

**L2**
- Walk through your deduplication logic end-to-end (likely `row_number()` over a partition/order by, similar to your eCDP Silver pattern) — why that approach over `dropDuplicates()`?
- Scenario: Two records for the same financial transaction arrive with different "last updated" timestamps but conflicting amounts. What's your business rule for picking the "true" record, and how do you make that decision auditable?
- How do you handle schema drift in PySpark notebooks when a financial source system adds a new column mid-project?

### 3.5 "Implemented incremental loading to efficiently process new and updated records, reducing pipeline execution time by 30%"

**L1**
- What's the difference between full load and incremental load?
- What do you use to track "what's new" — a watermark column, CDC, or something else?

**L2**
- Describe your incremental loading mechanism precisely — is it a watermark control table (like your eCDP pattern) driving a `WHERE updated_at > last_watermark` extraction from Oracle? How do you handle late-arriving records that fall behind the watermark?
- Scenario: The watermark table gets corrupted/reset to an old date. What happens on the next run, and how do you prevent duplicate ingestion or data loss?
- Why did incremental loading cut execution time by 30% specifically — was the bottleneck extraction volume, transformation compute, or write/merge cost?

### 3.6 "Loaded curated datasets into Azure SQL Database and Delta Lake, tuning workloads with Z-Ordering, clustering, and file compaction, writing SQL for validation — cutting query latency by 40% across 300+ business reports"

**L1**
- Why load into both Azure SQL Database AND Delta Lake — what's each used for?
- What is file compaction (small file problem) and why does it matter?

**L2**
- Explain the small-file problem in Delta Lake in detail — how many small files were you dealing with, and what's your compaction strategy (`OPTIMIZE`, scheduled job, auto-compaction)?
- Walk through choosing Z-Order columns for your Gold tables — how did you decide which columns to Z-Order on for those 300+ reports?
- What kind of "validation SQL" did you write — row counts, checksums, business-rule reconciliation against SAP source? Give a concrete example.
- Scenario: After Z-Ordering, one specific report is still slow. How do you diagnose whether it's a query plan issue, a Z-Order column mismatch, or a downstream Power BI/reporting layer issue?

### 3.7 "Monitored, scheduled and optimized pipelines, resolving failures and data quality issues while partnering with business stakeholders to deliver trusted datasets"

**L1**
- What tool did you use to schedule/monitor ADF pipelines — ADF triggers, Azure Monitor, alerts?
- Give an example of a data quality issue you personally resolved.

**L2**
- Walk through your on-call/failure-response process: pipeline fails at 2am — what's the alert path, what do you check first, how do you decide "reprocess vs skip vs escalate"?
- Scenario: A business stakeholder says the Gold table numbers "look wrong" but there's no pipeline failure logged. How do you investigate a silent data quality issue (not a hard failure)?
- How do you define/track data quality here — null checks, referential integrity, row-count reconciliation, schema checks? Which did you actually implement?

### 3.8 "Collaborated with cross-functional teams... using Python, SQL, ADF, Databricks, PySpark, ADLS Gen2 and Azure SQL Database"

**L1**
- Who were the cross-functional teams — finance analysts, BI/reporting, other engineers?
- What was your role in requirement gathering vs pure implementation?

**L2**
- Scenario: A finance stakeholder requests a new metric that requires joining a Gold table with an SAP field you haven't ingested yet. Walk through how you'd scope, design, and deliver that end-to-end.

---

## 4. e-Clinical Data Platform (eCDP) — Pharmaceutical MDM (Jul 2023–Feb 2025)

### 4.1 "Built a Golden Record MDM platform unifying pharmaceutical and clinical data across systems"

**L1**
- What is a "Golden Record" in MDM terms?
- Why does a pharma/clinical company need MDM specifically (vs. just a data warehouse)?

**L2**
- Walk through your Golden Record logic: when the same entity (e.g., a patient, site, or product) exists in Oracle, Snowflake, and a flat file with slightly different attribute values, how do you decide which value "wins" for the Golden Record?
- Scenario: A regulatory audit asks you to prove why a specific Golden Record field has its current value. What lineage do you have to answer that (this ties to your XREF-based identity resolution)?

### 4.2 "Designed and implemented large-scale data migration pipelines using PySpark and Apache Iceberg, modernizing 20+ TB of pharmaceutical data on Amazon S3 with zero data-loss cutover"

**L1**
- Why Iceberg specifically for this migration instead of Delta Lake or Hive tables?
- What does "zero data-loss cutover" mean in practice — how do you prove no data was lost?

**L2**
- Explain Iceberg's core advantage you actually leveraged — hidden partitioning, schema evolution, snapshot/time-travel, or catalog abstraction (Glue/REST catalog)? Be specific about which one mattered for this migration.
- Walk through your cutover strategy: parallel-run old and new systems? Checksum/row-count reconciliation before decommissioning legacy? How long was the parallel-run window?
- Scenario: Mid-migration, you discover 50,000 records in the legacy system that don't exist in the new Iceberg tables. How do you find the gap and validate the fix without re-running the entire 20TB migration?
- Why Amazon S3 + Iceberg here vs. ADLS Gen2 + Delta Lake used in your other project — was this purely because the client was AWS-based, or a deliberate technical choice?

### 4.3 "Built ingestion frameworks integrating Oracle, Snowflake, REST APIs, Flat files and Smartsheets, processing 10M+ records daily at 99.9% pipeline availability; reduced a core pipeline's runtime from 4 hours to 58 minutes through partition tuning"

**L1**
- What's the difference in how you'd ingest from a REST API vs. a flat file vs. Snowflake?
- What is partition tuning, in plain terms?

**L2**
- This is your strongest, most specific claim on the resume — be ready to go deep. What was the pipeline doing in those original 4 hours (shuffle-heavy joins? too many small partitions? skewed keys?), and exactly what change (repartition count, partition key choice, broadcast join, file size target) got it to 58 minutes?
- Walk through your REST API ingestion: pagination strategy, rate-limit handling, retry/backoff logic, and how you land raw JSON into Bronze.
- Smartsheet as a source is unusual — what connector/method did you use (API, export, connector)? What's the data quality risk of a source that's manually edited by business users, and how did you guard against it?
- Scenario: One of your 5 source systems (say Snowflake) is unavailable during a scheduled run. Does the whole pipeline fail, or does it gracefully skip and backfill later? Walk through the actual dependency/orchestration design.
- 10M+ records/day at 99.9% availability — what counts as the 0.1% downtime, and what's your SLA for catching up after an outage?

### 4.4 "Engineered Golden Record and XREF-based identity resolution workflows, improving cross-system data consistency and reducing duplicate/conflicting records"

**L1**
- What is a cross-reference (XREF) table and why is it needed alongside a Golden Record table?
- What causes duplicate records across systems in the first place?

**L2**
- Walk through the XREF table schema/design: how does it map a source-system ID to a Golden Record ID, and how do you handle a record that later turns out to be a duplicate of an existing Golden Record (a "merge" event)?
- Scenario: Two Golden Records are later discovered to actually be the same real-world entity (a "late merge"). How does your XREF design let you collapse them without losing history or breaking downstream reports that already reference the old IDs?
- What specific matching logic did you use to detect duplicates/conflicts — exact key match, fuzzy matching, business-rule-based? Be honest about the level of sophistication (rule-based vs. ML-based) since interviewers will probe this.

### 4.5 "Automated SQL and pipeline configuration generation with Python, eliminating over 50 hours/week of manual DDL and config work and accelerating onboarding"

**L1**
- What was being generated — CREATE TABLE statements, ADF pipeline JSON, Databricks job configs?
- How did you calculate the 50 hours/week figure?

**L2**
- Walk through the actual generator: input (a schema definition file? a metadata table?) → Python logic → output (DDL scripts, pipeline configs). What templating approach did you use (Jinja-style string templates, a config-driven class, etc.)?
- Scenario: A new pharma data source has a nested/complex schema (arrays, structs) that your DDL generator wasn't built for. How would you extend it without a full rewrite?
- Before this automation, who was doing the manual work, and what was error-prone about it that your tool specifically fixed?

### 4.6 "Built PySpark-based ETL pipelines supporting full and incremental data loads, enabling reliable and timely delivery of critical pharmaceutical datasets"

**L1**
- When would you choose a full load over incremental, even though incremental is more efficient?
- What's your MERGE logic look like for incremental upserts into Iceberg?

**L2**
- Walk through an Iceberg MERGE INTO statement you've written — how do you handle updates, inserts, and (if applicable) deletes in the same operation?
- Scenario: A full reload is triggered accidentally on a table that's normally incremental-only. What safeguards (if any) existed to prevent this, and what would the blast radius be if it happened?

### 4.7 "Engineered dual-target data migration pipelines delivering data simultaneously to Iceberg and Snowflake, ensuring data consistency across platforms"

**L1**
- Why deliver to two different targets (Iceberg on S3 and Snowflake) instead of one?
- Which target was the "source of truth" if they ever disagreed?

**L2**
- Walk through the dual-write architecture: was this two separate write paths from the same Spark job, or one canonical write with a downstream sync/replication to the second target? What are the trade-offs of each approach (dual-write consistency risk vs. added replication latency)?
- Scenario: A write to Iceberg succeeds but the corresponding write to Snowflake fails (or vice versa). How do you detect this drift, and what's the reconciliation/retry process?
- How did you validate "consistency across platforms" — row counts, checksums, a scheduled reconciliation job?

---

## 5. Cross-Cutting / Behavioral Questions Likely to Combine Both Projects

**L1**
- Compare Delta Lake and Iceberg from real hands-on experience, not textbook definitions.
- Which project had harder data quality challenges — SAP financial data or pharmaceutical clinical data — and why?
- What's the hardest bug you've personally debugged in a PySpark pipeline?

**L2**
- You've worked AWS S3/Iceberg on one project and Azure ADLS/Delta on another — if a company asked you to migrate a workload from one stack to the other, what would NOT translate directly and need re-architecture?
- Across both projects, what's your general framework for deciding "is this bug in the data, the pipeline logic, or the source system"?
- Tell me about a time your first fix for a data issue was wrong, and how you found the actual root cause.
- If you had unlimited time to go back and redesign one part of either pipeline, what would you change and why?

---

## 6. Preparation Notes

- For every "L2 walk through X in detail" question, prepare the answer in the **what → why → what breaks without it** format before the interview — don't improvise the trade-off reasoning live.
- Be ready to be honest about scope: if a bullet describes team-level work, be clear about your individual contribution vs. the team's.
- The strongest, most specific, most probable deep-dive target on this resume is the **4-hour → 58-minute partition tuning story** (eCDP) and the **40% query latency / Z-Ordering story** (FinOps) — both have hard numbers, so interviewers will push hardest here. Prepare these two in the most depth.
