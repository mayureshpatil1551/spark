# Deloitte – Python Data Engineer Interview Q&A
**Role:** Python Data Engineer | Pune  
**Based strictly on:** Mayuresh Patil's actual resume & project experience  
**Prepared:** June 2026

> ⚠️ Every answer below is based ONLY on what you actually did.  
> Do NOT add anything outside this file in the interview.

---

## SECTION 1 — Python Core: OOPs, Exception Handling, Parallel Processing

---

### Q1. Can you explain OOPs and how you apply it in data engineering?

**Answer:**  
I apply OOPs principles in my pipeline code through modular, class-based design — though I'll be honest that my focus has been on practical modularity rather than strict abstract class hierarchies.

In eCDP, I structured my code into separate classes for each responsibility:
- A **configuration class** that loads pipeline parameters, source credentials, and environment settings in one place — callers never access raw config directly
- **Source-specific connector classes** for Oracle JDBC, Salesforce API, and Smartsheet API — each encapsulates its own connection logic, pagination, and error handling
- **Transformation modules** as separate Python scripts with single responsibilities — one for Golden Record resolution using XREFs, one for raw-to-trusted cleansing

This separation means when the Salesforce API changed its pagination behaviour, I updated only the Salesforce connector class — zero impact on Oracle or Smartsheet pipelines. That's the practical value of encapsulation in data engineering.

---

### Q2. How do you handle exceptions and errors in a production data pipeline?

**Answer:**  
My error handling in production works at two levels — pipeline level and record level.

**Pipeline-level — Modak Nabu:**  
In eCDP, I configured Modak Nabu validation rules and retry workflows to handle data quality failures automatically. When a validation rule fails — say, a null in a mandatory clinical field — Nabu retries the workflow rather than failing the entire pipeline. This configuration reduced data quality errors by 25% and prevented 15+ hours of downtime in reporting windows.

**Record-level — PySpark bad record handling:**  
For file ingestion (CSV, XML from S3), I use PySpark's `badRecordsPath` option to route malformed records to a quarantine location rather than crashing the job. Bad records are logged with metadata so the data team can investigate and reprocess.

**API calls:**  
For Salesforce and Smartsheet API calls, I wrap requests in try-except blocks catching HTTP errors, timeout errors, and connection errors separately. Each has a different recovery action — timeouts retry, 401s trigger a token refresh, 500s alert and halt.

This layered approach means a single bad record or transient API error never takes down a pipeline processing 10M+ records per day.

---

### Q3. How do you handle parallel processing for large datasets?

**Answer:**  
My primary parallelism tool is PySpark on Azure Databricks — I rely on Spark's distributed execution model rather than Python-level threading.

Specifically, what I've done in production:

**Partition tuning:**  
For our 10M+ records/day pipeline in eCDP, I tuned `spark.sql.shuffle.partitions` based on data volume. The default 200 was too high for smaller daily increments and too low for full loads — I set it dynamically based on input size, which contributed to a 30% reduction in pipeline execution time.

**Broadcast joins:**  
When joining large fact tables against smaller lookup tables — for example, joining clinical records against a reference metadata table — I explicitly applied broadcast hints so Spark sends the small table to each executor rather than doing an expensive shuffle join. This was a key part of the 40% query speed improvement I achieved on 300+ analytical queries.

**Partitioned JDBC reads:**  
For Oracle JDBC extraction, I used PySpark's `numPartitions`, `lowerBound`, `upperBound`, and `partitionColumn` to parallelise the read across multiple executors — dramatically faster than a single-threaded JDBC read.

The combination of these three techniques is how I handle data at scale — Spark's distributed model does the heavy lifting.

---

### Q4. Explain Python's GIL and how it affects your work.

**Answer:**  
The GIL — Global Interpreter Lock — is a mutex in CPython that allows only one thread to execute Python bytecode at a time. This means multi-threading in Python does not achieve true CPU parallelism for compute-heavy tasks.

In my day-to-day work, this is why I rely on PySpark rather than Python threads for large-scale data processing:
- PySpark runs on the JVM — the GIL doesn't apply to Spark's distributed execution
- Python in Spark is used mainly for driver-side logic — the actual data processing happens in JVM executors
- For any CPU-heavy transformation, I use PySpark DataFrame operations (built-in functions) rather than Python UDFs, because built-ins execute in the JVM and avoid Python serialization entirely

The GIL is also why I'm careful about UDFs in PySpark — they bring data back to Python, which is both slower and GIL-constrained. I always prefer native Spark functions where possible.

---

### Q5. How do you approach testing and validation of data pipelines?

**Answer:**  
I'll be straightforward — formal pytest-based unit testing is an area I'm actively developing. My current validation approach is production-proven but operates at the pipeline and data level rather than the unit test level.

**What I currently do:**

**Modak Nabu validation rules:**  
In eCDP I configured Nabu rules to validate schema, null constraints, referential integrity, and value ranges at every pipeline stage. These rules fire automatically on every run and flag violations before bad data reaches downstream consumers.

**Row count and schema audits:**  
I built Python scripts that run post-load checks — comparing source row counts against target counts, validating schema consistency, and logging results to a control table. Any discrepancy triggers an alert.

**End-to-end pipeline validation:**  
For the FinOps PoC, I validated the full SAP XML flattening by comparing 29 output Hive tables against expected record counts and spot-checking key financial hierarchies manually.

**Going forward:**  
I'm building pytest skills and plan to add unit tests for transformation functions — mocking source data with small sample DataFrames and asserting output schema and values. I want to be transparent about where I am on this.

---

## SECTION 2 — Database Connectivity & Data Loading

---

### Q6. How do you connect Python to databases and handle large data loads?

**Answer:**  
In eCDP I worked with Oracle as the primary relational source, using PySpark's JDBC connector for extraction.

**Oracle JDBC setup:**
```python
df = spark.read.format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", "CLINICAL_RECORDS") \
    .option("user", dbutils.secrets.get("scope", "oracle-user")) \
    .option("password", dbutils.secrets.get("scope", "oracle-pass")) \
    .option("numPartitions", 8) \
    .option("partitionColumn", "LOAD_DATE") \
    .option("lowerBound", "2020-01-01") \
    .option("upperBound", "2024-12-31") \
    .load()
```

Key practices I follow:
- **Credentials always from Azure Key Vault** via `dbutils.secrets.get()` — never hardcoded
- **Partitioned reads** — `numPartitions` with a date or ID column so multiple executors read in parallel
- **Predicate pushdown** — filters applied at JDBC level so Oracle returns only needed rows, not full table scans
- **Incremental loads** — watermark-based extraction so we only pull records changed since the last successful run, not the full table every time

For the outbound direction, I also built sync workflows pushing processed data back to Oracle from the lakehouse — zero manual intervention, fully automated.

---

### Q7. How do you handle large-scale data transformations?

**Answer:**  
All my large-scale transformation work is in PySpark using native DataFrame operations — never pandas for anything above a few MB.

Real examples from eCDP:

**Golden Record resolution:**  
I built an XREF-based identity resolution system that unifies records from Oracle, Salesforce, and Smartsheet into a single Golden Record per entity. This involves multi-source joins, deduplication logic, and confidence scoring — all in PySpark using window functions and aggregations.

**Raw to Trusted cleansing:**  
In the Medallion pipeline, the Raw-to-Trusted stage handles: null replacement with domain defaults, date standardisation across source formats, string normalisation for clinical codes, and deduplication using `row_number()` window functions partitioned by entity key.

**SAP XML flattening (FinOps PoC):**  
I parsed deeply nested SAP financial XML and flattened it into 29 relational Hive tables using PySpark's XML reader and custom Python parsing logic. This was the hardest transformation I've done — SAP hierarchies are deeply recursive and irregular.

I always use built-in PySpark functions over UDFs. Built-ins run in the JVM, are Catalyst-optimised, and are significantly faster than Python UDFs.

---

### Q8. How do you handle batch pipelines processing large files — say 1–10 GB?

**Answer:**  
In eCDP I regularly process files in this range — daily clinical data loads and full historical migrations up to 20TB total.

My approach for large batch files:

**Columnar format conversion:**  
Raw files arrive as CSV or XML from S3. First thing I do is convert them to Iceberg Parquet format in the Raw layer. Parquet's columnar storage and compression means a 5 GB CSV becomes roughly 1–1.5 GB on disk and is 4–5x faster to read in downstream jobs.

**Schema enforcement upfront:**  
I define explicit `StructType` schemas for all file reads — no schema inference. Inference requires Spark to scan the entire file first, which doubles I/O. With an explicit schema, Spark reads once.

**Partition strategy:**  
For Iceberg tables, I partition on date columns — `LOAD_DATE` or `STUDY_DATE`. This means queries filtering by date skip entire partitions, dramatically reducing scan volume.

**Incremental vs full load:**  
I built both strategies — full loads for initial migration, incremental (watermark-based) for daily runs. Incremental loads process only changed records, so a 10 GB historical table might only need 50 MB processed on a given day.

**Checkpointing:**  
For multi-stage pipelines, I write intermediate results to Iceberg tables between stages. A failure in stage 4 re-runs from stage 3's checkpoint — not from the original 10 GB source file.

---

## SECTION 3 — REST APIs & Service Integration

---

### Q9. How do you consume REST APIs in data pipelines?

**Answer:**  
In eCDP I integrated two REST APIs in production — Salesforce and Smartsheet.

**Salesforce API:**  
I used the Salesforce REST API to pull CRM records into the ingestion layer. Key things I handled:
- OAuth2 authentication — client credentials flow, token stored in Azure Key Vault, refreshed before expiry
- Bulk API for large record sets vs standard REST for smaller queries
- Pagination — Salesforce uses cursor-based pagination via `nextRecordsUrl`; I loop until this field is null
- Rate limits — Salesforce enforces daily API call limits; I batch requests and monitor usage to avoid hitting limits mid-pipeline

**Smartsheet API:**  
Used for pulling project metadata and clinical study tracking data. Smartsheet uses a simpler token-based auth. I handled row-level updates — both reading sheet data and writing status updates back to Smartsheet from pipeline outputs.

**Common pattern across both:**
```python
import requests

def fetch_with_retry(url, headers, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.Timeout:
            if attempt == max_retries - 1:
                raise
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                headers = refresh_token()  # token refresh
            else:
                raise
```

---

### Q10. How do you handle authentication between multiple systems?

**Answer:**  
Security is non-negotiable in pharma. My standard pattern across all systems in eCDP:

**Azure Key Vault + Databricks Secrets:**  
All credentials — Oracle passwords, Salesforce client secrets, Smartsheet tokens, S3 access keys — are stored in Azure Key Vault and accessed at runtime via `dbutils.secrets.get(scope, key)`. Zero credentials in code, notebooks, or config files.

**Salesforce OAuth2:**  
Client credentials flow. I fetch a short-lived access token at pipeline startup using client ID and secret from Key Vault. Token is passed as Bearer header on all subsequent API calls. Before each pipeline run I check token expiry and refresh proactively.

**S3 access:**  
Using Boto3 with IAM role-based access — no static access keys. The Databricks cluster has an IAM role attached with least-privilege S3 permissions scoped to specific buckets.

**Oracle JDBC:**  
Username and password retrieved from Key Vault at job start, passed to the JDBC connection string at runtime. Connection pooled for the duration of the job.

This pattern means if a secret is rotated, only the Key Vault entry changes — no code changes, no redeployment needed.

---

## SECTION 4 — Architecture & Design

---

### Q11. How do you design a scalable data pipeline architecture?

**Answer:**  
In eCDP I implemented Medallion Architecture end to end — Raw, Trusted, and Curated layers — on Apache Iceberg on S3.

**Raw layer:**  
Data lands exactly as received from source — CSV, XML, JSON, API responses. No transformation. Every record gets metadata columns: `ingestion_timestamp`, `source_system`, `file_name`, `batch_id`. Nothing is ever deleted from Raw — it's the permanent audit trail. This matters enormously in pharma where regulatory audits require proof of what data arrived and when.

**Trusted layer:**  
Schema enforcement, null handling, deduplication, date standardisation, referential integrity checks. Data here is reliable and queryable. Golden Record resolution — XREF-based unification — happens at this layer.

**Curated layer:**  
Business-level aggregated views, joined datasets, KPI tables. Optimised for consumption by BI tools and downstream MDM workflows. Partitioned for query performance.

**Design principles I follow:**
- **Idempotency** — Every pipeline run produces the same result on retry. I use Iceberg MERGE (upsert) operations, never blind appends
- **Modularity** — Each layer is an independent job. A failure in Trusted doesn't re-run Raw
- **Configuration-driven** — Source mappings, transformation rules, load strategies are in metadata config, not hardcoded. Adding a new source means adding a config entry, not writing new pipeline code
- **Auditability** — Iceberg's time-travel lets me query any table as it existed at any point in time — critical for regulatory compliance

---

### Q12. How do you use Git in a team data engineering context?

**Answer:**  
Git is standard practice in my team. My workflow:

- **Branching** — `main` for production, `develop` for integration, `feature/<ticket-id>` for individual work items. No direct commits to main.
- **Pull Requests** — Every change reviewed before merge. I include what changed, why, and any deployment or config steps needed.
- **Databricks Git integration** — I use Databricks Repos to sync notebooks with our Git repository. Notebooks are version-controlled alongside Python modules — no untracked notebook changes floating in the workspace.
- **CI/CD** — Pipeline deployment is automated. Code merged to develop triggers a CI run that deploys to staging. Merge to main deploys to production. No manual file uploads.
- **`.gitignore`** — Secrets, local env files, `.pyc` files, and Databricks auto-generated files are always excluded.

This discipline meant that in eCDP, every production change was traceable, reviewable, and reversible.

---

### Q13. Tell me about Apache Iceberg — why did you choose it and what benefits did you get?

**Answer:**  
In eCDP we chose Apache Iceberg over plain Delta Lake for the S3-based lakehouse because of three specific requirements:

**1. ACID transactions:**  
Clinical trial data gets updated, corrected, and appended from multiple sources concurrently. Without ACID guarantees, concurrent writes would corrupt data. Iceberg's optimistic concurrency control handles this safely.

**2. Time-travel for regulatory audit trails:**  
Pharma is heavily regulated — auditors need to see exactly what data looked like at a specific point in time. Iceberg's snapshot-based time-travel lets me query:
```sql
SELECT * FROM clinical_records
FOR SYSTEM_TIME AS OF '2024-06-01 00:00:00'
```
This is not possible with plain Parquet files.

**3. Schema evolution without rewrites:**  
Clinical data schemas change as studies evolve — new columns get added, types get refined. Iceberg handles schema evolution without rewriting existing data files, unlike Hive which requires full table rewrites.

The result — 20TB+ of pharmaceutical data with full ACID compliance, time-travel capability, and zero manual partition management.

---

## SECTION 5 — Honest Gap Answers

---

### Q14. Do you have experience with event-driven programming in Python?

**Answer:**  
My pipeline triggers are currently schedule-based via ADF rather than event-driven. I understand the concept well — producers emit events, consumers react independently, typically through a message queue like Azure Service Bus or Event Hubs.

In practice, my eCDP pipelines use ADF triggers on a schedule and file-arrival triggers on S3 via ADF's storage event trigger — which is a lightweight form of event-driven design. A new file landing in S3 triggers the ingestion pipeline automatically without polling.

Fully event-driven architectures with Python `asyncio` or message queues is an area I'm actively developing beyond my current project scope.

---

### Q15. How familiar are you with unit testing in Python?

**Answer:**  
I'll be transparent — production `pytest` with mocking is something I'm actively building. My current validation is at the pipeline and data quality level through Modak Nabu rules and post-load audits.

I understand the concepts — isolating functions, mocking external dependencies, fixture-based test data, asserting on schema and values. I've written small test scripts for transformation functions but not a full structured test suite.

This is a skill gap I'm consciously addressing and it won't hold me back from delivering — I've maintained 99.9% data uptime in production without it, through strong data quality validation at the pipeline level.

---

## SECTION 6 — Consulting & Delivery

---

### Q16. Tell me about a time you automated routine development work.

**Answer:**  
In eCDP, setting up pipeline infrastructure for a new data source required manually writing SQL config files — table definitions, column mappings, load strategy flags. It was taking 15+ hours per week across the team.

I built a Python script that auto-generates these SQL config files from a simple input template. An engineer fills in source table name, target table, column list, and load type — the script generates the full config. This eliminated 15+ hours per week of manual setup.

The broader principle is configuration-driven pipeline design. Instead of writing new code per data source, I built a generic ingestion framework that reads source-to-target mappings from a metadata table. Adding a new data source means adding a config row — not writing a new pipeline. This is how we onboarded Oracle, Salesforce, and Smartsheet into the same framework.

---

### Q17. Tell me about a PoC you delivered and the business impact.

**Answer:**  
The FinOps Dataverse PoC is the clearest example.

The ask was to prove we could migrate complex SAP financial hierarchy data into a modern data platform. The original estimate was 8 weeks.

I delivered it in 2.5 weeks — 60% faster than estimated.

What made it complex: SAP financial hierarchies are deeply nested XML with irregular structure. I had to parse the XML, understand the hierarchy logic, and flatten it into 29 relational Hive tables that made business sense to analysts.

I chose Python + PySpark for the parsing — PySpark's XML reader for structure, custom Python logic for hierarchy traversal, and Spark for the parallel flattening into Hive.

The output went simultaneously into Delta Lake on Databricks and Snowflake — dual-target to give the client flexibility on their analytics platform choice.

The PoC sign-off secured $5,000 in immediate revenue for the business unit and opened the door to a larger engagement.

---

### Q18. How do you handle data governance and compliance in a regulated environment?

**Answer:**  
Pharma is one of the most regulated data environments — 21 CFR Part 11 adjacent requirements, audit trails, access controls, and data lineage are non-negotiable.

In eCDP I addressed governance through:

**Unity Catalog:**  
Centralised access control for all Databricks assets — table-level and column-level permissions. Sensitive clinical fields are restricted to authorised roles only. Data lineage is tracked automatically — you can see exactly which pipeline wrote which table and when.

**Iceberg time-travel:**  
Every table version is preserved. Auditors can query historical states without needing separate backup infrastructure.

**Raw layer immutability:**  
Raw layer data is never modified — only appended. Source data is always recoverable in its original form.

**Secrets management:**  
Zero credentials in code. All secrets in Azure Key Vault, accessed at runtime. Rotation handled at Key Vault level without code changes.

**Audit logging:**  
Pipeline runs log row counts, timestamps, source identifiers, and data quality results to a control table. Every data movement is traceable end-to-end.

---

## SECTION 7 — AI & Data Platform

---

### Q19. How have you modernised a large-scale data platform?

**Answer:**  
eCDP is exactly this — a legacy pharmaceutical data platform modernisation for a major pharma client.

The starting point: fragmented data across Oracle databases, flat files, Salesforce CRM, and Smartsheet trackers — no unified platform, no lineage, no audit trail, manual processes everywhere.

What I built or contributed to:

- **Unified ingestion layer** — Single framework ingesting from Oracle JDBC, Salesforce REST API, Smartsheet API, and S3 flat files (CSV, XML) into a Bronze/Raw Iceberg layer
- **Medallion architecture** — Raw → Trusted → Curated, replacing ad-hoc SQL scripts with structured, governed pipeline stages
- **Golden Record system** — XREF-based identity resolution unifying the same entity across multiple source systems into one master record for MDM workflows
- **Apache Iceberg lakehouse** — 20TB+ migrated from legacy storage to Iceberg on S3 with ACID transactions and time-travel
- **Automated outbound sync** — Processed data flows back to Oracle and CSV flat files automatically — zero manual exports
- **Governance** — Unity Catalog for access control and lineage across all assets

The result: 10M+ records/day processed with 99.9% uptime, 40% improvement in analytical query speed, and a fully auditable, compliant data platform.

---

### Q20. How do you use automation to manage a data ecosystem?

**Answer:**  
Automation is something I've applied at multiple levels in eCDP:

**Pipeline automation via ADF:**  
All pipeline scheduling, dependency management, and retry logic is in Azure Data Factory. No manual triggering of jobs. ADF handles sequencing — ingestion completes before transformation starts, transformation completes before curated layer refresh.

**Modak Nabu for data quality automation:**  
Validation rules fire automatically on every pipeline run. Failures trigger retry workflows without human intervention. I configured 25% reduction in data quality errors through this automated validation layer.

**SQL config auto-generation:**  
Python scripts generate pipeline infrastructure config automatically from templates — eliminating 15+ hours/week of manual setup.

**Metadata-driven ingestion:**  
The ingestion framework reads source configs from a metadata table and dynamically builds the pipeline. New sources are onboarded through config, not code.

**Monitoring:**  
Pipeline run metrics — row counts, durations, error rates — are logged to a control table and monitored. Anomalies are visible immediately.

---

## SECTION 8 — Behavioral Questions

---

### Q21. Tell me about your biggest technical achievement.

**Answer:**  
The Golden Record identity resolution system in eCDP.

The problem: clinical trial data for the same patient or study entity existed in three separate systems — Oracle, Salesforce, and Smartsheet — with different identifiers, different formats, and no way to link them reliably. Downstream MDM workflows were getting duplicate and conflicting records.

I built an XREF-based resolution system in PySpark. XREFs are cross-reference tables that map entity IDs across systems. The pipeline:
1. Pulls entity records from all three sources
2. Resolves cross-references using XREF lookup tables
3. Applies survivorship rules to select the authoritative value per attribute when sources conflict
4. Produces a single Golden Record per entity for downstream consumption

The outcome was a unified master data layer that eliminated duplicate records from MDM workflows and became the single source of truth for downstream analytical and operational systems.

This is the piece of work I'm most proud of because it required understanding both the technical architecture and the clinical data domain deeply.

---

### Q22. Tell me about a time you delivered under pressure.

**Answer:**  
The FinOps PoC had an 8-week estimate. My manager told me the client was losing confidence in the timeline and needed a result fast.

I assessed what was actually needed for PoC sign-off — not a production-grade solution, but a working demonstration of SAP hierarchy parsing and dual-target migration. I scoped to exactly that.

I worked through the SAP XML structure, built the PySpark parsing logic, handled the irregular hierarchy recursion, and delivered 29 Hive tables with correct financial relationships — in 2.5 weeks.

Three things made this possible:
1. Ruthless scope discipline — I delivered what sign-off needed, not everything imaginable
2. PySpark for parallelism — the XML parsing that would have taken days in Python alone ran in hours distributed across the cluster
3. Daily check-ins with my manager so blockers were resolved in hours not days

The PoC secured $5,000 in immediate revenue and opened a larger engagement conversation.

---

### Q23. Why Deloitte and why this role?

**Answer:**  
My entire career so far has been on one client — AbbVie — going deep on one platform. I've built production-grade systems, handled 20TB migrations, and maintained 99.9% uptime in a regulated environment. That depth has been valuable.

But I'm ready for breadth. Deloitte's AI & Engineering practice means working across industries, client challenges, and data platform patterns that one long-term engagement can't give you.

The technical requirements of this role — Python, PySpark, large-scale batch pipelines, REST API integration, cloud data platforms — match exactly what I do today. I won't need a ramp-up period on the core stack.

What I'm excited to gain at Deloitte is exposure to varied client problems, consulting delivery disciplines, and the kind of scale that comes from a global practice. That combination — my technical foundation plus Deloitte's platform — is where I believe I can grow fastest toward a Senior or Lead Data Engineer role.

---

## QUICK REFERENCE — Your Real Numbers

| Metric | Value | Where to use it |
|---|---|---|
| Data migrated | 20TB+ to Apache Iceberg | Any architecture or scale question |
| Daily throughput | 10M+ records/day | Pipeline scale questions |
| Data uptime | 99.9% | Reliability / production experience |
| Query speed improvement | 40% (300+ queries) | Spark optimisation questions |
| Pipeline execution reduction | 30% | Performance tuning questions |
| Data quality errors reduced | 25% | Data quality / Modak Nabu questions |
| Downtime prevented | 15+ hours in reporting windows | Business impact questions |
| Manual setup eliminated | 15+ hours/week | Automation questions |
| FinOps PoC delivery | 2.5 weeks vs 8-week estimate (60% faster) | Delivery under pressure questions |
| Revenue secured | $5,000 from PoC sign-off | Business impact questions |
| Certifications | Databricks Associate, DP-900, AWS CLF-C02 | Credentials questions |

---

## WHAT TO SAY IF ASKED ABOUT GAPS

| Gap | Honest Answer |
|---|---|
| **8–10 years experience** | "3 years but production-grade — 20TB migration, 10M records/day, 99.9% uptime in regulated pharma" |
| **Unit testing / pytest** | "Pipeline-level validation via Modak Nabu; unit testing is something I'm actively developing" |
| **Event-driven Python** | "Schedule and file-arrival triggers in ADF today; async event-driven patterns I'm building toward" |
| **PowerShell** | "Not in my current stack — I use ADF and Python for automation" |
| **SQL Server** | "Oracle in my current project; SQL skills transfer directly to SQL Server" |

---

*Prepared June 2026 | Based strictly on Mayuresh Patil resume dated 04 June 2026*
