# Deloitte – Python Data Engineer Interview Q&A
**Role:** Python Data Engineer | Hyderabad / Bengaluru / Pune / Chennai  
**Team:** AI & Engineering (AI & Data)  
**Prepared for:** Mayuresh | June 2026

---

## SECTION 1 — Python Core: OOPs, Exception Handling, Parallel Processing

---

### Q1. Can you explain the four pillars of OOPs and how you apply them in data engineering?

**Answer:**  
The four pillars are Encapsulation, Abstraction, Inheritance, and Polymorphism — and all four come into play in real data engineering work.

- **Encapsulation** — I wrap data and logic inside classes to prevent accidental modification. For example, in my eCDP project I built a `PipelineConfig` class that loads all Databricks secrets and environment parameters internally. Callers just instantiate the class; they never touch the raw secrets directly.
- **Abstraction** — I expose only what's needed. A base `DataIngestion` class defines abstract methods like `extract()`, `transform()`, `load()`. Each connector — Oracle, Salesforce, Smartsheet — inherits and implements only its specific logic.
- **Inheritance** — I avoid code duplication by having source-specific classes inherit common retry logic, logging, and schema validation from a base class.
- **Polymorphism** — A pipeline orchestrator can call `ingestion_job.run()` regardless of whether the underlying object is an `OracleIngestion` or `SalesforceIngestion`. The correct `run()` is dispatched at runtime.

In short, OOPs makes my pipelines maintainable, testable, and extensible — especially important when onboarding new data sources without touching existing code.

---

### Q2. How do you handle exceptions in a production data pipeline?

**Answer:**  
I follow a layered exception handling strategy:

1. **Specific before generic** — I always catch specific exceptions first. For example, `pyodbc.OperationalError` for DB connectivity failures, `requests.Timeout` for API calls, before falling back to a broad `Exception`.
2. **Custom exceptions** — I define custom exception classes like `SchemaValidationError` or `PartitionLoadError` so that downstream systems get meaningful error context, not a generic traceback.
3. **Retry with backoff** — For transient failures like network timeouts or rate limits, I implement exponential backoff using `tenacity`. I set max retries at 3 with a multiplier of 2 seconds.
4. **Dead Letter Queue / Fallback table** — Failed records are not dropped; they get written to a quarantine Delta table with the error reason, timestamp, and source identifier so data teams can investigate and reprocess.
5. **Alerting** — I integrate with Azure Monitor or send alerts via webhook so that pipeline failures are caught within minutes, not discovered the next morning.

In my AbbVie eCDP project, this pattern reduced undetected pipeline failures to near zero across 10+ production pipelines.

---

### Q3. How do you handle parallel processing in Python for large datasets?

**Answer:**  
Depending on the bottleneck — CPU-bound or I/O-bound — I choose the right tool:

- **I/O-bound tasks** (API calls, DB reads, file uploads): I use `concurrent.futures.ThreadPoolExecutor`. For example, when pulling data from multiple REST API endpoints simultaneously, I spawn threads per endpoint, collect futures, and merge results. This reduced my API ingestion time by roughly 60%.
- **CPU-bound tasks** (heavy data transformations, JSON parsing): I use `multiprocessing.Pool` or `ProcessPoolExecutor` to bypass the GIL and leverage all CPU cores.
- **Large-scale distributed processing**: I use PySpark on Azure Databricks, which handles parallelism at the cluster level — partitioning data across executors, which is far more efficient than Python-level parallelism for 1–10 GB+ batch files.

For a 5 GB clinical trial file in eCDP, PySpark processed it in under 8 minutes across a 4-node cluster vs. 45+ minutes in single-threaded Python.

---

### Q4. Explain Python's GIL and when it affects you as a data engineer.

**Answer:**  
The Global Interpreter Lock is a mutex in CPython that allows only one thread to execute Python bytecode at a time. This means multi-threading in Python does **not** achieve true CPU parallelism.

- It **matters** when I have CPU-bound operations — like parsing millions of JSON records or running heavy in-memory transformations. Threading won't help; I use `multiprocessing` instead.
- It **doesn't matter** for I/O-bound operations — threads release the GIL while waiting on network or disk I/O, so `ThreadPoolExecutor` works well for concurrent API calls or DB reads.
- It **doesn't matter** in PySpark — Spark runs on the JVM; Python threads just send instructions to the JVM, so the GIL is not a bottleneck in a distributed context.

Understanding the GIL helps me pick the right concurrency model so I don't waste time debugging performance problems that stem from the wrong tool choice.

---

### Q5. How do you write and structure unit tests for data pipeline code?

**Answer:**  
I use `pytest` as my primary testing framework and follow these principles:

1. **Test one unit at a time** — I isolate functions like `transform_clinical_records()` and test them with small sample DataFrames, not the full production dataset.
2. **Mocking** — I use `unittest.mock.patch` to mock external dependencies like DB connections, API responses, or Databricks secrets. This makes tests fast and independent of infrastructure.
3. **Fixture-based test data** — I define `pytest` fixtures for reusable sample data — a small Spark DataFrame, a mock API response JSON, a sample schema.
4. **Edge case coverage** — I always test: empty input, null values in key columns, schema mismatches, and malformed records.
5. **PySpark unit testing** — I use `pyspark.sql.SparkSession` locally with `master("local[*]")` to test transformations without a live cluster.

Example structure:
```
tests/
  conftest.py        ← shared fixtures
  test_transform.py  ← transformation logic
  test_ingestion.py  ← connector logic with mocked DB
  test_api_client.py ← REST client with mocked responses
```

In my project, unit tests caught 3 schema drift issues before they reached production.

---

## SECTION 2 — Database Connectivity & Data Loading

---

### Q6. How do you connect Python to databases and handle large data loads?

**Answer:**  
For relational databases like Oracle, I use `cx_Oracle` or `SQLAlchemy` with a connection pool. For SQL Server, I use `pyodbc` with a DSN or connection string. Key practices:

- **Connection pooling** — I never open a new connection per record. I use `SQLAlchemy`'s `create_engine` with `pool_size` and `max_overflow` configured to reuse connections efficiently.
- **Chunked reads** — For large tables, I use `pandas.read_sql` with `chunksize` or PySpark's JDBC reader with `numPartitions`, `lowerBound`, `upperBound`, and `partitionColumn` to parallelize reads across executors.
- **Parameterised queries** — I never use string interpolation for SQL. All user-supplied or dynamic values go through parameterised queries to prevent SQL injection.
- **Transaction management** — For writes, I wrap bulk inserts in explicit transactions and roll back on failure to maintain consistency.

For a 3 GB Oracle extract in eCDP, using PySpark JDBC with 8 partitions on the `LOAD_DATE` column reduced extraction time from 40 minutes to under 6 minutes.

---

### Q7. How do you handle data transformations and calculations on large datasets?

**Answer:**  
At scale, I rely primarily on PySpark for transformations:

- **Column-level transformations** — I use PySpark built-in functions (`pyspark.sql.functions`) over UDFs wherever possible, since built-ins run in the JVM and avoid Python serialization overhead.
- **Window functions** — For running totals, rank-based deduplication, or lag/lead calculations across partitions, I use `Window.partitionBy().orderBy()`.
- **Aggregations** — `groupBy().agg()` with multiple aggregate functions in a single pass to avoid multiple shuffles.
- **Broadcast joins** — When joining a large fact table with a small lookup/dimension table, I broadcast the smaller table to each executor to avoid an expensive shuffle join.
- **Incremental processing** — Instead of reprocessing all data, I filter by `watermark` or `last_modified_date` so only changed records go through the transformation layer.

For small to medium datasets in Python alone, I use `pandas` with vectorized operations and avoid `iterrows()` which is extremely slow.

---

### Q8. How do you handle batch data pipelines processing 1–10 GB files?

**Answer:**  
A 1–10 GB file sits in a range where both optimized Python and distributed PySpark can work, but my default for anything over 500 MB is PySpark on Databricks:

1. **Partitioned reads** — I split the file read across partitions. For CSV or Parquet, Spark handles this automatically based on block size (128 MB default in HDFS/ADLS).
2. **Schema enforcement** — I define an explicit schema upfront (`StructType`) rather than letting Spark infer it — schema inference requires an extra scan of the data.
3. **Predicate pushdown** — For Parquet and Delta/Iceberg, filters pushed down to file metadata skip entire row groups, dramatically reducing I/O.
4. **Columnar formats** — I convert raw CSV inputs to Parquet or Delta/Iceberg at the staging layer. A 6 GB CSV becomes ~1.2 GB Parquet, which is 5x faster to read in subsequent jobs.
5. **Checkpointing** — For multi-step pipelines, I write intermediate results to Delta as checkpoints so a failure in step 5 doesn't re-run steps 1–4.

In eCDP, this approach reduced pipeline execution time by approximately 35% compared to the previous pandas-based approach.

---

## SECTION 3 — REST APIs & Service Integration

---

### Q9. How do you design and consume RESTful APIs in Python for data pipelines?

**Answer:**  
I use the `requests` library for synchronous calls and `httpx` or `aiohttp` for async. My standard approach:

**Consuming an API:**
```python
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2))
def fetch_data(url, headers, params):
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()  # raises HTTPError for 4xx/5xx
    return response.json()
```

Key practices:
- **Authentication** — OAuth2 Bearer tokens stored in Azure Key Vault, retrieved at runtime via Databricks `dbutils.secrets`. Never hardcoded.
- **Pagination** — I handle cursor-based and offset-based pagination by looping until the `next_page` or `has_more` flag is `False`.
- **Rate limiting** — I check `Retry-After` headers and implement backoff. For bulk extracts, I use batch endpoints where available.
- **Response validation** — I validate the response schema against an expected structure before writing to the data lake.

In eCDP I integrated Salesforce REST API and Smartsheet API this way — pulling clinical trial metadata and status updates into the pipeline.

---

### Q10. How do you handle authentication between multiple systems in a pipeline?

**Answer:**  
My standard pattern across systems:

- **Azure services (ADLS, ADF, Databricks)** — I use Managed Identity or Service Principal with secrets stored in Azure Key Vault. No credentials in code or config files.
- **REST APIs (Salesforce, Smartsheet)** — OAuth2 client credentials flow. I fetch a short-lived access token at pipeline startup and refresh it before expiry using a token manager class.
- **Databases (Oracle, SQL Server)** — Connection strings stored in Azure Key Vault. Retrieved via `dbutils.secrets.get()` in Databricks notebooks.
- **Cross-service token passing** — When a Python service calls a downstream API, I use a service account token scoped to minimum required permissions (principle of least privilege).
- **Secret rotation** — I never hardcode secret expiry dates. I rely on Key Vault's rotation policies and test pipelines with rotated credentials in a staging environment before production rollout.

This pattern means no credentials exist in source code, notebooks, or logs — which is critical for pharma clients like AbbVie with strict compliance requirements.

---

## SECTION 4 — Data Pipeline Architecture & Design

---

### Q11. How do you design a scalable data pipeline architecture?

**Answer:**  
I follow the **Medallion Architecture** — Bronze, Silver, Gold — which aligns well with Deloitte's analytics platform work:

- **Bronze (Raw layer)** — Data lands as-is from source (CSV, JSON, Parquet, API response). No transformation, just ingestion with metadata columns: `ingestion_timestamp`, `source_system`, `file_name`.
- **Silver (Cleansed layer)** — Schema enforcement, deduplication, null handling, standardisation (date formats, casing). Data is queryable and reliable.
- **Gold (Curated layer)** — Business-level aggregations, KPIs, joined datasets. Optimised for analytical consumption by BI tools or ML models.

Design principles I apply:
- **Idempotency** — Every pipeline run produces the same result regardless of retries. I use MERGE (upsert) operations on Delta/Iceberg tables, not blind appends.
- **Modularity** — Each stage is an independent job. Failures are contained; re-runs start from the last successful stage.
- **Observability** — I log row counts, schema, null rates, and pipeline duration at each stage. Anomalies trigger alerts.
- **Late-arriving data** — I use watermarks and allow a configurable late-arrival window so records that arrive delayed still get processed correctly.

---

### Q12. How do you use Git in a team data engineering context?

**Answer:**  
Git is central to how I work. My standard practices:

- **Branching strategy** — `main` (production), `develop` (integration), `feature/<ticket-id>-description` (development). No direct commits to `main`.
- **Pull Requests** — Every change goes through a PR with at least one reviewer. I include a description of what changed, why, and any deployment steps.
- **Commit discipline** — Small, atomic commits with meaningful messages: `feat: add Oracle ingestion retry logic` not `fixed stuff`.
- **CI/CD integration** — PRs trigger automated unit test runs and linting (flake8/black) via Azure DevOps pipelines. A failing test blocks the merge.
- **Databricks integration** — In Databricks, I use Git folders (Repos) to sync notebooks with the remote repository, ensuring notebooks are version-controlled alongside Python modules.
- **`.gitignore`** — Secrets, `.env` files, compiled files, and local config are always excluded.

---

### Q13. How do you design for concurrency and error handling in a data pipeline handling 1–10 GB batches?

**Answer:**  
For a 1–10 GB batch, my concurrency and fault-tolerance design has three layers:

**Layer 1 — Partition-level parallelism (Spark)**  
The batch is split into partitions (~128 MB each). Spark tasks run concurrently across executors. I set `spark.sql.shuffle.partitions` based on data size — typically 200 for 5 GB, 400 for 10 GB.

**Layer 2 — Stage-level checkpointing**  
After each major stage (raw → cleansed, cleansed → curated), I write results to Delta. If a downstream stage fails, the pipeline re-runs from the last checkpoint — not from source.

**Layer 3 — Record-level error handling**  
I use PySpark's `.option("badRecordsPath", "/mnt/quarantine/")` or a custom filter-and-separate pattern to route malformed records to a quarantine table rather than failing the entire batch.

Combined, this means a failure in processing record 4,000,001 doesn't discard the 4,000,000 records already processed successfully.

---

## SECTION 5 — Good-to-Have Skills

---

### Q14. What do you know about event-driven programming in Python?

**Answer:**  
Event-driven programming decouples producers and consumers — a producer emits an event, and one or more consumers react independently. In Python, this can be implemented several ways:

- **Observer pattern** — A simple `EventBus` class where handlers register for event types and get invoked when the event fires. Useful for in-process pipelines.
- **Async event loops** — `asyncio` event loop with `asyncio.Queue` for lightweight async producer-consumer patterns.
- **Message queues** — For distributed pipelines, I use Azure Service Bus or Azure Event Hubs. A pipeline stage publishes a message when it completes; the next stage triggers on message receipt. This is production-grade event-driven architecture.

In a data engineering context, event-driven design is particularly powerful for real-time or near-real-time ingestion — for example, triggering a transformation pipeline the moment new files land in ADLS via an Event Grid notification, rather than polling on a schedule.

---

### Q15. How do you use PowerShell and SQL Server in a data engineering role?

**Answer:**  
**PowerShell:**  
I use PowerShell for infrastructure automation and DevOps tasks — deploying Azure resources, managing service principal permissions, triggering ADF pipelines from scripts, and rotating secrets in Key Vault. For example, `Invoke-AzDataFactoryV2Pipeline` to trigger an ADF pipeline run programmatically.

**SQL Server:**  
I use SQL Server for relational staging layers and metadata databases. Key skills:
- **T-SQL** — Complex joins, CTEs, window functions, stored procedures for transformation logic.
- **Bulk insert / BCP** — For high-speed loading of flat files into SQL Server staging tables.
- **Linked servers / OPENROWSET** — For querying across databases or external sources.
- **Index tuning** — Covering indexes, columnstore indexes for analytics-heavy tables.

In ADF pipelines, I often use SQL Server as the metadata store — tracking pipeline run status, watermarks, and error logs in control tables that ADF reads to decide what to process next.

---

### Q16. How would you approach scalable application design for a data platform?

**Answer:**  
My core design principles for a scalable data platform:

1. **Loose coupling** — Services communicate through well-defined interfaces (APIs, message queues). A change in the ingestion layer doesn't break the transformation layer.
2. **Stateless processing** — Pipeline workers are stateless. All state (watermarks, checkpoints, run history) is stored externally in a metadata database or Delta table.
3. **Horizontal scalability** — I design so that adding compute nodes (Spark workers) increases throughput linearly. No single-threaded bottlenecks in critical paths.
4. **Separation of concerns** — Ingestion, transformation, and serving are separate jobs with separate schedules and failure boundaries.
5. **Configuration-driven design** — Source-to-target mappings, transformation rules, and pipeline parameters are in config files or metadata tables — not hardcoded. Adding a new data source means adding a config row, not writing new code.
6. **Observability first** — Every pipeline emits structured logs, metrics, and row-count audits from day one. You can't optimise what you can't measure.

---

## SECTION 6 — Consulting & Stakeholder Skills

---

### Q17. How do you facilitate discussions with business stakeholders to understand data requirements?

**Answer:**  
In my role at Cognizant supporting AbbVie, I regularly interacted with business analysts, data coordinators, and IT stakeholders on both sides. My approach:

1. **Structured discovery** — I prepare a requirements template covering: source systems, data refresh frequency, volume estimates, key business entities, transformation rules, and downstream consumers.
2. **Active listening with clarifying questions** — When a stakeholder says "we need all the clinical data," I ask: Which study phases? What time range? What's the SLA for freshness? What BI tool will consume this?
3. **Prototyping** — I build a small proof-of-concept with sample data and share it early. Business users understand a working demo far better than a technical spec document.
4. **Documenting decisions** — After every requirements meeting, I send a written summary of decisions made, assumptions, and open items. This creates a paper trail and catches misunderstandings early.
5. **Managing ambiguity** — When requirements are unclear, I propose a reasonable default, flag it explicitly, and ask for confirmation rather than blocking progress.

---

### Q18. How do you design templates or scripts to automate routine development tasks?

**Answer:**  
Automation of repetitive work is something I've prioritised throughout my career. Examples from my experience:

- **Pipeline scaffolding template** — I built a Cookiecutter-style project template for new eCDP pipelines: standard folder structure, pre-configured logging, secrets setup, unit test boilerplate. A new pipeline can be scaffolded in 10 minutes.
- **Metadata-driven ADF pipelines** — Instead of creating one ADF pipeline per data source, I built a generic pipeline that reads source-to-target config from a metadata table and dynamically constructs the copy activity. Adding a new source = adding one config row.
- **Automated data quality reports** — A Python script that runs post-load, checks row counts, null rates, and schema against expectations, and writes a summary to a SharePoint list for stakeholders.
- **Reusable PySpark modules** — I packaged common transformation logic (deduplication, SCD Type 2, schema validation) as Python wheel files deployed to Databricks clusters so every team member uses the same tested code.

---

### Q19. How do you prepare status reports and manage deliverables in a consulting project?

**Answer:**  
In my eCDP engagement at AbbVie, we followed a structured delivery cadence:

- **Daily standups** — Quick sync on blockers, progress, and next steps. I kept updates concise: what's done, what's next, what's blocked.
- **Weekly status reports** — I prepared a one-page status update for Cognizant and AbbVie leadership covering: sprint progress (% complete against plan), risks/issues with mitigations, pipeline health metrics, and upcoming milestones.
- **Milestone tracking** — I maintained a delivery tracker in Smartsheet (which we also integrated into the data platform), updated weekly.
- **Executive-facing communication** — I kept technical language minimal in leadership-facing reports. Instead of "PySpark executor OOM errors," I wrote "pipeline performance issue identified; optimised — expected 20% reduction in run time."

This kind of structured communication builds client trust and keeps delivery on track, which is central to consulting engagements.

---

## SECTION 7 — AI & Data Platform Context

---

### Q20. How have you worked on managing and modernising large-scale data and analytics platforms?

**Answer:**  
In my eCDP project, I was part of a team modernising a legacy clinical data platform for AbbVie. The key transformation work included:

- **Migrating from legacy batch ETL to a lakehouse architecture** — Moving from flat-file-based Oracle extracts to an Apache Iceberg + Delta Lake medallion architecture on Azure Databricks.
- **Integrating structured and unstructured sources** — Oracle relational data, Salesforce CRM records, Smartsheet project metadata, and clinical trial flat files all flowing into a unified Bronze layer.
- **Cloud-native storage** — All data landed in ADLS Gen2, replacing on-premise NAS storage.
- **Governance** — Unity Catalog for centralised access control, data lineage, and column-level security — critical for pharma compliance (HIPAA, 21 CFR Part 11 adjacent requirements).
- **Analytical access** — Gold layer tables served by Databricks SQL warehouses to BI tools and data science teams.

This end-to-end modernisation gave AbbVie's data teams a single reliable platform replacing multiple fragmented systems.

---

### Q21. How do you leverage automation and AI/ML tools to manage data ecosystems?

**Answer:**  
While my primary work is in data engineering rather than model building, I've integrated automation and AI-adjacent tooling in several ways:

- **Automated data quality** — Using statistical profiling (row counts, null rates, value distribution checks) that automatically flags anomalies and pauses pipelines for review.
- **Anomaly detection on pipeline metrics** — Monitoring pipeline run times and record counts over time. A run that produces 50% fewer records than baseline triggers an alert — catching source-side issues automatically.
- **Databricks AutoLoader** — For continuous file ingestion from ADLS, AutoLoader uses file notifications and schema inference to automatically detect new files and schema evolution without manual intervention.
- **ADF dynamic metadata-driven pipelines** — Pipelines that automatically adapt to new data sources without code changes — effectively "configuring" a new pipeline through a metadata entry.
- **Future direction** — I'm actively exploring integrating LLM-based data documentation (auto-generating table descriptions and lineage notes) using Databricks + OpenAI APIs.

---

## SECTION 8 — Behavioral Questions

---

### Q22. Tell me about a time you delivered significant impact on a client project.

**Answer:**  
In my eCDP project at AbbVie, the existing clinical data pipelines had long execution times — the full daily load was taking over 4 hours, causing downstream reports to be stale by mid-morning.

I profiled the bottleneck and found two issues: first, the PySpark jobs were using the default 200 shuffle partitions for a 6–8 GB daily load, which was severely under-partitioned; second, several large joins were not using broadcast hints for smaller dimension tables.

I tuned the partition count to 400, added broadcast hints on three joins, and converted a critical intermediate step from CSV to Parquet format. Combined, pipeline execution time dropped by approximately 35%, bringing the daily load to completion before business hours started.

The client noticed the reports were fresh when they arrived at work. That's the kind of outcome that builds long-term trust in a consulting engagement.

---

### Q23. Tell me about a time you had to manage competing priorities in a fast-paced project.

**Answer:**  
During eCDP, I was simultaneously managing production support for 10 live pipelines and delivering a new FinOps data module for a proof-of-concept. A production pipeline failed mid-sprint due to a schema change on the Oracle source side.

I triaged immediately: production always takes priority, but I didn't want to lose the FinOps deadline either. I fixed the production schema issue within 2 hours by updating the schema inference logic and deploying a hotfix. I then coordinated with my manager to get one day's timeline extension on the FinOps PoC, delivered the PoC 2 weeks ahead of its revised deadline, and set up automated schema drift detection so the same Oracle issue would be caught proactively in future.

The ability to triage, communicate clearly, and stay calm under pressure is something I've developed specifically through consulting environments where multiple stakeholders depend on you simultaneously.

---

### Q24. Why Deloitte and why this role specifically?

**Answer:**  
Deloitte's AI & Engineering practice sits at exactly the intersection of where data engineering is going — not just moving data, but building intelligent, automated, and insight-generating platforms for some of the world's largest organisations.

What draws me to this specific role is that it combines deep technical work — Python, large-scale pipelines, REST APIs, DB integration — with the consulting dimension of working across industries and clients. I've spent 3+ years going deep on one pharma client platform. I'm now ready to apply those skills across multiple industries and more complex, enterprise-scale challenges.

Deloitte's investment in the DIU Leadership Centre in Hyderabad signals genuine commitment to developing talent in India — that aligns with my growth trajectory toward a Senior or Lead Data Engineer role.

I bring a production-tested background in exactly the stack this role needs — Python, PySpark, batch pipeline design, REST API integration, Git-based CI/CD, and Azure — and I'm excited to bring that into a consulting context where the impact scales across clients.

---

## QUICK REFERENCE — Key Numbers & Facts to Mention

| Metric | Value |
|--------|-------|
| Pipeline execution time improvement | ~35% reduction |
| Batch file size handled | 6–8 GB daily |
| Production pipelines maintained | 10+ |
| FinOps PoC delivery | 2 weeks ahead of schedule |
| API ingestion time reduction (threading) | ~60% |
| Certifications | Databricks Certified Data Engineer Associate, DP-900 |
| Tech stack | PySpark, Python, SQL, Azure Databricks, ADF, Delta Lake, Apache Iceberg, Unity Catalog, Oracle, Salesforce, Smartsheet REST APIs |

---

*Prepared June 2026 | Tailored to Deloitte Python Data Engineer JD*
