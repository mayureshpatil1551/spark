# Databricks Custom Software Engineer — Interview Prep (50 Q&A)
**Candidate:** Mayuresh Uttam Patil | **Role:** Custom Software Engineer — Databricks Unified Data Analytics Platform | **Location:** Bengaluru
**Style:** Spoken, first-person answers grounded in your FinOps Dataverse and e-Clinical Data Platform (eCDP) projects at Cognizant.

---

## Section 1: Databricks & Delta Lake Core (Q1–Q10)

**Q1. Walk me through your hands-on experience with the Databricks Unified Data Analytics Platform.**
I've been working on Azure Databricks for about 3 years now across two major projects at Cognizant. On my current project, FinOps Dataverse, I use Databricks notebooks and clusters to build a Medallion Lakehouse — Bronze, Silver, and Gold layers — where I ingest financial data from Oracle and flat files, transform it using PySpark, and store it in Delta Lake on ADLS Gen2. On my previous project, the e-Clinical Data Platform, I used Databricks with Apache Iceberg to modernize over 20TB of pharmaceutical data on Amazon S3. So I've worked with Databricks both on Azure with Delta Lake and in a multi-cloud setup with Iceberg.

**Q2. What is Delta Lake and why did you choose it over a regular data lake?**
Delta Lake is an open-source storage layer that brings ACID transactions, schema enforcement, and time travel to a plain data lake built on Parquet files. On FinOps Dataverse, we chose Delta Lake because we're dealing with financial data where consistency matters a lot — if two jobs write at the same time, we need to be sure we don't end up with corrupted or partial data. Delta Lake gives us that transactional guarantee, plus it lets us do upserts using MERGE, which is critical for our incremental loads.

**Q3. Explain ACID transactions in the context of Delta Lake and why they matter for your project.**
ACID stands for Atomicity, Consistency, Isolation, and Durability. In Delta Lake, every write operation is logged in a transaction log called the Delta Log, so either the whole write succeeds or none of it does — that's atomicity. For FinOps Dataverse, this was important because we have multiple pipelines writing to the same Gold layer tables that feed 300+ business reports. Without ACID guarantees, a failed job midway could leave reports showing half-updated numbers, which is unacceptable in financial reporting.

**Q4. What is Z-Ordering and how did you use it to reduce query latency by 40%?**
Z-Ordering is a technique in Delta Lake that co-locates related data in the same set of files based on the columns you specify, so that when a query filters on those columns, Spark can skip reading irrelevant files — this is called data skipping. On FinOps Dataverse, our Gold layer tables were being queried heavily by columns like transaction date and cost center. I ran OPTIMIZE with ZORDER BY on those columns, combined with file compaction to fix the small-file problem, and together that brought our report query latency down by 40% across 300+ business reports.

**Q5. What's the difference between OPTIMIZE and VACUUM in Delta Lake?**
OPTIMIZE compacts small files into larger ones and can also apply Z-Ordering — it's about improving read performance. VACUUM, on the other hand, physically deletes old data files that are no longer referenced by the Delta transaction log, after the retention period, typically 7 days by default. I use OPTIMIZE regularly on our high-traffic tables to keep query performance up, and I run VACUUM carefully, making sure I'm not breaking time travel queries that other teams might still need.

**Q6. Explain Delta Lake's time travel feature. Have you used it in a real scenario?**
Time travel lets you query a Delta table as it existed at a previous version or timestamp, using syntax like `VERSION AS OF` or `TIMESTAMP AS OF`. It's possible because Delta Lake keeps a full history of transactions in the transaction log. I've used it mainly for debugging — if a Gold layer report looked wrong on a given day, I could go back to the exact version of the table before a suspicious job ran and compare it, which helped me pinpoint whether the issue was in the data or the transformation logic.

**Q7. What is schema evolution in Delta Lake, and how do you handle it in your pipelines?**
Schema evolution lets a Delta table's schema change over time — for example, if a source system adds a new column, you don't want your whole pipeline to break. In Delta Lake, you can enable `mergeSchema` when writing, so new columns get added automatically. On the e-Clinical Data Platform, since we were ingesting from multiple sources like Oracle, Snowflake, REST APIs, and Smartsheet, source schemas would occasionally change. I built our ingestion framework to be configuration-driven so schema changes could be handled without hardcoding, which reduced onboarding time for new pipelines.

**Q8. How does Delta Lake handle concurrent writes and prevent conflicts?**
Delta Lake uses optimistic concurrency control. When multiple writers try to modify the same table, each one checks the transaction log before committing — if another writer already committed a conflicting change, the second writer's transaction fails and can be retried. This matters on FinOps Dataverse because we have multiple ingestion pipelines from Oracle and flat-file sources potentially writing to overlapping partitions, and Delta Lake protects us from silent data corruption.

**Q9. What's the difference between a managed table and an external table in Databricks?**
A managed table has both its metadata and data lifecycle controlled by Databricks — if you drop the table, the underlying data is deleted too. An external table only has its metadata managed by Databricks; the actual data lives independently, for example in ADLS Gen2, and dropping the table doesn't delete the files. On our projects, we mostly use external tables pointing to ADLS Gen2 paths, because we need the raw data to persist independently of the catalog for audit and reprocessing purposes.

**Q10. How do you approach performance tuning for a slow-running Delta Lake table in production?**
My first step is always checking the file layout — if there are too many small files, that's usually the biggest culprit, so I'd run OPTIMIZE. Next I'd look at whether the table would benefit from Z-Ordering on the columns most used in filters. I'd also check partition pruning — are we partitioning on a high-cardinality or low-cardinality column, since that affects performance in different ways. On FinOps Dataverse, this exact combination — compaction plus Z-Ordering — is what got us from slow report queries down to a 40% latency improvement.

---

## Section 2: PySpark & Distributed Computing (Q11–Q20)

**Q11. Explain how Spark's distributed architecture works — driver, executors, and cluster manager.**
Spark has a driver program that holds the SparkContext and is responsible for converting your code into a DAG of tasks. The cluster manager — in our case, Databricks manages this — allocates resources across worker nodes, and each worker runs one or more executors, which are the processes that actually execute tasks and hold data in memory or on disk. The driver schedules tasks to executors, and executors report results back. Understanding this helped me a lot when I was doing partition tuning — the goal is always to keep executors evenly loaded.

**Q12. You mentioned reducing Spark processing time by 50% through partition tuning and resource management. Walk me through what you actually did.**
On the e-Clinical Data Platform, one of our PySpark jobs was taking much longer than expected because of skewed partitions — a few partitions were huge and others were tiny, so a handful of executors were doing most of the work while others sat idle. I repartitioned the data on a more evenly distributed key instead of the default, and I also tuned the number of shuffle partitions to match our cluster size instead of using Spark's default of 200. Combined with adjusting executor memory and cores per the data volume, we cut processing time by 50%.

**Q13. What is data skew in Spark and how do you detect and fix it?**
Data skew happens when data isn't evenly distributed across partitions, so some tasks take much longer than others, and the whole job waits on the slowest one. I detect it by looking at the Spark UI — if I see a few tasks taking dramatically longer than the rest in a stage, that's a strong sign of skew. To fix it, I've used techniques like salting the skewed key to spread it across more partitions, or repartitioning on a better-distributed column. On my projects, this typically showed up when a small number of source records — like one large customer or vendor — dominated a partition key.

**Q14. Explain the difference between narrow and wide transformations in Spark, and why it matters for performance.**
A narrow transformation, like `filter` or `map`, doesn't require data to move between partitions — each output partition depends on only one input partition. A wide transformation, like `groupBy` or `join`, requires a shuffle, where data moves across the network between executors. Shuffles are expensive, so when I'm optimizing a pipeline, I try to minimize unnecessary wide transformations, or at least make sure the shuffle partition count is tuned so it doesn't become a bottleneck.

**Q15. What's the difference between `repartition()` and `coalesce()`, and when would you use each?**
`repartition()` does a full shuffle and can increase or decrease the number of partitions, giving you a more even distribution. `coalesce()` avoids a full shuffle by merging existing partitions, so it can only decrease partition count, and it's cheaper. I use `coalesce()` when I just need to reduce the number of output files before writing, like before writing to the Gold layer, and `repartition()` when I need to actually fix a skewed distribution before a heavy join or aggregation.

**Q16. How do you handle incremental data loads in your PySpark pipelines?**
On both projects, I built pipelines that support full and incremental loads. For incremental loads, I typically rely on a watermark column — like a last-modified timestamp — that gets stored, often in an Azure SQL table for our ADF-orchestrated pipelines. Each run picks up only records newer than the last successful watermark, processes them, and then updates the watermark. Combined with Delta Lake's MERGE operation, this lets us do efficient upserts instead of reprocessing the entire dataset every time.

**Q17. Describe how you'd design a PySpark job to process 10 million records daily with 99.9% availability.**
That's actually close to what we built on the e-Clinical Data Platform. The key design choices were: a configuration-driven ingestion framework so we're not hardcoding logic per source, incremental loading using watermarks so we're not reprocessing everything daily, and robust error handling with retries at the pipeline orchestration layer. We also built monitoring so failures in one source system, like Smartsheet or REST APIs, wouldn't block other sources from completing. That combination is how we maintained 99.9% pipeline availability while ingesting 10M+ records daily.

**Q18. What's the difference between `cache()` and `persist()` in Spark, and when should you use them?**
`cache()` is a shorthand for `persist()` using the default storage level, which is memory-only for RDDs or memory-and-disk for DataFrames. `persist()` lets you explicitly choose the storage level, like memory-only, disk-only, or a combination with replication. I use caching when a DataFrame is being reused multiple times in a job — for example, if I compute an intermediate result once and then join it against multiple other tables downstream, caching avoids recomputing that transformation each time.

**Q19. Explain broadcast joins in Spark. When did you use one?**
A broadcast join sends a smaller DataFrame to every executor's memory instead of shuffling both large datasets across the network, which is much faster when one side of a join is small enough to fit in memory — typically under a few hundred MB. On the e-Clinical platform, when joining our large transaction datasets against smaller reference or lookup tables — like a mapping table for Golden Record identity resolution — I used broadcast joins to avoid unnecessary shuffles and speed up the job significantly.

**Q20. How do you debug a failing or slow Spark job in production?**
I start with the Spark UI to look at the stages and tasks — checking for skewed tasks, spilled data to disk, or stages that are taking abnormally long. I also check the executor logs for actual error messages if it's a failure rather than a slowness issue. If it's a memory issue, I look at whether we're caching too aggressively or whether partition sizes are too large for the executor memory configured. On FinOps Dataverse, this systematic approach — UI first, then logs, then configuration — is how I've resolved most production issues without needing to guess.

---

## Section 3: Medallion Architecture & Data Modeling (Q21–Q26)

**Q21. Explain the Medallion Architecture and how you implemented it on FinOps Dataverse.**
Medallion Architecture organizes data into three layers: Bronze holds raw, unprocessed data as ingested from source; Silver holds cleaned, validated, and conformed data; and Gold holds business-level aggregated data ready for reporting and analytics. On FinOps Dataverse, I architected exactly this using Azure Databricks and Delta Lake — Bronze captures raw Oracle and flat-file data, Silver applies data quality rules and standardization, and Gold produces the curated tables that feed our 300+ business reports. This layered approach gave us traceability — if something looked wrong in Gold, I could trace it back through Silver to Bronze.

**Q22. Why not just transform data directly from source to a reporting table? Why use three layers?**
Because it gives you separation of concerns and recoverability. If I only had one transformation step and something broke, I'd have to reprocess everything from the source system again, which isn't always possible if the source doesn't retain history. With Bronze as an immutable raw layer, I can always reprocess Silver and Gold without going back to source. It also lets different teams work at different layers — data engineers work on Bronze-to-Silver quality rules, while analytics engineers can focus on Silver-to-Gold business logic, independently.

**Q23. What is Golden Record and XREF-based identity resolution? Explain it as if I don't know MDM.**
In Master Data Management, the same real-world entity — like a patient or a study site — often exists in multiple source systems with slightly different representations, maybe different IDs or slightly different names. A Golden Record is the single trusted version of that entity after resolving all the duplicates. On the e-Clinical Data Platform, I built workflows using a cross-reference, or XREF, table that maps each source system's local ID to a single master ID. That way, when data comes in from Oracle, Snowflake, or REST APIs, we can match it back to the correct Golden Record instead of creating duplicate entities.

**Q24. How do you ensure data quality between the Bronze and Silver layers?**
In Bronze, I don't apply any transformation — I want an exact, auditable copy of the source. Moving to Silver, I apply schema validation, null checks on mandatory fields, deduplication, and standardization of formats like dates and currency codes, especially important for financial data on FinOps Dataverse. I also log records that fail these checks into a separate quarantine location rather than silently dropping them, so we can investigate and reprocess if needed.

**Q25. What is a Star Schema, and how did you apply it in your training project?**
A Star Schema is a data modeling approach with a central fact table holding measurable events, like flight delays, surrounded by dimension tables holding descriptive attributes, like airline, airport, or date. In my Airlines Data Analysis training project, I modeled a fact table for flight performance metrics and dimension tables for things like route and airline, which let us efficiently run historical reporting queries on flight on-time performance without repeatedly joining large raw tables.

**Q26. How would you design the Gold layer for a reporting use case involving 300+ business reports?**
I'd design Gold tables around business subject areas rather than one-to-one with source systems — for example, a "financial transactions summary" table serving multiple related reports instead of a separate table per report. That reduces duplication and makes maintenance easier. I'd also pre-aggregate common metrics where possible, and apply Z-Ordering on the columns most frequently filtered in reports. This is essentially the approach I used on FinOps Dataverse, which is part of why we hit that 40% latency improvement.

---

## Section 4: Azure Data Factory & Pipeline Orchestration (Q27–Q32)

**Q27. How do you use Azure Data Factory alongside Databricks in your pipelines?**
ADF acts as the orchestrator in our setup — it handles scheduling, triggering, and coordinating dependencies between steps, while Databricks does the heavy lifting of actual data transformation using PySpark. On FinOps Dataverse, ADF pipelines trigger Databricks notebooks through the Databricks activity, pass parameters like source system and watermark values, and manage the overall control flow, including error handling and retry logic.

**Q28. What is a configuration-driven ingestion framework, and why did you build one?**
Instead of writing a separate hardcoded pipeline for every source system, I built a metadata-driven framework where source details — connection info, table names, watermark columns, target paths — are stored as configuration, often in a control table or JSON/YAML files. A single generic pipeline reads that configuration and adapts its behavior accordingly. On FinOps Dataverse this improved data onboarding and operational efficiency by 50%, because adding a new source became a configuration change rather than new development.

**Q29. How do you handle pipeline failures and retries in ADF?**
ADF has built-in retry policies at the activity level, where you can configure the number of retries and the interval between them. Beyond that, I design pipelines with proper error handling using Try-Catch patterns — if a critical activity fails, I route to a failure path that logs the error details and can send alerts. For our incremental pipelines, it's also important that a failed run doesn't advance the watermark, so the next run correctly retries the same window of data instead of skipping it.

**Q30. Explain how you'd design a pipeline to ingest data from Oracle and flat files into a Lakehouse.**
I'd have ADF orchestrate two ingestion paths — one using a Copy Activity or linked service to pull incremental data from Oracle based on a watermark, and another watching a landing folder for new flat files. Both would land in the Bronze layer in ADLS Gen2 in their raw form. Then a Databricks notebook, triggered by ADF, would read both raw sources, apply the Bronze-to-Silver transformations and validations, and finally aggregate into Gold. This is essentially the pattern I built on FinOps Dataverse for our financial reporting pipelines.

**Q31. What's the difference between a trigger-based and schedule-based pipeline in ADF, and which do you use?**
A schedule-based trigger runs a pipeline at fixed times, like every night at 2 AM, which is what we mostly use for our batch financial reporting pipelines since business reports need to be ready by a certain time each day. An event-based trigger fires in response to something happening, like a new file landing in a storage container, which is useful for flat-file ingestion where files arrive at irregular times. In practice, I've used a mix — scheduled triggers for predictable batch loads and event-based triggers for file-arrival scenarios.

**Q32. How do you manage secrets and credentials securely in your ADF and Databricks pipelines?**
We use Azure Key Vault to store connection strings, credentials, and other secrets, and both ADF and Databricks integrate with it directly — ADF through Key Vault-backed linked services, and Databricks through secret scopes backed by Key Vault. That way, no credentials are ever hardcoded in notebooks or pipeline JSON, and access can be controlled and audited centrally, which matters a lot given we're handling financial and pharmaceutical data under regulatory requirements.

---

## Section 5: Unity Catalog, Governance & Cloud Platforms (Q33–Q38)

**Q33. What is Unity Catalog and what problem does it solve?**
Unity Catalog is Databricks' centralized governance layer for data and AI assets. Before it, access control, auditing, and lineage tracking were often scattered — managed per-workspace or per-cluster. Unity Catalog gives you a single place to manage permissions across all your Databricks workspaces, track data lineage automatically, and audit who accessed what. It uses a three-level namespace — catalog, schema, table — instead of just schema and table, which gives you an extra layer for organizing data, for example by business domain or environment.

**Q34. How does data lineage help in a project like FinOps Dataverse?**
Data lineage shows the full journey of a piece of data — which source it came from, what transformations it went through, and which downstream reports or tables consume it. In a financial reporting context, this is critical for audit and compliance — if a number in a report looks wrong, lineage lets us trace it back through Gold, Silver, and Bronze to the exact source record, instead of manually digging through pipeline code.

**Q35. What's the difference between role-based access control at the Databricks workspace level versus Unity Catalog?**
Workspace-level access control in Databricks is more limited and applies per workspace, so if you have multiple workspaces, you'd have to manage permissions separately in each. Unity Catalog centralizes this, letting you define access at the catalog, schema, or even column and row level, and that policy applies consistently across every workspace attached to the metastore. This is especially valuable in regulated environments like ours, where we need to restrict access to sensitive financial and pharmaceutical data consistently.

**Q36. You've worked with both Azure (ADLS Gen2) and AWS (S3). How do you think about cloud-based data platform integration?**
Conceptually, ADLS Gen2 and S3 solve the same problem — durable, scalable object storage that a Lakehouse engine like Databricks can read and write directly — but the integration details differ. On FinOps Dataverse, I use ADLS Gen2 with hierarchical namespace, which gives directory-level operations and integrates tightly with Azure Databricks via service principals or managed identities. On the e-Clinical platform, we used Amazon S3, accessed through IAM roles. The key is always the same: secure, least-privilege access, and choosing the storage layout — like partitioning — that matches your query patterns.

**Q37. How would you design row-level or column-level security for sensitive financial data in Databricks?**
Using Unity Catalog, I'd apply dynamic views or Unity Catalog's row filters and column masks — for example, masking account numbers for users who aren't in a finance-admin group, or filtering rows so a regional analyst only sees their region's transactions. This is done declaratively at the catalog layer rather than duplicating logic across every downstream notebook, which keeps governance consistent and auditable.

**Q38. What governance challenges have you faced working with pharmaceutical and financial data specifically?**
Both domains are heavily regulated, so data consistency and traceability aren't optional — they're compliance requirements. On the e-Clinical platform, our Golden Record and XREF approach existed specifically to make sure a patient or study record wasn't duplicated or misrepresented across systems. On FinOps Dataverse, the requirement is more about ensuring every financial figure reported can be traced back and reconciled to source. In both cases, my approach has been to build governance and lineage into the pipeline design itself, not bolt it on afterward.

---

## Section 6: Scenario-Based / Project Deep-Dives (Q39–Q46)

**Q39. Scenario: A Databricks job that normally runs in 30 minutes suddenly takes 3 hours. Walk me through your troubleshooting process.**
First, I'd check whether the input data volume genuinely increased — sometimes it's not a bug, just more data. If volume is normal, I'd open the Spark UI and look for skewed stages, where a few tasks are taking far longer than others. I'd also check if a recent code change introduced a wide transformation like an unoptimized join or groupBy. Finally, I'd check cluster health — are we getting fewer executors than usual due to a cluster autoscaling issue or a spot instance eviction. This is close to the real troubleshooting I did on the e-Clinical platform, where I traced a slowdown to skewed partitions and fixed it through repartitioning.

**Q40. Scenario: Your incremental pipeline accidentally reprocessed the same day's data twice, causing duplicate records in the Gold layer. How would you fix it and prevent recurrence?**
Immediate fix: I'd use Delta Lake's MERGE with a proper key to deduplicate the Gold table, or restore to a prior version using time travel if the duplication is severe, then carefully replay. To prevent recurrence, I'd check the watermark logic — likely the watermark wasn't updated correctly after a run, or the pipeline didn't guard against concurrent triggers. I'd add a check to ensure the watermark only advances after a fully successful, verified write, and make sure our MERGE-based upserts use a proper natural or surrogate key so even accidental reprocessing is idempotent rather than duplicative.

**Q41. Scenario: A new source system needs to be onboarded to FinOps Dataverse within a week. How would your configuration-driven framework help?**
Since we already have config-driven ingestion, onboarding mostly means adding new entries to our configuration — connection details, watermark column, target Bronze path, and any source-specific mapping — rather than writing a new pipeline from scratch. I'd validate the configuration in a dev environment first, run a small sample load to confirm schema and data quality, then promote through our DEV to UAT to PROD deployment process using Azure DevOps CI/CD. This is exactly the efficiency gain that got us the 50% improvement in onboarding time.

**Q42. Scenario: Business stakeholders report that a Gold layer report shows numbers that don't match the source system. How do you investigate?**
I'd start at the Gold layer and use Delta Lake's transaction history to see when the table was last updated and by which job. Then I'd trace backward — checking the Silver layer for the same record to see if the transformation logic introduced the discrepancy, and then Bronze to confirm what was actually ingested from source. If Bronze already differs from source, it's an ingestion issue; if Bronze matches but Silver or Gold doesn't, it's a transformation logic bug. Having that layered Medallion structure is exactly what makes this kind of root-cause tracing possible.

**Q43. Scenario: You need to migrate SAP financial data to both Iceberg and Snowflake simultaneously and keep them consistent. How did you approach this on eCDP?**
I engineered dual-target pipelines where a single PySpark job read the SAP source once and wrote to both targets within the same orchestrated run, rather than running two independent pipelines that could drift out of sync. I made sure both writes used the same transformation logic and the same watermark, so if one target succeeded and the other failed, we could detect and reconcile that mismatch rather than silently having two different versions of the same data across platforms.

**Q44. Scenario: A junior engineer on your team writes a PySpark job that works fine in testing but fails on the full production dataset. What would you check with them?**
I'd first check data volume differences — production is often orders of magnitude larger, so issues like data skew or memory pressure that don't show up on a small sample suddenly matter. I'd walk them through checking the Spark UI for skewed tasks or spilled memory, review whether they're doing something expensive like a full shuffle join without considering partition strategy, and check cluster sizing. Mentoring on this kind of practical debugging is something I've done informally with newer team members, and it's part of why we were recognized for "Raising the Bar" in ETL transformation excellence.

**Q45. Scenario: Leadership wants to reduce cloud storage costs on ADLS Gen2 without impacting report performance. What levers would you pull?**
I'd look first at VACUUM to clean up stale Delta files past the retention window, since Delta tables can accumulate a lot of old file versions. I'd also review partitioning strategy — over-partitioning creates too many small files, which costs more in both storage overhead and read performance. Where appropriate, I'd introduce lifecycle policies to move older, less-accessed data to cooler storage tiers, while keeping the Gold layer used by the 300+ active reports on hot storage.

**Q46. Scenario: A REST API source you depend on suddenly changes its response schema without notice. How does your pipeline handle it, and how should it?**
On the e-Clinical platform, our ingestion frameworks integrated REST APIs alongside Oracle, Snowflake, and Smartsheet, so schema drift was a real risk. Ideally, the Bronze layer ingestion should be schema-flexible — landing the raw JSON response as-is rather than enforcing a rigid schema immediately. Then, schema validation happens at the Bronze-to-Silver step, where a broken or unexpected schema triggers an alert and routes to a quarantine path instead of silently corrupting downstream data. If this hadn't been handled yet on a given pipeline, that's exactly the kind of gap I'd prioritize fixing.

---

## Section 7: SDLC, Agile, CI/CD & Clean Code (Q47–Q50)

**Q47. How do you follow agile methodology in your day-to-day work as a data engineer?**
We work in sprints with daily stand-ups, sprint planning, and retrospectives. I break down data engineering work into user stories — for example, "onboard new Oracle source to Bronze layer" — estimate effort, and pick these up based on sprint priority. I also make sure to flag blockers early in stand-ups, like a data access issue, so the team can help unblock rather than losing days silently. Agile has helped us iterate quickly, especially when business requirements around financial reporting evolve mid-project.

**Q48. Describe your CI/CD process for deploying Databricks and ADF pipelines using Azure DevOps.**
I use Git integration for both Databricks notebooks and ADF pipeline JSON, so all changes are version-controlled. For deployment, I've worked with ARM templates and YAML pipelines in Azure DevOps, along with parameter override files to handle environment-specific settings — like different Key Vault names or storage paths across DEV, UAT, and PROD. Deployments go through approval gates, so a promotion from UAT to PROD requires sign-off, which adds a safety check before anything touches production financial data.

**Q49. What does "clean, maintainable code" mean to you in the context of PySpark pipelines?**
It means writing transformation logic that's modular and reusable rather than one giant monolithic notebook — for example, separating ingestion, validation, and transformation logic into distinct functions or notebooks that can be tested independently. It also means avoiding hardcoded values by using configuration, which is exactly why I built our Python-based SQL and pipeline configuration generator — it eliminated repetitive manual DDL writing and made our codebase consistent across environments instead of having slightly different hardcoded logic per source.

**Q50. How do you approach testing and debugging in a Databricks/PySpark environment?**
For testing, I validate transformation logic on smaller sample datasets in a dev workspace before running against full production volume, and I check for data quality issues like nulls, duplicates, or schema mismatches at each Medallion layer boundary. For debugging, my go-to tools are the Spark UI for performance issues, Delta Lake's transaction history for data discrepancies, and structured logging within notebooks so failures point directly to the stage and dataset involved, rather than requiring me to re-run the whole pipeline blind.

---

## Quick Reference: Your Key Metrics (keep these ready)
- 16+ TB financial data migrated to ADLS Gen2 (FinOps Dataverse)
- 20+ TB pharmaceutical data modernized on Amazon S3 (eCDP)
- 50% improvement in data onboarding/operational efficiency (config-driven framework)
- 40% reduction in report query latency (Z-Ordering + compaction, 300+ reports)
- 10M+ records processed daily, 99.9% pipeline availability (eCDP)
- 50% reduction in Spark processing time (partition tuning + resource management)
- Databricks Certified Data Engineer Associate, Azure Fundamentals, AWS Cloud Practitioner
