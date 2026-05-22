# 🎯 Optimized Interview Question Master List — Mayuresh Patil
> **Deduplicated | Grouped by Topic | Prioritized for Real DE Interviews**
> **Role: Data Engineer | AbbVie / Cognizant | PySpark + Iceberg + Azure**

---

## How to Use This Guide
- **🟢 Beginner** — Must answer confidently (no excuse to miss these)
- **🟡 Intermediate** — Core of most interviews (3–4 YOE level)
- **🔴 Advanced** — Senior/Lead level — answer these = stand out
- Questions marked with **⭐** = high-frequency in real DE interviews
- Questions marked with **📌** = directly tied to your resume

---

## SECTION 1 — Apache Spark Core
*Covers: Architecture, Execution Model, Memory, Optimization*

| # | Question | Level |
|---|---|---|
| S1 | ⭐ Explain Spark architecture — Driver, Executors, Cluster Manager, and how a job flows through them | 🟢 Beginner |
| S2 | ⭐ What is the difference between RDD, DataFrame, and Dataset? When would you use RDD? | 🟢 Beginner |
| S3 | ⭐ What is lazy evaluation? Why is it important for performance? | 🟢 Beginner |
| S4 | ⭐ What is the difference between narrow and wide transformations? | 🟢 Beginner |
| S5 | ⭐ What is a shuffle in Spark and why is it expensive? | 🟢 Beginner |
| S6 | ⭐ What are Jobs, Stages, and Tasks? How do they map to your DAG? | 🟢 Beginner |
| S7 | ⭐ What is the difference between `repartition()` and `coalesce()`? When to use each? | 🟢 Beginner |
| S8 | ⭐ What is the difference between `cache()` and `persist()`? What storage levels exist? | 🟡 Intermediate |
| S9 | ⭐ What is AQE (Adaptive Query Execution)? What 3 things does it optimize? | 🟡 Intermediate |
| S10 | ⭐ What is a Broadcast Join? When should you use it vs Sort-Merge Join? | 🟡 Intermediate |
| S11 | What is Dynamic Partition Pruning (DPP) and how does it improve performance? | 🟡 Intermediate |
| S12 | ⭐ 📌 What is data skew and how do you handle it? (Salting, AQE, Broadcast) | 🟡 Intermediate |
| S13 | How does Spark handle fault tolerance internally? (Lineage, Task retry, Stage rerun) | 🟡 Intermediate |
| S14 | What is the Catalyst Optimizer? Walk through its 4 phases. | 🟡 Intermediate |
| S15 | What is Tungsten and how does it improve Spark performance over RDDs? | 🟡 Intermediate |
| S16 | What is checkpointing in Spark? How is it different from caching? | 🟡 Intermediate |
| S17 | ⭐ 📌 You have a Spark job taking 4 hours. How do you diagnose and optimize it? | 🔴 Advanced |
| S18 | ⭐ Explain Spark memory management — Execution vs Storage memory regions | 🔴 Advanced |
| S19 | What is Cluster Mode vs Client Mode? Which does Databricks use? | 🟢 Beginner |
| S20 | ⭐ Why are Python UDFs expensive? What are the alternatives? | 🟡 Intermediate |

---

## SECTION 2 — Apache Iceberg
*Covers: Architecture, Partitioning, Time Travel, Schema, Write Modes*

| # | Question | Level |
|---|---|---|
| I1 | ⭐ What is Apache Iceberg and why did you choose it over Delta Lake or Hive? | 🟢 Beginner |
| I2 | ⭐ Explain Iceberg's table architecture — metadata file, manifest list, manifest file, data files | 🟡 Intermediate |
| I3 | ⭐ What is hidden partitioning in Iceberg? How is it different from Hive partitioning? | 🟡 Intermediate |
| I4 | What is partition evolution in Iceberg? Give a real example. | 🟡 Intermediate |
| I5 | ⭐ How does time travel work in Iceberg? How did it help with pharmaceutical audit trails? | 🟡 Intermediate |
| I6 | What is compaction (rewrite_data_files) in Iceberg and why is it necessary? | 🟡 Intermediate |
| I7 | ⭐ What is Copy-on-Write (CoW) vs Merge-on-Read (MoR)? When would you use each? | 🔴 Advanced |
| I8 | ⭐ How does Iceberg handle schema evolution? What changes are backward-compatible? | 🔴 Advanced |
| I9 | ⭐ 📌 Delta Lake vs Iceberg vs Hudi — key differences and when to use each | 🔴 Advanced |
| I10 | 📌 How did you migrate 20TB+ of legacy data to Iceberg with zero downtime? | 🔴 Advanced |

---

## SECTION 3 — Delta Lake
*Covers: ACID, _delta_log, Optimization, Governance*

| # | Question | Level |
|---|---|---|
| D1 | ⭐ What is Delta Lake? How does it differ from a plain Parquet data lake? | 🟢 Beginner |
| D2 | ⭐ Explain ACID transactions in Delta Lake. How does `_delta_log` enable them? | 🟡 Intermediate |
| D3 | What is Schema Enforcement vs Schema Evolution in Delta? | 🟡 Intermediate |
| D4 | What is the small files problem in Delta and how do OPTIMIZE and VACUUM solve it? | 🟡 Intermediate |
| D5 | What is Z-Ordering vs Liquid Clustering? When would you use each? | 🟡 Intermediate |
| D6 | ⭐ A pipeline accidentally deleted 50,000 records. How do you recover using Delta? | 🔴 Advanced |
| D7 | What are Deletion Vectors in Delta Lake and when would you enable them? | 🔴 Advanced |
| D8 | What is the difference between Shallow Clone and Deep Clone in Delta? | 🔴 Advanced |

---

## SECTION 4 — Data Architecture & Modeling
*Covers: Lakehouse, Medallion, Modeling, Partitioning*

| # | Question | Level |
|---|---|---|
| A1 | ⭐ What is a Data Lakehouse? How does it differ from a Data Lake and Data Warehouse? | 🟢 Beginner |
| A2 | ⭐ Explain the Medallion Architecture (Raw/Bronze → Trusted/Silver → Curated/Gold) | 🟢 Beginner |
| A3 | What is the difference between Star Schema and Snowflake Schema? When do you use each? | 🟢 Beginner |
| A4 | What is OLTP vs OLAP? How does your pipeline serve OLAP needs? | 🟢 Beginner |
| A5 | ⭐ How do you decide which column to partition on? What happens with high cardinality? | 🟡 Intermediate |
| A6 | What is Dimensional Modeling? What are Fact and Dimension tables? | 🟡 Intermediate |
| A7 | What is a Surrogate Key and why use it over a natural key? | 🟡 Intermediate |
| A8 | ⭐ 📌 How would you design a data pipeline for 500 tables from Oracle into an Iceberg Lakehouse? | 🔴 Advanced |
| A9 | 📌 How do you approach capacity planning for a pipeline processing 10M+ records daily? | 🔴 Advanced |

---

## SECTION 5 — MDM, Data Quality & Governance
*Covers: Golden Record, XREF, DQ Checks, Lineage, Compliance*

| # | Question | Level |
|---|---|---|
| G1 | ⭐ What is MDM (Master Data Management) and what problem does it solve? | 🟢 Beginner |
| G2 | ⭐ 📌 Explain your Golden Record / XREF cross-reference system. How does it unify disparate sources? | 🟡 Intermediate |
| G3 | What are survivorship rules in MDM? How do you implement them in PySpark? | 🟡 Intermediate |
| G4 | ⭐ What data quality checks do you apply in your Bronze → Silver layer? | 🟡 Intermediate |
| G5 | How do you handle duplicate records from a CDC ingestion pipeline? | 🟡 Intermediate |
| G6 | What is data lineage and how do you implement it in a modern data platform? | 🟡 Intermediate |
| G7 | 📌 What is Modak Nabu and what audit/governance features did you use from it? | 🟡 Intermediate |
| G8 | ⭐ What is GDPR? What techniques do you use for data masking and PII protection? | 🟡 Intermediate |
| G9 | How would you implement column-level security for PHI data in Databricks Unity Catalog? | 🔴 Advanced |
| G10 | ⭐ How do you handle schema drift — when a source system changes its schema unexpectedly? | 🔴 Advanced |

---

## SECTION 6 — Azure Cloud & Orchestration
*Covers: ADF, Databricks, Synapse, Key Vault, CI/CD*

| # | Question | Level |
|---|---|---|
| C1 | ⭐ What is Azure Data Factory? How does it differ from Databricks Workflows? | 🟢 Beginner |
| C2 | What trigger types does ADF support? When would you use Event-based vs Schedule? | 🟡 Intermediate |
| C3 | ⭐ What is Unity Catalog in Databricks? What governance benefits does it provide? | 🟡 Intermediate |
| C4 | What is Delta Live Tables (DLT)? How does it simplify pipeline development? | 🟡 Intermediate |
| C5 | What is Azure Key Vault? How do you use it in Databricks for secrets management? | 🟡 Intermediate |
| C6 | What is ADLS Gen2 and how does it differ from regular blob storage? | 🟡 Intermediate |
| C7 | When would you use Azure Synapse Analytics over Databricks? | 🟡 Intermediate |
| C8 | ⭐ 📌 How would you implement a CI/CD pipeline for Databricks notebooks using Azure DevOps? | 🔴 Advanced |
| C9 | 📌 How did you automate infrastructure deployment using Python SQL generation scripts? | 🔴 Advanced |

---

## SECTION 7 — Ingestion Patterns & Pipeline Design
*Covers: JDBC, API, Incremental, Exactly-Once, Idempotency*

| # | Question | Level |
|---|---|---|
| P1 | ⭐ What is JDBC? How did you use it to ingest from Oracle with parallel reads? | 🟢 Beginner |
| P2 | ⭐ What is the difference between full load and incremental load? How do you implement watermarking? | 🟡 Intermediate |
| P3 | ⭐ 📌 How did you integrate with Salesforce and Smartsheet APIs? What challenges came up? | 🟡 Intermediate |
| P4 | What is idempotency in pipelines? How do you ensure a pipeline is safe to re-run? | 🟡 Intermediate |
| P5 | ⭐ 📌 How did you achieve 99.9% data uptime for 10M+ daily records? | 🟡 Intermediate |
| P6 | ⭐ How would you design a pipeline for exactly-once semantics end-to-end? | 🔴 Advanced |
| P7 | ⭐ 📌 You need to migrate a 20TB live dataset with zero downtime. Walk me through your strategy. | 🔴 Advanced |
| P8 | How do you handle a source file that arrives with unexpected extra or missing columns? | 🟡 Intermediate |
| P9 | 📌 What is a metadata-driven ingestion framework? How did you build one for 500+ tables? | 🔴 Advanced |

---

## SECTION 8 — SQL & Query Optimization

| # | Question | Level |
|---|---|---|
| Q1 | ⭐ What is the difference between window functions and aggregate functions? | 🟢 Beginner |
| Q2 | ⭐ Explain `ROW_NUMBER()` vs `RANK()` vs `DENSE_RANK()` with an example | 🟢 Beginner |
| Q3 | Write SQL to rank customers by total purchases within each region | 🟡 Intermediate |
| Q4 | Convert a correlated subquery into a JOIN for performance | 🟡 Intermediate |
| Q5 | ⭐ How do you optimize a slow SQL query with large table joins? | 🟡 Intermediate |
| Q6 | What does `ANALYZE TABLE` do and when should you run it? | 🟡 Intermediate |
| Q7 | ⭐ Explain inner vs left vs right vs anti join with real use cases | 🟡 Intermediate |
| Q8 | Write SQL to find the second highest salary (and N-th highest) | 🟡 Intermediate |

---

## SECTION 9 — Streaming & Kafka

| # | Question | Level |
|---|---|---|
| K1 | ⭐ How does Kafka work internally? Topics, Partitions, Offsets, Consumer Groups | 🟡 Intermediate |
| K2 | What is the difference between Kafka and AWS Kinesis? | 🟡 Intermediate |
| K3 | ⭐ Write a Structured Streaming job: Kafka → Delta Lake with exactly-once semantics | 🔴 Advanced |
| K4 | How do you handle schema evolution in a Kafka streaming pipeline? | 🔴 Advanced |
| K5 | 📌 How would you design a real-time streaming pipeline on Azure for pharmaceutical data? | 🔴 Advanced |

---

## SECTION 10 — Behavioral & Situational
*Covers: STAR method answers for your resume bullets*

| # | Question | Level |
|---|---|---|
| B1 | ⭐ Tell me about yourself and your experience as a Data Engineer | 🟢 Beginner |
| B2 | ⭐ 📌 Walk me through your current AbbVie project end-to-end | 🟢 Beginner |
| B3 | ⭐ Why are you looking to move from Cognizant? What are you looking for next? | 🟢 Beginner |
| B4 | ⭐ 📌 Describe the biggest technical challenge you faced and how you solved it | 🟡 Intermediate |
| B5 | ⭐ 📌 How did you achieve a 30% reduction in pipeline execution time? | 🟡 Intermediate |
| B6 | 📌 How did you eliminate 15+ hours/week of manual setup using Python automation? | 🟡 Intermediate |
| B7 | ⭐ A stakeholder says data arrives 3 hours late. How do you diagnose and fix it? | 🟡 Intermediate |
| B8 | Describe a time you debugged a failing production pipeline under pressure | 🟡 Intermediate |
| B9 | How do you communicate technical decisions to non-technical stakeholders? | 🟡 Intermediate |
| B10 | ⭐ Where do you see Data Engineering in 3–5 years and how are you preparing? | 🟡 Intermediate |

---

## SECTION 11 — Practical Coding (Must-Know)

| # | Task | Level |
|---|---|---|
| H1 | ⭐ Read a nested JSON from S3 and flatten it (struct + explode for arrays) | 🟢 Beginner |
| H2 | ⭐ Read CSV from S3 with explicit schema (no inferSchema in production) | 🟢 Beginner |
| H3 | ⭐ MERGE INTO Delta/Iceberg table (upsert pattern) | 🟡 Intermediate |
| H4 | ⭐ Implement SCD Type 2 using Delta MERGE + window functions | 🟡 Intermediate |
| H5 | ⭐ PySpark JDBC read from Oracle with parallel partitions | 🟡 Intermediate |
| H6 | ⭐ Find new / updated / deleted records between source and target | 🟡 Intermediate |
| H7 | Implement two-phase salted aggregation for skewed groupBy | 🟡 Intermediate |
| H8 | ⭐ Write comprehensive DQ check function (null, duplicate, range, referential) | 🟡 Intermediate |
| H9 | Write a Kafka → Delta micro-batch job using foreachBatch | 🔴 Advanced |
| H10 | ⭐ Full Medallion Architecture pipeline (Bronze → Silver → Gold) with DQ gates | 🔴 Advanced |
| H11 | Pandas UDF vs Python UDF — write both and explain performance difference | 🔴 Advanced |
| H12 | Implement incremental load with watermark + MERGE into Iceberg | 🔴 Advanced |

---

## ⭐ BONUS — 10 High-Value Questions Based on Your Resume

*These are questions interviewers WILL ask because of specific bullets on your resume.*

| # | Question | Why They'll Ask | Level |
|---|---|---|---|
| R1 | 📌 **You built a Golden Record system using XREFs. What survivorship rules did you apply when two source systems had conflicting values for the same patient?** | "Golden Record" + "MDM" on resume | 🔴 Advanced |
| R2 | 📌 **Your pipeline processes 18+ years of pharma data. How do you ensure historical snapshots remain immutable for regulatory compliance?** | "18+ years of data" + "audit readiness" | 🔴 Advanced |
| R3 | 📌 **You integrated Oracle JDBC, Snowflake, Salesforce, and Smartsheet in one framework. How did you design it to be source-agnostic and extensible?** | "Architected high-availability ingestion frameworks" | 🔴 Advanced |
| R4 | 📌 **You mention Iceberg partition optimization. If a query was doing a full scan despite partition filters, how would you debug it?** | "Optimized Spark partitioning" + Iceberg expertise | 🔴 Advanced |
| R5 | 📌 **What does outbound data synchronization from a Lakehouse to Oracle look like? How do you handle conflicts when Oracle already has the record?** | "Outbound sync to Oracle and flat files" | 🟡 Intermediate |
| R6 | 📌 **You mention 99.9% data uptime. What does your alerting and recovery strategy look like when a pipeline fails at 2 AM?** | "99.9% data uptime" claim on resume | 🔴 Advanced |
| R7 | 📌 **How do you validate that a 20TB migration produced correct results? What reconciliation checks did you perform?** | "Migration of 20TB+" | 🔴 Advanced |
| R8 | 📌 **You use Modak Nabu for tracking. If a business user says 'the numbers changed between yesterday and today', how do you trace it using your audit system?** | "Modak Nabu" + "data lineage" | 🟡 Intermediate |
| R9 | 📌 **Walk me through how you refactored legacy SQL scripts into PySpark Medallion pipelines. What was the hardest part of the migration?** | "Refactored legacy SQL into Medallion" | 🟡 Intermediate |
| R10 | 📌 **You optimized shuffle operations and query execution plans for 30% improvement. Show me what the before/after looked like in the Spark physical plan.** | "30% reduction" claim needs proof | 🔴 Advanced |

---

## 📊 Summary Statistics

| Level | Count | % of Total |
|---|---|---|
| 🟢 Beginner | 28 | 27% |
| 🟡 Intermediate | 51 | 49% |
| 🔴 Advanced | 25 | 24% |
| **Total** | **104** | **100%** |

**Topics Covered:**

```
Spark Core ────────────── 20 questions
Iceberg ───────────────── 10 questions
Delta Lake ────────────── 8 questions
Data Architecture ──────── 9 questions
MDM & Governance ───────── 10 questions
Azure & Cloud ─────────── 9 questions
Ingestion Patterns ──────── 9 questions
SQL ───────────────────── 8 questions
Streaming & Kafka ──────── 5 questions
Behavioral ────────────── 10 questions
Practical Coding ────────── 12 questions
Resume-Specific Bonus ───── 10 questions (⭐ Most important)
```

---

## 🎯 Interview Preparation Priority Order

If you have **1 week**, focus in this order:

```
Day 1 → Spark Core (S1–S12) + Behavioral (B1–B5)
Day 2 → Iceberg (I1–I9) + Practical Coding (H1–H5)
Day 3 → Delta Lake (D1–D6) + Ingestion Patterns (P1–P7)
Day 4 → MDM & Governance (G1–G8) + SQL (Q1–Q8)
Day 5 → Azure & Cloud (C1–C8) + Architecture (A1–A8)
Day 6 → Advanced Spark (S17–S20) + Resume Bonus (R1–R10) ← Don't skip these
Day 7 → Mock interview using all Behavioral + your project Q&As
```

---

*Optimized Interview Question List | Mayuresh Patil | May 2026*
*Original questions: ~200+ | After deduplication: 104 unique, high-value questions*
