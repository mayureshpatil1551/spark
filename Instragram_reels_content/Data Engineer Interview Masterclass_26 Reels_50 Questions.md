# 🎬 Instagram Reel Series — Mayuresh Patil
## "Data Engineer Interview Masterclass" — 26 Reels | 50 Questions

---

## 📌 SERIES HASHTAGS (use on every reel)
```
#DataEngineer #DataEngineering #PySpark #ApacheIceberg #Databricks
#AzureDataEngineer #DataEngineerInterview #TechInterview #InterviewPrep
#SQLInterview #BigData #DataLakehouse #ETLPipeline #TechCareer
#SoftwareEngineer #DataScience #CloudComputing #CognizantLife
#TechIndia #PuneIT #DataEngineeringIndia #LearnDataEngineering
#CrackTheInterview #TechReels #ITCareer
```

---
---

# 🎬 REEL 01 — INTRO
## "From 0 to Data Engineer — My Story 🚀"

**⏱ Duration:** 45–60 seconds
**🎵 Music Vibe:** Upbeat, motivational (trending lo-fi or phonk)
**📍 Hook Type:** Face-to-camera, confident

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK (on screen text: "I engineer data pipelines that process 10 MILLION records every single day 🔥")**
> "If you're preparing for a Data Engineer interview — this series is for you."

**[3–8 sec]**
> "My name is Mayuresh Patil. I'm a Data Engineer at Cognizant with 3+ years of experience building enterprise-grade Data Lakehouse architectures."

**[8–15 sec]**
> "I work with PySpark, Apache Iceberg, Azure Databricks, and Delta Lake — in a pharmaceutical domain where data quality is literally life-critical."

**[15–25 sec]**
> "Some highlights from my work —
> ✅ Migrated 10TB+ of pharma data to Apache Iceberg on AWS S3
> ✅ Architected pipelines processing 10M+ records daily
> ✅ Built a Golden Record MDM system used across the enterprise
> ✅ Won the 'Owning It' award in 2025 for technical leadership"

**[25–38 sec]**
> "I hold the Databricks Certified Data Engineer Associate certification — and I'm starting this series to help YOU crack your Data Engineer interview."

**[38–50 sec]**
> "Over the next 25 reels, I'm going to cover 50 interview questions — real ones — across PySpark, Apache Iceberg, Azure, ETL architecture, MDM, data governance, and behavioral rounds."

**[50–60 sec] — CTA**
> "Follow me RIGHT NOW so you don't miss a single one. Drop a 🔥 in the comments if you're preparing for a Data Engineer interview. Let's go!"

**[ON SCREEN TEXT AT END:]**
> "25 reels. 50 questions. 1 goal — crack your interview. 🎯"

---

**📝 REEL NAME:** From 0 to Data Engineer — My Story
**📋 CAPTION:**
```
Starting a series I wish existed when I was preparing 👇

25 reels. 50 real Data Engineer interview questions. Covered with actual answers from my 3+ years of experience.

📌 Topics I'll cover:
→ PySpark & Apache Spark
→ Apache Iceberg
→ Azure & Databricks
→ ETL & Data Lakehouse Architecture
→ MDM & Golden Record
→ Data Governance
→ Behavioral & Scenario rounds

Follow + 🔔 so you don't miss any reel.

Drop 🔥 if you're in! 

#DataEngineer #InterviewPrep #DataEngineering #PySpark #Databricks #AzureDataEngineer #TechInterview #BigData #DataLakehouse #TechIndia
```

---
---

# 🎬 REEL 02 — Q1 & Q2
## "Spark Basics Every DE Must Know ⚡"

**⏱ Duration:** 50–60 seconds
**🎵 Music Vibe:** Energetic, focused
**📍 Hook Type:** Direct question throw

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Two Spark basics that trip up 80% of candidates in interviews. Watch till the end."

**[3–5 sec]**
> "Question 1 — What's the difference between a Transformation and an Action in Spark?"

**[5–18 sec]**
> "Transformations are LAZY — they don't execute immediately. filter(), select(), groupBy() — all lazy. Spark just builds a plan.
> Actions TRIGGER execution — count(), collect(), show(), write(). That's when Spark actually runs.
> Why lazy? So Spark can OPTIMIZE the entire plan before touching a single byte of data. That's your performance edge."

**[18–22 sec]**
> "Question 2 — What is a Shuffle and why is it EXPENSIVE?"

**[22–42 sec]**
> "A shuffle happens when data needs to cross partition boundaries — during groupBy, join, orderBy.
> It's expensive because Spark has to:
> 💾 Serialize data to disk
> 🌐 Transfer it over the network
> 📦 Deserialize on the other side
> And it BREAKS the pipeline — the next stage can't start until the shuffle is done.
> In my pipelines, I cut execution time by 30% by reducing shuffles using broadcast joins and smart partitioning."

**[42–50 sec] — CTA**
> "Save this. You WILL get asked one of these. Follow for Q3 and Q4 tomorrow."

---

**📝 REEL NAME:** Spark Basics Every DE Must Know ⚡
**📋 CAPTION:**
```
Reel 2/26 — PySpark Basics 🔥

Two questions that show up in almost every Data Engineer interview:

Q1: Transformation vs Action in Spark
Q2: What is a Shuffle and why is it expensive?

The lazy evaluation concept is what separates candidates who memorize from candidates who understand.

Save this for your interview prep 📌

Series: 50 DE Interview Q&A in 26 reels
Follow for the next drop tomorrow!

#PySpark #ApacheSpark #DataEngineer #DataEngineering #SparkInterview #BigData #InterviewPrep #TechInterview #DataEngineerInterview #LearnDataEngineering
```

---
---

# 🎬 REEL 03 — Q3 & Q4
## "repartition() vs coalesce() + Tuning 10M Records ⚡"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Focused, mid-tempo
**📍 Hook Type:** Common mistake hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Most candidates confuse these two Spark functions. If you're one of them — this is for you."

**[3–6 sec]**
> "Question 3 — repartition() vs coalesce(). What's the difference?"

**[6–20 sec]**
> "Both change the number of partitions — BUT:
> repartition() does a FULL SHUFFLE. It can increase or decrease partitions. Data is evenly spread. Use this when you need balance.
> coalesce() is SMART — it avoids the shuffle by merging existing partitions on the same node. It can only reduce partitions. Much faster.
> Rule of thumb: Shrinking before a write? Use coalesce. Need balanced data? Use repartition."

**[20–25 sec]**
> "Question 4 — You process 10 million records daily. How do you TUNE Spark for that scale?"

**[25–48 sec]**
> "Here's what I do in production:
> First — I use JDBC parallelism. Instead of reading Oracle data in one thread, I set partitionColumn, lowerBound, upperBound, and numPartitions — spawning 200 parallel connections. A 3-hour pull becomes 5 minutes.
> Second — broadcast joins. Small lookup tables? Broadcast them. No shuffle.
> Third — AQE. Adaptive Query Execution in Spark 3.x dynamically adjusts partition count after shuffles. Free optimization — just enable it.
> Fourth — persist intermediate DataFrames that are reused."

**[48–58 sec] — CTA**
> "If you're preparing for a Data Engineer role — these are non-negotiables. Save this. Drop your experience level in the comments 👇"

---

**📝 REEL NAME:** repartition() vs coalesce() + 10M Record Tuning
**📋 CAPTION:**
```
Reel 3/26 — Spark Optimization 🚀

Q3: repartition() vs coalesce()
Q4: How to tune Spark for 10M+ records daily

I process 10M+ records DAILY from Oracle and Salesforce in production.
The JDBC parallelism trick alone cut our ingestion from 3 hours to 5 minutes.

Save this. You will get asked this. ✅

Follow for Q5 & Q6 — Medallion Architecture + Catalyst Optimizer

#PySpark #SparkOptimization #DataEngineer #BigData #ApacheSpark #DataEngineering #InterviewPrep #TechInterview #JDBC #AzureDatabricks
```

---
---

# 🎬 REEL 04 — Q5 & Q6
## "Medallion Architecture + Catalyst Optimizer Explained 🏅"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Clean, professional
**📍 Hook Type:** Architecture visual hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "The Medallion Architecture question is asked in EVERY senior Data Engineer interview. Here's the full answer in 30 seconds."

**[3–6 sec]**
> "Question 5 — Explain the Medallion Architecture — Raw, Trusted, Curated."

**[6–22 sec]**
> "Three layers:
> 🥉 Raw — data ingested AS-IS from source systems. No transformations. Think of it as your immutable audit log.
> 🥈 Trusted — cleaned, validated, de-duplicated. Schema enforced. This is where your business logic lives.
> 🥇 Curated — business-ready aggregates. Optimized for dashboards, ML, downstream APIs.
> Key insight: If something breaks downstream, you can ALWAYS reprocess from Raw without re-ingesting from the source."

**[22–27 sec]**
> "Question 6 — How does Spark's Catalyst Optimizer work?"

**[27–48 sec]**
> "Catalyst is Spark's query optimizer. It works in 4 phases:
> 1️⃣ Analysis — resolves column names and types
> 2️⃣ Logical Optimization — predicate pushdown, column pruning
> 3️⃣ Physical Planning — picks the cheapest execution plan
> 4️⃣ Code Generation — generates tight Java bytecode via Tungsten
> How to influence it? Avoid UDFs — they're black boxes to Catalyst and KILL your optimizations. Collect table statistics. Filter on partition columns."

**[48–58 sec] — CTA**
> "These two concepts alone can carry you through a senior DE round. Follow for the next drop. Drop 💡 if you learned something new."

---

**📝 REEL NAME:** Medallion Architecture + Spark Catalyst Optimizer
**📋 CAPTION:**
```
Reel 4/26 — Architecture & Optimization 🏗️

Q5: Medallion Architecture (Raw → Trusted → Curated)
Q6: Spark Catalyst Optimizer — how it works and how to influence it

I implemented the Medallion Architecture on an enterprise pharma platform.
The 3-layer structure isn't just best practice — it's what keeps your data lineage clean and your regulatory audits passing. ✅

Follow for Q7 & Q8 — Data Skew + Narrow vs Wide Transformations

#MedallionArchitecture #ApacheSpark #DataEngineer #PySpark #DataLakehouse #CatalystOptimizer #DataEngineering #BigData #ETL #InterviewPrep
```

---
---

# 🎬 REEL 05 — Q7 & Q8
## "Data Skew is KILLING Your Spark Jobs 🔥"

**⏱ Duration:** 55–60 seconds
**🎵 Music Vibe:** Urgent, energetic
**📍 Hook Type:** Problem-pain hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "If your Spark job has 999 tasks finishing in 2 seconds and 1 task running for 20 minutes — you have data skew. Here's how to fix it."

**[3–6 sec]**
> "Question 7 — What is data skew and how do you handle it?"

**[6–25 sec]**
> "Skew = some partitions have WAY more data than others. One task becomes the straggler that holds everything up.
> How to detect it? Spark UI — if most tasks finish fast but a few take forever, that's skew. Or run a groupBy + count to see the distribution.
> Fixes:
> 🧂 Salting — append a random number to the skewed key, explode the right table, join on the salted key.
> 🎯 AQE Skew Handling — Spark 3.x automatically splits skewed partitions. Just enable it.
> 📡 Broadcast Join — if one side is small, broadcast it. No shuffle, no skew."

**[25–30 sec]**
> "Question 8 — Narrow vs Wide Transformations. What's the difference?"

**[30–48 sec]**
> "NARROW — each output partition depends on ONE input partition. No shuffle. map(), filter(), select(). Fast. Same stage.
> WIDE — each output partition may depend on MULTIPLE input partitions. Shuffle required. groupBy(), join(), orderBy(). Creates a new stage.
> Every wide transformation is a potential performance bottleneck. The fewer wide transformations in your pipeline, the faster it runs.
> In my refactoring work, replacing unnecessary self-joins with window functions eliminated two shuffle stages."

**[48–58 sec] — CTA**
> "These are advanced-level answers that will separate you from the pack. Save this. Follow for Q9 and Q10 — Apache Iceberg starts next."

---

**📝 REEL NAME:** Data Skew is KILLING Your Spark Jobs
**📋 CAPTION:**
```
Reel 5/26 — Spark Performance 🚀

Q7: Data Skew — what it is and how to handle it (salting, AQE, broadcast joins)
Q8: Narrow vs Wide Transformations

Data skew cost us hours of pipeline time before I identified it using the Spark UI.
The AQE fix in Spark 3.x is genuinely a game-changer.

Next up: APACHE ICEBERG series starts with Reel 6! 🧊

#DataSkew #PySpark #ApacheSpark #DataEngineer #DataEngineering #BigData #SparkOptimization #InterviewPrep #TechInterview #DataEngineerInterview
```

---
---

# 🎬 REEL 06 — Q9 & Q10
## "Why Apache Iceberg is the Future of Data 🧊"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Futuristic, clean
**📍 Hook Type:** Bold claim hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "I migrated 10 terabytes of pharmaceutical data to Apache Iceberg — and I'll never go back to a traditional data lake. Here's why."

**[3–7 sec]**
> "Question 9 — What is Apache Iceberg and why choose it over a traditional data lake?"

**[7–25 sec]**
> "Apache Iceberg is an open table format that sits on top of cloud storage — like S3 or ADLS — and adds superpowers that raw Parquet files don't have:
> ✅ ACID transactions — no more data corruption from concurrent writes
> ✅ Time travel — query your data at ANY point in the past
> ✅ Schema evolution — add or rename columns without rewriting the dataset
> ✅ Partition evolution — change your partitioning strategy WITHOUT migrating data
> In pharma, time travel alone was worth the migration. FDA audits require you to prove exactly what data looked like when a trial was submitted."

**[25–30 sec]**
> "Question 10 — Explain Iceberg's snapshot model. What's a manifest file?"

**[30–50 sec]**
> "Three levels of metadata — think of it like a Russian doll:
> 🗂️ Snapshot — the state of the table at a point in time. Every write creates a new one.
> 📋 Manifest List — lists all manifest files in this snapshot. Has partition-level stats for pruning.
> 📄 Manifest File — lists the actual data files. Has column-level stats for each file.
> When you query, Iceberg opens the snapshot → prunes partitions using the manifest list → prunes files using column stats → reads ONLY the relevant files.
> This is why Iceberg queries are so fast even on petabyte-scale tables."

**[50–60 sec] — CTA**
> "If your company isn't on Iceberg yet — they will be soon. Follow for Q11 and Q12 — Time Travel and Compaction."

---

**📝 REEL NAME:** Why Apache Iceberg is the Future of Data
**📋 CAPTION:**
```
Reel 6/26 — Apache Iceberg 🧊

Q9: What is Apache Iceberg and why choose it?
Q10: Iceberg's snapshot model — manifest files explained

I've run Iceberg in production for pharmaceutical data pipelines where audit trails aren't optional — they're regulatory requirements.

The metadata architecture is genuinely brilliant once you understand it.

Next: Time Travel queries + Compaction in Reel 7!

#ApacheIceberg #DataLakehouse #DataEngineer #DataEngineering #BigData #AWSS3 #ETL #OpenTableFormat #InterviewPrep #TechInterview
```

---
---

# 🎬 REEL 07 — Q11 & Q12
## "Time Travel Queries + The Small Files Problem 🕐"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Thoughtful, precise
**📍 Hook Type:** "Did you know" hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "With Apache Iceberg, you can query your database as it existed on ANY date in the past. This is called Time Travel — and it saved us during a pharma audit."

**[3–6 sec]**
> "Question 11 — How does time travel work in Iceberg?"

**[6–22 sec]**
> "Every write to an Iceberg table creates a new immutable snapshot with a unique ID and timestamp.
> To query the past, you just say:
> SELECT * FROM patients TIMESTAMP AS OF '2024-06-01'
> Or use a snapshot ID.
> In PySpark — spark.read.option('as-of-timestamp', '1717200000000').table('patients')
> Real-world use: When regulators asked us to prove what patient records existed when a clinical trial was submitted — we ran one query. No archive tables. No data backups. Just Iceberg time travel."

**[22–27 sec]**
> "Question 12 — What is compaction in Iceberg and why is it necessary?"

**[27–48 sec]**
> "Every time you do a micro-batch write or a streaming insert, Iceberg creates a small file. Over time — you accumulate THOUSANDS of small files.
> The problem? Each file needs its own metadata entry, its own S3 API call, its own JVM overhead to open.
> 10,000 files of 1MB each is WAY slower than 100 files of 100MB each.
> The fix is compaction — merging small files into larger ones.
> In Spark SQL: CALL catalog.system.rewrite_data_files('db.table')
> Schedule this as a daily maintenance job. Also run expire_snapshots to clean up old metadata."

**[48–58 sec] — CTA**
> "These Iceberg questions are rare gold in interviews. Most candidates can't answer them. Save this. Follow for Q13 and Q14."

---

**📝 REEL NAME:** Time Travel Queries + The Small Files Problem
**📋 CAPTION:**
```
Reel 7/26 — Apache Iceberg Deep Dive 🕐

Q11: Time Travel in Iceberg — how it works + real pharma audit use case
Q12: Compaction — what it is and why you need it

Time travel queries literally saved us during an FDA audit.
We queried the exact state of patient records at the time of submission — in seconds.

Most Data Engineers know Iceberg exists. Few understand HOW it works. Be the second group. ✅

#ApacheIceberg #TimeTravel #DataLakehouse #DataEngineer #BigData #DataEngineering #ETL #CloudData #InterviewPrep #TechInterview
```

---
---

# 🎬 REEL 08 — Q13 & Q14
## "Iceberg vs Hive + Copy-on-Write vs Merge-on-Read 🏆"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Competitive, energetic
**📍 Hook Type:** Comparison hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Hive was the standard for years. Apache Iceberg is why it's being replaced. Here's the head-to-head."

**[3–6 sec]**
> "Question 13 — How does Iceberg handle schema evolution differently from Hive?"

**[6–22 sec]**
> "In Hive — renaming a column BREAKS your queries. Changing partitions means migrating ALL your data. Schema changes often require rewriting the entire dataset.
> In Iceberg — every column has a unique field ID that never changes, regardless of name or position. You can add, rename, drop, reorder columns — existing files don't need to be rewritten.
> Partition evolution is versioned — old data keeps the old partition spec, new data uses the new one. Iceberg handles both simultaneously during scans. ZERO migration required.
> This is massive in production when business requirements change and you can't afford downtime."

**[22–27 sec]**
> "Question 14 — Copy-on-Write vs Merge-on-Read. When do you use each?"

**[27–50 sec]**
> "Copy-on-Write — every update rewrites the affected files completely. Reads are FAST — no merging needed. But writes are slow for frequent updates. Best for batch workloads with lots of reads and few writes.
> Merge-on-Read — updates write small delta files instead of rewriting base files. Writes are FAST. But reads are slower because they merge base files with delta files at query time. Best for streaming and frequent upserts.
> In my architecture — Curated layer uses CoW (read-heavy). Raw and Trusted use MoR (write-heavy). Compaction converts MoR files back to clean base files on a schedule."

**[50–60 sec] — CTA**
> "This level of Iceberg knowledge will genuinely surprise most interviewers. Save this. Azure and Databricks start next."

---

**📝 REEL NAME:** Iceberg vs Hive + CoW vs MoR
**📋 CAPTION:**
```
Reel 8/26 — Apache Iceberg Advanced 🧊

Q13: Schema evolution in Iceberg vs Hive — why Iceberg wins
Q14: Copy-on-Write vs Merge-on-Read — when to use which

Understanding CoW vs MoR is the difference between a good Iceberg answer and a GREAT one.

I've implemented both in production — one for the Curated layer, one for Raw/Trusted.

Next: Azure & Databricks series begins in Reel 9! ☁️

#ApacheIceberg #DataLakehouse #DataEngineer #BigData #SchemaEvolution #OpenTableFormat #DataEngineering #InterviewPrep #AzureDatabricks #TechInterview
```

---
---

# 🎬 REEL 09 — Q15 & Q16
## "ADF vs Databricks — And Delta Lake vs Iceberg ☁️"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Tech-focused, clean
**📍 Hook Type:** "Which one?" hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Azure Data Factory or Databricks — which one should you use? The answer is BOTH. Here's why."

**[3–6 sec]**
> "Question 15 — What is Azure Data Factory and how does it differ from Databricks?"

**[6–22 sec]**
> "ADF is your ORCHESTRATOR — it's the conductor of the orchestra. It schedules pipelines, moves data between systems using 90+ connectors, monitors runs, and sends alerts. It's low-code and powerful for data movement.
> Databricks is your COMPUTE ENGINE — it's where the heavy lifting happens. PySpark transformations, ML workloads, streaming, complex business logic.
> In my architecture: ADF triggers Databricks jobs at the right time and handles data movement from Oracle and Salesforce into ADLS. Databricks does the transformation. Two tools, one architecture."

**[22–27 sec]**
> "Question 16 — Delta Lake vs Apache Iceberg. What's the difference?"

**[27–48 sec]**
> "Both add ACID transactions and time travel to cloud storage. But here's the key difference:
> Delta Lake — created by Databricks. Best when you're fully in the Azure/Databricks ecosystem. Transaction log in _delta_log. OPTIMIZE and Z-ordering are brilliant.
> Apache Iceberg — created at Netflix. Engine-agnostic — works with Spark, Flink, Trino, Hive, Presto. Better for multi-cloud or multi-engine setups.
> I used Iceberg for our AWS S3 migration because we needed portability. For Azure-only workloads, Delta Lake is often the better choice."

**[48–58 sec] — CTA**
> "If you're going for Azure Data Engineer roles — knowing both answers cold is non-negotiable. Save this. Follow for Q17 and Q18."

---

**📝 REEL NAME:** ADF vs Databricks + Delta Lake vs Iceberg
**📋 CAPTION:**
```
Reel 9/26 — Azure & Databricks ☁️

Q15: Azure Data Factory vs Databricks — roles and differences
Q16: Delta Lake vs Apache Iceberg — when to use which

Both ADF vs Databricks AND Delta Lake vs Iceberg are classic comparison questions.

The real answer? It's never either/or. It's knowing when to use what.

Follow for Q17 & Q18 — Unity Catalog + Synapse Analytics!

#AzureDataFactory #Databricks #DeltaLake #ApacheIceberg #DataEngineer #AzureDataEngineer #DataEngineering #BigData #CloudComputing #InterviewPrep
```

---
---

# 🎬 REEL 10 — Q17 & Q18
## "Unity Catalog + Synapse Analytics — Azure DE Must Know 🔐"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Professional, polished
**📍 Hook Type:** Governance + security angle

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Data access control, audit logs, column masking — this is what Unity Catalog does, and interviewers LOVE asking about it."

**[3–6 sec]**
> "Question 17 — How does Unity Catalog work in Databricks?"

**[6–25 sec]**
> "Unity Catalog is Databricks' unified governance layer. It uses a three-level namespace — catalog dot schema dot table. Like: pharma_prod.trusted.patient_xref.
> It gives you:
> 🔐 Centralized access control — one place for permissions across ALL workspaces
> 🎭 Column-level security — mask PII fields for unauthorized users
> 🔍 Row-level filters — restrict which rows a user can see
> 📈 Automatic lineage — column-level data flow tracking
> 📋 Audit logs — who accessed what, when
> In pharma, Unity Catalog's audit logs and column masking are how you pass compliance reviews."

**[25–30 sec]**
> "Question 18 — Azure Synapse Analytics vs Databricks — when do you choose which?"

**[30–50 sec]**
> "Synapse is Microsoft's all-in-one — it has dedicated SQL pools for warehousing, serverless SQL to query ADLS directly, Spark clusters, AND orchestration — all in one service.
> Choose Synapse when: your team is SQL-native, you need tight Power BI integration, or your budget favors an all-in-one Microsoft solution.
> Choose Databricks when: you have heavy Python/ML workloads, streaming pipelines, multi-cloud needs, or you need the best-in-class Spark experience.
> Both can coexist — I've seen architectures where Synapse handles SQL analytics and Databricks handles ML and streaming."

**[50–60 sec] — CTA**
> "Azure governance questions are rare — knowing them makes you stand out. Save this. Follow for the CI/CD reel next!"

---

**📝 REEL NAME:** Unity Catalog + Synapse Analytics Explained
**📋 CAPTION:**
```
Reel 10/26 — Azure Governance ☁️🔐

Q17: Unity Catalog — what it is and why it matters for governance
Q18: Azure Synapse Analytics vs Databricks — choose the right tool

Unity Catalog is Databricks' answer to enterprise data governance.
Column masking, row filters, audit logs — these aren't nice-to-haves in pharma. They're mandatory.

Next up: CI/CD for Databricks in Reel 11! ⚙️

#UnityCatalog #AzureDatabricks #AzureSynapse #DataGovernance #DataEngineer #DataEngineering #BigData #CloudData #InterviewPrep #TechInterview
```

---
---

# 🎬 REEL 11 — Q19 & Q20
## "CI/CD for Databricks + What is a Data Lakehouse? 🚀"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** DevOps vibes, energetic
**📍 Hook Type:** "Stop doing this" hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "If you're running notebooks directly in production Databricks — stop. Here's how CI/CD for Databricks actually works."

**[3–6 sec]**
> "Question 19 — How would you implement CI/CD for Databricks?"

**[6–25 sec]**
> "Here's the proper flow:
> All notebooks and job definitions go into Git — Azure DevOps or GitHub. Use Databricks Repos to sync.
> On a Pull Request — automated unit tests run using pytest and chispa for PySpark DataFrame assertions.
> On merge to main — deploy to the Dev workspace using the Databricks CLI.
> After QA approval — deploy to Staging.
> Manual gate — deploy to Production.
> For infrastructure as code, use the Databricks Terraform provider — clusters, jobs, secrets, permissions — all version-controlled.
> No clicking in production. No manual notebook runs. That's how you build reliable pipelines."

**[25–30 sec]**
> "Question 20 — What is a Data Lakehouse and how is it different from a warehouse or a lake?"

**[30–50 sec]**
> "Data Lake — cheap storage, flexible, raw files on S3 or ADLS. Problem: no ACID, no schema enforcement, performance is unpredictable.
> Data Warehouse — structured, fast SQL, great for BI. Problem: expensive, rigid schema, can't handle unstructured data.
> Data Lakehouse — the best of both. Files on cheap object storage, but a table format layer — Iceberg, Delta — adds ACID transactions, schema enforcement, and query performance.
> My architecture is a classic Lakehouse: raw files on AWS S3, Apache Iceberg on top, Spark as the compute engine. Warehouse reliability at lake-scale cost."

**[50–60 sec] — CTA**
> "CI/CD for Databricks is a senior-level question. If you can answer it — you're already ahead. Save this. Follow for JDBC and ETL next."

---

**📝 REEL NAME:** CI/CD for Databricks + Data Lakehouse Explained
**📋 CAPTION:**
```
Reel 11/26 — DevOps + Architecture 🚀

Q19: CI/CD pipeline for Databricks notebooks and jobs
Q20: Data Lakehouse vs Data Lake vs Data Warehouse

CI/CD for Databricks is a question that instantly signals seniority.
Most candidates say "we deploy notebooks" — the right answer is: everything in Git, tested, automated.

Next: JDBC ingestion + 99.9% uptime in Reel 12!

#Databricks #CICD #DataLakehouse #DataEngineer #DataEngineering #DevOps #AzureDatabricks #BigData #ETL #InterviewPrep
```

---
---

# 🎬 REEL 12 — Q21 & Q22
## "JDBC Parallelism + How I Achieved 99.9% Data Uptime 💪"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Confident, technical
**📍 Hook Type:** Results-first hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "I architected pipelines that process 10 million records daily with 99.9% data uptime. Here's exactly how."

**[3–6 sec]**
> "Question 21 — What is JDBC and how do you use it for Oracle ingestion?"

**[6–22 sec]**
> "JDBC is the Java standard for connecting to relational databases. Spark has built-in JDBC connectors.
> The BASIC approach reads Oracle data in a single thread — fine for small tables, terrible for millions of records.
> The PRO approach: set partitionColumn, lowerBound, upperBound, and numPartitions.
> This spawns PARALLEL JDBC connections — say 200 — each reading a range of IDs simultaneously.
> What was a 3-hour sequential pull becomes a 5-minute parallel read. That's the kind of answer that gets you an offer."

**[22–27 sec]**
> "Question 22 — How do you achieve 99.9% data uptime in production pipelines?"

**[27–50 sec]**
> "Five layers of reliability:
> 1️⃣ Idempotent ingestion — use MERGE instead of append. Re-runs don't create duplicates.
> 2️⃣ Checkpointing — track watermarks in a control table. On failure, resume from the last checkpoint, not from zero.
> 3️⃣ Retry with exponential backoff — for transient Salesforce API failures.
> 4️⃣ Dead-letter queues — malformed records go to a quarantine table, not the entire pipeline fails.
> 5️⃣ Monitoring and alerting — ADF alerts, row-count assertions, anomaly detection on volumes.
> Each layer covers a different failure mode. Together they give you enterprise-grade uptime."

**[50–60 sec] — CTA**
> "If you're building production pipelines, you need ALL five of these. Save this. Follow for Incremental Loading and Exactly-Once next."

---

**📝 REEL NAME:** JDBC Parallelism + 99.9% Pipeline Uptime
**📋 CAPTION:**
```
Reel 12/26 — Production Engineering 💪

Q21: JDBC parallel ingestion from Oracle — the right way
Q22: How to achieve 99.9% data uptime in production

The JDBC parallelism trick is one of the most impactful optimizations I've applied in my career.

200 parallel connections instead of 1. The math speaks for itself.

Next: Incremental Loading + Exactly-Once Semantics in Reel 13!

#JDBC #OracleDatabase #DataEngineer #DataEngineering #PySpark #BigData #ETL #DataPipeline #Production #InterviewPrep
```

---
---

# 🎬 REEL 13 — Q23 & Q24
## "Incremental Loading + Exactly-Once Semantics ⚡"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Technical precision vibes
**📍 Hook Type:** "The difference between junior and senior" hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Loading full datasets every day is rookie behavior. Here's how senior Data Engineers do it."

**[3–6 sec]**
> "Question 23 — What is incremental loading and how do you implement it in PySpark?"

**[6–25 sec]**
> "Full load = read everything every run. Expensive. Slow. Doesn't scale.
> Incremental load = only process what's NEW or CHANGED since the last run.
> How to implement:
> Track a watermark — the MAX timestamp of your last processed record — in a control table.
> Next run: filter the source for records WHERE updated_at is greater than the watermark.
> Then UPSERT using a MERGE statement — not append.
> MERGE handles new records with INSERT and changed records with UPDATE, in one atomic operation.
> This is how you handle 10 million daily records without running for hours."

**[25–30 sec]**
> "Question 24 — How do you design a pipeline for EXACTLY-ONCE semantics?"

**[30–50 sec]**
> "Exactly-once means: every record is processed and written PRECISELY once. No duplicates. No losses. Even when failures happen.
> The challenge: a job fails after processing but before confirming success. The system retries and re-processes.
> Solutions:
> For batch — use MERGE with a unique business key. Even if processed twice, the second run finds a match and doesn't insert a duplicate.
> For streaming — enable Spark checkpointing. Structured Streaming's write-ahead log tracks offsets. On restart, only unprocessed offsets are replayed.
> Combine idempotent sinks with checkpointing — that's how you guarantee exactly-once in production."

**[50–60 sec] — CTA**
> "Exactly-once semantics is an advanced concept that junior candidates stumble on. Now you won't. Save this. Follow for the SQL optimization reel."

---

**📝 REEL NAME:** Incremental Loading + Exactly-Once Semantics
**📋 CAPTION:**
```
Reel 13/26 — Pipeline Reliability ⚡

Q23: Incremental loading — watermarks, CDC, MERGE in PySpark
Q24: Exactly-once semantics — how to design for it

Full loads don't scale. Idempotent incremental pipelines do.

The watermark + MERGE pattern is what I use in production to handle 10M+ daily records reliably.

Next: The SQL Optimizations That Cut My Pipeline Time by 30% in Reel 14!

#IncrementalLoading #DataEngineer #PySpark #DataEngineering #ETL #BigData #StreamingData #ApacheSpark #InterviewPrep #TechInterview
```

---
---

# 🎬 REEL 14 — Q25 & Q26
## "How I Cut Pipeline Time by 30% + What is MDM? 🎯"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Results-driven, confident
**📍 Hook Type:** Achievement hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "I reduced our end-to-end pipeline execution time by 30%. Here are the exact SQL and Spark optimizations I used."

**[3–6 sec]**
> "Question 25 — What SQL optimizations improved your pipeline performance by 30%?"

**[6–27 sec]**
> "Six optimizations, in order of impact:
> 1️⃣ Partition pruning — filter on partition columns so Iceberg skips entire files before reading.
> 2️⃣ Predicate pushdown — push filters into the JDBC query so less data crosses the network.
> 3️⃣ Column pruning — never SELECT *. Read only the columns you need. Massive for wide Parquet tables.
> 4️⃣ Broadcast joins — small tables under 10MB get broadcast to all executors. No shuffle.
> 5️⃣ Z-ordering — co-locate frequently filtered columns within files. Same query scans 50 files instead of 1000.
> 6️⃣ Eliminate shuffles — window functions instead of self-joins. Saves entire shuffle stages."

**[27–32 sec]**
> "Question 26 — What is MDM and what problem does it solve?"

**[32–52 sec]**
> "MDM stands for Master Data Management. It's about creating ONE authoritative view of a business entity across all your systems.
> Here's the problem it solves:
> The same customer could be 'John Smith' in your CRM, 'J. Smith' in your ERP, and 'JOHN SMITH' in your clinical database.
> Three records. One person. No system knows they're the same.
> This causes duplicate outreach, wrong reporting, compliance failures.
> MDM detects and links these duplicates — and creates a GOLDEN RECORD — one master record that all downstream systems use.
> I built this system at enterprise scale in pharma. The complexity is real."

**[52–62 sec] — CTA**
> "The 30% improvement wasn't magic — it was systematic optimization. Save this checklist. Follow for the Golden Record deep dive next!"

---

**📝 REEL NAME:** 30% Faster Pipelines + What is MDM
**📋 CAPTION:**
```
Reel 14/26 — Optimization + MDM 🎯

Q25: SQL & Spark optimizations that cut pipeline time by 30%
Q26: What is MDM (Master Data Management) and what problem it solves

30% reduction in execution time = real business impact.
These six optimizations are your checklist for every pipeline review.

Next: Golden Record XREF System — How it works in Reel 15!

#SQL #SparkOptimization #MDM #MasterDataManagement #DataEngineer #DataEngineering #BigData #ETL #DataPipeline #InterviewPrep
```

---
---

# 🎬 REEL 15 — Q27 & Q28
## "Golden Record System — How XREF Actually Works 🏆"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Deep-dive, serious
**📍 Hook Type:** "Behind the scenes" hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "I engineered a Golden Record system that unified customer data across 3 enterprise systems. Here's how it works under the hood."

**[3–6 sec]**
> "Question 27 — Explain how a Golden Record XREF system works."

**[6–27 sec]**
> "XREF stands for Cross-Reference. It's a mapping table that links the same entity's IDs across different source systems.
> For example — Golden ID GLD-0001 maps to SF-A12345 in Salesforce, ORA-78901 in Oracle, CLN-00234 in the clinical database.
> How do you build it? Four steps:
> 🔹 Blocking — group records by rough keys like last name and date of birth to avoid comparing every record with every other record.
> 🔹 Matching — apply rules: exact match on national ID, fuzzy match on name using Jaro-Winkler or Levenshtein distance.
> 🔹 Scoring — assign confidence scores to each potential match.
> 🔹 Merge — records above the threshold get linked under one Golden ID."

**[27–32 sec]**
> "Question 28 — What are survivorship rules and how do you implement them in PySpark?"

**[32–52 sec]**
> "Once you've linked duplicates — you need to decide which VALUE wins for each attribute. That's survivorship.
> Common strategies:
> 📅 Most recent — take the value with the latest update timestamp.
> 🏆 Most trusted source — rank your sources: Oracle ERP beats Salesforce beats manual entry.
> ✅ Most complete — prefer non-null over null.
> In PySpark — use a Window function partitioned by golden_id, ordered by source priority and updated timestamp, then row_number to take the top record. Then coalesce to fill gaps from secondary sources.
> Survivorship rules should be documented as business policy — not just code."

**[52–62 sec] — CTA**
> "Golden Record is an advanced MDM topic. Knowing this puts you in the top 5% of candidates. Save this. Follow for pharma-specific survivorship next."

---

**📝 REEL NAME:** Golden Record XREF System Deep Dive
**📋 CAPTION:**
```
Reel 15/26 — MDM & Golden Record 🏆

Q27: How a Golden Record XREF cross-reference system works
Q28: Survivorship rules — strategies and PySpark implementation

I built this at enterprise scale for a pharmaceutical client.

The identity resolution pipeline handles hundreds of millions of records — and the survivorship logic determines which data your entire business trusts.

Next: Pharma-specific survivorship challenges in Reel 16!

#GoldenRecord #MDM #MasterDataManagement #DataEngineer #DataEngineering #PySpark #BigData #ETL #IdentityResolution #InterviewPrep
```

---
---

# 🎬 REEL 16 — Q29 & Q30
## "Pharma Data Conflicts + What is Data Governance? 💊"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Authoritative, domain-specific
**📍 Hook Type:** High stakes hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "In pharmaceutical data, a wrong value isn't just a data quality problem — it can invalidate years of clinical research. Here's how you handle survivorship conflicts."

**[3–6 sec]**
> "Question 29 — How do you handle survivorship conflicts in pharmaceutical MDM?"

**[6–25 sec]**
> "Pharma has a layered approach:
> 🔴 Regulatory fields — patient IDs, drug codes, study IDs — one authoritative source, defined by policy. No fuzzy logic. The designated system ALWAYS wins.
> 🟡 Confidence-scored fields — contact info, addresses — rank sources by historical accuracy. Apply Bayesian scoring to pick the winner.
> 🟠 Conflict flagging — when sources disagree beyond a threshold, don't silently pick a winner. Write a conflict record to a review queue for human stewards.
> 🟢 Audit trail — ALWAYS preserve which source contributed which value. With Iceberg time travel, you can reconstruct the exact golden record state during any regulatory audit."

**[25–30 sec]**
> "Question 30 — What is data governance and why is it critical in pharmaceutical environments?"

**[30–50 sec]**
> "Data governance is the set of policies, processes, and standards that ensure data is accurate, secure, and used appropriately.
> In pharma — it's not optional. Three regulations make it mandatory:
> 📜 FDA 21 CFR Part 11 — electronic records must have audit trails, access controls, and validation.
> 🔒 GDPR and HIPAA — patient data is sensitive. Every access must be logged.
> 🧪 GxP compliance — data integrity controls are required throughout the entire data lifecycle.
> A governance failure in pharma doesn't just mean bad data — it can invalidate a drug approval submission worth billions."

**[50–60 sec] — CTA**
> "Domain knowledge like this — combining data engineering with pharmaceutical compliance — is what makes you irreplaceable. Save this. Follow for Modak Nabu next."

---

**📝 REEL NAME:** Pharma MDM Conflicts + Data Governance
**📋 CAPTION:**
```
Reel 16/26 — MDM + Data Governance 💊

Q29: Survivorship conflicts in pharmaceutical MDM — layered approach
Q30: What is data governance and why pharma can't afford to ignore it

Working in pharma taught me that data quality isn't just a best practice.
It's a regulatory requirement with real consequences.

FDA 21 CFR Part 11. GxP compliance. These aren't just buzzwords — they define how you architect systems.

Next: Modak Nabu + Data Lineage in Reel 17!

#DataGovernance #Pharma #MDM #DataEngineer #DataEngineering #FDA #HIPAA #Compliance #BigData #InterviewPrep
```

---
---

# 🎬 REEL 17 — Q31 & Q32
## "Modak Nabu + Data Lineage — Governance in Practice 🔍"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Analytical, methodical
**📍 Hook Type:** Tool spotlight hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "I reduced data quality errors by 25% using a governance platform. Here's what Modak Nabu does and how data lineage works in practice."

**[3–6 sec]**
> "Question 31 — What is Modak Nabu and how did you use it?"

**[6–25 sec]**
> "Modak Nabu is an enterprise data governance and quality management platform. It lets you define, enforce, and monitor data quality rules as CODE — not just documentation.
> What I used it for:
> ✅ Rule-based validation — completeness checks, uniqueness checks, referential integrity, range checks — all programmatic.
> 📊 Profiling — automatic null rates, cardinality, min/max distribution detection.
> 📈 Quality scoring — each dataset gets a score. Below 95%? Pipeline doesn't promote to the next Medallion layer.
> 📋 Audit logging — every validation run is logged with results and timestamps — ready for regulatory review.
> The 25% reduction in data quality errors came from catching issues at the Raw-to-Trusted boundary — not after they'd polluted the Curated layer."

**[25–30 sec]**
> "Question 32 — How do you implement data lineage?"

**[30–50 sec]**
> "Data lineage tracks where data came from, how it was transformed, and where it went.
> Four approaches:
> 1️⃣ Metadata-level lineage — tools like OpenLineage or Unity Catalog parse your Spark and SQL execution plans and automatically capture lineage.
> 2️⃣ Column-level lineage — Unity Catalog tracks exactly which columns derived from which source columns.
> 3️⃣ Medallion Architecture as lineage — Raw -> Trusted -> Curated is itself a lineage structure. You always know which layer to trace back to.
> 4️⃣ Control tables — custom logging of job run IDs, source files, record counts, and transformation parameters.
> The goal: when a business user asks 'where did this number come from?' — you can answer in minutes, not days."

**[50–60 sec] — CTA**
> "Data governance and lineage are what separate engineering from DATA ENGINEERING. Save this. Column-level security is next."

---

**📝 REEL NAME:** Modak Nabu + Data Lineage in Practice
**📋 CAPTION:**
```
Reel 17/26 — Data Governance Tools 🔍

Q31: Modak Nabu — capabilities and how to use it for governance
Q32: Data lineage — how to implement it in a modern data platform

25% reduction in data quality errors.
That's what programmatic governance rules give you over documentation-only approaches.

Next: Column-Level Security in Databricks in Reel 18! 🔐

#DataGovernance #DataLineage #ModakNabu #DataEngineer #DataEngineering #DataQuality #Databricks #BigData #ETL #InterviewPrep
```

---
---

# 🎬 REEL 18 — Q33 & Q34
## "Column-Level Security + Tell Me About Yourself 🔐"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Confident shift — technical to personal
**📍 Hook Type:** Dual hook — security + career

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Column masking in Databricks means a doctor sees a patient's real date of birth — and an unauthorized analyst sees 1900-01-01. Let me show you exactly how."

**[3–6 sec]**
> "Question 33 — Column-level security for pharmaceutical data in Databricks."

**[6–25 sec]**
> "Unity Catalog gives you two tools:
> First — column masking. You create a function that checks the user's group membership and returns either the real value or a masked value. Then you attach it to the column with ALTER TABLE.
> So: clinical researchers see the actual date of birth. Everyone else sees 1900-01-01. Dynamic. No query changes needed from the consumer.
> Second — row filters. Same idea but for rows. A user in the EMEA group only sees EMEA patient records. You attach the filter to the table — it applies automatically to every query.
> No code changes in downstream tools. No user education needed. Governance is enforced at the data layer."

**[25–30 sec]**
> "Now switching to the behavioral round. Question 34 — Tell me about yourself."

**[30–50 sec]**
> "Here's the structure that works:
> 🎯 Open with your current role and headline — 'I'm a Data Engineer at [company] with X years of experience in [core skills].'
> 🏆 Name your signature achievement — the one result you're most proud of.
> 🔧 Cover your technical pillars — 2-3 technologies you're expert in.
> 🚀 Close with your direction — where you're headed and why this role aligns.
> Under 90 seconds. Confident. No reading from your resume.
> The best 'tell me about yourself' answers feel like a confident story — not a CV recitation."

**[50–60 sec] — CTA**
> "We're moving into the behavioral round now. Save this — both the technical and behavioral answers matter. Follow for debugging under pressure next!"

---

**📝 REEL NAME:** Column Security + Tell Me About Yourself
**📋 CAPTION:**
```
Reel 18/26 — Governance + Behavioral 🔐

Q33: Column-level security and row filters in Databricks Unity Catalog
Q34: How to answer "Tell me about yourself" as a Data Engineer

The behavioral round trips up technical candidates more often than the coding round.

A strong "tell me about yourself" is a 90-second story — not a CV walkthrough.

Next: Debugging under pressure + Why I'm leaving Cognizant in Reel 19!

#UnityCatalog #DataGovernance #DataEngineer #BehavioralInterview #InterviewPrep #DataEngineering #Databricks #TechInterview #CareerAdvice #TechIndia
```

---
---

# 🎬 REEL 19 — Q35 & Q36
## "Why Leave Your Job? + Debugging Under Pressure 🔥"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Storytelling, genuine
**📍 Hook Type:** Real talk hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "The 'why are you leaving?' question is a trap for most candidates. Here's how to answer it without sounding negative — and how to handle a production crisis."

**[3–6 sec]**
> "Question 35 — Why do you want to leave your current company?"

**[6–22 sec]**
> "Three rules for this answer:
> Rule 1 — Never criticize your employer. Never.
> Rule 2 — Frame it as seeking growth, not escaping problems.
> Rule 3 — Connect your answer to THEIR company specifically.
> Sound like: 'My current role gave me a strong foundation — I led enterprise migrations and built MDM systems I'm proud of. But I'm ready for an environment where data engineering is core to the product, not a client delivery model. Your company's focus on [specific tech or domain] is exactly where I want to grow.'
> Research the company. Make the last sentence specific. Generic answers get filtered."

**[22–27 sec]**
> "Question 36 — Describe debugging a failing production pipeline under pressure."

**[27–50 sec]**
> "Here's the STAR answer structure:
> Situation: Our overnight pipeline processing 10M Oracle records failed silently. Downstream teams reported stale data.
> Task: Identify root cause and restore data continuity without losing records.
> Action: Opened the Spark UI. Found OOM exception in the JDBC stage — data volume had spiked 3x due to a source system backfill. Increased JDBC partitions to distribute load. Added a checkpoint so the re-run resumed from the failure point — not from zero.
> Result: Pipeline restored in 2 hours. Added row-count monitoring so we alert BEFORE failure, not after.
> The key: systematic debugging using the Spark UI, not guessing."

**[50–60 sec] — CTA**
> "Behavioral answers need structure AND specificity. Vague stories don't land. Specific ones do. Save this. Follow for the 'Owning It' award story next."

---

**📝 REEL NAME:** Why Leave? + Debugging Under Pressure
**📋 CAPTION:**
```
Reel 19/26 — Behavioral Round 🎯

Q35: "Why do you want to leave?" — how to answer without sounding negative
Q36: Debugging a failing production pipeline — STAR method answer

The behavioral round isn't softer than the technical round.
It's just evaluated differently.

Vague answers kill candidacies that strong technical skills built.
Be specific. Use STAR. Name the numbers.

Next: The 'Owning It' award story + Python automation in Reel 20!

#BehavioralInterview #InterviewPrep #DataEngineer #TechInterview #CareerAdvice #DataEngineering #STAR #JobSearch #TechIndia #CrackTheInterview
```

---
---

# 🎬 REEL 20 — Q37 & Q38
## "'Owning It' Award + 15 Hours Saved With Python 🏅"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Motivated, proud
**📍 Hook Type:** Award story hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "I won Cognizant's 'Owning It' award in 2025. Not for writing the best code — but for doing THIS."

**[3–6 sec]**
> "Question 37 — What did you do to earn the 'Owning It' award?"

**[6–25 sec]**
> "Late in a sprint, we discovered a critical data quality issue in the Curated layer — affecting 15% of patient records — right before a regulatory data submission deadline. The formal process would have pushed the release to next quarter.
> I took OWNERSHIP. Stayed through the weekend. Traced the issue through all three Medallion layers. Found the root cause — a survivorship rule conflict in the Golden Record system. Fixed it. Ran a full regression.
> The release went out on time.
> That's what 'owning it' means — not just completing your assigned tasks — but treating the OUTCOME as your personal responsibility.
> Interviews love this kind of story because it shows leadership without a title."

**[25–30 sec]**
> "Question 38 — How did you eliminate 15+ hours of manual setup every week using Python?"

**[30–50 sec]**
> "Every new pipeline environment required manually creating SQL configuration files — table schemas, column mappings, partition specs. With 30+ tables and 3 environments — it was eating the entire sprint.
> I built a Python code generator. Engineers fill in a YAML config file with the table schema. The generator reads it and produces all the SQL DDL and pipeline config files automatically.
> Used Jinja2 for SQL templating. Used Pydantic to validate the YAML before generating anything.
> What took 3-4 hours per table became a 5-minute YAML edit and one command.
> 15+ hours per week recovered. Zero configuration drift between environments."

**[50–60 sec] — CTA**
> "Automation isn't just about saving time — it's about removing human error from repetitive work. Save this. Follow for data quality and stakeholder communication next."

---

**📝 REEL NAME:** Owning It Award + 15 Hours Saved With Python
**📋 CAPTION:**
```
Reel 20/26 — Achievement Stories 🏅

Q37: The story behind my 'Owning It' award at Cognizant
Q38: How I automated 15+ hours/week of manual setup with Python + Jinja2 + Pydantic

The award wasn't for writing clever code.
It was for treating a business outcome as a personal responsibility — on a tight deadline — in a regulated environment.

That's the story. That's the answer.

Next: Ensuring data quality + stakeholder communication in Reel 21!

#DataEngineer #CareerStories #Python #Automation #DataEngineering #InterviewPrep #BehavioralInterview #CognizantLife #TechIndia #OwningIt
```

---
---

# 🎬 REEL 21 — Q39 & Q40
## "Data Quality Framework + Stakeholder Disaster Script 📊"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Problem-solving energy
**📍 Hook Type:** "3 hours late" crisis hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "A stakeholder tells you: your pipeline is 3 hours late and my report is wrong. What do you do? Here's the exact playbook."

**[3–6 sec]**
> "Question 39 — How do you ensure data quality in your pipelines?"

**[6–25 sec]**
> "My five-layer quality framework:
> 1️⃣ Prevention at ingestion — schema enforcement at the Raw layer. Bad records fail loudly, not silently.
> 2️⃣ Validation between layers — at every Medallion transition: null rate checks, uniqueness checks, referential integrity, row count consistency within ±10% of yesterday.
> 3️⃣ Automated rules — in Modak Nabu. Failures block pipeline promotion to the next layer.
> 4️⃣ Monitoring — dashboards showing data freshness and quality scores. Alerts on deviations.
> 5️⃣ Incident loop — every quality issue in production triggers root cause analysis and a new rule to prevent recurrence.
> Quality is not a step at the end. It's built into every layer."

**[25–30 sec]**
> "Question 40 — A stakeholder says your pipeline is 3 hours late. How do you diagnose it?"

**[30–52 sec]**
> "Four-step systematic approach:
> Step 1 — Define the baseline. Is this new or always slow? Pull the last 30 run times to find when degradation started.
> Step 2 — Identify the bottleneck layer. ADF monitoring → Databricks job UI → Spark UI. Zero in on the exact stage and task that's slow.
> Step 3 — Common culprits: Data volume growth. Data skew. Small files accumulation. Missing partition pruning. Resource contention.
> Step 4 — Short-term mitigation while you fix properly. Run the job earlier. Scale the cluster. Communicate the timeline to stakeholders.
> Never guess at the cause. Use the tools — Spark UI and ADF monitoring — to pinpoint it."

**[52–62 sec] — CTA**
> "The diagnostic approach matters as much as the technical fix. Save this framework. Follow for schema drift and Salesforce API next."

---

**📝 REEL NAME:** Data Quality Framework + Pipeline Crisis Diagnosis
**📋 CAPTION:**
```
Reel 21/26 — Quality + Crisis Management 📊

Q39: End-to-end data quality framework — 5 layers
Q40: "Your pipeline is 3 hours late" — how to diagnose and respond

Quality is built in at every layer. It's not a final check.

The diagnosis framework — ADF → Databricks job UI → Spark UI — is the difference between random guessing and systematic problem-solving.

Next: Duplicate records + stakeholder communication in Reel 22!

#DataQuality #DataEngineer #DataEngineering #PySpark #ETL #BigData #DataPipeline #InterviewPrep #TechInterview #Production
```

---
---

# 🎬 REEL 22 — Q41 & Q42
## "Handle Duplicates Like a Pro + Talk Tech to Non-Tech 🎯"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Practical, communication-focused
**📍 Hook Type:** Both technical and soft-skill hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Your source system is sending duplicate records. Your manager wants an update in plain English. Two problems — two skills. Let's solve both."

**[3–6 sec]**
> "Question 41 — How do you handle duplicate records from a source system?"

**[6–27 sec]**
> "Step 1 — Detect first. Use a Window function, partitioned by your business key, ordered by updated timestamp. Row number 1 is the winner.
> Step 2 — Define the deduplication key CAREFULLY. It's often not just the primary key. For events — it might be entity ID + event type + event timestamp. Consult business stakeholders. Never assume.
> Step 3 — Upsert using MERGE. Not append. MERGE handles the deduplication at write time — if the record exists, update it. If not, insert it.
> Step 4 — Track and report duplicate counts per run in a metrics table. A sudden spike signals a source system issue that needs escalation.
> Step 5 — True conflicts — same key, different values — go to a quarantine table for human review. Never silently pick one."

**[27–32 sec]**
> "Question 42 — How do you communicate technical decisions to non-technical stakeholders?"

**[32–52 sec]**
> "Four principles:
> 🎯 Outcome first. 'This change will make your reports update 3 hours earlier' lands better than 'we switched to incremental ETL with watermarking.'
> 🧠 Use analogies. 'Iceberg time travel is like Google Docs version history for your data.'
> ⚠️ Risk in plain language. 'If we don't run compaction, queries will be twice as slow within 3 months.'
> ⚖️ Decision framing. Don't just say what you recommend — show the options and trade-offs. Let stakeholders make informed decisions.
> The best technical communicators make complexity disappear. They don't show off — they clarify."

**[52–62 sec] — CTA**
> "Communication is a technical skill. Save this. Follow for Salesforce API and schema drift next."

---

**📝 REEL NAME:** Handle Duplicates + Communicate Tech to Non-Tech
**📋 CAPTION:**
```
Reel 22/26 — Engineering + Communication 🎯

Q41: How to handle duplicate records from source systems — MERGE, deduplication, quarantine
Q42: How to communicate technical decisions to non-technical stakeholders

The duplicate handling is technical. The communication framework is what keeps your stakeholders informed and your career growing.

Both are skills. Both need practice.

Next: Salesforce API integration + schema drift in Reel 23!

#DataEngineer #DataEngineering #Stakeholder #Communication #PySpark #ETL #BigData #TechSoftSkills #InterviewPrep #TechIndia
```

---
---

# 🎬 REEL 23 — Q43 & Q44
## "Salesforce API + Schema Drift — Production Nightmares 😅"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Battle-tested, experienced
**📍 Hook Type:** "This will happen to you" hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Two things that WILL happen in every production data engineering career: integrating Salesforce APIs and dealing with schema drift. Here's how to handle both."

**[3–6 sec]**
> "Question 43 — What's your experience with Salesforce API integration?"

**[6–25 sec]**
> "Salesforce has four main APIs: REST, Bulk 2.0, SOAP, and Streaming.
> For ingestion into your Lakehouse — use the Bulk API 2.0. It's built for millions of records — creates asynchronous batch jobs that Salesforce processes server-side. Fast. Scalable.
> For outbound sync — Lakehouse back to Salesforce — use the REST API's upsert endpoint with composite requests. Use the External ID field to match your Golden Record IDs to Salesforce records.
> Three things to never forget: rate limits — Salesforce has daily API call limits. Authentication — use OAuth 2.0 JWT Bearer for server-to-server. Error handling — Salesforce returns per-record success and failure in the response. Handle partial failures."

**[25–30 sec]**
> "Question 44 — How do you handle schema drift?"

**[30–50 sec]**
> "Schema drift = a source system silently changes its schema. A column is renamed. A new column appears. A type changes. Without protection — your pipeline fails or writes garbage.
> My approach:
> First — detect drift in code. Compare expected columns vs actual columns on every run. New columns → log and notify. Missing required columns → fail loudly.
> Second — handle safely. New columns go through Iceberg schema evolution — no rewrite needed. Removed required columns → pipeline fails and alerts. Type narrowing → fail.
> Third — data contracts. Maintain written agreements with source system owners. Breaking changes require advance notice. No surprises in production."

**[50–60 sec] — CTA**
> "Schema drift is a production reality, not an edge case. Having a detection strategy before it happens is what separates reactive engineers from proactive ones. Save this. Follow for the zero-downtime migration masterclass next!"

---

**📝 REEL NAME:** Salesforce API + Schema Drift Handling
**📋 CAPTION:**
```
Reel 23/26 — Production Engineering 😅

Q43: Salesforce API integration — Bulk API 2.0, REST, OAuth 2.0
Q44: Schema drift — detection, safe handling, and data contracts

Schema drift doesn't announce itself. One day your pipeline just starts failing.

The fix isn't reactive debugging — it's proactive detection and data contracts.

Next: Zero-downtime 10TB migration strategy in Reel 24!

#Salesforce #SalesforceAPI #DataEngineer #SchemaDrift #DataEngineering #ETL #BigData #DataPipeline #InterviewPrep #TechInterview
```

---
---

# 🎬 REEL 24 — Q45 & Q46
## "Zero Downtime 10TB Migration + Maintainable PySpark Code 🚀"

**⏱ Duration:** 60–70 seconds
**🎵 Music Vibe:** Epic, achievement-focused
**📍 Hook Type:** Scale hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Migrating 10 terabytes of data with ZERO downtime. No service interruption. No data loss. I did this in production. Here's the exact strategy."

**[3–6 sec]**
> "Question 45 — Walk me through a zero-downtime 10TB migration strategy."

**[6–28 sec]**
> "Five phases:
> Phase 1 — Parallel run. Stand up the new Iceberg system alongside the legacy system. Begin backfilling historical data in the background. Legacy remains the system of record.
> Phase 2 — Incremental catch-up. Once history is loaded, switch to incremental mode — only process records changed since the backfill started. Run BOTH systems and compare outputs.
> Phase 3 — Cutover preparation. Document the procedure with rollback steps. Test on staging. Communicate to ALL downstream consumers.
> Phase 4 — Cutover window. Freeze writes to legacy. Process the final delta into Iceberg. Switch the read pointer — update connection strings in ADF. Run smoke tests. Keep legacy in read-only mode for 2 weeks as a rollback safety net.
> Phase 5 — Decommission after 2-4 weeks of stable operation."

**[28–33 sec]**
> "Question 46 — What's your approach to writing maintainable PySpark code?"

**[33–55 sec]**
> "Six principles I follow in every pipeline:
> 1️⃣ Modular design — small single-responsibility functions. Each function does ONE thing: read, transform, validate, or write.
> 2️⃣ Configuration over hardcoding — table names, paths, thresholds in YAML config files, not buried in code.
> 3️⃣ Type annotations and docstrings — future you will thank present you.
> 4️⃣ Unit tests — use chispa for DataFrame equality assertions in pytest. Mock external dependencies.
> 5️⃣ Meaningful naming — df_raw_patients_oracle tells you everything. df1 tells you nothing.
> 6️⃣ Idempotency — every pipeline must be safely re-runnable. No append-only operations without deduplication."

**[55–65 sec] — CTA**
> "The migration strategy is senior-level knowledge. The code quality principles are what make your work maintainable long after you leave. Save both. Follow for streaming and Z-ordering next!"

---

**📝 REEL NAME:** Zero Downtime Migration + Maintainable PySpark Code
**📋 CAPTION:**
```
Reel 24/26 — Migration + Code Quality 🚀

Q45: Zero-downtime 10TB migration strategy — 5 phases
Q46: Writing maintainable PySpark code — 6 principles

A migration of this scale with zero downtime requires planning, not heroics.

The parallel-run + incremental catch-up approach is how you de-risk a 10TB move.

Next: Real-time streaming on Azure + Z-ordering in Reel 25!

#DataMigration #PySpark #DataEngineer #DataEngineering #ApacheIceberg #BigData #ETL #ZeroDowntime #InterviewPrep #TechInterview
```

---
---

# 🎬 REEL 25 — Q47 & Q48
## "Real-Time Streaming on Azure + Z-Ordering Explained ⚡"

**⏱ Duration:** 55–65 seconds
**🎵 Music Vibe:** Fast-paced, modern
**📍 Hook Type:** Real-time tech hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "Batch pipelines run overnight. Real-time streaming runs NOW. Here's how to design a streaming pipeline on Azure — the right way."

**[3–6 sec]**
> "Question 47 — Design a real-time streaming pipeline on Azure for pharmaceutical data."

**[6–27 sec]**
> "The architecture stack:
> Source events → Azure Event Hubs (Kafka-compatible ingestion buffer) → Databricks Structured Streaming → Delta Lake on ADLS → downstream consumers.
> In Structured Streaming:
> Read from Event Hubs, parse the JSON payload with a defined schema, write to Delta Lake with append mode, micro-batch trigger every 1 minute, checkpoint to ADLS so failures resume from the right offset.
> Governance considerations in pharma:
> Validate schema before writing — reject malformed events to a dead-letter queue.
> Use Unity Catalog for your Delta tables — lineage and access control apply to streaming tables too.
> Watermarking for late data — withWatermark of 10 minutes handles events that arrive delayed."

**[27–32 sec]**
> "Question 48 — What is Z-ordering in Delta Lake and when should you use it?"

**[32–52 sec]**
> "Z-ordering is a multi-dimensional clustering technique. It co-locates records with similar values in the same files using a Z-curve algorithm.
> The command: OPTIMIZE table ZORDER BY (region, trial_id)
> What it does: instead of scanning 1000 files to answer a query filtering by region and trial_id — Spark reads the per-file column statistics and scans only 50 relevant files.
> When to use it: columns frequently in WHERE filters but too high-cardinality to partition on. Columns in join conditions.
> When NOT to: don't run it in the hot path. It rewrites files. Schedule it as a weekly maintenance job alongside compaction."

**[52–62 sec] — CTA**
> "Streaming and Z-ordering are advanced — most candidates don't go here. You just did. Two final questions in the last reel tomorrow. Follow so you don't miss it!"

---

**📝 REEL NAME:** Real-Time Streaming on Azure + Z-Ordering
**📋 CAPTION:**
```
Reel 25/26 — Streaming + Optimization ⚡

Q47: Real-time streaming pipeline on Azure — Event Hubs + Structured Streaming + Delta Lake
Q48: Z-ordering in Delta Lake — what it is and when to use it

Streaming is the future of data pipelines.
Understanding Event Hubs + Structured Streaming + Unity Catalog governance = a complete answer.

FINAL REEL TOMORROW — Q49 & Q50: Capacity Planning + Future of Data Engineering! 🚀

#AzureStreaming #EventHubs #StructuredStreaming #DeltaLake #Databricks #DataEngineer #DataEngineering #BigData #InterviewPrep #ZOrdering
```

---
---

# 🎬 REEL 26 — Q49 & Q50
## "FINAL REEL — Capacity Planning + Future of Data Engineering 🎓"

**⏱ Duration:** 60–70 seconds
**🎵 Music Vibe:** Triumphant, closing energy
**📍 Hook Type:** Series finale hook

---

### 🎙️ SCRIPT

**[0–3 sec] — HOOK**
> "This is the FINAL reel. 50 questions. 26 reels. We finish strong — with the two questions that show interviewers you think like a senior engineer."

**[3–6 sec]**
> "Question 49 — How do you approach capacity planning for a Lakehouse processing 10M records daily?"

**[6–27 sec]**
> "Three areas: Storage, Compute, and Cost.
> Storage — 10M records × 500 bytes per row = about 5GB raw data. After Parquet compression — 500MB to 1GB per day. With 3 Medallion layers and 1-year retention — budget around 1 to 2TB per year for data. Metadata overhead is typically 1 to 5% on top.
> Compute — profile your Spark jobs today. Measure CPU, memory, and shuffle bytes. Then apply your expected growth rate — if volume grows 20% per year, your cluster must handle 1.5 to 2x current load. Use Databricks autoscaling for peak loads like month-end reporting.
> Cost — use spot instances for ETL workloads. 70 to 90% cheaper than on-demand. Archive old Iceberg snapshots to S3 Glacier after your retention period."

**[27–33 sec]**
> "Question 50 — Where is Data Engineering going in the next 3 to 5 years?"

**[33–55 sec]**
> "Four trends that are reshaping the field:
> 1️⃣ AI-native pipelines — Data Engineers will build for LLM training, RAG, and feature stores. Vector databases become a core skill.
> 2️⃣ Streaming-first — the overnight batch job is becoming the exception. Real-time micro-batch becomes the default.
> 3️⃣ Declarative engineering — dbt and data mesh shift the focus from 'how to move data' to 'what the data should look like.'
> 4️⃣ Data contracts — formal, versioned producer-consumer agreements. Breaking changes go through a review process.
> How am I preparing? My Iceberg expertise is already aligned with the open table format future. Next focus — vector storage, LLM feature engineering, and unstructured data pipelines. That's what AI-Ready on my resume means."

**[55–68 sec] — SERIES CLOSE + CTA**
> "That's it. 50 Data Engineer interview questions. Covered across PySpark, Iceberg, Azure, ETL, MDM, Governance, and Behavioral.
> If you found value in this series — share it with one person who's preparing for a Data Engineer interview. Save every reel. And drop a comment: which question surprised you the most?
> Follow me — I'll keep posting. This is just the beginning. 🚀"

**[ON SCREEN TEXT:]**
> "50 Questions ✅ | 26 Reels ✅ | Your interview is ready 🎯"

---

**📝 REEL NAME:** FINALE — Capacity Planning + Future of Data Engineering
**📋 CAPTION:**
```
🎓 Reel 26/26 — SERIES COMPLETE

Q49: Capacity planning for a 10M records/day Data Lakehouse
Q50: Where is Data Engineering going in the next 3-5 years?

That's a wrap on 50 Data Engineer interview questions across 26 reels.

If this series helped you — share it with one person preparing for a Data Engineer role.

📌 Save this reel. Save the full series. Use it for your prep.

Drop a comment: which question was the most useful for you? 👇

Thank you for following along. More content coming — this was just the beginning. 🚀

#DataEngineer #DataEngineering #InterviewPrep #PySpark #ApacheIceberg #AzureDatabricks #BigData #DataLakehouse #TechInterview #TechIndia #CrackTheInterview #DataCareer #50Questions #SeriesComplete
```

---

---

# 📊 SERIES OVERVIEW — QUICK REFERENCE

| Reel | Questions | Topic |
|------|-----------|-------|
| 01 | Intro | About Mayuresh Patil |
| 02 | Q1, Q2 | Transformation vs Action, Shuffle |
| 03 | Q3, Q4 | repartition vs coalesce, 10M record tuning |
| 04 | Q5, Q6 | Medallion Architecture, Catalyst Optimizer |
| 05 | Q7, Q8 | Data Skew, Narrow vs Wide |
| 06 | Q9, Q10 | What is Iceberg, Snapshot Model |
| 07 | Q11, Q12 | Time Travel, Compaction |
| 08 | Q13, Q14 | Schema Evolution, CoW vs MoR |
| 09 | Q15, Q16 | ADF vs Databricks, Delta vs Iceberg |
| 10 | Q17, Q18 | Unity Catalog, Synapse vs Databricks |
| 11 | Q19, Q20 | CI/CD Databricks, What is Lakehouse |
| 12 | Q21, Q22 | JDBC Ingestion, 99.9% Uptime |
| 13 | Q23, Q24 | Incremental Loading, Exactly-Once |
| 14 | Q25, Q26 | 30% Optimization, What is MDM |
| 15 | Q27, Q28 | Golden Record XREF, Survivorship Rules |
| 16 | Q29, Q30 | Pharma Conflicts, Data Governance |
| 17 | Q31, Q32 | Modak Nabu, Data Lineage |
| 18 | Q33, Q34 | Column Security, Tell Me About Yourself |
| 19 | Q35, Q36 | Why Leave, Debug Under Pressure |
| 20 | Q37, Q38 | Owning It Award, Python Automation |
| 21 | Q39, Q40 | Data Quality, Pipeline Crisis |
| 22 | Q41, Q42 | Handle Duplicates, Non-Tech Communication |
| 23 | Q43, Q44 | Salesforce API, Schema Drift |
| 24 | Q45, Q46 | Zero Downtime Migration, Maintainable Code |
| 25 | Q47, Q48 | Azure Streaming, Z-Ordering |
| 26 | Q49, Q50 | Capacity Planning, Future of DE |

---
*Script written for Mayuresh Patil — Data Engineer | PySpark & Apache Iceberg | AI-Ready*
