# Kafka in 10 Hours — Roadmap
### For a Data Engineer with ~3 yrs PySpark/Databricks/Delta/Iceberg batch experience

**Goal:** Explain Kafka confidently in interviews + build a real Kafka → PySpark → Delta/Iceberg streaming pipeline.

**Ground rule:** everything is taught by mapping onto what you already know — Spark, batch ETL, Medallion architecture, partitions, shuffle. No "what is a variable" energy.

| Hour | Topic | Priority |
|---|---|---|
| 1 | Kafka fundamentals — what/why, architecture, core vocabulary | ⭐⭐⭐⭐⭐ |
| 2 | Topics, Partitions, Offsets, Replication | ⭐⭐⭐⭐⭐ |
| 3 | Producers — internals, config, delivery guarantees, Python code | ⭐⭐⭐⭐⭐ |
| 4 | Consumers, Consumer Groups, rebalancing, lag | ⭐⭐⭐⭐⭐ |
| 5 | Kafka internals & reliability — replication, ISR, delivery semantics | ⭐⭐⭐⭐ |
| 6 | Hands-on: local Kafka via Docker/KRaft + Python producer/consumer | ⭐⭐⭐⭐ |
| 7 | Spark Structured Streaming concepts (readStream/writeStream, checkpoints) | ⭐⭐⭐⭐⭐ |
| 8 | End-to-end: Kafka → Spark Structured Streaming → Delta/Iceberg | ⭐⭐⭐⭐⭐ |
| 9 | Production patterns — topic design, DLQ, CDC, Debezium, Schema Registry | ⭐⭐⭐⭐⭐ |
| 10 | Interview drill — Level 1 to 4, scenario/system-design questions | ⭐⭐⭐⭐⭐ |

**Format each hour follows:**
1. Concept teaching (what / why it exists / how it works / real example / interview Q)
2. ASCII diagram where useful
3. Explicit Kafka ↔ Spark/batch analogy
4. Practical exercise
5. Hour recap: 5 key concepts, 5 interview Qs, 1 hands-on exercise, what to revise

**Pace:** one hour-file at a time, delivered as a separate `.md`, so nothing gets dumped in one giant wall of text. I'll check your understanding before moving to the next one.

Say **"next"** (or ask questions on Hour 1 first) when you're ready for Hour 2.
