# Apache Iceberg / Delta / Lakehouse – Interview Notes

# 1. Delta vs Iceberg vs Hudi

| Feature              | Delta Lake           | Apache Iceberg              | Apache Hudi             |
| -------------------- | -------------------- | --------------------------- | ----------------------- |
| Developed By         | Databricks           | Netflix                     | Uber                    |
| Best For             | Databricks ecosystem | Open lakehouse architecture | Fast ingestion & CDC    |
| Metadata Handling    | Transaction log      | Metadata tree structure     | Timeline-based metadata |
| Query Engine Support | Strong in Databricks | Multi-engine support        | Good streaming support  |
| Hidden Partitioning  | Limited              | Yes                         | Partial                 |
| Streaming Support    | Good                 | Good                        | Excellent               |

---

## Interview Answer

> “Delta Lake is tightly integrated with Databricks, Iceberg is preferred for open multi-engine lakehouse architectures, and Hudi is optimized for streaming ingestion and CDC workloads.”

---

# 2. How does Iceberg handle metadata?

> “Iceberg stores metadata separately using metadata files, manifest files, and snapshot information. Instead of scanning all files, query engines use metadata pruning to identify only relevant files, improving query performance.”

---

## Key Components

* Metadata file
* Manifest list
* Manifest files
* Data files
* Snapshots

---

## Important Benefit

> “Iceberg reduces file scanning using metadata-based file pruning.”

---

# 3. Table Formats vs File Formats

## File Formats

> “File formats define how data is physically stored.”

Examples:

* Parquet
* ORC
* Avro

---

## Table Formats

> “Table formats manage metadata, transactions, schema evolution, and ACID operations on top of data files.”

Examples:

* Iceberg
* Delta Lake
* Hudi

---

## Interview One-Liner

> “Parquet stores the data, while Iceberg manages the table.”

---

# 4. Explain Iceberg Partitioning & Hidden Partitions

> “Iceberg supports hidden partitioning, where partition logic is managed internally instead of exposing partition columns directly to users.”

---

## Example

Instead of manually creating:

```text id="zjlwmf"
year=2026/month=05/day=19
```

Iceberg automatically handles partition transformations.

---

## Benefits

* Simplifies queries
* Avoids partition management issues
* Better query optimization
* Easier schema evolution

---

## Interview One-Liner

> “Hidden partitioning improves usability because users query normal columns while Iceberg internally manages partition pruning.”

---

# 5. Scenario: How did you migrate 20 TB to Iceberg? What were challenges?

> “We migrated around 20 TB of legacy analytical data into Iceberg tables using Spark-based bulk ingestion pipelines.”

---

## Migration Steps

1. Extract data from legacy storage
2. Load into Bronze layer
3. Validate schema and data quality
4. Write data into Iceberg tables
5. Apply partitioning and sort ordering
6. Perform reconciliation and validation

---

## Challenges Faced

### Small File Problem

Solved using:

* compaction
* optimized write strategy

---

### Schema Mismatches

Handled using:

* schema validation
* controlled schema evolution

---

### Performance Optimization

Used:

* partitioning
* sort ordering
* metadata pruning

---

### Data Validation

Performed:

* row count reconciliation
* checksum validation
* duplicate checks

---

## Interview Closing Line

> “Iceberg improved scalability, ACID reliability, schema evolution support, and query performance for large analytical workloads.”

---

# 6. Scenario: How did you ensure schema consistency across layers?

> “We maintained schema consistency using centralized schema definitions, schema validation rules, and controlled schema evolution across Bronze, Silver, and Gold layers.”

---

## Approach

### Bronze

* Store raw source schema
* Minimal transformation

---

### Silver

* Enforce standardized schema
* Apply datatype validation
* Remove invalid records

---

### Gold

* Business-ready curated schema

---

## Techniques Used

* Schema registry/reference schema
* Validation checks
* DLT expectations
* Automated schema comparison
* Controlled evolution process

---

## Important Interview Point

> “We avoided uncontrolled schema drift by validating incoming schemas before processing.”

---

# Strong Interview Closing Statement

> “Apache Iceberg provides scalable metadata management, schema evolution, hidden partitioning, and ACID reliability, making it highly suitable for large-scale lakehouse architectures.”
