# 🚀 Data Engineering Interview Q&A (3–5 YOE)

---

## 1️⃣ Handling Large Volume of Data

### Answer
- Use partitioning, bucketing
- Use efficient file formats (Parquet)
- Optimize joins (broadcast)
- Use caching wisely
- Enable AQE

### Problems Faced
- Data skew
- OOM errors
- Slow jobs

### Resolution
- Repartition
- Salting
- Memory tuning

---

## 2️⃣ Data Skewness

### Answer
Data skew occurs when some partitions have disproportionately large data.

### Fix
```python
df.withColumn("salt", F.rand())
```
- Salting
- AQE
- Broadcast join

---

## 3️⃣ groupBy("userId").count()

### Explanation
- Triggers shuffle
- Groups data by userId
- Counts records

---

## 4️⃣ DataProc

### Answer
Managed Spark/Hadoop service in GCP for running jobs without managing infrastructure.

---

## 5️⃣ Airflow

### Answer
Workflow scheduler to manage DAG-based pipelines.

---

## 6️⃣ Scheduling

- Cron-based scheduling
- Dependency-based execution

---

## 7️⃣ Broadcast Join vs Shuffle Join

| Broadcast | Shuffle |
|----------|--------|
| Small table | Large tables |
| No shuffle | Shuffle |

---

## 8️⃣ Drawback of Broadcast Join

- Memory issue
- Not suitable for large tables

---

## 9️⃣ Reduce Shuffling

- Use broadcast
- Repartition properly
- Avoid wide transformations

---

## 🔟 AQE

- Optimizes query at runtime
- Handles skew
- Changes join strategy dynamically

---

## 11️⃣ Window Function Question

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

w = Window.partitionBy("emp_id").orderBy("effective_date")

df.withColumn("prev_salary", lag("salary").over(w))
```

---

## 12️⃣ Revenue Growth (Lag/Lead)

```python
w = Window.partitionBy("store").orderBy("date")

df.withColumn("prev", lag("amount").over(w)) \
  .withColumn("next", lead("amount").over(w))
```

---

## 13️⃣ Top 3 Users per Day

```python
from pyspark.sql.functions import row_number

w = Window.partitionBy("date").orderBy(F.desc("count"))
```

---

## 14️⃣ Data Skew Detection

- Spark UI
- Uneven partitions

---

## 15️⃣ Parquet Advantages

- Columnar
- Compression
- Predicate pushdown

---

## 16️⃣ Compression vs Compaction

- Compression → reduce size
- Compaction → merge small files

---

## 17️⃣ Airflow DAG Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
```

---

## 🎯 Interview Tip

Always explain with:
👉 Problem → Solution → Impact
