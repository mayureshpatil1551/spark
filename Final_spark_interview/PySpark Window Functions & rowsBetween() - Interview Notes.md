# PySpark Window Functions & rowsBetween() - Interview Notes

## What is a Window Function?

A Window Function performs calculations across a set of rows related to the current row without collapsing the rows like a GROUP BY.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import *
```

---

# 1. ROW_NUMBER()

## Use Case
Deduplication (Most Common)

```python
w = Window.partitionBy("customer_id") \
          .orderBy(col("updated_date").desc())

df = df.withColumn(
    "rn",
    row_number().over(w)
)

df = df.filter(col("rn") == 1)
```

### Production Example
- CDC Pipelines
- Latest Customer Record
- SCD Processing

---

# 2. RANK()

## Use Case
Top N Analysis

```python
w = Window.partitionBy("dept") \
          .orderBy(col("salary").desc())

df.withColumn(
    "rank",
    rank().over(w)
)
```

### Example

| Salary | Rank |
|----------|--------|
| 100 | 1 |
| 100 | 1 |
| 90 | 3 |

---

# 3. DENSE_RANK()

## Use Case
Ranking without gaps

```python
df.withColumn(
    "dense_rank",
    dense_rank().over(w)
)
```

### Example

| Salary | Dense Rank |
|----------|------------|
| 100 | 1 |
| 100 | 1 |
| 90 | 2 |

---

# 4. LAG()

## Use Case
Compare Current vs Previous Record

```python
w = Window.partitionBy("cust_id") \
          .orderBy("txn_date")

df.withColumn(
    "prev_amt",
    lag("amount", 1).over(w)
)
```

### Production Example
- CDC Validation
- Revenue Growth
- Previous Transaction Analysis

---

# 5. LEAD()

## Use Case
Access Next Record

```python
df.withColumn(
    "next_amt",
    lead("amount",1).over(w)
)
```

### Production Example
- Next Purchase Prediction
- SCD Type 2 End Date Calculation

---

# 6. FIRST()

## Use Case
Find First Value

```python
df.withColumn(
    "first_salary",
    first("salary").over(w)
)
```

### Production Example
- First Purchase
- First Login Date

---

# 7. LAST()

## Use Case
Latest Value

```python
w = Window.partitionBy("emp_id") \
          .orderBy("effective_date") \
          .rowsBetween(
              Window.unboundedPreceding,
              Window.unboundedFollowing
          )

df.withColumn(
    "latest_salary",
    last("salary").over(w)
)
```

---

# 8. Running Total

## Use Case
Cumulative Sales

```python
w = Window.orderBy("date") \
          .rowsBetween(
              Window.unboundedPreceding,
              Window.currentRow
          )

df.withColumn(
    "running_total",
    sum("sales").over(w)
)
```

### Example

| Sales | Running Total |
|--------|--------------|
| 100 | 100 |
| 200 | 300 |
| 300 | 600 |

---

# 9. Moving Average

## Use Case
Last 3 Days Average

```python
w = Window.orderBy("date") \
          .rowsBetween(-2,0)

df.withColumn(
    "moving_avg",
    avg("sales").over(w)
)
```

---

# 10. Rolling Sum

## Use Case
Last N Transactions

```python
w = Window.orderBy("txn_date") \
          .rowsBetween(-6,0)

df.withColumn(
    "rolling_sum",
    sum("amount").over(w)
)
```

---

# 11. Percentage Contribution

## Use Case
Customer Contribution

```python
w = Window.partitionBy("department")

df.withColumn(
    "percent",
    col("sales") /
    sum("sales").over(w) * 100
)
```

---

# 12. Year-To-Date (YTD)

## Use Case
Financial Reporting

```python
w = Window.partitionBy("year") \
          .orderBy("month") \
          .rowsBetween(
              Window.unboundedPreceding,
              Window.currentRow
          )

df.withColumn(
    "ytd_sales",
    sum("sales").over(w)
)
```

---

# 13. Cumulative Count

```python
w = Window.orderBy("date") \
          .rowsBetween(
              Window.unboundedPreceding,
              Window.currentRow
          )

df.withColumn(
    "running_count",
    count("*").over(w)
)
```

---

# rowsBetwe*n() Cheat Sheet

## Syntax

```pyt*on
rowsBetween(start, end)
```

--*

## Running Total

```python
rows*etween(
    Window.unboundedPreceding,
    Window.currentRow
)
```

Meaning:

```text
First Row → Current Row
```

---

## Previous 3 Rows

```python
rowsBetween(-3,0)
```

Meaning:

```text
Current Row
+ Previous 3 Rows
```

---

## Previous Row Only

```python
rowsBetween(-1,-1)
```

Meaning:

```text
Only Previous Row
```

---

## Current Row Only

```python
rowsBetween(0,0)
```

Meaning:

```text
Only Current Row
```

---

## Next 3 Rows

```python
rowsBetween(0,3)
```

Meaning:

```text
Current Row
+ Next 3 Rows
```

---

## Entire Partition

```python
rowsBetween(
    Window.unboundedPreceding,
    Window.unboundedFollowing
)
```

Meaning:

```text
First Row → Last Row
```

---

# Most Common Interview Questions

### Q1: Difference between row_number(), rank() and dense_rank()?

- row_number() → Unique sequence
- rank() → Skips ranks
- dense_rank() → No rank gaps

---

### Q2: Most common use of row_number()?

Answer:
Deduplication and Latest Record Selection.

---

### Q3: Why use rowsBetween()?

Answer:
To define a custom window frame such as running totals, moving averages, rolling sums, and cumulative metrics.

---

### Q4: What does Window.unboundedPreceding mean?

Answer:
Start window from the first row of the partition.

---

### Q5: What does Window.currentRow mean?

Answer:
End the window at the current row.

---

# Data Engineer Production Use Cases

✅ Deduplication → row_number()

✅ Latest Record Selection → row_number()

✅ SCD Type 2 → lead(), lag()

✅ CDC Validation → lag()

✅ Running Total → sum() + rowsBetween()

✅ Moving Average → avg() + rowsBetween()

✅ Top N Records → rank(), dense_rank()

✅ Revenue Trend Analysis → lag()

✅ Financial Reporting (YTD) → cumulative sum

✅ Time Series Analytics → rolling windows
