# Database Normalization — Complete Guide
## All Normal Forms + Real-World Interview Q&A

---

## What is Normalization?

Normalization is the process of **organizing a database to reduce data redundancy and improve data integrity** by splitting data into related tables and defining relationships between them.

**Goals:**
- Eliminate redundant (duplicate) data
- Ensure data dependencies make sense (only storing related data in a table)
- Prevent anomalies — Insert, Update, Delete anomalies

---

## Why Normalization Matters — The 3 Anomalies

Before learning the forms, understand WHY we normalize. All problems come from these 3 anomalies.

### Example: A BAD un-normalized table

| order_id | customer_id | customer_name | customer_city | product_id | product_name | product_price | qty |
|---|---|---|---|---|---|---|---|
| 101 | C01 | Mayuresh | Pune | P01 | Laptop | 80000 | 1 |
| 102 | C01 | Mayuresh | Pune | P02 | Mouse | 500 | 2 |
| 103 | C02 | Rahul | Mumbai | P01 | Laptop | 80000 | 1 |

### Insert Anomaly
> You cannot add a new customer unless they place an order.  
> You cannot add a new product unless someone orders it.

### Update Anomaly
> Laptop price changes to 85000.  
> You must update EVERY row where Laptop appears.  
> Miss one → data becomes inconsistent.

### Delete Anomaly
> If Rahul (C02) cancels his only order (103), you lose all information about Rahul forever.

---

## The Normal Forms

```
Unnormalized (UNF)
      ↓
  1NF — Eliminate repeating groups
      ↓
  2NF — Eliminate partial dependencies
      ↓
  3NF — Eliminate transitive dependencies
      ↓
 BCNF — Stronger version of 3NF
      ↓
  4NF — Eliminate multi-valued dependencies
      ↓
  5NF — Eliminate join dependencies
```

> In real-world projects, **3NF or BCNF is the practical target**.  
> 4NF and 5NF are mostly theoretical / academic.

---

## 1NF — First Normal Form

### Rule
1. Each column must contain **atomic (indivisible) values** — no lists, no arrays
2. Each column must contain values of a **single type**
3. Each row must be **uniquely identifiable** (has a primary key)
4. **No repeating groups** (no multiple columns for the same thing)

### Violation Example

| student_id | student_name | subjects |
|---|---|---|
| S01 | Mayuresh | Maths, Physics, Chemistry |
| S02 | Rahul | Maths, Biology |

❌ Violates 1NF — `subjects` column has multiple values (not atomic)

Also this violates 1NF:

| student_id | student_name | subject_1 | subject_2 | subject_3 |
|---|---|---|---|---|
| S01 | Mayuresh | Maths | Physics | Chemistry |
| S02 | Rahul | Maths | Biology | NULL |

❌ Violates 1NF — repeating groups (`subject_1`, `subject_2`, `subject_3`)

### Fixed — 1NF Compliant

| student_id | student_name | subject |
|---|---|---|
| S01 | Mayuresh | Maths |
| S01 | Mayuresh | Physics |
| S01 | Mayuresh | Chemistry |
| S02 | Rahul | Maths |
| S02 | Rahul | Biology |

✅ Atomic values. Primary key = (`student_id`, `subject`). Each cell has one value.

---

## 2NF — Second Normal Form

### Rule
1. Must already be in **1NF**
2. **No partial dependency** — every non-key column must depend on the **WHOLE primary key**, not just part of it

> This only applies when the primary key is **composite** (made of 2+ columns).  
> If your PK is a single column, you automatically satisfy 2NF once in 1NF.

### Violation Example

Table: `order_items` with composite PK = (`order_id`, `product_id`)

| order_id | product_id | product_name | product_price | qty |
|---|---|---|---|---|
| 101 | P01 | Laptop | 80000 | 1 |
| 101 | P02 | Mouse | 500 | 2 |
| 102 | P01 | Laptop | 80000 | 3 |

**Dependency analysis:**
- `qty` → depends on BOTH `order_id` + `product_id` ✅ (how many of this product in this order)
- `product_name` → depends ONLY on `product_id` ❌ (partial dependency)
- `product_price` → depends ONLY on `product_id` ❌ (partial dependency)

### Fixed — Split into 2 tables

**Table: `order_items`** (PK = `order_id` + `product_id`)

| order_id | product_id | qty |
|---|---|---|
| 101 | P01 | 1 |
| 101 | P02 | 2 |
| 102 | P01 | 3 |

**Table: `products`** (PK = `product_id`)

| product_id | product_name | product_price |
|---|---|---|
| P01 | Laptop | 80000 |
| P02 | Mouse | 500 |

✅ Now every non-key column depends on the WHOLE key of its table.

---

## 3NF — Third Normal Form

### Rule
1. Must already be in **2NF**
2. **No transitive dependency** — a non-key column must NOT depend on another non-key column

> In simple words: Non-key columns should depend on the key, the whole key, and **nothing but the key**.

### Violation Example

**Table: `employees`** (PK = `emp_id`)

| emp_id | emp_name | dept_id | dept_name | dept_location |
|---|---|---|---|---|
| E01 | Mayuresh | D01 | Data Eng | Pune |
| E02 | Rahul | D01 | Data Eng | Pune |
| E03 | Priya | D02 | Analytics | Mumbai |

**Dependency analysis:**
- `emp_name` → depends on `emp_id` ✅
- `dept_id` → depends on `emp_id` ✅
- `dept_name` → depends on `dept_id`, NOT directly on `emp_id` ❌ (transitive)
- `dept_location` → depends on `dept_id`, NOT directly on `emp_id` ❌ (transitive)

**The chain:** `emp_id` → `dept_id` → `dept_name`, `dept_location`  
This is a **transitive dependency** — `dept_name` reaches `emp_id` only THROUGH `dept_id`.

### Fixed — Split into 2 tables

**Table: `employees`** (PK = `emp_id`)

| emp_id | emp_name | dept_id |
|---|---|---|
| E01 | Mayuresh | D01 |
| E02 | Rahul | D01 |
| E03 | Priya | D02 |

**Table: `departments`** (PK = `dept_id`)

| dept_id | dept_name | dept_location |
|---|---|---|
| D01 | Data Eng | Pune |
| D02 | Analytics | Mumbai |

✅ No transitive dependencies. Update `dept_name` in ONE place only.

---

## BCNF — Boyce-Codd Normal Form

### Rule
1. Must already be in **3NF**
2. For every **functional dependency** X → Y, X must be a **superkey** (candidate key)

> BCNF is a stricter version of 3NF. Most tables in 3NF are also in BCNF.  
> BCNF catches edge cases that 3NF misses when there are **multiple overlapping candidate keys**.

### Violation Example (3NF but NOT BCNF)

A student can enroll in a subject. Each subject is taught by only one teacher. Each teacher teaches only one subject.

**Table: `enrollments`**

| student_id | subject | teacher |
|---|---|---|
| S01 | Maths | Prof. Shah |
| S01 | Physics | Prof. Joshi |
| S02 | Maths | Prof. Shah |

**Candidate Keys:** (`student_id`, `subject`) OR (`student_id`, `teacher`)

**Functional Dependencies:**
- `teacher` → `subject` (each teacher teaches one subject)
- But `teacher` is NOT a superkey — it's just a non-key attribute

This violates BCNF because `teacher` → `subject` but `teacher` alone is not a superkey.

### Fixed — BCNF

**Table: `student_teacher`** (PK = `student_id` + `teacher`)

| student_id | teacher |
|---|---|
| S01 | Prof. Shah |
| S01 | Prof. Joshi |
| S02 | Prof. Shah |

**Table: `teacher_subject`** (PK = `teacher`)

| teacher | subject |
|---|---|
| Prof. Shah | Maths |
| Prof. Joshi | Physics |

✅ Every determinant is a superkey now.

---

## 4NF — Fourth Normal Form

### Rule
1. Must be in **BCNF**
2. **No multi-valued dependencies** — a table should not have two independent multi-valued facts about an entity

### Violation Example

An employee can have multiple skills AND multiple hobbies — independently.

| emp_id | skill | hobby |
|---|---|---|
| E01 | Python | Cricket |
| E01 | Python | Chess |
| E01 | SQL | Cricket |
| E01 | SQL | Chess |

`emp_id` →→ `skill` (multi-valued)  
`emp_id` →→ `hobby` (multi-valued)  
Skills and hobbies are **independent** of each other — combining them creates unnecessary rows.

### Fixed — 4NF

**Table: `emp_skills`**

| emp_id | skill |
|---|---|
| E01 | Python |
| E01 | SQL |

**Table: `emp_hobbies`**

| emp_id | hobby |
|---|---|
| E01 | Cricket |
| E01 | Chess |

---

## 5NF — Fifth Normal Form

### Rule
1. Must be in **4NF**
2. **No join dependency** — a table cannot be reconstructed by joining smaller tables unless those joins are through candidate keys

> 5NF is mostly theoretical. In practice, almost no one designs to 5NF explicitly.  
> It handles very complex many-to-many-to-many relationships.

---

## Quick Summary Cheat Sheet

| Normal Form | Eliminates | Key Question to Ask |
|---|---|---|
| **1NF** | Repeating groups, non-atomic values | Does every cell have ONE value? |
| **2NF** | Partial dependencies | Does every non-key column depend on the WHOLE PK? |
| **3NF** | Transitive dependencies | Does every non-key column depend DIRECTLY on the PK? |
| **BCNF** | Anomalies from overlapping candidate keys | Is every determinant a superkey? |
| **4NF** | Multi-valued dependencies | Are there two independent multi-valued facts in one table? |
| **5NF** | Join dependencies | Can the table be losslessly decomposed? |

---

## Denormalization — When to Break the Rules

> In **OLTP** (transactional) systems → Normalize (3NF/BCNF)  
> In **OLAP** (analytical / data warehouse) systems → Denormalize for query performance

**Why denormalize in data warehouses?**
- Fewer JOINs = faster analytical queries
- Star schema and Snowflake schema are intentionally denormalized
- Tools like Power BI, Tableau perform better on wide, flat tables

**In your project context:**
- Silver layer (Iceberg) → closer to normalized (clean, structured, no duplicates)
- Gold layer (Delta) → denormalized aggregations for reporting speed

---

## Real-World Interview Q&A

---

### Q1. What is normalization and why do we need it?

**A:**
Normalization is the process of structuring a relational database to minimize redundancy and prevent data anomalies. Without normalization, databases suffer from three problems:

- **Insert anomaly** — cannot add data without adding unrelated data (e.g., can't add a product unless someone orders it)
- **Update anomaly** — changing one fact requires updating multiple rows, and missing one causes inconsistency
- **Delete anomaly** — deleting one record accidentally destroys unrelated information

Normalization solves these by decomposing tables into smaller, well-structured tables with clear relationships, ensuring each fact is stored in exactly one place.

---

### Q2. What is the difference between 2NF and 3NF?

**A:**

| | 2NF | 3NF |
|---|---|---|
| **Removes** | Partial dependency | Transitive dependency |
| **Dependency type** | Non-key → part of composite PK | Non-key → another non-key |
| **Requires** | Composite primary key to be relevant | Any primary key |

**2NF example:** In `order_items(order_id, product_id, product_name, qty)`, `product_name` depends only on `product_id` — partial dependency on the composite key. Move `product_name` to a `products` table.

**3NF example:** In `employees(emp_id, dept_id, dept_name)`, `dept_name` depends on `dept_id` which depends on `emp_id` — transitive. Move `dept_name` to a `departments` table.

---

### Q3. Can a table be in 2NF but not 3NF? Give an example.

**A:** Yes. 2NF only removes partial dependencies; it does NOT remove transitive dependencies.

**Example:**
```
student(student_id, zip_code, city, state)
PK = student_id (single column → automatically 2NF)
```

Dependency: `zip_code → city → state`

- `zip_code` depends on `student_id` ✅ (direct)
- `city` depends on `zip_code`, not directly on `student_id` ❌ (transitive)
- `state` depends on `zip_code` → `city`, not directly on `student_id` ❌ (transitive)

This is in 2NF (no partial dependency — single column PK) but violates 3NF (transitive dependency through `zip_code`).

**Fix — 3NF:**
```
student(student_id, zip_code)
zip_location(zip_code, city, state)
```

---

### Q4. What is the difference between 3NF and BCNF?

**A:** BCNF is stricter than 3NF. Every table in BCNF is in 3NF, but not every table in 3NF is in BCNF.

The difference arises when a table has **multiple overlapping candidate keys**.

**3NF allows** a non-key attribute to determine part of a candidate key, as long as it's not a transitive dependency through a non-key attribute.

**BCNF requires** that for EVERY functional dependency X → Y, X must be a superkey — no exceptions.

**In practice:** Most tables that are in 3NF are also in BCNF. You only hit the difference in rare cases with complex overlapping keys. For interviews, knowing this distinction plus an example is sufficient.

---

### Q5. In your data engineering project, where did you apply normalization principles?

**A:**
*"In our pharmaceutical data pipeline, we applied normalization principles primarily in the Silver (staging) Iceberg layer.*

For example, our source CSV files came in as wide, flat records — a single row would contain patient details, doctor details, and prescription details all together. This was essentially an unnormalized structure.

In our Silver transformation:
- We split patient master data into `silver.patients` — one row per patient, no repeating groups
- Doctor information went into `silver.doctors` — eliminating the transitive dependency where doctor details depended on the doctor ID, not the patient ID
- Prescription records went into `silver.prescriptions` with foreign keys back to both patients and doctors

This was essentially normalizing to 3NF in our Silver layer.

In the Gold layer, we intentionally **denormalized** — we joined and aggregated these tables into wide, flat Delta tables optimized for Power BI reporting, following a star schema pattern. This is standard practice in data warehousing — normalize for storage integrity, denormalize for query performance."*

---

### Q6. What is a functional dependency? Explain with an example.

**A:**
A functional dependency means that the value of one column (or set of columns) **uniquely determines** the value of another column.

Written as: **X → Y** (X determines Y)

**Examples:**
```
emp_id    → emp_name        (knowing emp_id tells you exactly one emp_name)
emp_id    → dept_id         (one employee belongs to one department)
dept_id   → dept_name       (one dept_id maps to one dept_name)
(order_id, product_id) → qty  (the quantity is specific to this order+product combo)
```

**NOT a functional dependency:**
```
dept_id → emp_id   ✗  (one dept has MANY employees — not functional)
```

Normalization is essentially the process of identifying and correctly placing functional dependencies so that:
- Each dependency is represented in exactly one table
- The left side (determinant) of every dependency is a key

---

### Q7. What is denormalization? When would you use it?

**A:**
Denormalization is the deliberate process of introducing redundancy into a database by combining tables or adding redundant columns — the opposite of normalization.

**When to use it:**
- **Read-heavy analytical workloads (OLAP)** — fewer JOINs = faster queries. A report that joins 8 tables can be rewritten as a scan on one wide table.
- **Data warehouses and lakehouses** — Gold/reporting layers are intentionally denormalized
- **High-throughput APIs** — when latency matters more than storage cost
- **Pre-aggregated summary tables** — store already-computed totals to avoid recalculating

**Trade-offs:**
| | Normalized | Denormalized |
|---|---|---|
| **Storage** | Less (no duplication) | More (redundant data) |
| **Write performance** | Better (update one place) | Worse (update many places) |
| **Read performance** | Slower (multiple JOINs) | Faster (single scan) |
| **Data integrity** | High | Risk of inconsistency |
| **Best for** | OLTP | OLAP / Reporting |

*"In our project, we normalized in Silver and denormalized in Gold — this is the standard Medallion pattern."*

---

### Q8. A table has the following structure. Which normal form does it violate and how would you fix it?

```
courses(course_id, course_name, teacher_id, teacher_name, teacher_phone, student_id, student_name)
```

**A:**

This table violates **multiple normal forms**:

**1NF violation check:** Assuming all values are atomic — passes 1NF.

**2NF violation:** The primary key is composite: (`course_id`, `student_id`).
- `course_name` depends only on `course_id` — **partial dependency** ❌
- `teacher_id` depends only on `course_id` — **partial dependency** ❌
- `teacher_name` depends only on `teacher_id` — **partial dependency** ❌
- `student_name` depends only on `student_id` — **partial dependency** ❌

**3NF violation:** Even after fixing 2NF — `teacher_name` depends on `teacher_id`, not directly on `course_id` — **transitive dependency** ❌

**Fix — normalize step by step:**

```
-- 2NF: Remove partial dependencies
courses(course_id, course_name, teacher_id)        PK: course_id
students(student_id, student_name)                  PK: student_id
enrollments(course_id, student_id)                  PK: course_id + student_id

-- 3NF: Remove transitive dependency (teacher_name via teacher_id)
courses(course_id, course_name, teacher_id)         PK: course_id
teachers(teacher_id, teacher_name, teacher_phone)   PK: teacher_id
students(student_id, student_name)                  PK: student_id
enrollments(course_id, student_id)                  PK: course_id + student_id
```

Now every non-key column depends on its table's PK directly — fully 3NF.

---

### Q9. What is the difference between a primary key, candidate key, and superkey?

**A:**

| Key Type | Definition | Example |
|---|---|---|
| **Superkey** | Any set of columns that uniquely identifies a row | `(emp_id)`, `(emp_id, emp_name)`, `(email)` |
| **Candidate Key** | Minimal superkey — no unnecessary columns | `emp_id`, `email` (both uniquely identify, can't remove any column) |
| **Primary Key** | The chosen candidate key for the table | `emp_id` (chosen by DBA) |
| **Alternate Key** | Candidate keys NOT chosen as primary key | `email` (if `emp_id` is PK) |
| **Foreign Key** | Column referencing a PK in another table | `dept_id` in `employees` references `dept_id` in `departments` |

**Why it matters for BCNF:** BCNF requires that the LEFT side of every functional dependency must be a **superkey**. If a non-key attribute determines another attribute, BCNF is violated.

---

### Q10. Your Silver layer Iceberg table is receiving duplicate records from the source CSV. How do you handle deduplication while maintaining normalization principles?

**A:**
*"This is a common real-world problem in our pharmaceutical pipeline. Here's how we handled it:*

**Step 1 — Identify the natural key:**
We first identified which columns form the business natural key — in our case `patient_id + record_date + source_system`.

**Step 2 — Deduplication in PySpark before writing:**
```python
from pyspark.sql.functions import row_number, desc
from pyspark.sql.window import Window

# Keep latest record per natural key
window = Window.partitionBy("patient_id", "record_date") \
               .orderBy(desc("file_modified_ts"))

df_deduped = df_raw \
    .withColumn("rn", row_number().over(window)) \
    .filter("rn = 1") \
    .drop("rn")
```

**Step 3 — MERGE into Iceberg (upsert):**
```python
spark.sql("""
    MERGE INTO silver.patients AS target
    USING source_data AS source
    ON target.patient_id = source.patient_id
       AND target.record_date = source.record_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

**Normalization aspect:** By using MERGE with the natural key, we ensure each patient-date combination exists exactly once in the Silver table — no redundancy — maintaining 1NF and preventing insert anomalies.*

*The key principle is: deduplication logic enforces the uniqueness constraint that normalization requires."*

---

*Prepared for: Mayuresh | Data Engineer Interview Prep | April 2026*
