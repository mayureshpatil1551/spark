# PySpark Coding Interview Questions — Instagram Reel Series (Part 2)

**54 hands-on PySpark coding interview questions** — each with a runnable code solution, a ~2-minute spoken script explaining the approach (first-person, interview style), a hook description, hashtags, and a background music vibe. Show the code on-screen as text overlay while narrating the script for the reel.

---

## Q1. Find the Second Highest Salary Per Department

**Hook / Short Description (for caption):**
The second-highest-salary problem is a classic — here's the clean one-line dense_rank solution in PySpark.

**Coding Problem:**
Given an employee dataframe with columns emp_id, dept, salary — write PySpark code to find the second highest salary in each department.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank, col

w = Window.partitionBy("dept").orderBy(col("salary").desc())

result = (
    df.withColumn("rnk", dense_rank().over(w))
      .filter(col("rnk") == 2)
      .select("dept", "emp_id", "salary")
)
result.show()
```

**2-Minute Script** (~160 words, ~1m 9s at natural pace):

> This is one of the most asked PySpark coding questions, and the trick is using dense_rank instead of row_number. I define a window partitioned by department, ordered by salary descending, then apply dense_rank over that window. I specifically use dense_rank rather than row_number because if two employees are tied for the highest salary, row_number would arbitrarily split them into rank 1 and rank 2, incorrectly treating a tie as first and second place. Dense_rank correctly gives both of them rank 1, and the next distinct salary value correctly becomes rank 2. Once I have the ranked dataframe, I simply filter where rank equals 2, and select the columns I care about. This same pattern extends naturally to finding the Nth highest salary — I'd just filter on rank equals N instead. It's a clean, single-pass solution without needing a self-join or a subquery, which is what makes window functions so much more elegant than the equivalent SQL approach without them.

**Background Music Vibe:** Focused, brainy problem-solving beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #TechInterview

---

## Q2. Find Duplicate Rows in a DataFrame

**Hook / Short Description (for caption):**
Need to actually find and count duplicate rows, not just drop them? Here's the dynamic groupBy pattern that works on any dataframe.

**Coding Problem:**
Write PySpark code to identify all rows that are exact duplicates (appear more than once) in a dataframe.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import count

dup_rows = (
    df.groupBy(df.columns)
      .agg(count("*").alias("cnt"))
      .filter(col("cnt") > 1)
)
dup_rows.show()
```

**2-Minute Script** (~188 words, ~1m 21s at natural pace):

> To find duplicates, I group by every single column in the dataframe using df.columns, which gives me all the distinct combinations of values across every column, and then I count how many times each combination appears using an aggregation. Any group where the count is greater than one represents a row that appears as an exact duplicate somewhere in the dataset. I like this approach because it's dynamic — I'm passing df.columns directly into groupBy, so this same snippet works on any dataframe regardless of how many columns it has, without me having to hardcode column names. One thing to be careful about at scale is that grouping by every column can be expensive on a very wide table with many columns, so if I only care about duplicates based on a specific business key rather than every single column, I'd group by just those key columns instead. I'd also mention that if I just need to remove duplicates rather than list them, dropDuplicates is the simpler one-liner, but this groupBy-and-count approach is what actually lets me see and audit which rows were duplicated and how many times.

**Background Music Vibe:** Curious, investigative beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #BigData

---

## Q3. Remove Duplicates Keeping the First Occurrence

**Hook / Short Description (for caption):**
Need the earliest — not just any — duplicate removed? Row_number with a window function gives you deterministic control.

**Coding Problem:**
Given a dataframe with an event_time column, write code to remove duplicate customer_id rows while keeping only the earliest record for each.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

w = Window.partitionBy("customer_id").orderBy(col("event_time").asc())

result = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .drop("rn")
)
result.show()
```

**2-Minute Script** (~172 words, ~1m 14s at natural pace):

> This is a deterministic deduplication problem, meaning I need to control exactly which duplicate gets kept, not just remove duplicates arbitrarily. I use row_number instead of dropDuplicates here specifically because dropDuplicates doesn't guarantee which row it keeps when there are ties on the dedup key — it's essentially arbitrary based on partition ordering. So instead, I define a window partitioned by customer_id, and ordered by event_time ascending, since I want the earliest record. Row_number then assigns a sequential number starting at 1 for the earliest row in each customer's group, 2 for the next, and so on. I filter to keep only row number equal to 1, which guarantees I'm always keeping the chronologically first record per customer, and then I drop the helper row number column since it was just scaffolding for the logic. If I instead wanted the most recent record per customer, I'd simply flip the ordering to descending — that single change is the entire difference between keeping the earliest versus the latest version of a duplicated record.

**Background Music Vibe:** Steady, precise beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #WindowFunctions

---

## Q4. Top 3 Highest Paid Employees Per Department

**Hook / Short Description (for caption):**
Top-N per group is one of the most reusable PySpark patterns — here's the exact window function template, and the row_number vs dense_rank gotcha.

**Coding Problem:**
Write PySpark code to return the top 3 highest paid employees within each department.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

w = Window.partitionBy("dept").orderBy(col("salary").desc())

top3 = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") <= 3)
      .drop("rn")
)
top3.show()
```

**2-Minute Script** (~191 words, ~1m 22s at natural pace):

> This is the generalized version of the top-N-per-group pattern, and it's one of the most reusable snippets in PySpark. I define a window partitioned by department, ordered by salary descending, and apply row_number over that window, which assigns 1 to the highest earner in each department, 2 to the second highest, and so on, independently within each department's own partition. Then I just filter for row number less than or equal to 3. I specifically use row_number here rather than dense_rank, because with row_number, if I want exactly 3 employees per department, I get exactly 3, even if there's a salary tie at the third-place spot. If I used dense_rank instead, a tie at rank 3 would let in extra employees beyond three, which is sometimes actually what's wanted, so which ranking function to use really depends on whether ties should expand the result set or not, and that's usually the first clarifying question I'd ask in an actual interview before writing this code. Changing the 3 to any other number N is trivial, which is exactly why interviewers love asking this as a base pattern before layering on follow-up variations.

**Background Music Vibe:** Confident, competitive beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q5. Employees Earning More Than Their Manager

**Hook / Short Description (for caption):**
A self-join is really just joining a table to itself with two aliases — here's the exact pattern for employee-manager comparisons.

**Coding Problem:**
Given an employee table with emp_id, name, salary, and manager_id, write PySpark code to find all employees who earn more than their direct manager.

**Code Solution (show on screen):**
```python
emp = df.alias("e")
mgr = df.alias("m")

result = (
    emp.join(mgr, col("e.manager_id") == col("m.emp_id"), "inner")
       .filter(col("e.salary") > col("m.salary"))
       .select(
           col("e.name").alias("employee"),
           col("e.salary").alias("emp_salary"),
           col("m.name").alias("manager"),
           col("m.salary").alias("mgr_salary"),
       )
)
result.show()
```

**2-Minute Script** (~175 words, ~1m 15s at natural pace):

> This is a self-join problem, which trips people up in interviews mainly because of alias handling, not the logic itself. Since I'm joining the same dataframe against itself — once representing the employee, and once representing that employee's manager — I have to alias both sides distinctly, here calling them e for employee and m for manager, otherwise Spark can't tell which salary column I mean in the filter and select. I join on the employee's manager_id matching the manager's own emp_id, which is exactly how a self-referencing hierarchy is modeled in a single table. Once joined, each row now represents an employee sitting right next to their manager's data, so I simply filter for cases where the employee's salary is greater than the manager's salary. I always double check I'm using an inner join here, since an employee with a null manager_id, like a CEO with no manager, should naturally get excluded from this comparison rather than causing a null-related error, and an inner join handles that correctly by simply not matching those rows.

**Background Music Vibe:** Clever, puzzle-solving beat

**Top 5 Hashtags:** #PySpark #CodingInterview #SelfJoin #DataEngineering #SparkSQL

---

## Q6. Cumulative Sum Per Group

**Hook / Short Description (for caption):**
Cumulative sums come down to one specific window frame: unboundedPreceding to currentRow. Here's exactly how it works.

**Coding Problem:**
Write PySpark code to calculate a running cumulative sum of sales for each store, ordered by date.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import sum as _sum, col

w = (
    Window.partitionBy("store_id")
          .orderBy("date")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result = df.withColumn("cumulative_sales", _sum(col("sales")).over(w))
result.show()
```

**2-Minute Script** (~197 words, ~1m 24s at natural pace):

> For a cumulative or running sum, the key detail is the frame specification within the window, not just the partition and order. I define a window partitioned by store_id, so each store's running total is calculated independently, and ordered by date, since a running total is inherently sequential. The important part is rowsBetween unbounded preceding and current row — this explicitly tells Spark to sum every row from the very first row in that store's partition, all the way up through the current row, which is exactly the definition of a cumulative sum. I always import sum as an aliased name, typically _sum, because sum is also a built-in Python function, and shadowing it accidentally causes confusing bugs later in the same script if I ever need the native Python sum function elsewhere. One subtlety worth mentioning in an interview is that if I hadn't ordered the window, or if I used rowsBetween with a fixed window like the last 3 rows instead of unbounded preceding, I'd get a moving sum or moving average instead of a true cumulative total, so the specific frame boundary is really what defines the difference between these related but distinct calculations.

**Background Music Vibe:** Building momentum beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q7. Moving Average Over the Last 3 Rows

**Hook / Short Description (for caption):**
A 3-day moving average is just a sliding window frame — rowsBetween(-2, currentRow) — here's exactly how the edges behave.

**Coding Problem:**
Write PySpark code to calculate a 3-day moving average of stock closing prices per stock symbol.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import avg, col

w = (
    Window.partitionBy("symbol")
          .orderBy("date")
          .rowsBetween(-2, Window.currentRow)
)

result = df.withColumn("moving_avg_3d", avg(col("close_price")).over(w))
result.show()
```

**2-Minute Script** (~207 words, ~1m 29s at natural pace):

> This builds directly on the cumulative sum pattern, but with a fixed-size window frame instead of an unbounded one. I partition by symbol so each stock's moving average is computed independently, and order by date since the calculation is inherently sequential. The key line is rowsBetween minus 2 and current row, which tells Spark to only look at the current row plus the two rows immediately before it in that ordered partition — exactly 3 rows total, giving me a proper 3-day moving average rather than a cumulative one. I use avg instead of sum here since I want the average of those 3 values, not their total. One thing worth calling out is what happens at the very start of each symbol's data — for the first row, there are no prior rows available, so Spark just averages over whatever rows actually exist within that frame boundary, meaning the first row's moving average is really just that single value, and the second row's is an average of just 2 values, not a full 3-row window yet. That's usually the expected, correct behavior, but it's worth explicitly mentioning in an interview so the interviewer knows you understand the edge case rather than assuming it magically works out.

**Background Music Vibe:** Rolling, wave-like beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q8. Find Consecutive Login Streak Days

**Hook / Short Description (for caption):**
Finding the longest login streak looks hard until you know this one trick — subtracting row_number from the date to group consecutive days.

**Coding Problem:**
Given a user login table with user_id and login_date, write PySpark code to find the longest consecutive daily login streak for each user.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, date_sub, count

w = Window.partitionBy("user_id").orderBy("login_date")

streaks = (
    df.withColumn("rn", row_number().over(w))
      .withColumn("streak_group", date_sub(col("login_date"), col("rn")))
)

longest = (
    streaks.groupBy("user_id", "streak_group")
           .agg(count("*").alias("streak_length"))
           .groupBy("user_id")
           .agg({"streak_length": "max"})
           .withColumnRenamed("max(streak_length)", "longest_streak")
)
longest.show()
```

**2-Minute Script** (~203 words, ~1m 27s at natural pace):

> This is a genuinely clever trick that comes up a lot, called the group-by-difference technique for finding consecutive sequences. I first assign a row number to each user's logins ordered by date, so if a user logged in every day, row number and the actual date increase in perfect lockstep. The key insight is that if I subtract the row number, in days, from the actual login date, consecutive days will all produce the exact same resulting date, because both the date and the row number are incrementing by one together, so their difference stays constant. Any gap in login days breaks that lockstep, producing a different resulting value, effectively creating a new streak group. So after that subtraction, I group by user_id and this new streak_group value, and count how many rows fall into each group, which gives me the length of each individual streak. Finally, I take the max streak length per user across all their streak groups, which gives me their single longest consecutive login streak. This date-minus-row-number trick is one of those patterns that seems almost like magic the first time you see it, but it's genuinely one of the most useful techniques for any consecutive-sequence problem in PySpark.

**Background Music Vibe:** Aha-moment reveal beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #TechInterview

---

## Q9. Pivot Table: Rows to Columns

**Hook / Short Description (for caption):**
Pivoting rows to columns in PySpark is one line — but there's a performance trick most people miss for large tables.

**Coding Problem:**
Given sales data with columns region, quarter, and revenue, write PySpark code to pivot the quarters into columns, showing revenue per region per quarter.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import sum as _sum

pivoted = (
    df.groupBy("region")
      .pivot("quarter")
      .agg(_sum("revenue"))
)
pivoted.show()
```

**2-Minute Script** (~196 words, ~1m 24s at natural pace):

> Pivoting in PySpark is refreshingly simple once you know the pattern, and it mirrors what most people already know from pandas or Excel pivot tables. I start with a normal groupBy on the column I want to remain as rows, which here is region, and then instead of going straight to an aggregation, I call pivot on the column whose distinct values I want turned into new columns, which is quarter in this case. After pivot, I chain the actual aggregation, summing revenue, and Spark automatically creates one output column for every distinct value found in the quarter column, populated with that aggregated value. One important performance note I always mention in interviews is that by default, Spark has to first scan the data once just to discover all the distinct values in the pivot column before it can build the pivoted schema, which is expensive on very large datasets. So if I already know the possible quarter values in advance, like Q1 through Q4, I pass them explicitly as a list into the pivot function itself, like pivot('quarter', ['Q1','Q2','Q3','Q4']), which skips that expensive discovery scan entirely and is a meaningful performance optimization on large tables.

**Background Music Vibe:** Clean, transformative beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #BigData

---

## Q10. Unpivot: Columns to Rows

**Hook / Short Description (for caption):**
Unpivoting isn't a built-in method in PySpark — here's how the stack() SQL expression does the job instead.

**Coding Problem:**
Given a wide dataframe with columns region, Q1, Q2, Q3, Q4, write PySpark code to unpivot it into a long format with columns region, quarter, revenue.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import expr

unpivoted = df.select(
    "region",
    expr("stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, revenue)")
)
unpivoted.show()
```

**2-Minute Script** (~199 words, ~1m 25s at natural pace):

> Unpivoting, sometimes called melting, is the reverse of the pivot problem, and PySpark handles it through the stack function inside a SQL expression, since there's no dedicated unpivot method the way there is for pivot. The stack function takes a count of how many rows each original row should expand into, here 4 since I have 4 quarter columns, followed by pairs of a label and its corresponding value — 'Q1' paired with the Q1 column, 'Q2' paired with the Q2 column, and so on. I alias the resulting two output columns as quarter and revenue. What's actually happening under the hood is that each original wide row gets expanded into 4 separate long-format rows, one per quarter, with the region value duplicated across all 4 of them, since region wasn't part of the stack expression and just carries through normally in the select. I always mention that this pattern requires me to know and explicitly list the wide columns upfront, unlike pivot's optional dynamic discovery, so for a dataframe with a genuinely dynamic or unknown number of wide columns, I'd need to programmatically build that stack expression string first by inspecting df.columns, rather than hardcoding it by hand.

**Background Music Vibe:** Reverse, unfolding beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #ETL

---

## Q11. Word Frequency Count

**Hook / Short Description (for caption):**
The classic word count problem in PySpark — split, explode, groupBy — here's the clean pattern, plus the punctuation gotcha.

**Coding Problem:**
Given a dataframe with a single text column containing sentences, write PySpark code to count the frequency of each word across the entire dataset.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import explode, split, lower, trim, col

word_counts = (
    df.select(explode(split(lower(trim(col("text"))), r"\s+")).alias("word"))
      .groupBy("word")
      .count()
      .orderBy(col("count").desc())
)
word_counts.show()
```

**2-Minute Script** (~184 words, ~1m 19s at natural pace):

> This is basically the classic word count problem, which is almost a rite of passage in any big data framework, and PySpark's DataFrame API makes it very clean. I start by lowercasing and trimming the text column, to make sure the same word in different cases or with extra whitespace counts as one consistent word. Then I split each sentence on whitespace using a regex, which turns each row's text into an array of individual words. The key step is explode, which takes that array and turns each element into its own separate row, so if a sentence had 10 words, I now have 10 separate rows, each holding just one word. From there, it's a simple groupBy on the word column with a count aggregation, and I order by count descending to see the most frequent words first. In a real interview, I'd also mention handling punctuation — this basic version would treat 'word.' and 'word' as different tokens because of the trailing period, so for genuinely clean word counting, I'd add a regexp_replace step before splitting to strip out punctuation characters first.

**Background Music Vibe:** Classic, foundational beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #BigData

---

## Q12. Explode an Array Column Into Rows

**Hook / Short Description (for caption):**
Exploding an array column is simple — but explode vs explode_outer is the edge case that trips people up.

**Coding Problem:**
Given a dataframe where each row has a customer_id and a tags array column like ['vip','new','online'], write PySpark code to turn each tag into its own row.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import explode, col

result = df.select("customer_id", explode(col("tags")).alias("tag"))
result.show()
```

**2-Minute Script** (~183 words, ~1m 18s at natural pace):

> This is a foundational technique for working with nested or array-type data, and explode is the function purpose-built for exactly this. Explode takes an array column and creates one new row per element in that array, while automatically duplicating every other column's value across those new rows. So a customer with 3 tags in their array becomes 3 separate rows, each with the same customer_id but a different individual tag value. One important edge case I always mention is what happens with an empty array or a null value in that array column — a plain explode will actually drop that row entirely, since there's nothing to explode into, which can silently lose customers who happen to have no tags at all. If I need to preserve those rows instead, with a null in the resulting tag column, I'd use explode_outer instead of plain explode, which behaves like a left join in that sense, keeping the row even when the array is empty or null. That distinction between explode and explode_outer is a common follow-up question interviewers ask right after this base problem.

**Background Music Vibe:** Light, expanding beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #ETL

---

## Q13. Flatten a Nested Struct Column

**Hook / Short Description (for caption):**
Flattening a struct column is just dot notation in select() — here's the manual version and when to make it dynamic instead.

**Coding Problem:**
Given a dataframe with a nested struct column named address containing street, city, and zip, write PySpark code to flatten it into separate top-level columns.

**Code Solution (show on screen):**
```python
flattened = df.select(
    "customer_id",
    col("address.street").alias("street"),
    col("address.city").alias("city"),
    col("address.zip").alias("zip"),
)
flattened.show()
```

**2-Minute Script** (~185 words, ~1m 19s at natural pace):

> For a struct column, which is essentially a nested object rather than an array, flattening it is just a matter of selecting the specific dot-notation paths I want and aliasing them to clean, flat column names. So address.street, address.city, and address.zip each become their own independent top-level column. This works because Spark treats struct fields as directly addressable using standard dot notation, similar to accessing a property on a nested object in most programming languages. For a real interview follow-up, I'd mention that hardcoding each field name like this doesn't scale well if the struct has many fields, or if I don't know all the field names in advance, so a more dynamic approach would be to inspect df.schema, programmatically find the struct field, iterate over its nested fields, and build the list of select expressions automatically. That dynamic version is genuinely more useful in production pipelines where the nested schema might not be fully known ahead of time or might change over time, but for a quick interview answer, explicitly selecting each dot-notation path is the clearest way to demonstrate the core concept first.

**Background Music Vibe:** Simple, clarifying beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #JSON

---

## Q14. Find Missing Dates in a Date Range Per User

**Hook / Short Description (for caption):**
Finding gaps in data means generating what SHOULD exist first — here's the sequence + explode + left_anti join pattern for missing dates.

**Coding Problem:**
Given a table of user activity with user_id and activity_date, write PySpark code to find which dates are missing for each user between their first and last activity date.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import sequence, explode, min as _min, max as _max, col

date_range = (
    df.groupBy("user_id")
      .agg(_min("activity_date").alias("start_date"), _max("activity_date").alias("end_date"))
      .withColumn("all_dates", explode(sequence(col("start_date"), col("end_date"))))
      .select("user_id", "all_dates")
)

missing = date_range.join(
    df, (date_range.user_id == df.user_id) & (date_range.all_dates == df.activity_date), "left_anti"
)
missing.show()
```

**2-Minute Script** (~200 words, ~1m 26s at natural pace):

> This problem requires generating dates that don't exist in the data at all, which means I can't solve it with a simple filter — I actually need to construct the full expected date range first, then compare it against what's real. I start by finding each user's earliest and latest activity date using a groupBy aggregation. Then I use the sequence function, which generates an array of every date between a start and end date, and I explode that array to turn it into one row per expected date per user — this gives me the complete, ideal calendar of dates each user should have activity for, whether or not they actually do. The key step is the final join, and I specifically use a left_anti join, which is a join type that returns only rows from the left side that have no match on the right side. So joining my generated full date range against the actual activity table using left_anti gives me exactly the dates that were expected but never actually appear in the real data — the missing dates. This sequence-plus-explode-plus-left-anti-join combination is a really powerful, reusable pattern for any kind of gap analysis, not just dates.

**Background Music Vibe:** Detective, gap-finding beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #TechInterview

---

## Q15. Employee-Manager Hierarchy Names

**Hook / Short Description (for caption):**
Same self-join pattern as before, but left join instead of inner — here's why that one word matters for hierarchy queries.

**Coding Problem:**
Given an employee table with emp_id, name, and manager_id, write PySpark code to produce a dataframe showing each employee's name next to their manager's name.

**Code Solution (show on screen):**
```python
emp = df.alias("e")
mgr = df.alias("m")

result = (
    emp.join(mgr, col("e.manager_id") == col("m.emp_id"), "left")
       .select(
           col("e.name").alias("employee_name"),
           col("m.name").alias("manager_name"),
       )
)
result.show()
```

**2-Minute Script** (~186 words, ~1m 20s at natural pace):

> This is the same self-join foundation as the earlier salary comparison problem, but a simpler variant since there's no filtering condition, just a direct lookup. I alias the dataframe twice, once as e for the employee perspective and once as m for the manager perspective, and join them on the employee's manager_id matching the manager's emp_id. The important detail here compared to the salary comparison question is that I deliberately use a left join instead of an inner join, because I want every employee to appear in the result, including someone like a CEO who has a null manager_id and therefore no matching manager row at all. With a left join, that CEO's row would still appear, just with a null value in manager_name, which correctly reflects reality, whereas an inner join would silently drop that person from the results entirely. This distinction between when to use an inner join versus a left join for the exact same self-join structure is a subtle but important thing to explicitly call out in an interview, since it shows you're thinking about data completeness, not just mechanically joining tables.

**Background Music Vibe:** Steady, connecting beat

**Top 5 Hashtags:** #PySpark #CodingInterview #SelfJoin #DataEngineering #SparkSQL

---

## Q16. Rank Employees Within Department by Salary

**Hook / Short Description (for caption):**
Row_number, rank, and dense_rank all handle ties completely differently — here's the exact distinction, side by side.

**Coding Problem:**
Write PySpark code to assign a rank to each employee within their department based on salary, handling ties appropriately, and explain the difference between the ranking functions available.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import rank, dense_rank, row_number, col

w = Window.partitionBy("dept").orderBy(col("salary").desc())

result = (
    df.withColumn("row_num", row_number().over(w))
      .withColumn("rnk", rank().over(w))
      .withColumn("dense_rnk", dense_rank().over(w))
)
result.show()
```

**2-Minute Script** (~192 words, ~1m 22s at natural pace):

> PySpark actually gives me three different ranking functions, and picking the right one depends entirely on how I want ties handled, so I like showing all three side by side to make the difference concrete. Row_number always assigns a unique, sequential number to every row, regardless of ties, so two employees with the identical salary would still get different numbers, like 2 and 3, based purely on arbitrary tie-breaking order. Rank, on the other hand, gives tied rows the exact same rank, but then skips the next rank number to account for how many rows tied — so if two people are tied for rank 2, the next distinct salary jumps straight to rank 4, not 3. Dense_rank also gives tied rows the same rank, but critically does not skip any numbers afterward, so that same scenario would go from rank 2 straight to rank 3 for the next distinct salary, with no gap. In interviews, I always explain it with this line: row_number never ties, rank ties but leaves gaps, dense_rank ties with no gaps — and which one is 'correct' entirely depends on the specific business requirement being asked for.

**Background Music Vibe:** Comparative, clarifying beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q17. Convert Wide Format to Long Format

**Hook / Short Description (for caption):**
Wide-to-long is the exact same stack() pattern every time — recognizing that repetition is the real interview skill here.

**Coding Problem:**
Given a dataframe with columns product_id, jan_sales, feb_sales, mar_sales, write PySpark code to convert it to a long format with columns product_id, month, sales.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import expr

long_df = df.select(
    "product_id",
    expr("stack(3, 'Jan', jan_sales, 'Feb', feb_sales, 'Mar', mar_sales) as (month, sales)")
)
long_df.show()
```

**2-Minute Script** (~177 words, ~1m 16s at natural pace):

> This is functionally the same technique as the earlier unpivot problem, and I want to emphasize that recognizing this pattern repeat is actually a valuable interview skill — wide-to-long conversion always uses the same stack function approach, just with different column names plugged in. I pass 3 as the row-expansion count since there are 3 month columns, followed by pairs of a label string and the corresponding column — Jan paired with jan_sales, Feb with feb_sales, and Mar with mar_sales — and alias the two resulting output columns as month and sales. Each original row expands into exactly 3 long-format rows, one per month, with product_id duplicated across all 3 since it wasn't part of the stack expression. I always mention in interviews that the number I pass into stack has to exactly match the number of label-value pairs I provide afterward, and a common mistake is miscounting that number when there are many columns involved, which causes a runtime error rather than a silently wrong result, so at least the mistake is loud rather than quiet.

**Background Music Vibe:** Recognizing patterns beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #ETL

---

## Q18. Calculate Percentage of Total Per Group

**Hook / Short Description (for caption):**
Percentage of total per group without collapsing your rows — this is exactly what makes window aggregations different from groupBy.

**Coding Problem:**
Given sales data with region and revenue, write PySpark code to calculate what percentage each row's revenue represents of its region's total revenue.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import sum as _sum, col, round as _round

w = Window.partitionBy("region")

result = df.withColumn(
    "pct_of_region_total",
    _round((col("revenue") / _sum("revenue").over(w)) * 100, 2)
)
result.show()
```

**2-Minute Script** (~176 words, ~1m 15s at natural pace):

> This is a nice example of using a window function purely for its aggregation capability without needing any explicit ordering, since a percentage-of-total calculation isn't sequential at all — it just needs the group's total available alongside each individual row. I define a window partitioned by region with no orderBy clause, since order genuinely doesn't matter for a plain sum aggregation like this. I then use sum of revenue over that window, which, unlike a regular groupBy aggregation, doesn't collapse the dataframe down to one row per region — instead, it broadcasts that region's total back onto every original row belonging to that region. Then I simply divide each row's own revenue by that broadcasted region total, multiply by 100, and round to 2 decimal places for a clean percentage. This pattern of using a window function to keep row-level granularity while still having access to a group-level aggregate value is genuinely one of the most useful things window functions enable, and it's something a groupBy alone simply can't do without an extra join step afterward.

**Background Music Vibe:** Balanced, proportional beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q19. Find First and Last Transaction Per Customer

**Hook / Short Description (for caption):**
Using last() in a window function without extending the frame gives you a silent bug, not an error — here's the fix.

**Coding Problem:**
Given a transactions table with customer_id, transaction_date, and amount, write PySpark code to return each customer's first and last transaction in a single row.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import first, last, col

w = (
    Window.partitionBy("customer_id")
          .orderBy("transaction_date")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

result = (
    df.withColumn("first_amount", first("amount").over(w))
      .withColumn("last_amount", last("amount").over(w))
      .select("customer_id", "first_amount", "last_amount")
      .distinct()
)
result.show()
```

**2-Minute Script** (~192 words, ~1m 22s at natural pace):

> The subtle but critical detail in this problem is the window frame, because first and last are misleading function names if you don't control the frame explicitly. I partition by customer_id and order by transaction_date, but the important part is rowsBetween unbounded preceding and unbounded following — without this, Spark's default frame for an ordered window is actually unbounded preceding up to the current row, which means the last function would just return the current row's own value every single time, not the true final value in the whole partition, which is a really common mistake. By explicitly extending the frame to unbounded following as well, I'm telling Spark to consider the entire customer's partition when evaluating both first and last, regardless of which row is currently being evaluated. After adding both columns, every row for a given customer ends up with the identical first and last amount values, so I finish with select and distinct to collapse it down to just one summary row per customer. I always highlight this default-frame gotcha in interviews, because it's exactly the kind of subtle bug that silently produces wrong results without throwing any error.

**Background Music Vibe:** Revealing, gotcha-focused beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q20. Detect and Handle Duplicate Column Names

**Hook / Short Description (for caption):**
Ambiguous column errors after a join are annoying — here's the clean rename-before-join fix I always reach for.

**Coding Problem:**
You load two dataframes that both have a column named 'status', and joining them causes an ambiguous column reference error. Write PySpark code to join them safely.

**Code Solution (show on screen):**
```python
df1_renamed = df1.withColumnRenamed("status", "status_orders")
df2_renamed = df2.withColumnRenamed("status", "status_shipments")

result = df1_renamed.join(df2_renamed, df1_renamed.order_id == df2_renamed.order_id, "inner")
result.show()
```

**2-Minute Script** (~174 words, ~1m 15s at natural pace):

> This ambiguous column error is extremely common in real pipelines, and the cleanest, most explicit fix is renaming the conflicting columns before the join even happens, rather than trying to reference them ambiguously afterward with tricks like dataframe aliasing at select time. I use withColumnRenamed on each dataframe separately, giving each status column a distinct, meaningful name that reflects which source it came from — status_orders and status_shipments — before performing the join. This way, once the join executes, there's no ambiguity at all in the resulting dataframe, since every column name is now unique, and anyone reading the downstream code immediately understands which status refers to which source without needing to trace back through the join logic. I mention in interviews that an alternative fix is using dataframe aliases, like df1.alias('a').join(df2.alias('b'), ...) and then referencing col('a.status') versus col('b.status'), but I actually prefer the upfront rename approach for anything beyond a quick one-off query, because the renamed columns stay meaningfully labeled throughout the rest of the pipeline, not just within that one join statement.

**Background Music Vibe:** Practical, fix-it beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #Debugging

---

## Q21. Find Gaps in a Sequence of IDs

**Hook / Short Description (for caption):**
Missing IDs in a sequence is the same gap-finding pattern as missing dates — generate the ideal range, then left_anti join.

**Coding Problem:**
Given a table of order_id values that should be sequential integers, write PySpark code to find which order_id values are missing from the sequence.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import min as _min, max as _max, sequence, explode, col

bounds = df.agg(_min("order_id").alias("min_id"), _max("order_id").alias("max_id")).collect()[0]

full_range = spark.range(bounds["min_id"], bounds["max_id"] + 1).withColumnRenamed("id", "order_id")

missing_ids = full_range.join(df, "order_id", "left_anti")
missing_ids.show()
```

**2-Minute Script** (~179 words, ~1m 17s at natural pace):

> This is really the same core idea as the missing dates problem from earlier, just applied to integers instead of dates, which is a great thing to point out in an interview since it shows you recognize the underlying pattern rather than treating every problem as unique. I first find the minimum and maximum order_id values present in the actual data, which defines the full range that should theoretically exist if nothing were missing. I use spark.range to generate every single integer within that min-to-max bound, giving me the complete, ideal sequence. Then, just like before, I use a left_anti join between that generated full sequence and the real data, joined on order_id, which returns exactly the id values that exist in my generated ideal sequence but have no match in the actual table — meaning they're missing. I always call out that spark.range is a great, efficient way to generate a sequence of integers directly on the cluster without needing to collect anything to the driver first, which matters if the range itself could potentially be very large.

**Background Music Vibe:** Pattern-recognition beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #DataQuality

---

## Q22. Compute Running Distinct Count

**Hook / Short Description (for caption):**
There's no built-in running-distinct-count window function — here's how collect_set and size build one, plus the memory tradeoff.

**Coding Problem:**
Given a table of user_id and event_date, write PySpark code to compute, for each date, the cumulative count of distinct users seen up to and including that date.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import collect_set, size, col

w = (
    Window.orderBy("event_date")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result = (
    df.withColumn("users_so_far", collect_set("user_id").over(w))
      .withColumn("running_distinct_users", size(col("users_so_far")))
      .drop("users_so_far")
)
result.show()
```

**2-Minute Script** (~195 words, ~1m 24s at natural pace):

> Running distinct counts are trickier than a regular cumulative sum, because window functions don't have a built-in running-distinct-count aggregation the way they do for sum or count — I have to build it myself using collect_set. I order by event_date with a frame of unbounded preceding to current row, same cumulative pattern as before, but instead of summing a numeric column, I use collect_set on user_id, which accumulates all the unique user_id values seen so far into an array, automatically de-duplicating as it goes since collect_set only keeps distinct values. Then I just take the size of that array using the size function, which gives me the actual running distinct count as a number, and I drop the intermediate array column since I only needed it as scaffolding. I always flag the performance consideration here honestly in interviews — collect_set within a window function has to hold a growing array in memory for every row, so on a dataset with a huge number of distinct users, this can become memory-intensive, and at truly massive scale, an approximate distinct count using approx_count_distinct combined with a different strategy might be a more scalable, if less exact, alternative.

**Background Music Vibe:** Building, accumulating beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q23. Reverse a String Column

**Hook / Short Description (for caption):**
Reversing a string is trivial in PySpark — but the real lesson is always checking for a built-in before reaching for a UDF.

**Coding Problem:**
Write PySpark code to reverse the characters in each string within a text column.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import reverse, col

result = df.withColumn("reversed_text", reverse(col("text")))
result.show()
```

**2-Minute Script** (~172 words, ~1m 14s at natural pace):

> This one's refreshingly simple because PySpark has a built-in reverse function that works directly on string columns, so there's no need to write a UDF or manually manipulate characters. I just wrap the column in reverse, and Spark handles reversing the character order natively at the JVM level, which is both correct and fast since it avoids any Python overhead entirely. What I always highlight in an interview, even for a simple question like this, is the broader principle behind it — a lot of candidates jump straight to writing a Python UDF for basic string manipulation tasks like this, but that's actually a performance anti-pattern, since it forces data to serialize out to a separate Python process unnecessarily. So before reaching for a UDF on any string or text problem, I always check Spark's built-in string functions first, things like reverse, upper, lower, trim, substring, and regexp_replace, because native functions run natively and stay fully optimized by Catalyst, while a UDF becomes an opaque black box the optimizer can't see inside.

**Background Music Vibe:** Light, quick-win beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #PythonUDF

---

## Q24. Check for Palindrome Strings

**Hook / Short Description (for caption):**
Palindrome checking is just composing two built-in functions — lower() and reverse() — no custom logic needed.

**Coding Problem:**
Given a column of words, write PySpark code to add a boolean column indicating whether each word is a palindrome.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import reverse, lower, col

result = df.withColumn(
    "is_palindrome",
    lower(col("word")) == reverse(lower(col("word")))
)
result.show()
```

**2-Minute Script** (~175 words, ~1m 15s at natural pace):

> This builds directly on the previous reverse function, and it's a nice example of composing simple built-in functions together instead of writing custom logic. I lowercase the word first, since palindrome checks are usually meant to be case-insensitive — otherwise 'Level' wouldn't equal its own reverse due to the capital L only appearing on one side. Then I compare that lowercased word directly against the reverse of that same lowercased word using a simple equality check, which naturally produces a boolean column, true when the word reads the same forwards and backwards, false otherwise. What I like about pointing this out in an interview is that it shows a pattern of building up more complex logic from small, composable, native Spark functions, rather than immediately reaching for something more heavyweight like a UDF. If the requirement expanded to also ignore spaces or punctuation, like checking whether 'race car' is a palindrome, I'd chain in a regexp_replace step beforehand to strip out anything that's not a letter, again using built-in functions rather than custom Python logic.

**Background Music Vibe:** Playful, wordplay beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #StringFunctions

---

## Q25. Count Vowels in Each String

**Hook / Short Description (for caption):**
No built-in 'count character occurrences' function in PySpark? Here's the length-before-and-after trick that solves it anyway.

**Coding Problem:**
Write PySpark code to count the number of vowels in each string within a text column.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import length, regexp_replace, col

result = df.withColumn(
    "vowel_count",
    length(col("text")) - length(regexp_replace(col("text"), "(?i)[aeiou]", ""))
)
result.show()
```

**2-Minute Script** (~183 words, ~1m 18s at natural pace):

> This uses a really neat trick that avoids needing to actually extract or iterate over individual characters at all. Instead of trying to count vowels directly, I calculate the difference in string length before and after removing all the vowels. I use regexp_replace with a case-insensitive vowel pattern to strip out every vowel character from the string, replacing them with an empty string, then compare that resulting length against the original string's length — the difference between those two lengths is exactly the number of vowel characters that got removed, which is the vowel count I'm after. This length-difference trick is a genuinely useful general pattern any time I need to count occurrences of a character pattern within a string using only built-in Spark functions, since Spark doesn't have a direct 'count occurrences of pattern' string function out of the box. I always mention the case-insensitive flag, that little (?i) at the start of the regex pattern, since forgetting it would mean uppercase vowels like 'A' or 'E' don't get matched and removed, silently undercounting the vowels in any string containing capital letters.

**Background Music Vibe:** Clever trick reveal beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #StringFunctions

---

## Q26. Extract Domain from Email Column

**Hook / Short Description (for caption):**
Extracting an email domain is a simple split() and getItem() — but here's the messy-data edge case worth knowing.

**Coding Problem:**
Given a column of email addresses, write PySpark code to extract just the domain portion (everything after the @ symbol).

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import split, col

result = df.withColumn("domain", split(col("email"), "@").getItem(1))
result.show()
```

**2-Minute Script** (~189 words, ~1m 21s at natural pace):

> For this, I use the split function, which breaks a string into an array based on a delimiter — here splitting on the @ symbol, which cleanly separates the local part of the email from the domain part into a two-element array. Then I use getItem with index 1, which grabs the second element of that array, zero-indexed, giving me just the domain portion. I always mention the edge case worth considering here, which is what happens with a malformed email that has no @ symbol at all, or has multiple @ symbols — split would either produce an array with just one element, in which case getItem(1) would return null rather than erroring, which is actually a fairly graceful failure, or it would produce more than 2 elements if there were multiple @ symbols, in which case getItem(1) would just grab everything up to the second @, not necessarily the full correct domain. So for genuinely messy or unvalidated email data, I'd usually pair this with an upstream data quality check that validates emails match a proper single-@ pattern before trusting this extraction to be fully accurate downstream.

**Background Music Vibe:** Everyday, practical beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #StringFunctions

---

## Q27. Split a Comma-Separated String Into Multiple Rows

**Hook / Short Description (for caption):**
Split then explode — this two-function combo turns any delimited string column into proper normalized rows.

**Coding Problem:**
Given a column containing comma-separated tags like 'vip,new,online', write PySpark code to split each row into multiple rows, one per tag.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import split, explode, col

result = df.select("customer_id", explode(split(col("tags"), ",")).alias("tag"))
result.show()
```

**2-Minute Script** (~149 words, ~1m 4s at natural pace):

> This combines two functions I've used separately before, split and explode, chained directly together, which is a really common combo for turning delimited string data into properly normalized rows. Split first breaks the comma-separated string into an actual array type column, turning 'vip,new,online' into the array ['vip', 'new', 'online']. Then explode takes that array and, just like with the earlier array-column question, expands it into one separate row per element, duplicating the customer_id across each of them. I always point out that this split-then-explode pattern is essentially how I'd handle any denormalized, delimiter-packed string column that should really be modeled as separate rows in a properly normalized table, and it's one of the most common real-world data cleaning patterns, since a lot of source systems, especially exports from spreadsheets or legacy systems, store multi-valued fields as a single delimited string rather than a true array or a separate table.

**Background Music Vibe:** Two-step transformation beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #ETL

---

## Q28. Merge Multiple Columns Into One

**Hook / Short Description (for caption):**
Concat_ws beats plain concat for combining columns — here's why null-handling makes all the difference.

**Coding Problem:**
Given a dataframe with separate first_name, last_name, and city columns, write PySpark code to create a single formatted full_address_label column combining them.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import concat_ws, col

result = df.withColumn(
    "full_label",
    concat_ws(" - ", concat_ws(" ", col("first_name"), col("last_name")), col("city"))
)
result.show()
```

**2-Minute Script** (~182 words, ~1m 18s at natural pace):

> For combining multiple columns into one, I specifically use concat_ws rather than plain concat, and that's a deliberate choice worth explaining in an interview. Concat_ws stands for concatenate with separator, and it takes the separator character as its first argument, then combines all the following columns using that separator between them. The real advantage over plain concat is how it handles nulls — if any of the columns being combined is null, concat_ws simply skips it and continues joining the remaining non-null values, whereas plain concat would return an entirely null result the moment any single input column is null, which is almost never the actual desired behavior in a real report or label. In this example, I nest two concat_ws calls, first joining first_name and last_name with a space to build a full name, then joining that result with city using a different separator, a dash, to build the final combined label. This nested concat_ws pattern is really flexible for building any kind of formatted, human-readable label from several underlying columns, while staying resilient to missing data in any individual field.

**Background Music Vibe:** Warm, assembling beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #StringFunctions

---

## Q29. Find the Longest Word in Each Row

**Hook / Short Description (for caption):**
Comparing array elements by one property but returning another needs a struct trick inside transform() — here's exactly how.

**Coding Problem:**
Given a text column containing sentences, write PySpark code to find the longest word in each row.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import split, expr, col

result = df.withColumn(
    "longest_word",
    expr("array_max(transform(split(text, ' '), x -> struct(length(x) as len, x as word))).word")
)
result.show()
```

**2-Minute Script** (~210 words, ~1m 30s at natural pace):

> This one requires a slightly more advanced technique, using a higher-order function inside a SQL expression, because I need to compare words by their length while still returning the actual word itself, not just the length number. I first split the sentence into an array of individual words, then use the transform higher-order function, which applies a lambda-style expression to every element of that array — here, transforming each word x into a small struct containing both its length and the word itself. The reason I wrap it in a struct rather than just an array of lengths is specifically so I can find the maximum by length while still keeping the corresponding word attached to it, since struct comparisons in Spark SQL compare field by field in order, meaning array_max will correctly find the struct with the largest length value first. After getting that maximum struct, I just access its word field to get the actual longest word itself. I always mention in interviews that higher-order functions like transform, filter, and aggregate operating directly on arrays are a relatively newer but very powerful part of Spark SQL, letting me avoid exploding a column out to separate rows and then re-aggregating, just to answer a per-row question like this one.

**Background Music Vibe:** Advanced, leveling-up beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #HigherOrderFunctions

---

## Q30. Calculate Difference Between Consecutive Rows

**Hook / Short Description (for caption):**
Day-over-day change is a one-line lag() function — here's the pattern, plus why the first row is correctly null, not broken.

**Coding Problem:**
Given a stock price table with symbol, date, and close_price, write PySpark code to calculate the day-over-day price change for each stock.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import lag, col

w = Window.partitionBy("symbol").orderBy("date")

result = df.withColumn(
    "price_change",
    col("close_price") - lag("close_price", 1).over(w)
)
result.show()
```

**2-Minute Script** (~186 words, ~1m 20s at natural pace):

> The lag function is exactly built for this kind of consecutive-row comparison, and it's one of the most frequently used window functions in real analytical pipelines. I partition by symbol so each stock's day-over-day comparison stays independent from every other stock, and order by date since the comparison is inherently about sequence. Lag with an offset of 1 pulls the value from exactly one row before the current row, within that ordered partition, which here gives me yesterday's closing price sitting right alongside today's row. Subtracting that lagged value from the current close_price gives me the actual day-over-day change. I always mention the first-row edge case in interviews, since the very first date for each stock symbol has no previous row to look back at, so lag naturally returns null there, and the resulting price_change for that first row is also null, which is the correct, expected behavior rather than a bug that needs fixing. If I instead needed to look forward, comparing today's price to tomorrow's, I'd use the lead function instead, which works identically but looks ahead in the ordered partition rather than behind.

**Background Music Vibe:** Comparative, tracking beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q31. Find Customers Who Purchased in Every Month of the Year

**Hook / Short Description (for caption):**
'Purchased every month' sounds complex — until you reframe it as a simple distinct-month-count-equals-12 check.

**Coding Problem:**
Given an orders table with customer_id and order_date, write PySpark code to find customers who made at least one purchase in every single month of a given year.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import month, col, countDistinct, lit

year_orders = df.filter(col("order_date").substr(1, 4) == "2025")

result = (
    year_orders.withColumn("order_month", month("order_date"))
               .groupBy("customer_id")
               .agg(countDistinct("order_month").alias("distinct_months"))
               .filter(col("distinct_months") == 12)
)
result.show()
```

**2-Minute Script** (~174 words, ~1m 15s at natural pace):

> This is really a distinct-count problem in disguise, and recognizing that reframing is the key to solving it cleanly. Instead of trying to check for 12 specific individual months explicitly, I extract just the month number from each order date, and then count how many distinct month values each customer actually has across their orders that year. If a customer purchased in every single month, their distinct month count will be exactly 12, since there are only 12 possible month values total, and countDistinct naturally de-duplicates, so multiple purchases within the same month only count once toward that total. I filter down to customers where that distinct count equals exactly 12, which gives me exactly the customers who never had a gap month with zero purchases. I always highlight this reframing technique in interviews, since a lot of candidates initially try to write much more complex logic checking for each month explicitly, when converting the requirement into a simple distinct-count-equals-N check is both far simpler to write and much more efficient to actually execute.

**Background Music Vibe:** Reframing, insight beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #TechInterview

---

## Q32. Compute Median Per Group

**Hook / Short Description (for caption):**
There's no plain median() function in PySpark — percentile_approx is the standard answer, and the word 'approx' actually matters.

**Coding Problem:**
Given a dataframe with dept and salary columns, write PySpark code to compute the median salary per department.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import expr

result = df.groupBy("dept").agg(
    expr("percentile_approx(salary, 0.5)").alias("median_salary")
)
result.show()
```

**2-Minute Script** (~214 words, ~1m 32s at natural pace):

> Spark doesn't have a plain built-in median function, but percentile_approx with 0.5 as the percentile value gives exactly the median, and it's the standard, expected way to compute this in PySpark. I use it inside a SQL expression through the expr function, grouped by department, so I get one median salary value per department. The word approx in the function name is deliberate and important to explain in an interview — this isn't calculating an exact, mathematically precise median, it's using an approximation algorithm under the hood that trades a small amount of accuracy for dramatically better performance at scale, since computing a truly exact median would require a full sort of all the data, which is expensive on very large distributed datasets. Percentile_approx actually accepts an optional third argument controlling the accuracy versus performance tradeoff, where a higher value gives a more precise result at the cost of more computation, and the default without specifying it is usually accurate enough for most real business reporting use cases. I always mention that if a use case genuinely requires an exact, non-approximate median, that's a case where I'd need a different, more expensive approach involving an actual full sort and row-position lookup, and I'd flag that performance cost explicitly to whoever needs that exact precision.

**Background Music Vibe:** Thoughtful, precise beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #Statistics

---

## Q33. Compute Mode Per Group

**Hook / Short Description (for caption):**
No built-in mode() function either — but two patterns you already know (groupBy count + top-1 window) combine to solve it.

**Coding Problem:**
Given a dataframe with customer_id and product_category, write PySpark code to find each customer's most frequently purchased product category.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import count, row_number, col

counts = df.groupBy("customer_id", "product_category").agg(count("*").alias("cnt"))

w = Window.partitionBy("customer_id").orderBy(col("cnt").desc())

mode_result = (
    counts.withColumn("rn", row_number().over(w))
          .filter(col("rn") == 1)
          .select("customer_id", "product_category", "cnt")
)
mode_result.show()
```

**2-Minute Script** (~181 words, ~1m 18s at natural pace):

> Since Spark doesn't have a direct built-in mode function either, I build it from two patterns I've already used elsewhere — a groupBy count followed by a top-1-per-group window function. First, I group by both customer_id and product_category together, counting how many times each customer purchased each specific category, which gives me a frequency table. Then, I apply the same top-N-per-group window pattern from earlier, partitioning by customer_id and ordering by that count descending, using row_number to rank each customer's categories from most to least frequently purchased, and filtering for row number equal to 1, which gives me exactly their single most frequent category. I always mention the tie-handling consideration here — if a customer purchased two different categories the exact same number of times, row_number would arbitrarily pick just one of them as 'the' mode, which might not reflect the true ambiguity in the data. If I wanted to surface all tied modes instead of just picking one, I'd swap row_number for rank instead, which would correctly return every category tied for the top count rather than silently choosing one.

**Background Music Vibe:** Combining, synthesizing beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #WindowFunctions

---

## Q34. Find Anagram Groups From a List of Words

**Hook / Short Description (for caption):**
Anagram detection comes down to one insight — sorted letters become a shared signature. Here's the honest case for using a UDF.

**Coding Problem:**
Given a dataframe with a single column of words, write PySpark code to group together all words that are anagrams of each other.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

@udf(StringType())
def sort_chars(word):
    return "".join(sorted(word.lower()))

result = (
    df.withColumn("signature", sort_chars(col("word")))
      .groupBy("signature")
      .agg({"word": "collect_list"})
      .withColumnRenamed("collect_list(word)", "anagram_group")
)
result.show()
```

**2-Minute Script** (~209 words, ~1m 30s at natural pace):

> The core insight for detecting anagrams is that any two words are anagrams of each other if and only if sorting their individual letters produces the identical resulting string — so 'listen' and 'silent' both sort down to the exact same letter sequence. I create a small UDF that lowercases a word and sorts its characters alphabetically, producing what I call a signature string. I do use a UDF here rather than a built-in function, since Spark doesn't have a native character-sorting function for strings, and this is a genuinely reasonable, honest case for a UDF, since there's no equivalent built-in to reach for first, unlike some of the earlier string problems. Once every word has this sorted-letter signature computed, any words that are true anagrams of each other will share the exact same signature value, so I simply group by that signature column and collect all the original words sharing each signature into a list using collect_list, which gives me each group of anagrams together. I always mention that for very large datasets, this UDF-based character sorting does carry the usual Python UDF performance cost we discussed earlier, so at genuinely massive scale, I'd look into whether a Pandas UDF version could vectorize this character-sorting logic more efficiently.

**Background Music Vibe:** Clever discovery beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #PythonUDF

---

## Q35. Find Pairs of Numbers That Sum to a Target

**Hook / Short Description (for caption):**
Two-sum in Spark isn't a hash map — it's a self-join. Here's the distributed, set-based way to think about it.

**Coding Problem:**
Given a dataframe with a single numeric column, write PySpark code to find all pairs of values that sum to a specific target number, similar to the classic 'two sum' problem.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import col

a = df.withColumnRenamed("value", "value_a")
b = df.withColumnRenamed("value", "value_b")

target = 100

pairs = (
    a.join(b, col("value_a") + col("value_b") == target, "inner")
     .filter(col("value_a") < col("value_b"))
     .select("value_a", "value_b")
)
pairs.show()
```

**2-Minute Script** (~215 words, ~1m 32s at natural pace):

> This is the classic two-sum coding interview problem, but adapted to Spark's distributed, set-based way of thinking rather than the typical single-threaded hash-map approach you'd use in plain Python. Since Spark works on entire distributed dataframes rather than iterating element by element, the natural approach is a self-join, similar to the earlier employee-manager pattern, but this time the join condition itself is the actual sum check — I join the dataframe against itself where value_a plus value_b equals the target. The important extra step is that final filter, value_a less than value_b, and I always explain why that's necessary in interviews — without it, a self-join like this naturally produces every pair twice, once as (a, b) and once as (b, a), plus it could even match a value against itself if the target happens to be exactly double that value. Filtering for value_a strictly less than value_b eliminates both of those problems at once, ensuring each true pair appears exactly one time in the result, with a consistent, deterministic ordering. I always point out that this join-based approach is the fundamentally correct mental model shift from single-machine algorithmic thinking to distributed, set-based dataframe thinking, which is exactly what interviewers are usually testing for when they bring a classic algorithm question into a Spark context.

**Background Music Vibe:** Mental-shift, aha beat

**Top 5 Hashtags:** #PySpark #CodingInterview #SelfJoin #DataEngineering #SparkSQL

---

## Q36. Find Max Consecutive Sequence Length Per Group

**Hook / Short Description (for caption):**
Consecutive uptime streaks use the same trick as login streaks — a running count of resets becomes your streak identifier.

**Coding Problem:**
Given a table of machine_id and status ('up' or 'down') ordered by timestamp, write PySpark code to find the longest consecutive streak of 'up' status for each machine.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, sum as _sum, when

w = Window.partitionBy("machine_id").orderBy("timestamp")

flagged = df.withColumn("is_up", when(col("status") == "up", 1).otherwise(0))

streak_group = flagged.withColumn(
    "reset_flag",
    _sum(when(col("is_up") == 0, 1).otherwise(0)).over(
        w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
)

longest = (
    streak_group.filter(col("is_up") == 1)
                .groupBy("machine_id", "reset_flag")
                .count()
                .groupBy("machine_id")
                .agg({"count": "max"})
                .withColumnRenamed("max(count)", "longest_up_streak")
)
longest.show()
```

**2-Minute Script** (~206 words, ~1m 28s at natural pace):

> This is a more advanced version of the earlier login streak problem, using a similar group-by-difference idea, but adapted for a binary status column instead of dates. I first flag each row as 1 if the status is up, or 0 otherwise. Then, the key trick is computing a running cumulative count of how many 'down' events, meaning is_up equals zero, have occurred so far, up through the current row, using a cumulative sum window. This running down-count stays constant during any consecutive run of up statuses, since it only increments when a down status actually occurs, which means it effectively acts as a streak identifier — every row within the same unbroken up-streak shares the identical running down-count value. So I filter down to just the up rows, then group by machine_id and that running down-count value, and each resulting group's size is exactly the length of that particular up-streak, since only up rows are included at this point and they're all grouped by their shared streak identifier. Finally, I take the max streak length per machine across all of that machine's individual streak groups. This running-count-as-streak-identifier technique generalizes the earlier date-based streak trick to any kind of state-based consecutive sequence problem, not just dates.

**Background Music Vibe:** Technical deep-dive beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q37. Calculate Year-Over-Year Growth

**Hook / Short Description (for caption):**
Year-over-year growth is lag() plus a percentage formula — here's the pattern and the zero-revenue edge case to watch for.

**Coding Problem:**
Given a table with year and total_revenue per year, write PySpark code to calculate the year-over-year percentage growth in revenue.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import lag, col, round as _round

w = Window.orderBy("year")

result = df.withColumn(
    "yoy_growth_pct",
    _round(((col("total_revenue") - lag("total_revenue", 1).over(w)) / lag("total_revenue", 1).over(w)) * 100, 2)
)
result.show()
```

**2-Minute Script** (~223 words, ~1m 36s at natural pace):

> This directly extends the lag-based day-over-day change pattern from earlier, just applied at a yearly grain and converted into a percentage rather than a raw difference. I order by year, since there's no separate grouping dimension needed here if this is already aggregated to one row per year, and use lag with an offset of 1 to pull the previous year's revenue value alongside the current year's row. The growth percentage formula itself is standard — current value minus previous value, divided by that previous value, multiplied by 100 to express it as a percentage, and I wrap the whole thing in a round to 2 decimal places for a clean, readable output. I always mention the edge case of the very first year in the dataset, where lag naturally returns null since there's no prior year to compare against, which correctly makes that first year's growth percentage also null, rather than some kind of division error — and that's the expected, correct behavior since you genuinely can't calculate growth without a prior baseline to grow from. I'd also flag, if this were real production reporting code, the importance of protecting against a genuine division-by-zero case if a prior year's revenue happened to be exactly zero, which would need an explicit null or zero-handling check before this calculation to avoid a runtime error.

**Background Music Vibe:** Growth-oriented, upward beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q38. Detect Outliers Using Z-Score

**Hook / Short Description (for caption):**
Z-score outlier detection needs global stats first — here's the legitimate case for collecting a small aggregate to the driver.

**Coding Problem:**
Given a dataframe with a numeric column value, write PySpark code to flag rows as outliers if their z-score (number of standard deviations from the mean) exceeds 3.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import mean, stddev, col, abs as _abs

stats = df.select(mean("value").alias("avg_val"), stddev("value").alias("std_val")).collect()[0]

result = df.withColumn(
    "z_score",
    (col("value") - stats["avg_val"]) / stats["std_val"]
).withColumn(
    "is_outlier",
    _abs(col("z_score")) > 3
)
result.show()
```

**2-Minute Script** (~215 words, ~1m 32s at natural pace):

> For z-score outlier detection, I first need the overall mean and standard deviation of the entire column, which are single global values, not something calculated per row, so I compute them separately using an aggregation and collect that single result row back to the driver — this is one of the rare, legitimate cases where collecting a small, single-row result to the driver is completely fine, since it's just two scalar numbers, not a large dataset. Once I have those two values available as plain Python variables, I compute each row's z-score using the standard formula, subtracting the mean from the value and dividing by the standard deviation, which tells me how many standard deviations that specific value sits away from the overall average. Then I flag a row as an outlier if the absolute value of that z-score exceeds 3, which is a commonly used statistical threshold, since values beyond 3 standard deviations are relatively rare in a roughly normal distribution. I always mention in interviews that z-score based outlier detection assumes the underlying data is at least roughly normally distributed, and for data that's heavily skewed or has a very different distribution shape, a different outlier detection method, like using the interquartile range instead of standard deviation, would actually be more statistically appropriate.

**Background Music Vibe:** Analytical, statistical beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #Statistics

---

## Q39. Custom UDF to Validate Email Format

**Hook / Short Description (for caption):**
A real, legitimate UDF use case — but the regex-compiled-once-outside detail is what separates good UDF code from slow UDF code.

**Coding Problem:**
Write a PySpark UDF that validates whether each value in an email column matches a proper email format, returning True or False.

**Code Solution (show on screen):**
```python
import re
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType

EMAIL_PATTERN = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")

@udf(BooleanType())
def is_valid_email(email):
    if email is None:
        return False
    return bool(EMAIL_PATTERN.match(email))

result = df.withColumn("is_valid_email", is_valid_email(col("email")))
result.show()
```

**2-Minute Script** (~201 words, ~1m 26s at natural pace):

> This is actually a solid, legitimate use case for a UDF, since regular expression-based validation like this isn't something Spark's built-in string functions handle as cleanly as Python's re module does for a genuinely complex pattern like a full email validation regex. I define the compiled regex pattern once, outside the UDF function itself, rather than inside it, which is a small but real performance detail worth mentioning — compiling a regex pattern has some overhead, and if I compiled it fresh inside the UDF, it would get recompiled on every single row's function call, which adds up significantly across millions of rows, whereas compiling it once at module level means it's reused efficiently across every invocation. I explicitly declare the return type as BooleanType in the udf decorator, since PySpark needs to know the expected output type upfront to properly integrate the UDF's results back into the dataframe's schema. I also explicitly handle the None case first, returning False immediately for null emails, since calling regex match on a None value would throw a Python error rather than gracefully returning a boolean, and defensive null-handling like this is something interviewers specifically watch for when they hand you a UDF-writing question.

**Background Music Vibe:** Careful, craftsmanship beat

**Top 5 Hashtags:** #PySpark #CodingInterview #PythonUDF #DataEngineering #SparkSQL

---

## Q40. Convert a List of Dictionaries to a DataFrame With Explicit Schema

**Hook / Short Description (for caption):**
Never let Spark guess your schema in production — here's why explicit StructType definitions beat inference every time.

**Coding Problem:**
Write PySpark code to convert a Python list of dictionaries into a dataframe with an explicitly defined schema, rather than relying on schema inference.

**Code Solution (show on screen):**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [
    {"name": "Asha", "age": 29},
    {"name": "Rohit", "age": 34},
]

schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
])

df = spark.createDataFrame(data, schema=schema)
df.show()
df.printSchema()
```

**2-Minute Script** (~214 words, ~1m 32s at natural pace):

> For creating a dataframe from raw Python data, I always explicitly define the schema using StructType and StructField rather than letting Spark infer it automatically, and I make a point of explaining why in interviews, since it's a genuinely important best practice, not just a stylistic preference. Schema inference works by sampling some or all of the actual data to guess appropriate types, which is both an added performance cost on large datasets, and genuinely unreliable — inferred types can quietly turn out wrong in edge cases, like a numeric-looking column that actually contains an occasional non-numeric value, or a column that looks like it's always populated in a small sample but turns out to have nulls elsewhere. Explicitly defining the schema up front avoids that ambiguity entirely, guarantees consistent, predictable types every single run regardless of the underlying data's specific values, and also lets me be intentional about nullability constraints, like marking name as non-nullable here since it's a required field. I always mention that in any real production pipeline, especially one reading from a file-based or API source where the schema could shift over time, explicit schemas act as a form of validation and documentation simultaneously, immediately surfacing a schema mismatch error rather than letting bad or unexpected data silently flow downstream.

**Background Music Vibe:** Deliberate, disciplined beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #DataQuality

---

## Q41. Read Multiple CSV Files and Add a Filename Column

**Hook / Short Description (for caption):**
Ingesting a whole directory of files at once? input_file_name() gives you row-level traceability back to the source file.

**Coding Problem:**
Write PySpark code to read all CSV files from a directory and add a new column containing the source filename for each row.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import input_file_name, col

df = spark.read.option("header", True).csv("/path/to/directory/*.csv")
result = df.withColumn("source_file", input_file_name())
result.show()
```

**2-Minute Script** (~204 words, ~1m 27s at natural pace):

> This is a really common real-world ingestion requirement, since knowing exactly which source file a given row originated from is essential for debugging and auditing when you're ingesting many files at once, especially in a daily or incremental load pattern. Spark's read.csv naturally handles reading an entire directory or a wildcard pattern like this all at once, treating every matching file as part of a single unified dataframe, which is convenient but also means, by default, I lose track of which specific file each row came from. The input_file_name function solves exactly this — it's a built-in Spark function that returns the full path of the source file that each specific row was actually read from, so adding it as a new column with withColumnRenamed gives me full traceability all the way back to the exact originating file for every single row, without needing to loop through files individually and tag them manually myself. I always mention that this becomes especially valuable when I combine it with the earlier corrupt record quarantine pattern, since knowing exactly which upstream file contained bad or malformed records makes investigating and communicating the issue back to the source team dramatically faster than just knowing 'somewhere in today's batch.'

**Background Music Vibe:** Traceable, orderly beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #ETL

---

## Q42. Handle Multiple Date Formats in One Column

**Hook / Short Description (for caption):**
Mixed date formats in one column? Coalesce and multiple to_date attempts let each row silently self-select its correct parser.

**Coding Problem:**
Given a date column where some rows use 'MM/dd/yyyy' format and others use 'yyyy-MM-dd', write PySpark code to parse them all into a consistent date type.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import coalesce, to_date, col

result = df.withColumn(
    "parsed_date",
    coalesce(
        to_date(col("date_str"), "MM/dd/yyyy"),
        to_date(col("date_str"), "yyyy-MM-dd"),
    )
)
result.show()
```

**2-Minute Script** (~211 words, ~1m 30s at natural pace):

> This is a great, honest use case for coalesce, which I use very differently here than the typical null-fallback-to-default pattern people usually associate with it. I call to_date twice on the exact same source column, but with two different expected format strings — once assuming MM/dd/yyyy, and once assuming yyyy-MM-dd. The key mechanic that makes this work is that to_date returns null whenever the input string doesn't actually match the format pattern it was given, rather than throwing an error, so a date string that's genuinely in MM/dd/yyyy format will successfully parse on the first attempt, but will return null on the second attempt since it doesn't match the yyyy-MM-dd pattern, and vice versa for the other format. Coalesce then simply takes the first non-null value among its arguments, in order, which means it naturally picks whichever of the two parsing attempts actually succeeded for that particular row. I always mention that this pattern scales to however many distinct date formats might be present, just by adding more to_date attempts with different format strings as additional coalesce arguments, though at some point, if there are genuinely many different formats mixed together, it's worth pushing back upstream and asking why the source system isn't producing consistent date formatting in the first place.

**Background Music Vibe:** Adaptive, flexible beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #DataQuality

---

## Q43. Convert RDD to DataFrame With a Schema

**Hook / Short Description (for caption):**
Converting a legacy RDD to a proper DataFrame is one function call — but explicit schema still matters even more here.

**Coding Problem:**
Given a plain RDD of tuples like (1, 'Asha', 29), write PySpark code to convert it into a proper DataFrame with named columns and explicit types.

**Code Solution (show on screen):**
```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

rdd = spark.sparkContext.parallelize([(1, "Asha", 29), (2, "Rohit", 34)])

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True),
])

df = spark.createDataFrame(rdd, schema)
df.show()
```

**2-Minute Script** (~210 words, ~1m 30s at natural pace):

> Even though I generally steer people away from RDDs for everyday pipeline work, knowing how to convert an existing RDD into a proper DataFrame is still a genuinely useful skill, especially when working with legacy code or a library that only exposes an RDD-based interface. I use spark.createDataFrame, passing in the RDD directly along with an explicitly defined schema, the same StructType and StructField pattern I'd use for any other explicit schema definition. The main reason I go out of my way to define the schema explicitly here, rather than letting Spark try to infer it from the RDD's tuple structure, is that RDDs don't carry any inherent schema or type metadata at all, unlike a DataFrame — Spark would have to sample the RDD's actual data to guess reasonable types, which carries the same inference risks and cost I mentioned earlier, but is honestly even less reliable here since RDDs of raw tuples don't have any column names attached at all without a schema being explicitly provided. Once converted into a proper DataFrame with a defined schema like this, I immediately get full access to Catalyst's query optimization, columnar operations, and all the DataFrame API's built-in functions, none of which are available while working directly with the raw RDD.

**Background Music Vibe:** Bridging, connecting beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #RDD

---

## Q44. Optimize a Naive Python Loop Into Vectorized PySpark Code

**Hook / Short Description (for caption):**
Converting a Python for-loop to PySpark isn't just syntax — it's a fundamental shift from row-by-row to columnar thinking.

**Coding Problem:**
You're given Python code that loops through a list of records one at a time, applying a discount calculation and appending results to a new list. Rewrite this logic as efficient, vectorized PySpark code.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import when, col

result = df.withColumn(
    "final_price",
    when(col("category") == "electronics", col("price") * 0.9)
    .when(col("category") == "clothing", col("price") * 0.8)
    .otherwise(col("price"))
)
result.show()
```

**2-Minute Script** (~209 words, ~1m 30s at natural pace):

> This question is really testing whether a candidate understands the fundamental shift from row-by-row, imperative thinking to Spark's columnar, declarative way of expressing transformations, which is honestly one of the most important mental models to internalize when moving from plain Python into distributed data engineering. A naive loop-based approach processes one record at a time sequentially, checking a condition and computing a value for each individual row in turn, which simply doesn't parallelize or distribute across a cluster at all. The vectorized PySpark equivalent uses when and otherwise, chained together like a case statement, which conceptually describes the same conditional logic — different discount percentages depending on category — but describes it as a single declarative expression that gets applied to the entire column all at once, across every row and every partition in parallel, rather than being manually iterated. I always explain that this same shift in thinking is exactly why regular Python UDFs, which do bring back that row-by-row, imperative processing model into Spark, tend to be so much slower than native column expressions like when and otherwise — the whole performance advantage of Spark's DataFrame API comes specifically from staying in this declarative, columnar style wherever a native function or expression can express the needed logic.

**Background Music Vibe:** Enlightening, paradigm-shift beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #TechInterview

---

## Q45. Find Rows Present in One DataFrame but Not Another

**Hook / Short Description (for caption):**
Churned customers = a left_anti join, full stop. Here's how to map plain-English business questions directly onto join types.

**Coding Problem:**
Given two dataframes of customer_id values — one from this month's active customers and one from last month's — write PySpark code to find customers who were active last month but not this month (churned customers).

**Code Solution (show on screen):**
```python
churned = last_month_df.join(this_month_df, "customer_id", "left_anti")
churned.show()
```

**2-Minute Script** (~203 words, ~1m 27s at natural pace):

> This is exactly the kind of business question — finding churned customers — that translates directly into a left_anti join, and it's worth explicitly naming that connection in an interview, since recognizing when a plain-English business requirement maps onto a specific join type is a genuinely important practical skill. I join last month's active customers against this month's active customers on customer_id, using left_anti, which by definition returns only the rows from the left dataframe that have no matching row at all on the right side. In this context specifically, that means customers who were active last month but have no corresponding matching row in this month's active customer list — exactly the churned customers I'm looking for. I always contrast this briefly against a regular inner join, which would instead give me the customers who are active in both months, and against a left_anti's logical opposite, a semi-join, which would give me the customers active last month who ARE still active this month, meaning the retained customers rather than the churned ones. Being fluent in exactly which join type maps to which specific business question is honestly one of the highest-value, most practical skills in any real analytics or data engineering role.

**Background Music Vibe:** Business-focused, applied beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #BigData

---

## Q46. Find the Nth Occurrence of an Event Per User

**Hook / Short Description (for caption):**
Finding someone's 3rd purchase is just filter-then-row_number — and this exact combo generalizes to any Nth-occurrence question.

**Coding Problem:**
Given a user event log with user_id, event_type, and event_time, write PySpark code to find the 3rd purchase event for each user.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

purchases = df.filter(col("event_type") == "purchase")

w = Window.partitionBy("user_id").orderBy("event_time")

third_purchase = (
    purchases.withColumn("purchase_rn", row_number().over(w))
             .filter(col("purchase_rn") == 3)
)
third_purchase.show()
```

**2-Minute Script** (~176 words, ~1m 15s at natural pace):

> This combines a simple filter with the row_number pattern I've used repeatedly throughout this series, and recognizing that combination is really the whole trick. I first filter the raw event log down to just purchase-type events, since I only care about ranking purchases specifically, not every event type mixed together. Then I apply the standard row_number window pattern, partitioned by user_id and ordered by event_time ascending, which assigns 1 to each user's very first purchase, 2 to their second, and so on chronologically. Filtering for purchase_rn equal to 3 then gives me exactly each user's third purchase event. I always point out in interviews that this exact same pattern instantly generalizes to finding literally any Nth occurrence of any event type — the 1st login, the 5th support ticket, the 10th page view — just by changing the filter condition on event_type and the specific rank number in the final filter, which is exactly why row_number combined with an upfront filter is such a genuinely foundational, reusable pattern across so many different real-world PySpark interview questions.

**Background Music Vibe:** Full-circle, tying-together beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q47. Compute Percentage Change Between Two Specific Rows

**Hook / Short Description (for caption):**
First-to-last percentage change reuses the same unbounded window frame trick — recognizing recurring patterns is the real skill.

**Coding Problem:**
Given a table of monthly metrics with month and value columns, write PySpark code to calculate the percentage change specifically between the first and last month in the dataset.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import first, last, col
from pyspark.sql import Window

w = Window.orderBy("month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df.withColumn("first_val", first("value").over(w))
      .withColumn("last_val", last("value").over(w))
      .withColumn("pct_change", ((col("last_val") - col("first_val")) / col("first_val")) * 100)
      .select("first_val", "last_val", "pct_change")
      .distinct()
)
result.show()
```

**2-Minute Script** (~200 words, ~1m 26s at natural pace):

> This reuses the exact same unbounded-frame first-and-last pattern from the earlier customer transaction question, just applied to a different business context, comparing the very first and very last values in an ordered sequence rather than comparing consecutive neighboring rows like the lag-based questions did. I order the window by month, and critically extend the frame explicitly to unbounded preceding and unbounded following, so both first and last correctly consider the entire ordered dataset when evaluating any given row, not just up to that row's own position. This gives every single row in the dataframe the same first_val and last_val values attached to it, representing the true overall first and last months' values across the whole dataset. I compute the percentage change using the standard formula, and since every row now redundantly holds the identical result, I finish with select and distinct to collapse it down to just one clean summary row. I always point out in interviews how this same unbounded-frame first-and-last technique keeps reappearing across genuinely different-sounding business questions, and being able to recognize that recurring underlying pattern, rather than treating every new question as something entirely unfamiliar, is really what separates strong PySpark interview performance from weaker performance.

**Background Music Vibe:** Reflective, connecting-dots beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q48. Filter Rows Where a Column Value Is in a Dynamic List

**Hook / Short Description (for caption):**
Filtering on a dynamic list should always be isin() with a config-driven list — never a hardcoded chain of OR conditions.

**Coding Problem:**
Given a dataframe and a Python list of valid status codes that changes at runtime, write PySpark code to filter the dataframe to only rows matching one of those valid statuses.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import col

valid_statuses = ["active", "pending", "review"]

result = df.filter(col("status").isin(valid_statuses))
result.show()
```

**2-Minute Script** (~183 words, ~1m 18s at natural pace):

> This is a simple but genuinely important pattern to get right, since it comes up constantly in real pipelines where filter criteria are driven by configuration rather than hardcoded directly into the pipeline logic. I use the isin function, passing in the Python list directly, and Spark handles translating that list into an efficient filter condition under the hood, checking whether each row's status column value exists anywhere within that provided list. What I always emphasize in interviews is why this dynamic, config-driven approach is meaningfully better than hardcoding a long chain of OR conditions directly in the code, like status equals active or status equals pending, and so on — with isin and an external list, that same filtering logic can be driven entirely by a configuration file, a database lookup, or even a parameter passed into the pipeline at runtime, meaning the valid statuses list can change over time, or even differ across different environments, like dev versus production, without requiring any code changes at all, which is exactly the kind of flexibility that well-designed, maintainable production pipelines need to have.

**Background Music Vibe:** Practical, config-driven beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #BigData

---

## Q49. Add a Row Number Column Without Any Partitioning

**Hook / Short Description (for caption):**
Global row numbering forces your entire dataset into one partition — here's the performance trap and the parallel alternative.

**Coding Problem:**
Write PySpark code to add a simple global row number column to an entire dataframe, numbering every row sequentially from 1.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, col

w = Window.orderBy(monotonically_increasing_id())

result = df.withColumn("global_row_num", row_number().over(w))
result.show()
```

**2-Minute Script** (~206 words, ~1m 28s at natural pace):

> Adding a truly global, sequential row number sounds simple, but it actually has a genuinely important performance caveat worth explicitly raising in an interview. I use row_number over a window with no partitionBy clause at all, which means the entire dataframe is treated as a single window, and I order by monotonically_increasing_id, a function that generates a unique, though not necessarily perfectly sequential or gap-free, increasing ID for ordering purposes when there's no other genuinely meaningful column to order by. The real caveat here is that a window function with no partition column forces every single row in the entire dataset to be shuffled into one single partition, so that Spark can compute a truly sequential row number across the whole dataframe at once — which completely destroys parallelism and can be a severe performance bottleneck on any genuinely large dataset, since it essentially reduces that entire operation down to running on just one executor. I always mention that if I only need a unique identifier, not a specific, meaningful global ordering, then monotonically_increasing_id used directly as its own column, without any window function at all, is a vastly more scalable, fully parallel alternative, since it doesn't require this single-partition shuffle to generate distinct row identifiers.

**Background Music Vibe:** Cautionary, insightful beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #SparkOptimization

---

## Q50. Compare Two DataFrames for Differences

**Hook / Short Description (for caption):**
Comparing two table snapshots for changes has a null-handling trap — here's the fix, and why hardcoding every comparison doesn't scale.

**Coding Problem:**
Given two dataframes representing yesterday's and today's snapshot of a customer table, write PySpark code to find rows that changed between the two snapshots.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import col

changed = yesterday_df.alias("y").join(
    today_df.alias("t"), "customer_id", "inner"
).filter(
    (col("y.status") != col("t.status")) | (col("y.email") != col("t.email"))
).select("customer_id", col("y.status").alias("old_status"), col("t.status").alias("new_status"))

changed.show()
```

**2-Minute Script** (~219 words, ~1m 34s at natural pace):

> For comparing two snapshots to detect actual changes, I join both dataframes together on the shared primary key, customer_id, and then explicitly compare specific columns I care about between the two aliased sides, here status and email, combining those comparisons with an OR condition so a row is flagged as changed if any one of those tracked columns differs between yesterday and today. I always mention the subtle null-handling trap in this kind of comparison — using a plain not-equals operator like this will actually fail to correctly flag a change where a column went from having an actual value to being null, or from null to having a value, because in SQL, comparing anything against null using standard equality or inequality operators returns null itself, not true, which means that specific type of change would silently be missed by this filter. For genuinely robust snapshot comparison logic in production, I'd use the null-safe equality operator instead, which correctly handles null comparisons as true differences. I also always point out that for a table with many columns to compare, rather than hardcoding each comparison by hand like this, a more scalable approach would be to dynamically generate the full list of column comparisons by iterating over the schema, especially if the set of tracked columns might grow over time.

**Background Music Vibe:** Comparative, thorough beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #DataQuality

---

## Q51. Calculate Time Between Events Per User in Minutes

**Hook / Short Description (for caption):**
Subtracting timestamps directly doesn't work in Spark — unix_timestamp() is the conversion step that makes duration math possible.

**Coding Problem:**
Given a user session log with user_id, event_name, and event_timestamp, write PySpark code to calculate the time in minutes between each event and the previous event for the same user.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import lag, col, unix_timestamp

w = Window.partitionBy("user_id").orderBy("event_timestamp")

result = df.withColumn(
    "minutes_since_last_event",
    (unix_timestamp(col("event_timestamp")) - unix_timestamp(lag("event_timestamp", 1).over(w))) / 60
)
result.show()
```

**2-Minute Script** (~183 words, ~1m 18s at natural pace):

> This combines the lag pattern I've used for numeric differences with a timestamp-specific conversion step, since directly subtracting two timestamp columns doesn't give a clean, usable number the way subtracting two numeric columns does. I partition by user_id and order by event_timestamp, then use lag to pull the previous event's timestamp for that same user, exactly like the earlier stock price difference example. The key extra step here is wrapping both the current and the lagged timestamp in unix_timestamp, which converts each timestamp into a plain numeric value representing seconds since a fixed reference point, and subtracting two of those numeric values gives me a clean number of seconds between the two events, which I then divide by 60 to convert into minutes. I always mention that this unix_timestamp conversion trick generalizes to any kind of duration or elapsed-time calculation between two timestamp columns in Spark, and it's a genuinely essential technique any time raw timestamp arithmetic is needed, since Spark's timestamp type doesn't support plain subtraction the way a numeric type does, without first going through this kind of explicit numeric conversion.

**Background Music Vibe:** Time-tracking, sequential beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---

## Q52. Find the Department With the Highest Average Salary

**Hook / Short Description (for caption):**
Just need the single top result, not a full ranking? orderBy + limit(1) can be genuinely more efficient than a window function.

**Coding Problem:**
Write PySpark code to find which single department has the highest average salary across the entire company.

**Code Solution (show on screen):**
```python
from pyspark.sql.functions import avg, col

result = (
    df.groupBy("dept")
      .agg(avg("salary").alias("avg_salary"))
      .orderBy(col("avg_salary").desc())
      .limit(1)
)
result.show()
```

**2-Minute Script** (~182 words, ~1m 18s at natural pace):

> This is a straightforward aggregation problem, but the important detail worth explaining in an interview is exactly how I isolate just the single top result rather than returning the full ranked list. I start with a normal groupBy on department, aggregating the average salary per department using avg. Then I order that aggregated result by avg_salary descending, so the department with the highest average salary naturally becomes the very first row. Finally, I use limit 1 to keep just that single top row. I always mention that limit here, after an orderBy, is meaningfully different in terms of performance characteristics from something like a full window-function-based ranking approach when I truly only need the single top result and nothing else — limit lets Spark potentially short-circuit and avoid fully materializing and sorting the entire result set across every partition once it's confident it has the top row, whereas a window function ranking approach, useful when you need every row's rank, would generally do more overall work than strictly necessary for a case as simple as just wanting the single overall top department.

**Background Music Vibe:** Direct, to-the-point beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #SparkOptimization

---

## Q53. Create a Cross-Tabulation (Contingency Table)

**Hook / Short Description (for caption):**
There's a dedicated crosstab() method for contingency tables — no need to manually build it from groupBy and pivot.

**Coding Problem:**
Given a survey dataframe with columns age_group and satisfaction_level, write PySpark code to create a cross-tabulation showing the count of respondents for each combination of age group and satisfaction level.

**Code Solution (show on screen):**
```python
crosstab = df.crosstab("age_group", "satisfaction_level")
crosstab.show()
```

**2-Minute Script** (~197 words, ~1m 24s at natural pace):

> This one's a nice example of Spark having a purpose-built, dedicated function for a specific common statistical task, so I don't need to manually build it out of a groupBy and pivot combination the way I might for a more general-purpose pivot table. The crosstab method, called directly on the dataframe, takes the names of two categorical columns and automatically produces a proper cross-tabulation, also called a contingency table, showing the count of every combination of values between those two columns, with one column's distinct values becoming the rows, and the other column's distinct values becoming the columns of the output. I always mention this function specifically because a lot of people don't realize it exists and instead reach for a manual groupBy plus pivot plus count combination to build the exact same result, which absolutely works too, but crosstab is a cleaner, more directly self-documenting way to express that specific, well-known statistical operation when it's genuinely what's being asked for, and knowing that it exists is honestly a good example of the broader interview habit of checking whether Spark already has a dedicated built-in for a common task before manually reconstructing it from more general-purpose pieces.

**Background Music Vibe:** Discovery, hidden-gem beat

**Top 5 Hashtags:** #PySpark #CodingInterview #DataEngineering #SparkSQL #Statistics

---

## Q54. Sample a Fixed Number of Rows Per Group

**Hook / Short Description (for caption):**
Random sampling per group is the same top-N window pattern — just swap the meaningful order-by for rand(). Full circle.

**Coding Problem:**
Given a large dataframe with a category column, write PySpark code to randomly sample exactly 100 rows from each category for a balanced training dataset.

**Code Solution (show on screen):**
```python
from pyspark.sql import Window
from pyspark.sql.functions import rand, row_number, col

w = Window.partitionBy("category").orderBy(rand())

sampled = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") <= 100)
      .drop("rn")
)
sampled.show()
```

**2-Minute Script** (~182 words, ~1m 18s at natural pace):

> This reuses the exact same top-N-per-group window pattern from earlier in this series, but with a clever twist — instead of ordering by a meaningful business column like salary or date, I order by the rand function, which generates a random value for each row, effectively randomizing the row order within each category's partition before ranking. Once that randomized ordering is in place, row_number assigns sequential numbers in that random order, and filtering for row number less than or equal to 100 gives me a genuinely random sample of exactly 100 rows from each category, rather than a biased sample based on any real, meaningful ordering. I always highlight in interviews that this is functionally the exact same top-N-per-group template I've used throughout this whole series for very different-looking problems, top salaries, top purchases, top streaks, just substituting a random ordering column in place of a meaningful one, which is a really good, concrete example of how a small number of core, well-understood window function patterns can be recombined and adapted to solve a surprisingly wide range of seemingly unrelated real-world problems.

**Background Music Vibe:** Full-circle, series-closing beat

**Top 5 Hashtags:** #PySpark #CodingInterview #WindowFunctions #DataEngineering #SparkSQL

---
