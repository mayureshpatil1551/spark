# PySpark Date & Time Functions — Complete Reference

> All examples use: `from pyspark.sql.functions import *`

---

## Setup (Common for All Examples)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DateFunctions").getOrCreate()

df = spark.createDataFrame([
    ("2024-06-15 10:30:45",),
    ("2023-12-01 08:00:00",),
], ["datetime_str"])

df = df.withColumn("dt", to_timestamp("datetime_str")) \
       .withColumn("d",  to_date("datetime_str"))
```

---

## 1. Current Date & Time

### `current_date()`
Returns today's date (DateType).

```python
df.withColumn("today", current_date()).show()
# +-------------------+------------+
# | today             |
# +-------------------+------------+
# | 2024-06-15        |
```

---

### `current_timestamp()`
Returns current date + time (TimestampType).

```python
df.withColumn("now", current_timestamp()).show()
# | now                     |
# | 2024-06-15 10:30:45.123 |
```

---

## 2. Extracting Date Parts

### `year(col)`
Extracts the year from a date/timestamp.

```python
df.withColumn("yr", year("d")).show()
# | yr   |
# | 2024 |
# | 2023 |
```

---

### `month(col)`
Extracts the month (1–12).

```python
df.withColumn("mon", month("d")).show()
# | mon |
# | 6   |
# | 12  |
```

---

### `dayofmonth(col)`
Extracts the day of the month (1–31).

```python
df.withColumn("dom", dayofmonth("d")).show()
# | dom |
# | 15  |
# | 1   |
```

---

### `dayofyear(col)`
Extracts the day of the year (1–366).

```python
df.withColumn("doy", dayofyear("d")).show()
# | doy |
# | 167 |
# | 335 |
```

---

### `weekofyear(col)`
Extracts the ISO week number (1–53).

```python
df.withColumn("woy", weekofyear("d")).show()
# | woy |
# | 24  |
# | 48  |
```

---

### `quarter(col)`
Extracts the quarter (1–4).

```python
df.withColumn("qtr", quarter("d")).show()
# | qtr |
# | 2   |
# | 4   |
```

---

### `hour(col)`
Extracts the hour (0–23) from a timestamp.

```python
df.withColumn("hr", hour("dt")).show()
# | hr |
# | 10 |
# | 8  |
```

---

### `minute(col)`
Extracts the minute (0–59).

```python
df.withColumn("min", minute("dt")).show()
# | min |
# | 30  |
# | 0   |
```

---

### `second(col)`
Extracts the second (0–59).

```python
df.withColumn("sec", second("dt")).show()
# | sec |
# | 45  |
# | 0   |
```

---

## 3. Date Arithmetic

### `date_add(col, days)`
Adds N days to a date.

```python
df.withColumn("plus5", date_add("d", 5)).show()
# | d          | plus5      |
# | 2024-06-15 | 2024-06-20 |
# | 2023-12-01 | 2023-12-06 |
```

---

### `date_sub(col, days)`
Subtracts N days from a date.

```python
df.withColumn("minus3", date_sub("d", 3)).show()
# | d          | minus3     |
# | 2024-06-15 | 2024-06-12 |
# | 2023-12-01 | 2023-11-28 |
```

---

### `datediff(endDate, startDate)`
Returns number of days between two dates. (end - start)

```python
df.withColumn("diff", datediff(current_date(), "d")).show()
# | diff |
# | 0    |   ← depends on when you run it
# | 197  |
```

---

### `add_months(col, months)`
Adds N months to a date.

```python
df.withColumn("plus2m", add_months("d", 2)).show()
# | d          | plus2m     |
# | 2024-06-15 | 2024-08-15 |
# | 2023-12-01 | 2024-02-01 |
```

---

### `months_between(date1, date2)`
Returns number of months between two dates (fractional).

```python
df.withColumn("months", months_between(current_date(), "d")).show()
# | months |
# | 0.0    |
# | 6.45   |
```

---

## 4. Type Conversion

### `to_date(col, format=None)`
Converts string → DateType.

```python
df2 = spark.createDataFrame([("15/06/2024",)], ["dt_str"])
df2.withColumn("date", to_date("dt_str", "dd/MM/yyyy")).show()
# | date       |
# | 2024-06-15 |
```

---

### `to_timestamp(col, format=None)`
Converts string → TimestampType.

```python
df2 = spark.createDataFrame([("15-06-2024 10:30:00",)], ["ts_str"])
df2.withColumn("ts", to_timestamp("ts_str", "dd-MM-yyyy HH:mm:ss")).show()
# | ts                  |
# | 2024-06-15 10:30:00 |
```

---

### `date_format(col, format)`
Converts a date/timestamp → formatted string.

```python
df.withColumn("formatted", date_format("d", "dd-MMM-yyyy")).show()
# | formatted   |
# | 15-Jun-2024 |
# | 01-Dec-2023 |
```

> Common format tokens: `yyyy` year, `MM` month, `dd` day, `HH` hour (24h), `mm` minute, `ss` second

---

## 5. Unix Timestamp

### `unix_timestamp(col, format=None)`
Converts date/string → Unix epoch (seconds since 1970-01-01).

```python
df.withColumn("epoch", unix_timestamp("d")).show()
# | epoch      |
# | 1718409600 |
# | 1701388800 |
```

---

### `from_unixtime(col, format)`
Converts Unix epoch → formatted string.

```python
df2 = spark.createDataFrame([(1718409600,)], ["epoch"])
df2.withColumn("dt_str", from_unixtime("epoch", "yyyy-MM-dd HH:mm:ss")).show()
# | dt_str              |
# | 2024-06-15 00:00:00 |
```

---

### `unix_timestamp()` — No args
Returns current time as Unix epoch (seconds).

```python
df.withColumn("now_epoch", unix_timestamp()).show()
# | now_epoch  |
# | 1718447445 |
```

---

### `from_unixtime(col)` — Default format
Returns epoch as string in `yyyy-MM-dd HH:mm:ss` format.

```python
df2 = spark.createDataFrame([(1718409600,)], ["epoch"])
df2.withColumn("dt_str", from_unixtime("epoch")).show()
# | dt_str              |
# | 2024-06-15 00:00:00 |
```

---

## 6. Truncation

### `trunc(col, format)`
Truncates date to the start of YEAR or MONTH.

```python
df.withColumn("start_of_month", trunc("d", "MONTH")).show()
# | d          | start_of_month |
# | 2024-06-15 | 2024-06-01     |

df.withColumn("start_of_year", trunc("d", "YEAR")).show()
# | d          | start_of_year |
# | 2024-06-15 | 2024-01-01    |
```

---

### `date_trunc(format, col)`
Truncates timestamp to YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, WEEK, QUARTER.

```python
df.withColumn("trunc_hour", date_trunc("hour", "dt")).show()
# | dt                  | trunc_hour          |
# | 2024-06-15 10:30:45 | 2024-06-15 10:00:00 |

df.withColumn("trunc_day", date_trunc("day", "dt")).show()
# | dt                  | trunc_day           |
# | 2024-06-15 10:30:45 | 2024-06-15 00:00:00 |
```

> **Difference:** `trunc` → DateType, `date_trunc` → TimestampType and supports more levels.

---

## 7. Start / End of Period

### `last_day(col)`
Returns the last day of the month for a given date.

```python
df.withColumn("last", last_day("d")).show()
# | d          | last       |
# | 2024-06-15 | 2024-06-30 |
# | 2023-12-01 | 2023-12-31 |
```

---

### `next_day(col, "Mon")`
Returns the next occurrence of the given weekday after the date.

```python
df.withColumn("next_mon", next_day("d", "Mon")).show()
# | d          | next_mon   |
# | 2024-06-15 | 2024-06-17 |

# Day abbreviations: Mon, Tue, Wed, Thu, Fri, Sat, Sun
```

---

## 8. Greatest & Least

### `greatest(col1, col2, ...)`
Returns the maximum value across multiple date columns row-wise.

```python
df2 = spark.createDataFrame([
    ("2024-01-01", "2024-06-15"),
    ("2023-05-10", "2022-12-01"),
], ["date1", "date2"])

df2 = df2.withColumn("d1", to_date("date1")).withColumn("d2", to_date("date2"))
df2.withColumn("max_date", greatest("d1", "d2")).show()
# | d1         | d2         | max_date   |
# | 2024-01-01 | 2024-06-15 | 2024-06-15 |
# | 2023-05-10 | 2022-12-01 | 2023-05-10 |
```

---

### `least(col1, col2, ...)`
Returns the minimum value across multiple date columns row-wise.

```python
df2.withColumn("min_date", least("d1", "d2")).show()
# | d1         | d2         | min_date   |
# | 2024-01-01 | 2024-06-15 | 2024-01-01 |
# | 2023-05-10 | 2022-12-01 | 2022-12-01 |
```

---

## 9. Timezone Conversion

### `from_utc_timestamp(col, tz)`
Converts a UTC timestamp to a given timezone.

```python
df.withColumn("ist_time", from_utc_timestamp("dt", "Asia/Kolkata")).show()
# | dt                  | ist_time            |
# | 2024-06-15 10:30:45 | 2024-06-15 16:00:45 |  ← UTC+5:30
```

---

### `to_utc_timestamp(col, tz)`
Converts a local timestamp to UTC.

```python
df.withColumn("utc_time", to_utc_timestamp("dt", "Asia/Kolkata")).show()
# | dt                  | utc_time            |
# | 2024-06-15 10:30:45 | 2024-06-15 05:00:45 |  ← minus 5:30
```

> Common timezone strings: `Asia/Kolkata`, `America/New_York`, `Europe/London`, `UTC`

---

## 10. Interval Expressions

### `expr("INTERVAL N DAY")`
Adds/subtracts intervals using SQL-style expressions.

```python
df.withColumn("plus1day",  col("d") + expr("INTERVAL 1 DAY")).show()
df.withColumn("plus1month",col("d") + expr("INTERVAL 1 MONTH")).show()
df.withColumn("plus1year", col("d") + expr("INTERVAL 1 YEAR")).show()
df.withColumn("minus7days",col("d") - expr("INTERVAL 7 DAYS")).show()

# | d          | plus1day   |
# | 2024-06-15 | 2024-06-16 |
```

---

## 11. Make Date / Timestamp

### `make_date(year, month, day)`
Constructs a DateType from year, month, day columns.

```python
df2 = spark.createDataFrame([(2024, 6, 15)], ["yr", "mon", "day"])
df2.withColumn("date", make_date("yr", "mon", "day")).show()
# | date       |
# | 2024-06-15 |
```

---

### `make_timestamp(year, month, day, hour, min, sec)`
Constructs a TimestampType from individual components.

```python
df2 = spark.createDataFrame([(2024, 6, 15, 10, 30, 45)], ["yr","mon","day","hr","mi","sec"])
df2.withColumn("ts", make_timestamp("yr","mon","day","hr","mi","sec")).show()
# | ts                  |
# | 2024-06-15 10:30:45 |
```

---

## 12. Window Functions (Date-Based)

### `lag(col)`
Returns the value of the previous row (useful for date comparison).

```python
from pyspark.sql.window import Window

w = Window.orderBy("d")
df.withColumn("prev_date", lag("d", 1).over(w)).show()
# | d          | prev_date  |
# | 2023-12-01 | null       |
# | 2024-06-15 | 2023-12-01 |
```

---

### `lead(col)`
Returns the value of the next row.

```python
df.withColumn("next_date", lead("d", 1).over(w)).show()
# | d          | next_date  |
# | 2023-12-01 | 2024-06-15 |
# | 2024-06-15 | null       |
```

---

## 13. Date Filtering

### `col("date") > current_date()`
Filter rows where date is in the future.

```python
df.filter(col("d") > current_date()).show()
# Returns rows where d is after today
```

---

### `col("date").between(date1, date2)`
Filter rows where date falls within a range (inclusive).

```python
from pyspark.sql.functions import lit

df.filter(col("d").between(lit("2024-01-01"), lit("2024-12-31"))).show()
# Returns only 2024 rows
# | d          |
# | 2024-06-15 |
```

---

## Quick Reference Cheatsheet

| Function | Returns | Use Case |
|---|---|---|
| `current_date()` | Date | Today's date |
| `current_timestamp()` | Timestamp | Now |
| `year / month / dayofmonth` | Int | Extract date parts |
| `hour / minute / second` | Int | Extract time parts |
| `dayofyear / weekofyear / quarter` | Int | Calendar analytics |
| `date_add / date_sub` | Date | Simple day arithmetic |
| `add_months` | Date | Month arithmetic |
| `datediff` | Int | Days between two dates |
| `months_between` | Double | Fractional months between |
| `to_date / to_timestamp` | Date/Timestamp | Parse strings |
| `date_format` | String | Format for display |
| `unix_timestamp` | Long | Convert to epoch |
| `from_unixtime` | String | Epoch to readable |
| `trunc` | Date | Start of YEAR/MONTH |
| `date_trunc` | Timestamp | Start of any period |
| `last_day` | Date | End of month |
| `next_day` | Date | Next weekday |
| `greatest / least` | Date | Row-wise min/max |
| `from_utc_timestamp` | Timestamp | UTC → Local |
| `to_utc_timestamp` | Timestamp | Local → UTC |
| `expr("INTERVAL")` | Date/Timestamp | SQL-style arithmetic |
| `make_date` | Date | Build from parts |
| `make_timestamp` | Timestamp | Build from parts |
| `lag / lead` | Any | Previous/Next row value |
| `.between()` | Boolean | Range filter |
