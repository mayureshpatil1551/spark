09_Cache, Persist & StorageLevels In Apache Spark
----------------------------------------------------------------------------------------------------------


00:00 Introduction
00:39 Why Should You Use Caching? 
06:45 Lazy Evaluation & How Could Caching Help You? 
10:12 Code + Spark UI Explanation Caching vs No Caching
14:21 Persist & Storage Levels In Persist

----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------


* Introduction
-->
Caching is a mechanism in Spark to store DataFrames / Datasets / RDDs in:
    Memory
    Disk
    Or both

This allows you to reuse a computed DataFrame in multiple downstream operations without recomputing it.
Caching is helpful when:
    You reuse the same dataset multiple times
    You have expensive transformations
    You want to reduce execution time
----------------------------------------------------------------------------------------------------------

* Why Should You Use Caching? '
-->
Caching is useful when:
✔ You reuse the same DataFrame multiple times
✔ Your pipeline has expensive transformations
✔ You run multiple actions on the same DataFrame
✔ You want to reduce job execution time
Without caching:
    Spark recomputes the entire transformation chain every time an action is triggered
    This means more CPU, more shuffle, more cost

With caching:
    Spark computes once
    Stores results
    Reuses the cached version for all future operations
----------------------------------------------------------------------------------------------------------

* Lazy Evaluation & How Could Caching Help You? 
-->
it wait util the action is called.
then it will compute everything
-->
Spark transformations are lazy, meaning:
    They do not run immediately
    Spark creates a logical plan (lineage)
    Actual computation happens only when an action (show, count, collect) is triggered

* How caching helps lazy evaluation:
Without caching:
    Each action → Spark recomputes entire plan from source
    Re-runs filter, withColumn, join, groupBy, etc.

With caching:
    First action computes & stores data
    Future actions directly read from cache
    No recomputation → much faster

----------------------------------------------------------------------------------------------------------

* Code + Spark UI Explanation Caching vs No Caching

df_base = (
    df_customers
    .filter(F.col("city") == "boston")
    .withColumn(
        "customer_group", 
        F.when(
            F.col("age").between(20, 30), 
            F.lit("young") 
        )
        .when(
            F.col("age").between(31, 50), 
            F.lit("mid") 
        )
        .when(
            F.col("age") > 51, 
            F.lit("old") 
        )
        .otherwise(F.lit("kid"))
     )
    .select("cust_id", "name", "age", "gender", "birthday", "zip", "city", "customer_group")
)

df_base.cache() 
df_base.show(5, False)


when u do cache().
When ever u call that cache df. I will scan and take from In_memory_table_scan


----------------------------------------------------------------------------------------------------------

* Persist & Storage Levels In Persist
-->
type of cache. u can cache ur df.

deserialized format - ur data not stored in bytes, it store as JVM object. So, easy to read them

## `StorageLevel` Types:

(As of Spark `3.4`)

- `DISK_ONLY`: CPU efficient, memory efficient, slow to access, data is serialized when stored on disk
- `DISK_ONLY_2`: disk only, replicated 2x
- `DISK_ONLY_3`: disk only, replicated 3x

- `MEMORY_AND_DISK`: spills to disk if there's no space in memory
- `MEMORY_AND_DISK_2`: memory and disk, replicated 2x
- `MEMORY_AND_DISK_DESER`(default): same as `MEMORY_AND_DISK`, deserialized in both for fast access

- `MEMORY_ONLY`: CPU efficient, memory intensive
- `MEMORY_ONLY_2`: memory only, replicated 2x - for resilience, if one executor fails

**Note**: 
- `SER` is CPU intensive, memory saving as data is compact while `DESER` is CPU efficient, memory intensive
- Size of data on disk is lesser as data is in serialized format, while deserialized in memory as JVM objects for faster access

### When to use what?
```
Storage Level    Space used  CPU time  In memory  On-disk  Serialized
---------------------------------------------------------------------
MEMORY_ONLY          High        Low       Y          N        N         
MEMORY_ONLY_SER      Low         High      Y          N        Y     
MEMORY_AND_DISK      High        Medium    Some       Some     Some  
MEMORY_AND_DISK_SER  Low         High      Some       Some     Y     
DISK_ONLY            Low         High      N          Y        Y     
```

df_base.unpersist()
df_base.persist(StorageLevel.MEMORY_ONLY)

df2 = (
    df_base
    .withColumn("test_column_1", F.lit("test_column_1"))
    .withColumn("birth_year", F.split("birthday", "/").getItem(2))
)

df1.show(5, False)

spark.stop()
----------------------------------------------------------------------------------------------------------

cache() --
- u cant provide store level

persist() --
- u can provide StorageLevel