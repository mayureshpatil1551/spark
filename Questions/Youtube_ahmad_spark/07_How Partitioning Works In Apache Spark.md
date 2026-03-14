07_How Partitioning Works In Apache Spark?
---------------------------------------

00:00 Introduction
02:22 Code for understanding partitioning
05:44 Problems that partitioning solves
09:48 Factors to consider when choosing a partition column
13:36 Code to show single/multi level partitioning
18:19 Understanding spark.sql.files.maxPartitionBytes

---------------------------------------


* Introduction
-->
PARTITIONING is divied ur large dataset into small more manageable chunks.


* Code for understanding partitioning
-->
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .config("spark.driver.memory", "10g")
    .master("local[*]")
    .appName("6_0_partitioning")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")

listening_activity_file = "../data/partitioning/raw/Spotify_Listening_Activity.csv"
df_listening_actv = spark.read.csv(listening_activity_file, header=True, inferSchema=True)
df_listening_actv.show(5, False)


df_listening_actv = (
    df_listening_actv
    .withColumnRenamed("listen_date", "listen_time")
    .withColumn("listen_date", F.to_date("listen_time", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    .withColumn("listen_hour", F.hour("listen_time"))
)

df_listening_actv.show(5, False)
df_listening_actv.printSchema()
df_listening_actv.count()

+-----------+-------+--------------------------+---------------+-----------+-----------+
|activity_id|song_id|listen_time               |listen_duration|listen_date|listen_hour|
+-----------+-------+--------------------------+---------------+-----------+-----------+
|1          |12     |2023-06-27 10:15:47.008867|69             |2023-06-27 |10         |
|2          |44     |2023-06-27 10:15:47.008867|300            |2023-06-27 |10         |
|3          |75     |2023-06-27 10:15:47.008867|73             |2023-06-27 |10         |
|4          |48     |2023-06-27 10:15:47.008867|105            |2023-06-27 |10         |
|5          |10     |2023-06-27 10:15:47.008867|229            |2023-06-27 |10         |
+-----------+-------+--------------------------+---------------+-----------+-----------+



1. Partitioning By listen_date
    Lets say we want to analyse the listening behaviours of user over time. If we're given the complete dataset (with no partitions), Spark would scan the whole dataset for finding a particular date (similar to the bookshelf analogy where you would scan the entire bookself for finding a book if it is not organized). Given that our usecase needs analysis by date, partitioning (creating folders) on date would help Spark pin point to the exact folder. This makes searching very easy and Spark doesn't scan the entire dataset

# Partitioning listening activity by the listen date
(
    df_listening_actv
    .write
    .partitionBy("listen_date")
    .mode("overwrite")
    .parquet("../data/partitioning/partitioned/listening_activity_pt")
)

Partition Pruning ->
df_listening_actv_pt_pruned = spark.read.parquet("../data/partitioning/partitioned/listening_activity_pt")
df_listening_actv_pt_pruned.filter("listen_date = '2019-01-01'").explain()




* Problems that partitioning solves
-->
    a. Fast/easy access
    b. parallelism / Resource Utilization


What Problems Does Partitioning Solve?
    - Fast Search (Query Performance): Spark will only process the relevant partition instead of the entire dataset (example above). This greatly reduces I/O and query execution time.
    - Parallelism / Resource Utilization: Each core processes 1 partition; More number of partitions, more is the parallelism; again this does not mean we forcefully increase the number of partitions. Each partition should be 128MB in size.


Partitioning Examples :-
    1. Single/multi level partitioning
    2. Using repartition/coalesce with partitionBy (controlling number of files inside each partition):
        - parititionBy affects how data is laid out in the storage and is going to ensure that the output directory is organized into subdirectories based on the value given in partitionBy.
        - Number of files in each value directory of partitionBy depends on the number supplied in the repartition/coalesce.


* Factors to consider when choosing a partition column  
-->
how i fine col for partition.
1. ur col will a high cardinality col(no. of unique values). eg. customer_id
    It dont help spark in reducing search base configuring out the right col to select.
            or
 ur col will a low cardinality col(no. of unique values). 
    eg. dept_no, state
    we dont want super low cardinality col.

2. Filter condition - spark can specifucally select the cond. and speed will fast




* Code to show single/multi level partitioning
-->


1. Single/multi level partitioning --> partition based on given column name/names
(
    df_listening_actv
    .write
    .mode("overwrite")
    .partitionBy("listen_date", "listen_hour")
    .parquet("../data/partitioning/partitioned/listening_activity_pt_2")
)
file path is like below --:
../data/partitioning/partitioned/listening_activity_pt_3/listen_hour=23-09-2024/
listen_date=10
../data/partitioning/partitioned/listening_activity_pt_3/listen_hour=23-09-2024/


(
    df_listening_actv
    .write
    .mode("overwrite")
    .partitionBy("listen_hour", "listen_date")
    .parquet("../data/partitioning/partitioned/listening_activity_pt_3")
)
file path is like below [it will create a partition in given order]
../data/partitioning/partitioned/listening_activity_pt_3/listen_hour=23-09-2024/listen_date=10
../data/partitioning/partitioned/listening_activity_pt_3/listen_hour=23-09-2024/listen_date=09
../data/partitioning/partitioned/listening_activity_pt_3/listen_hour=22-09-2024/listen_date=08



* if you want to control the partition.
then we do repartition before doing the partition

2. Using repartition/coalesce with partitionBy
(
    df_listening_actv
    .repartition(3)
    .write
    .mode("overwrite")
    .partitionBy("listen_date")
    .parquet("../data/partitioning/partitioned/listening_activity_pt_4")
)
output -- 
../data/partitioning/partitioned/listening_activity_pt_4/listen_date=23-09-2025/part-0-56768.parquet
                                                                               /part-1-56768.parquet
                                                                               /part-2-56768.parquet
../data/partitioning/partitioned/listening_activity_pt_4/listen_date=24-09-2025/part-0-56768.parquet
                                                                               /part-1-56768.parquet
                                                                               /part-2-56768.parquet



# The coalesce method reduces the number of partitions in a DataFrame. 
# It avoids full shuffle, instead of creating new partitions, it shuffles the data using default Hash Partitioner, 
# and adjusts into existing partitions, this means it can only decrease the number of partitions.

(
    df_listening_actv
    .coalesce(3)
    .write
    .mode("overwrite")
    .partitionBy("listen_date")
    .parquet("../data/partitioning/partitioned/listening_activity_pt_5")
)


* Understanding eith spark.sql.files.maxPartitionBytes
-->
spark.sql.files.maxPartitionBytes 
    > It deside what is going tobe max size of partition that spark is going to run & this value it going to split the file.


Experimenting With spark.sql.files.maxPartitionBytes
spark.stop()
spark = SparkSession.builder.appName("Test spark.sql.files.maxPartitionBytes").getOrCreate()

df_default = spark.read.csv("../data/partitioning/raw/listening_activity.csv", header=True, inferSchema=True)
# to find how many partition is going to create default.
default_partitions = df_default.rdd.getNumPartitions()
print(f"Number of partitions with default maxPartitionBytes: {default_partitions}")
#op/==>Number of partitions with default maxPartitionBytes: 1


spark.conf.set("spark.sql.files.maxPartitionBytes", "1000")


file is 448kb

# 448k = 1kb ==>448 partitions
i wnat to divied into 1kb partition. read each file sizes of 1kb need 448 partition


df_modified = spark.read.csv("../data/partitioning/raw/listening_activity.csv", header=True, inferSchema=True)
modified_partitions = df_modified.rdd.getNumPartitions()
print(f"Number of partitions with modified maxPartitionBytes: {modified_partitions}")
spark.stop()

# o/p ==> Number of partitions with modified maxPartitionBytes: 437

maxPartitionBytes help u deside the no of partitions and size of partition on read time.

 