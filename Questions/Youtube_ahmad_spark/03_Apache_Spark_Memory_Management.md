Apache_Spark_Memory_management
=========================================
topic -
1. Executor memory management
2. Unified memory
3. Off Heap Memory 

-----------------------------------------------------------------------------
Executor memory layout -

most of spark op. run on on-heap memory . and its managed by JVM
JVM - used for running java program. spark writen in scala. 
pyspark --> scala


Spark Executor Container :- [spark.executor.memory = 10GB]
A. on-heap memory hv 3 section [ most of spark op. take place here. managed by JVM ]
    1. Unified Memory - [spark.memory.fraction = 0.6 [6GB]]
        1. Execution memory - join,shuffle,sorting, GrpBy
        2. Stg memeory - RDD, DF caching, Broadcast variables  --> spark.memory.storageFraction = 0.5 [3GB]
        * If Execution memory is full but he want more memory then it will use stg memory if the block are avaiable and vicevesa.
        * If DF are not going to reuse then dont cach.


    2. User Memory - User defined data structure, variables, Objeccts --> [4000MB-300MB= 3.7GB]
    3. Reserved Memory - 300MB [spark need to run itself.]            --> 300MB
B. OverHead Memory-used for some internal sys level op.               --> 1GB  
C. off-head memory -                                                     
    1. Execution [ same like onheap]
    2. stg 
    * if onheap memory is full. there going to be garbeg collection(GC) cycle.primary use case is,it is going to pause the program. clean the all unwanted obj to make program resume.this puase GC dones time to time. it affect on performance of the program. So, In this case we can use off-heap memory.
    * off- heap memory is managed by OS.no GC. there spark dev responsiable for allocation and deallocation of off heap memory.
    * off-heap memory is slower than on-heap memory.

--executor memory 10GB

spark.executor.memory = 10GB
spark.memory.fraction = 0.6 [6GB]
spark.memory.storageFraction = 0.5 [3GB]

spark.executor.memoryOverhead = 384MB [10% of executormemory. SO, its 1GB]
spark.memory.offheap.enabled = true
spark.memory.offheap.size = 0 [good to give 10/20% of executor memory]

when user ask to YARN for 10Gb executor memory.
then Yarn gives 11GB(spark.executor.memory = 10GB + overhrad Memory = 1GB)




