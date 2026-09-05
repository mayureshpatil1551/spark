# Kafka — Practical End-to-End Guide (Install → Real Project)

Focus: hands-on, on your own laptop, building toward the kind of pipeline you'd actually ship — not interview theory. Every command is runnable. Do each step yourself before moving to the next; don't just read it.

---

## PART 1 — Install Kafka locally (Docker, KRaft mode, no Zookeeper)

### Prerequisites
- Docker Desktop installed and running (check: `docker --version`)
- Python 3.9+ (check: `python3 --version`)
- ~4 GB free RAM for containers

### 1.1 docker-compose.yml

Create a folder `kafka-lab/` and inside it a file `docker-compose.yml`:

```yaml
version: "3.8"
services:
  kafka:
    image: apache/kafka:3.7.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

This is a **single-broker KRaft cluster** — no Zookeeper needed (Zookeeper is legacy/deprecated). Fine for learning; production clusters run 3+ brokers.

### 1.2 Start / stop

```bash
cd kafka-lab
docker compose up -d          # start
docker compose ps             # confirm it's running
docker compose logs -f kafka  # watch logs (ctrl+C to exit, container keeps running)
docker compose down           # stop and remove containers
docker compose down -v        # stop AND wipe all topic data (clean slate)
```

### 1.3 Get a shell inside the container for CLI tools

Kafka ships CLI scripts inside the image itself:

```bash
docker exec -it kafka bash
cd /opt/kafka/bin
```

Run everything below from inside this shell (or prefix each with `docker exec -it kafka /opt/kafka/bin/...`).

---

## PART 2 — CLI operations (do these yourself, in order)

```bash
# Create a topic with 3 partitions, replication factor 1 (single broker)
kafka-topics.sh --create --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe a topic (partitions, leader, replicas, ISR)
kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092

# Produce messages interactively (type lines, Ctrl+C to stop)
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092

# Consume messages from the beginning
kafka-console-consumer.sh --topic orders --bootstrap-server localhost:9092 --from-beginning

# List consumer groups
kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# Describe a group (this shows CONSUMER LAG — the most important production metric)
kafka-consumer-groups.sh --describe --group my-group --bootstrap-server localhost:9092
```

**Exercise:** open two terminals. In terminal A run the console producer and type 5 order lines. In terminal B run the console consumer with `--from-beginning`. Confirm you see all 5. Then stop the consumer, produce 3 more, restart the consumer *without* `--from-beginning` — you should only see the new 3. This is offsets in action, not theory.

---

## PART 3 — Python Producer & Consumer

Install the client library:

```bash
pip install confluent-kafka
```

### 3.1 Producer — `producer.py`

```python
import json
import time
import random
from confluent_kafka import Producer

conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")

def make_order(order_id):
    return {
        "order_id": order_id,
        "customer_id": random.randint(500, 510),
        "amount": round(random.uniform(100, 5000), 2),
        "event_type": "ORDER_CREATED",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
    }

for i in range(1, 21):
    order = make_order(i)
    # key = customer_id -> guarantees all events for the same customer land in the same partition, in order
    producer.produce(
        topic="orders",
        key=str(order["customer_id"]),
        value=json.dumps(order),
        callback=delivery_report
    )
    producer.poll(0)   # trigger delivery callbacks
    time.sleep(0.5)

producer.flush()  # block until all messages are delivered
```

### 3.2 Consumer — `consumer.py`

```python
import json
from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-processor",
    "auto.offset.reset": "earliest"   # start from beginning if no committed offset
}
consumer = Consumer(conf)
consumer.subscribe(["orders"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        order = json.loads(msg.value())
        print(f"P{msg.partition()} offset={msg.offset()} -> {order}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
```

**Run it:** terminal A → `python consumer.py`, terminal B → `python producer.py`. Watch messages for the same `customer_id` (key) always land on the same partition — that's key-based partitioning, not random luck.

---

## PART 4 — Spark Structured Streaming (this is where your PySpark skills plug in directly)

### 4.1 Batch vs Streaming — the only mental shift you need

```
BATCH (what you do today)              STREAMING (Kafka source)
spark.read.parquet(path)               spark.readStream.format("kafka")...
   .transform(...)                        .transform(...)   <- same DataFrame API
   .write.format("delta")                 .writeStream.format("delta")
   .save(path)                              .option("checkpointLocation", ...)
                                             .start()
```

Same transformations (`filter`, `select`, `withColumn`, joins, aggregations). The differences are: (1) the source is unbounded, (2) you need a **checkpoint** location, and (3) output is continuous, via a trigger.

### 4.2 PySpark dependencies

```bash
pip install pyspark==3.5.1 delta-spark==3.2.0
```

### 4.3 Full working script — `kafka_to_delta.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.appName("KafkaToDelta")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Read raw stream from Kafka — this is your "Bronze" ingestion, schema-less
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)
# raw has columns: key, value, topic, partition, offset, timestamp (all binary/generic)

# 2) Define the event schema and parse JSON out of the `value` column
order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
])

parsed = (
    raw.select(
        col("key").cast("string").alias("kafka_key"),
        col("partition"),
        col("offset"),
        from_json(col("value").cast("string"), order_schema).alias("data")
    )
    .select("kafka_key", "partition", "offset", "data.*")
    .withColumn("event_ts", col("timestamp").cast("timestamp"))
)

# 3) Write to Delta (Bronze/Silver table) — this is your Silver layer, structured & typed
query = (
    parsed.writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/kafka-lab/checkpoints/orders_silver")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start("/tmp/kafka-lab/delta/orders_silver")
)

query.awaitTermination()
```

**Run it:** `python kafka_to_delta.py` while `producer.py` is running in another terminal. Then in a third terminal/notebook:

```python
df = spark.read.format("delta").load("/tmp/kafka-lab/delta/orders_silver")
df.show()
```

You now have a real Kafka → Spark Structured Streaming → Delta Lake pipeline running end to end on your laptop.

### 4.4 The two settings that matter most in real projects

- **`checkpointLocation`** — Spark's own record of progress (Kafka offsets consumed + any aggregation state). If the job crashes and restarts pointed at the same checkpoint, it resumes exactly where it left off, with no duplicates and no gaps (this is what gives you exactly-once *within Spark's write*, when combined with Delta).
- **`trigger(processingTime=...)`** — controls micro-batch frequency. Without it, Spark runs as fast as possible; in production you usually set an interval matched to your SLA and cluster cost tolerance.

### 4.5 Adding a windowed aggregation (common real-world need)

If you also want a rolling metric — e.g. total order amount per customer per 1-minute window — add before the write:

```python
agg = (
    parsed
    .withWatermark("event_ts", "2 minutes")  # tolerate up to 2 min late-arriving events
    .groupBy(window(col("event_ts"), "1 minute"), col("customer_id"))
    .sum("amount")
)
```

`withWatermark` is what lets Spark know when it's safe to finalize and drop state for a window — without it, streaming aggregations would hold state forever and grow unbounded.

---

## PART 5 — How this maps to a real production project

```
Application → Kafka (raw events) → Spark Structured Streaming → Bronze (raw+metadata)
                                                                → Silver (parsed, typed, deduped)
                                                                → Gold (aggregated, business-ready)
                                                                      → BI / ML / APIs
```

In a real setup, the pieces you'd add on top of what you just built:
- **Schema Registry** (Avro/Protobuf instead of raw JSON) so producers/consumers agree on schema and can evolve it safely
- **Multiple partitions across multiple brokers**, replication factor 3, for durability
- **Dead Letter Queue topic** (`orders-dlq`) for records that fail parsing/validation, instead of dropping them
- **Deduplication** in the Silver write, typically via `dropDuplicates` on a business key within the watermark window, or a Delta `MERGE` keyed on `order_id`
- **Monitoring** on consumer lag (the `kafka-consumer-groups.sh --describe` number you saw in Part 2) — this is the #1 signal that your Spark job can't keep up with incoming volume

---

## What to do next
1. Run Part 1–3 yourself right now — get the local cluster up and see messages flow through the CLI and Python.
2. Run Part 4's script and confirm data lands in Delta.
3. Come back and tell me what broke or what you don't understand — I'll debug with you rather than hand you more theory.
4. When you're comfortable, tell me and we'll extend this into the CDC (Debezium → Kafka → Delta) pattern, which is the most common real-world use case for a Data Engineer.
