# Part 6 — CDC: Database → Debezium → Kafka → Spark → Delta

This is the single most common real-world use of Kafka for a Data Engineer: instead of a producer app manually publishing events, changes made directly to a database table are captured automatically and streamed into Kafka.

```
Postgres/MySQL (source DB)
      │  (row INSERT/UPDATE/DELETE)
      ▼
Debezium (Kafka Connect source connector — reads the DB's write-ahead/binlog)
      ▼
Kafka topic (one topic per table, auto-created, e.g. "orders_db.public.orders")
      ▼
Spark Structured Streaming (same readStream code pattern as before)
      ▼
Delta Lake (Silver table kept in sync with the source table, near-real-time)
```

**Why this matters:** no application code changes are needed on the source system. Debezium reads the database's internal change log (the same mechanism the DB uses for replication/crash recovery), so every insert/update/delete is captured with zero impact on the source app.

---

## 6.1 Extend docker-compose.yml

Add Postgres, Kafka Connect, and Debezium to the same `kafka-lab/docker-compose.yml`:

```yaml
  postgres:
    image: debezium/postgres:16
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: orders_db

  connect:
    image: debezium/connect:2.6
    container_name: connect
    depends_on:
      - kafka
      - postgres
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: connect-cluster
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
```

Bring it up: `docker compose up -d`

## 6.2 Create a source table

```bash
docker exec -it postgres psql -U postgres -d orders_db
```
```sql
CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  customer_id INT,
  amount NUMERIC,
  status VARCHAR(20),
  updated_at TIMESTAMP DEFAULT now()
);
ALTER TABLE orders REPLICA IDENTITY FULL;  -- needed so UPDATE/DELETE include full row data
```

## 6.3 Register the Debezium connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "orders_db",
      "topic.prefix": "orders_db",
      "table.include.list": "public.orders"
    }
  }'
```

Check it's running: `curl http://localhost:8083/connectors/orders-connector/status`

A new topic `orders_db.public.orders` is auto-created. Every row change now flows there automatically.

## 6.4 Test it — insert/update/delete and watch Kafka react

```sql
INSERT INTO orders VALUES (1, 501, 2500.00, 'CREATED', now());
UPDATE orders SET status = 'SHIPPED' WHERE order_id = 1;
DELETE FROM orders WHERE order_id = 1;
```

Consume the topic and watch each change appear as a Kafka message:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders_db.public.orders --bootstrap-server localhost:9092 --from-beginning
```

Each message's payload has a `before`, `after`, and `op` field (`c`=create, `u`=update, `d`=delete) — this is what lets your downstream logic tell an insert from an update from a delete.

## 6.5 Consume it in Spark and MERGE into Delta

This is where it differs from the earlier append-only pipeline: CDC needs `MERGE`, not append, so your Delta table reflects current state, not just an event log.

```python
from pyspark.sql.functions import from_json, col, get_json_object
from delta.tables import DeltaTable

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders_db.public.orders")
    .option("startingOffsets", "earliest")
    .load()
)

# Debezium payload: extract op, before, after as JSON strings first
parsed = raw.select(
    get_json_object(col("value").cast("string"), "$.payload.op").alias("op"),
    get_json_object(col("value").cast("string"), "$.payload.after").alias("after_json"),
)

def upsert_to_delta(batch_df, batch_id):
    if not DeltaTable.isDeltaTable(spark, "/tmp/kafka-lab/delta/orders_current"):
        batch_df.write.format("delta").save("/tmp/kafka-lab/delta/orders_current")
        return
    target = DeltaTable.forPath(spark, "/tmp/kafka-lab/delta/orders_current")
    (
        target.alias("t")
        .merge(batch_df.alias("s"), "t.order_id = s.order_id")
        .whenMatchedDelete(condition="s.op = 'd'")
        .whenMatchedUpdateAll(condition="s.op = 'u'")
        .whenNotMatchedInsertAll(condition="s.op = 'c'")
        .execute()
    )

query = (
    parsed.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/tmp/kafka-lab/checkpoints/orders_cdc")
    .start()
)
query.awaitTermination()
```

`foreachBatch` + `MERGE` is the standard pattern for turning a CDC stream (insert/update/delete events) into a Delta table that always reflects current source-table state — this is exactly how most production CDC pipelines are built.

---

## What you now have, end to end
1. A real database with real DML
2. Debezium capturing every change with zero app-side code
3. Kafka as the durable, replayable transport layer
4. Spark Structured Streaming consuming and applying changes
5. A Delta table that mirrors the source table in near-real-time

This is the pattern worth being able to rebuild from memory — it's the highest-frequency real-world Kafka use case for a Data Engineer, more common in practice than pure event-streaming pipelines.

**Next steps to solidify this yourself:**
- Break something on purpose — stop the connect container mid-stream, restart it, confirm no data is lost (this is Kafka's durability doing its job)
- Add a second table to `table.include.list` and confirm a second topic appears automatically
- Try `whenMatchedUpdateAll` failing silently if columns don't match — inspect the schema mismatch and fix it

Tell me what you actually run and where it breaks — that's where the real learning happens from here.
