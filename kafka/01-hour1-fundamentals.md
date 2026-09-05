# HOUR 1 — Kafka Fundamentals ⭐⭐⭐⭐⭐

## 1. What is Apache Kafka?

**What:** Kafka is a distributed, append-only, replicated commit log that stores streams of records and lets producers write and consumers read them, independently and continuously.

**Why it exists:** Batch pipelines (your world today) work like this:

```
Source (files/DB) → Spark job (runs every N hours) → Sink
```

That's fine when "fresh enough" means hourly or nightly. It breaks when a business needs data **the moment it happens** — fraud detection, live dashboards, order tracking, inventory sync. Kafka exists to decouple the systems that *produce* events from the systems that *consume* them, in real time, at scale, without the producer needing to know who's listening.

**How it works (one-line version):** Producers append records to a topic. Kafka stores those records durably and in order, on disk, replicated across brokers. Consumers read records at their own pace by tracking an offset (a position pointer) — Kafka doesn't push, consumers pull.

**Real-world example:** An e-commerce checkout service publishes an `ORDER_CREATED` event the instant a customer completes checkout. Inventory, billing, notifications, and analytics each consume that same event independently, without the checkout service knowing any of them exist.

**Interview question:** *"Why not just use a database or a REST API for this instead of Kafka?"*
Answer shape: a DB/API is pull-based and point-to-point — every consumer must know where to ask and hit the source system directly, which doesn't scale to many consumers and couples systems together. Kafka is pub-sub: producers publish once, any number of independent consumers read the same stream at their own pace, and the source system is never hit directly by consumers.

---

## 2. Why Kafka? Real-world use cases

- **Real-time pipelines:** Kafka → Spark Structured Streaming → Delta/Iceberg (this is literally Hour 8)
- **Microservice decoupling:** services communicate via events instead of direct API calls
- **CDC (Change Data Capture):** database changes streamed into Kafka via Debezium, then into a lake
- **Log aggregation:** application/infra logs centralized for monitoring
- **Metrics/telemetry pipelines:** IoT, clickstream, sensor data at high volume

**Batch analogy:** think of a Kafka topic the way you think of a Delta table that's *continuously appended to* rather than loaded in scheduled batches — except readers can also each independently track "how far I've read."

---

## 3. Event-driven architecture vs traditional (request-response)

```
TRADITIONAL (request-response)          EVENT-DRIVEN (Kafka)
Service A --HTTP call--> Service B      Service A --publish--> Kafka Topic
(A knows B exists, waits for reply)     (A doesn't know who reads it)
                                         Service B --subscribe-->
                                         Service C --subscribe-->
                                         Service D --subscribe-->
```

Traditional = tight coupling, synchronous, one-to-one.
Event-driven = loose coupling, asynchronous, one-to-many.

---

## 4. Kafka vs traditional messaging (e.g. RabbitMQ/JMS)

| Traditional MQ | Kafka |
|---|---|
| Message deleted once consumed | Message retained for a configured period regardless of consumption |
| Typically no replay | Consumers can rewind/replay by resetting offset |
| Good for task queues (one consumer per message) | Good for streams (many independent consumer groups can each read everything) |
| Lower throughput, per-message routing logic | Built for very high throughput, sequential disk I/O |

Interview one-liner: *"RabbitMQ is a queue — a message goes to one consumer and is gone. Kafka is a log — a message stays, and many independent consumers can read it, even replay it later."*

---

## 5. Kafka Architecture

```
 Producers                    KAFKA CLUSTER                       Consumer Group
┌──────────┐        ┌────────────────────────────────┐        ┌────────────────┐
│Producer 1│──┐      │  Broker 1   Broker 2   Broker 3 │        │  Consumer A    │
│Producer 2│──┼─────▶│                                  │───────▶│  Consumer B    │
│Producer 3│──┘      │   Topic: orders                  │        │  Consumer C    │
└──────────┘         │   ├── Partition 0                │        └────────────────┘
                      │   ├── Partition 1                │
                      │   └── Partition 2                │
                      └────────────────────────────────┘
```

### Core vocabulary — each with what / why / how / example / interview Q

**Kafka Cluster**
- *What:* a group of broker servers working together.
- *Why:* one machine can't hold all the throughput or survive failure alone.
- *How:* brokers coordinate via a metadata layer (Zookeeper historically, now KRaft — Kafka's own Raft-based consensus, no external dependency).
- *Example:* a 3-broker cluster serving hundreds of topics for an org.
- *Interview Q:* "What replaced Zookeeper in modern Kafka?" → KRaft mode (Kafka Raft), removing the external Zookeeper dependency.

**Broker**
- *What:* a single Kafka server — it stores data and serves producer/consumer requests.
- *Why:* horizontal scalability and fault tolerance come from having many brokers, not one giant server.
- *How:* each broker owns a subset of partitions (as leader) and stores replicas of others.
- *Example:* broker-1 might be the leader for `orders-partition-0` and a follower/replica for `orders-partition-1`.
- *Interview Q:* "If a broker goes down, is data lost?" → Not if replication factor > 1: another broker holding a replica is promoted to leader (covered fully in Hour 5).

**Topic**
- *What:* a named, logical stream of records — like a table name, but for events instead of rows-at-rest.
- *Why:* organizes events by category (`orders`, `payments`, `clickstream`).
- *How:* a topic is split into partitions for parallelism; it has no fixed schema at the Kafka level (schema is your application's/Schema Registry's job).
- *Example:* an `orders` topic holds every `ORDER_CREATED`, `ORDER_CANCELLED` event.
- *Interview Q:* "Is a topic ordered?" → Not globally — only *within* each partition (critical distinction, Hour 2).

**Partition**
- *What:* an ordered, immutable, append-only sequence of records — the actual unit of parallelism and storage.
- *Why:* a single partition = single sequential log = single point of parallel throughput. More partitions = more parallel consumers possible.
- *How:* each record gets an offset (its position within that partition) when appended.
- *Example:* `orders` topic with 3 partitions can be read in parallel by up to 3 consumers in a group.
- *Interview Q:* "Why can't a topic with 3 partitions be consumed in parallel by more than 3 consumers in the same group?" → because a partition is the smallest unit assigned to a consumer; the 4th+ consumer sits idle (full breakdown in Hour 2/4).

**Producer**
- *What:* the client application that writes records to a topic.
- *Why:* the "source" side of the pipeline — anything generating events.
- *How:* chooses (or lets Kafka choose) which partition a record lands in, usually based on a key.
- *Example:* checkout service producing `ORDER_CREATED` events.
- *Interview Q:* covered deeply in Hour 3 (acks, idempotency, retries).

**Consumer**
- *What:* the client application that reads records from a topic, tracking its own offset.
- *Why:* the "sink" side — anything that needs to react to events.
- *How:* pulls (polls) batches of records; commits offsets to mark progress.
- *Example:* inventory service consuming `ORDER_CREATED` to decrement stock.
- *Interview Q:* "Does Kafka push data to consumers?" → No — pull-based via `poll()`, consumer controls its own pace (unlike push-based systems that can overwhelm slow consumers).

**Consumer Group**
- *What:* a named set of consumers cooperating to consume a topic, where each partition is owned by exactly one consumer in the group at a time.
- *Why:* enables horizontal scaling of consumption while guaranteeing each record is processed once *per group*.
- *How:* Kafka assigns partitions across the group's live consumers; if a consumer dies, its partitions are reassigned (rebalance).
- *Example:* `inventory-service` group with 3 consumers, each owning one of 3 partitions.
- *Interview Q:* "If two different consumer groups read the same topic, does each group get all the messages independently?" → Yes — offsets are tracked *per group*, so each group reads the full stream independently (this is what makes Kafka pub-sub, not just a queue).
- Kafka pub-sub --> In Kafka, producers publish messages to a topic, and consumers subscribe to that topic.

**Offset**
- *What:* a monotonically increasing integer identifying a record's position within a partition.
- *Monotonically increasing* means a value never decreases as you move through the data. It can stay the same or increase.
- *Why:* lets a consumer say "I've processed up to here" and resume exactly from there — this is Kafka's replay/fault-tolerance mechanism.
- *How:* consumer commits offsets (auto or manual) to a special internal topic (`__consumer_offsets`).
- *Example:* consumer crashes after committing offset 1045; on restart it resumes from 1046.
- *Interview Q:* "How is a Kafka offset different from a Spark checkpoint?" → Offset = position in the Kafka log itself; Spark checkpoint = Spark's own record of progress + state, which *includes* offsets it has processed but also things like watermark state and aggregation state (full detail in Hour 7).

---

## 6. Mapping onto what you already know

| Batch/Spark concept | Kafka equivalent |
|---|---|
| Delta table (append-only) | Topic (conceptually — append-only log) |
| File split / partition file in ADLS | Kafka partition |
| Reading a batch checkpoint / watermark to resume incremental load | Consumer offset |
| Parallel Spark tasks per partition | Parallel consumers per partition (1:1 max within a group) |
| Replication in Delta (via ADLS redundancy) | Kafka replication factor across brokers |
| A scheduled ADF trigger pulling new files | A consumer continuously polling a topic |

The single biggest mental shift: in batch, *you* control when the read happens (schedule). In Kafka, the *data arrives continuously* and your consumer decides its own pace — but the data itself never waits for you.

---

## Hour 1 Recap

**5 key concepts to remember**
1. Kafka = distributed, replicated, append-only commit log — pub-sub, not point-to-point.
2. A topic is split into partitions; ordering is guaranteed only within a partition, never across the whole topic.
3. Producers write, consumers pull (never pushed to) and track progress via offsets.
4. A consumer group scales consumption — but max useful parallel consumers per group = number of partitions.
5. Different consumer groups reading the same topic get fully independent copies of the stream.

**5 interview questions to be able to answer cold**
1. What is Kafka and why would you use it over a REST API or database polling?
2. What's the difference between a topic and a partition?
3. What is an offset and who tracks it?
4. What is a consumer group, and what happens with more consumers than partitions?
5. How does Kafka differ from a traditional message queue like RabbitMQ?

**1 hands-on exercise (no cluster needed yet — pure reasoning)**
Draw out (on paper or in a note) what happens to a 3-partition `orders` topic under these three scenarios, and write one sentence for each:
- 1 consumer in the group
- 3 consumers in the group
- 5 consumers in the group

(We'll verify your answers explicitly in Hour 2 — don't peek ahead.)

**What to revise before Hour 2**
Make sure you can explain, without hesitation: topic vs partition vs offset, and why ordering is per-partition not per-topic. Hour 2 goes deep on exactly this.

---

Reply with your answers to the hands-on exercise, any questions on Hour 1, or **"next"** to move to Hour 2 (Topics, Partitions & Offsets).
