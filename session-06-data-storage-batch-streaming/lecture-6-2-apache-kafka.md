# Modern Data Architectures for Big Data I
## Lecture 7.2 — Apache Kafka

*Updated edition, October 2026 · Kafka 4*

---

## Agenda

1. **What is it?** — an event streaming platform
2. **Core concepts** — messages and schemas, topics and partitions, producers and consumers, brokers and clusters
3. **Hands-on in OSBDET** — Kafka from the command line and Python
4. **Summary**

---

Kafka is the **streaming storage** of the architecture:

```
Batch:      batch ingestion  ──► batch storage  ──► batch processing
                                      ▲ streaming data is also persisted to batch storage
Streaming:  stream ingestion ──► ┃STREAM STORAGE: KAFKA┃ ──► stream processing
```

---

## 1. What Is It?

- Kafka is an **event streaming platform**. An **event** is something that happened, plus when (a bike docked, a payment, a page view, a sensor reading).
- Born as an internal system at **LinkedIn**, powering real-time apps and data flows; open-sourced in 2011 and an **Apache** top-level project since 2012.
- **Motivation:** plenty of systems stored data, but nothing handled the **continuous flow of data between systems**.

### Without Kafka vs with Kafka

| Without Kafka | With Kafka |
|---|---|
| Point-to-point "spaghetti": every source wired to many destinations | One central hub: producers publish once, any consumer subscribes |

### Built from scratch, with a bigger idea

LinkedIn tried traditional messaging systems (e.g. RabbitMQ), log aggregation tools and ETL tools without success. The idea: a streaming platform to **publish & subscribe**, **store** and **process** streams.

Three big differences from messaging systems:

1. **A modern distributed system** that scales to the most massive companies
2. **A true storage system**: data is **replicated**, **persistent** and kept **as long as you like** (reading doesn't delete)
3. **A higher level of abstraction**: compute on streams with less code

### Who's behind Kafka today?

- **Apache Kafka** is developed by a large open-source community. Current series: **Kafka 4** (4.3 released May 2026). OSBDET ships Kafka 4.1.
- **Confluent**, founded in 2014 by Kafka's creators, leads much of its development and sells an enterprise platform and cloud service. **IBM acquired Confluent in March 2026** (about $11 billion).
- Kafka-compatible services: Amazon MSK, Azure Event Hubs, Google Managed Service for Apache Kafka, Redpanda…

---

## 2. Core Concepts

### 2.1 Messages and schemas

A **message** (record) is the unit of data in Kafka:

| Part | Example | Notes |
|---|---|---|
| **key** | `"BikePoints_1"` | Optional; decides the partition |
| **value** | `{"name": "River Street", "bikes": 8}` | The payload |
| **timestamp** | `2026-10-16T09:15:00Z` | When it happened |
| **headers** | `source=tfl-api` | Optional metadata |

Physically, key and value are **arrays of bytes**: Kafka doesn't care what's inside. Producers and consumers agree on the format through a **schema**.

| Encoding | Per message | Verdict |
|---|---|---|
| **JSON** | Field names repeated every time | Human-readable, inefficient |
| **Avro + full schema** | Schema travels with every message | Binary, still heavy |
| **Avro + schema ID** | Only a tiny ID | Compact, optimal |

**JSON** is easy and readable; **Avro** (or **Protobuf**) gives compact binary, strong typing and schema evolution.

**Schema registry:** the producer registers the schema and gets an ID; messages carry `[id][payload]`; the consumer fetches the schema by ID. It decouples teams and blocks incompatible changes (Confluent Schema Registry, Apicurio, AWS Glue Schema Registry).

> **In this course** we skip the registry and write/read **text/JSON as bytes**.

### 2.2 Topics and partitions

```
topic: tfl.bikepoint.status
Partition 0: [0][1][2][3][4][5][6][7][8][9]   ← writes go to the end
Partition 1: [0][1][2][3][4][5][6]
Partition 2: [0][1][2][3][4][5][6][7][8]
Partition 3: [0][1][2][3][4][5][6][7][8][9][10][11]
```

- Messages are grouped into **topics** (roughly a table), written **append-only** and read **in order**
- Topics are split into **partitions**; each message gets an **offset** (its position)
- Ordering is **guaranteed within a partition**, not across the topic
- Partitions bring **scalability and redundancy**: each can live on a different server

**The key decides the partition:** `partition = hash(key) % number_of_partitions`. Same key → same partition → events for one station stay in order. Messages without a key are spread across partitions.

**Retention:**

- **Delete** (default): messages deleted after the retention time (7 days by default, `retention.ms=604800000`) or a size limit
- **Compact**: keep only the **latest value per key** (`cleanup.policy=compact`), ideal for "current state" topics
- **Tiered storage** can move old segments to object storage

### 2.3 Producers and consumers

**Producers** create and publish messages (publishers, writers) to a topic. Without a key they spread messages across partitions; with a key (or a custom partitioner) you get full control. `acks=all` waits for all in-sync replicas (the default since Kafka 3).

**Consumers** read messages (subscribers, readers) in the order they were produced (per partition). They track progress by **committing offsets**. Reading **doesn't delete**: another consumer can read the same data. The distance to the latest message is the **lag**.

**Consumer groups:** one or more consumers working together on a topic; **each partition is consumed by only one member**.

```
topic (4 partitions)              consumer group "dashboard"
Partition 0 ─┐
Partition 1 ─┴──► Consumer 0
Partition 2 ────► Consumer 1
Partition 3 ────► Consumer 2
```

- **Scale out** by adding consumers (up to the number of partitions)
- **Fault tolerance:** if one fails, the group **rebalances**
- Different groups read the same topic **independently**
- Kafka 4 adds a faster rebalance protocol and **share groups** ("queues for Kafka")

Advanced clients not covered: **Kafka Connect** (integration) and **Kafka Streams** (stream processing).

### 2.4 Brokers and clusters

A **broker** is a single Kafka server that receives messages, assigns offsets, commits them to disk (as log segments) and serves consumers' fetch requests. One broker handles thousands of partitions and millions of messages per second.

```
          KRaft controllers (assign partitions, watch for failures)
Broker 1: A-P0 leader     A-P1 follower
Broker 2: A-P0 follower   A-P1 leader
Broker 3: A-P0 follower   A-P1 follower
```

- Brokers form a **cluster**; **controllers** assign partitions and monitor failures
- Each partition has one **leader**; producers and consumers talk to it
- **Replicas** on other brokers (replication factor, often 3) take over if the leader fails

> **Kafka 4:** cluster metadata is managed by Kafka itself (**KRaft**). ZooKeeper is no longer needed.

---

## 3. Hands-on in OSBDET

### Create and inspect a topic

Turn **Kafka** on in the OSBDET WebUI and open a terminal (scripts live in Kafka's `bin/` folder):

```bash
# create a topic with 3 partitions
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic tfl.bikepoint.status --partitions 3

# see its partitions, leaders and replicas
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic tfl.bikepoint.status

# later: who is reading, and how far behind are they?
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group dashboard
```

OSBDET runs **one broker**, so every partition has a single replica.

### Produce live TfL events from Python

```python
import json, requests
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092"})
stations = requests.get("https://api.tfl.gov.uk/BikePoint").json()

for s in stations:
    bikes = next(int(x["value"]) for x in s["additionalProperties"]
                 if x["key"] == "NbBikes")
    event = {"id": s["id"], "name": s["commonName"], "bikes": bikes}
    p.produce("tfl.bikepoint.status",
              key=s["id"],                 # same station → same partition
              value=json.dumps(event))

p.flush()                                 # wait until all 799 are delivered
print("sent", len(stations), "events")
```

*Install the client with `pip install confluent-kafka` if needed.*

### Consume them as a group

```python
import json
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "dashboard",           # the consumer group
    "auto.offset.reset": "earliest",   # new group: start at offset 0
})
c.subscribe(["tfl.bikepoint.status"])

while True:
    msg = c.poll(1.0)
    if msg is None: continue
    if msg.error(): print(msg.error()); continue
    e = json.loads(msg.value())
    print(f"P{msg.partition()} @{msg.offset()}  {e['bikes']:>3} bikes  {e['name']}")
```

**Experiment:** run the consumer in two terminals with the same `group.id`; Kafka splits the partitions between them. Stop one and watch the other take over (rebalance).

---

## 4. Summary

- Kafka is an **event streaming platform**
- The unit of data is the **message** (key, value, timestamp, headers)
- Messages are published by **producers** and read by **consumers**
- Messages are stored in **topics**, ideally with a **schema**; how long they live is the **retention**
- A topic is roughly the equivalent of a **table**; topics are split into **partitions**, ordered by **offset**
- Partitions are spread and **replicated** across **brokers**; Kafka runs as a **cluster** (KRaft, no ZooKeeper)

```
Producer ─► ┌──────── Kafka cluster ────────┐ ─► Consumer
            │ Broker 1        Broker 2      │
            │ click: P1, P2   click: P3     │
            │ upload: P1      upload: P2, P3│
            └───────────────────────────────┘
```

---

## Learn More

- [Confluent Developer: free Kafka courses](https://developer.confluent.io/courses/)
- [Everything you need to know about Kafka in 10 minutes](http://www.youtube.com/watch?v=FKgi3n-FyNU)
- [How to paint a bike shed: Kafka topic naming conventions](https://cnr.sh/essays/how-paint-bike-shed-kafka-topic-naming-conventions) — a common pattern is `<domain>.<dataset>.<event>`, e.g. `tfl.bikepoint.status`

**Gwen Shapira, Todd Palino, Rajini Sivaram, Krit Petty — *Kafka: The Definitive Guide*. 2nd edition, O'Reilly, 2021.** Covers Kafka 2.x/3.x; the concepts still apply, though ZooKeeper sections are now historical.

---
