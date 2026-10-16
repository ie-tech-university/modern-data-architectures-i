# Modern Data Architectures for Big Data I
## Lecture 6.1 — Data Storage

*Updated edition, October 2026*

---

## Agenda

1. **What is it?** — the storage layer and its two flavours
2. **Batch storage** — from Hadoop to cloud object storage
3. **Streaming storage** — Kafka and friends
4. **Summary** — the storage layer by provider
5. **Hands-on in OSBDET** — HDFS, MinIO and Kafka

---

## 1. What Is It?

The storage layer sits between ingestion and processing:

```
Sources ──► Ingestion ──► ┃ STORAGE ┃ ──► Processing ──► Serving
            NiFi,         ┃ HDFS, MinIO (batch)  ┃
            Kafka Connect ┃ Kafka (streaming)    ┃
```

The goal of this layer is to **store all the data at scale**. There are two main types:

| | Batch storage | Streaming storage |
|---|---|---|
| **V it serves** | Volume | Velocity |
| **What** | A **file-based** store that keeps all the data **in bulk** | A specialised store for **streaming and real-time** data |
| **How long** | Years | Usually **transient**: hours or days |
| **Examples** | HDFS, MinIO, Amazon S3, ADLS, Cloud Storage | Kafka, Pulsar, Event Hubs, Pub/Sub |

### Two paths through the platform

```
Batch:      batch ingestion  ──► BATCH STORAGE  ──► batch processing
                                      ▲
                                      │ streaming data is also persisted to batch storage
Streaming:  stream ingestion ──► STREAM STORAGE ──► stream processing
```

**Example:** bike-station events flow through Kafka for a live dashboard *and* land in the data lake for next month's analysis.

### The most important layer: the data lake

This layer is often called the **data lake**.

> **Watch out:** many big data projects fail because they don't invest enough time designing this layer. A lake without design becomes a *data swamp*.

Typical zones:

| Zone | Contents |
|---|---|
| **Raw · bronze** | Data exactly as it arrived (e.g. `raw/tfl/bikepoint/2026/10/15/`) |
| **Curated · silver** | Cleaned, typed, deduplicated |
| **Consumption · gold** | Business-ready tables |

Design decisions: zones and folder layout, file formats and partitioning (next lecture), security, data quality and governance.

---

## 2. Batch Storage

### File systems and object storage

| Distributed file system (HDFS) | Object storage (S3, MinIO) |
|---|---|
| Real **directories** and files, spread across many machines | A flat **bucket** of objects; "folders" are just prefixes in the **key** |
| `/data/raw/tfl/bikepoint_1000.json` | bucket `datalake`, key `raw/tfl/bikepoint_1000.json` |
| Accessed through HDFS clients | Accessed over HTTP (the S3 API) |

In the cloud, **object storage** has become the default foundation of the data lake.

### Hadoop's core idea: data locality

Hadoop runs the **processing on the nodes where the data is stored**: *move the code to the data, not the data to the code.* In 2008, moving data through the network was the bottleneck.

**The catch:** storage and compute share the same machines, so **every node must stay up** 24×7×365, even when no job runs, just to read and write files. In a pay-per-use cloud, that's very expensive.

### The cloud paradigm shift

- **Hadoop** brought storage and compute **together**.
- **Clouds** split them **apart**: always-on, cheap object storage + on-demand compute, connected by data-centre networks that move petabits per second.

### Ephemeral clusters

```
   1 Create cluster → 2 Read data → 3 Compute → 4 Write results → 5 Destroy cluster
                             ↖──────── object storage ────────↙
```

Reserve the processing power you need, process, store the results, switch the cluster off. Many services now go further with **serverless** engines (BigQuery, Databricks serverless, AWS Glue).

### Can we still use Hadoop and Spark in the cloud?

**Of course!** Only the path changes:

| Storage | Path in Spark |
|---|---|
| HDFS | `hdfs://namenode:9000/data/raw/tfl/` |
| Amazon S3 / MinIO | `s3a://datalake/raw/tfl/` |
| Azure Data Lake Storage | `abfss://datalake@myaccount.dfs.core.windows.net/raw/tfl/` |
| Google Cloud Storage | `gs://datalake/raw/tfl/` |

```python
df = spark.read.json("s3a://datalake/raw/tfl/")      # same code, any storage
df.write.parquet("s3a://datalake/curated/tfl/")
```

---

### 2.1 Open source

#### Hadoop HDFS

The [Hadoop Distributed File System](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html) is a distributed, scalable and portable file system that can store any type of data. Created at **Yahoo!**, inspired by Google's GFS paper.

- Files are split into **blocks** (128 MB by default)
- Each block is **replicated 3 times** on different nodes
- The **NameNode** keeps the metadata: which blocks make up each file and where they live

```
trips.csv (300 MB) = [A 128 MB][B 128 MB][C 44 MB]

NameNode: A → 1,2,3 · B → 2,3,4 · C → 1,3,4
DataNode 1: A C   DataNode 2: A B   DataNode 3: A B C   DataNode 4: B C
```

Lose a node? Every block still has two copies.

#### MinIO

[MinIO](https://github.com/minio/minio) is a high-performance **object storage** server, **API-compatible with Amazon S3**. It stores photos, videos, logs, backups, container images and data lake files, and any S3 tool works with it. It's included in **OSBDET** as your local S3.

> **2025–2026 update:** in May 2025 the admin console was removed from the free community edition; in October 2025 free binaries and Docker images stopped; in December 2025 the community repository entered maintenance mode and was later archived. OSBDET ships an earlier release, fine for learning. Open-source S3 alternatives: **Ceph RGW**, **SeaweedFS**, **Garage**.

---

### 2.2 Cloud

| Service | Provider | Notes |
|---|---|---|
| **Amazon S3** | AWS | **S**imple **S**torage **S**ervice, the reference object store. Now also **S3 Tables** (managed Apache Iceberg tables). |
| **Azure Data Lake Storage** | Azure | Blob storage with a **hierarchical namespace** (real folders, fine-grained permissions). Trillions of files; files over a petabyte. Gen1 was retired in 2024; use **Gen2**. |
| **Google Cloud Storage** | Google Cloud | Object storage used directly by BigQuery, Dataproc and Vertex AI. Regional, dual-region or multi-region buckets. |

Object stores are often called "file-based" because you store files in them, but under the hood they're buckets of objects reached over HTTP.

#### Storage tiers

Storage gets cheaper as data gets colder, but reading it becomes slower and more expensive.

| Provider | Hot | Cool | Cold | Archive |
|---|---|---|---|---|
| AWS S3 | Standard | Standard-IA | Glacier Instant Retrieval | Glacier Deep Archive |
| Azure | Hot | Cool | Cold | Archive |
| Google Cloud | Standard | Nearline | Coldline | Archive |

Lifecycle rules move old data to cheaper tiers automatically (e.g. raw files to Archive after 90 days).

#### Databricks: from DBFS to Unity Catalog volumes

The **Databricks File System (DBFS)** was a file system mounted into every workspace. Databricks now recommends **Unity Catalog volumes** for files, with governance built in; DBFS root and mounts are legacy.

```
catalog main → schema raw → volume landing → telegram/
Path: /Volumes/main/raw/landing/telegram/
```

---

## 3. Streaming Storage

Hadoop was designed with only **two Vs** in mind: volume and variety. As data gained **velocity**, we needed to react, analyse and process it **while still in motion** (fraud alerts, live bike availability, sensor alarms). That calls for a dedicated streaming store.

### 3.1 Open source

#### Apache Kafka

[Kafka](https://kafka.apache.org/) is a distributed streaming platform: a massively scalable, **append-only log** with high throughput and low latency. Created at **LinkedIn**.

```
Producers ──►  topic: bike-status
               P0: [0][1][2][3][4][5][6] ◄─ new events appended at the end
               P1: [0][1][2][3][4]
               P2: [0][1][2][3][4][5]
               kept for the retention period (7 days by default), then deleted
                         │                    │
                Dashboard (offset 6)   Data lake loader (offset 3)
```

Kafka 4 no longer needs ZooKeeper, and tiered storage can offload old events to object storage.

#### Apache Pulsar

[Apache Pulsar](https://pulsar.apache.org/) is a distributed **pub-sub messaging** platform similar to Kafka, created at **Yahoo!**. It separates serving (brokers) from storage (Apache BookKeeper) and offers multi-tenancy and geo-replication, but its adoption has been much smaller than Kafka's.

### 3.2 Cloud

| Service | Provider | Notes |
|---|---|---|
| **Azure Event Hubs** | Azure | Fully managed real-time ingestion, millions of events per second. Kafka-compatible endpoint. |
| **Amazon MSK** and **Kinesis Data Streams** | AWS | MSK: fully managed Apache Kafka (also serverless). Kinesis: AWS's own streaming store. |
| **Google Cloud Pub/Sub** | Google Cloud | Durable message storage and real-time delivery. Google also offers a Managed Service for Apache Kafka. |

**Pattern to notice:** the Kafka API has become a standard, so Kafka skills transfer everywhere.

---

## 4. Summary

- Almost every big data solution needs **both a batch store and a streaming store**.
- The options depend on where you deploy: **on premises or in the cloud**.
- The cloud **separates storage from compute**: cheap, always-on object storage plus on-demand processing.
- The storage layer holds **all the data** (the data lake), so design it carefully for **security, data quality and governance**.

### Ingestion → storage, by provider

| | Batch ingestion | Batch storage | Stream ingestion | Stream storage |
|---|---|---|---|---|
| **Open source** | NiFi, Kafka Connect | **HDFS, MinIO** | NiFi, Kafka clients | **Kafka, Pulsar** |
| **Azure** | Data Factory | **Data Lake Storage** | IoT Hub | **Event Hubs** |
| **AWS** | Glue, Data Firehose | **Amazon S3** | IoT Core, apps | **MSK, Kinesis** |
| **Google Cloud** | Storage Transfer | **Cloud Storage** | apps, devices | **Pub/Sub** |

---

## 5. Hands-on in OSBDET

### HDFS from the command line

Turn **Hadoop** on in the WebUI, open a terminal in OSBDET's Jupyter and try:

```bash
# create a folder and upload a file
hdfs dfs -mkdir -p /data/raw/tfl
curl -s https://api.tfl.gov.uk/BikePoint -o bikepoint.json
hdfs dfs -put bikepoint.json /data/raw/tfl/

# list it and see how HDFS stored it (blocks and replicas)
hdfs dfs -ls -h /data/raw/tfl
hdfs fsck /data/raw/tfl/bikepoint.json -files -blocks
```

Browse the same files in the **HDFS UI** at `localhost:50070` → *Utilities → Browse the file system*. OSBDET runs a single node, so you'll see **1 replica** instead of 3.

### MinIO with Python (the S3 API)

```python
import boto3, requests

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",  # MinIO inside OSBDET
    aws_access_key_id="osbdet",
    aws_secret_access_key="osbdet123$",
)

s3.create_bucket(Bucket="datalake")

data = requests.get("https://api.tfl.gov.uk/BikePoint").content
s3.put_object(Bucket="datalake", Key="raw/tfl/bikepoint.json", Body=data)

for obj in s3.list_objects_v2(Bucket="datalake")["Contents"]:
    print(obj["Key"], obj["Size"])
```

The **same code works with Amazon S3**: remove `endpoint_url` and use your AWS credentials. Check the result in the **MinIO console** at `localhost:29001` (user `osbdet`, password `osbdet123$`).

### Kafka from the command line

Turn **Kafka** on and open **two terminals** (the `.sh` scripts live in Kafka's `bin/` folder):

```bash
# 1. create a topic with 3 partitions
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bike-status --partitions 3

# 2. terminal A: type one event per line
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic bike-status
> {"id": "BikePoints_1", "NbBikes": 7}

# 3. terminal B: read everything from the beginning
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bike-status --from-beginning
```

**Try this:** stop the consumer, send more events, start it again with `--from-beginning`. The old events are still there: Kafka **stores** them.

---

## Links & References

- [HDFS user guide](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
- [Amazon S3](https://aws.amazon.com/s3/) · [Azure Data Lake Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) · [Google Cloud Storage](https://cloud.google.com/storage)
- [Databricks: Unity Catalog volumes](https://docs.databricks.com/en/volumes/index.html)
- [Apache Kafka](https://kafka.apache.org/) · [Apache Pulsar](https://pulsar.apache.org/)
- [Azure Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/) · [Amazon MSK](https://aws.amazon.com/msk/) · [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs/overview)

---