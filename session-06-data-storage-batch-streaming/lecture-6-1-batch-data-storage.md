# Modern Data Architectures for Big Data I
## Lecture 7.1 — Batch Data Storage

*Updated edition, October 2026*

---

## Agenda

1. **Batch data storage** — types and challenges
2. **Data lakes** — types, design and zones
3. **From data lake to lakehouse** — what table formats add
4. **Summary**

---

## 1. Batch Data Storage

```
Batch:      batch ingestion  ──► ┃BATCH STORAGE┃ ──► batch processing
                                       ▲  streaming data is also persisted here
Streaming:  stream ingestion ──►  stream storage  ──► stream processing
```

### 1.1 Storage types

| | Distributed file system | Object / blob store |
|---|---|---|
| Unit of storage | File | Object (blob: binary large object) |
| Unit of structure | Folder | None (only the bucket/container) |
| Structure | Hierarchical | Flat |
| Data access | Path: `/raw/tfl/bikepoint.json` | Key: `raw/tfl/bikepoint.json` in bucket `datalake` |
| Examples | HDFS · Azure Data Lake Storage (hierarchical namespace) · Databricks volumes | Amazon S3 · MinIO · Google Cloud Storage · Azure Blob Storage |

*ADLS Gen2 is the hybrid: blob storage with a hierarchical namespace switched on. The old DBFS is legacy; Databricks now uses Unity Catalog volumes.*

#### In object stores, folders don't exist

Object stores have a **flat namespace**. An object named `folder1/file.txt` in `bucket1` is **one object** whose key contains `folder1/`; there is no real folder. Tools pretend there are folders by splitting keys on `/`:

```python
# "list the folder" = list keys with a prefix
s3.list_objects_v2(Bucket="bucket1", Prefix="folder1/", Delimiter="/")
```

> **Consequence:** "renaming a folder" in S3 means copying and deleting every object under that prefix. In HDFS it's a single, instant metadata change.

#### File system or object store?

In terms of functionality there's **no difference** for a data lake. Differences:

- **Permissions:** file systems set access once per folder; object stores use policies per object or per key prefix
- **Renames and moves:** instant in a file system, copy + delete in an object store
- **Cost and operations:** cloud object stores are cheap, always on and fully managed

*In practice (2026), most new data lakes run on cloud object storage, with governance tools (Unity Catalog, AWS Lake Formation) adding folder-like permissions.*

---

### 1.2 Challenges

Batch storage is **not** intended for:

| Challenge | Solution |
|---|---|
| ⏱ **Low-latency access** (tens of milliseconds) | A **serving layer** (NoSQL datastores) |
| 🗂 **Lots of small files** | **Compaction** into fewer, bigger files |
| ✎ **Random modifications** | **Rewrite** files, or use a **table format** |

#### Challenge 1: low-latency access

Batch storage is optimised for **high throughput** at the expense of **latency** (time to first byte: hundreds of ms to seconds). Apps needing answers in tens of milliseconds read from a **serving layer** instead. *Example:* the bike app reads "bikes available now" from a key-value store, not from the lake.

#### Challenge 2: lots of small files

Every file costs a list call, a metadata entry, an open/read request and often a processing task. *Example:* one TfL snapshot every 5 minutes = 288 files of ~7 KB per day; compacted, one ~2 MB Parquet file. Aim for files of **hundreds of MB** (typically 128 MB–1 GB).

```python
# Spark: read many small files, write a few big ones
spark.read.json("s3a://datalake/raw/tfl/bikepoint/2026/10/15/") \
     .coalesce(1).write.parquet("s3a://datalake/std/mobility/bikepoint_status/dt=2026-10-15/")
# Delta Lake does it for you:  OPTIMIZE bikepoint_status
```

#### Challenge 3: no random modifications

You can't edit a file like on your laptop: only **append** (HDFS) or **rewrite the whole file** (object stores).

> **2026 solution: table formats.** Delta Lake and Apache Iceberg support `UPDATE`, `DELETE` and `MERGE`: they rewrite only the affected files and record the change in a transaction log.

```sql
MERGE INTO std.bikepoint t
USING updates u ON t.id = u.id
WHEN MATCHED THEN UPDATE SET t.bikes = u.bikes
WHEN NOT MATCHED THEN INSERT *
```

---

## 2. Data Lakes

A **centralised repository** that lets companies store **all their data at any scale**, **as-is**, without structuring it first, to make all company data available for analysis. Data lakes are the main reason the batch storage layer exists.

### 2.1 Data lakes grow in stages

```
Data ▲  all data          ┌──────────────────────── DATA OCEAN
     │  future needs      ├─────────────── DATA LAKE
     │                    ├────── DATA POND
     │  immediate need    ├─ PUDDLE
     └────────────────────┴────────────────────────► Users
        high touch (IT does it)            self-service
```

| Stage | Description |
|---|---|
| **Data puddle** | A single-project data mart built with big data tech. Well-known data; big data lowers cost or improves performance. Usually the first step. |
| **Data pond** | A collection of puddles. Data limited to what projects need; high IT cost, little democratisation or self-service. |
| **Data lake** | Adds **self-service** (users find and use data without IT) and **data relevance** (data kept even if no project needs it yet). |
| **Data ocean** | Extends self-service and data-driven decisions to **all** company data. |

> **Data swamp:** a failed data lake, with no governance, irrelevant and poor-quality data.

*Stages from Alex Gorelik, The Enterprise Big Data Lake.*

### 2.2 Design: two prerequisites

- **The right data** — the lake is a **piggy bank**: you don't know what you're saving for, so don't transform prematurely; keep data in its **native (raw) format**.
- **The right interfaces** — data scientists want raw ingredients; analysts want a cooked meal. So the lake has **multiple zones**, data at the right level for each user, and a catalog so users can **find** it.

### 2.3 Zones

```
Sources ─► LANDING ─► ┌────────────── DATA LAKE (batch storage) ──────────────┐ ─► Consumers
           (outside)  │ RAW/bronze ─► STANDARDIZED/silver ─► CURATED/gold     │   BI, ML, apps
                      │ WORK/sandbox                     🔒 SENSITIVE (GDPR)  │
                      └───────────────────────────────────────────────────────┘
```

| Zone | Also called | Contents |
|---|---|---|
| **Landing** | staging, exchange | Where incoming data is received; often outside the lake |
| **Raw** | bronze | Ingested data in its original format, as-is |
| **Standardized** | silver, trusted | Data from raw in an optimised format (Parquet, Delta, Iceberg) |
| **Curated** | gold, production, service | Clean, modeled data |
| **Work** | sandbox, development | Where users experiment |
| **Sensitive** | — | Data that must be protected (GDPR) |

Zones are **not a standard**. They're folders in a file system, buckets (or prefixes) in an object store.

#### The medallion architecture

```
Kafka, object storage, DBs ─► BRONZE (raw ingestion) ─► SILVER (filtered, cleaned, augmented) ─► GOLD (business aggregates) ─► streaming analytics, BI, AI
```

Popularised by Databricks, **bronze / silver / gold** is now the most common naming.

#### Naming conventions

| Zone | Pattern | Example (TfL bike data) |
|---|---|---|
| Raw | `/raw/<source>/<dataset>/<ingestion date>/` | `/raw/tfl/bikepoint/2026/10/15/` |
| Standardized | `/std/<business area>/<dataset>/<partition>/` | `/std/mobility/bikepoint_status/dt=2026-10-15/` |
| Curated | `/gold/<project>/<dataset>/` | `/gold/bike_ops/daily_availability_by_station/` |
| Work | `/work/projects/<project>/` · `/work/users/<user>/` | `/work/projects/rebalancing_model/` |
| Sensitive | `/sensitive/<source>/<dataset>/` | `/sensitive/crm/customers/` |

`dt=2026-10-15` is a **Hive-style partition**: engines read the value from the folder name and skip folders a query doesn't need.

#### Raw zone (bronze)

- **Original format and structure** (structured, semi-structured or unstructured)
- **Unclean**: not for direct consumption
- **Immutable**: never edited; changes arrive as new copies
- **Kept forever** (old data can move to cheaper storage tiers)
- Access limited to data engineers and technical developers

**Why keep raw data?** To **replay** (rebuild silver and gold after a bug) and to **repurpose** (answer new questions).

#### Standardized zone (silver)

- Data from raw **standardized** in a big data format (Parquet, Delta, Iceberg) for performance
- **Curated**: cleaned, sanitised, standardised and deduplicated (veracity)
- Organised by **business area**, **partitioned** by how it's consumed
- Clean and secure, so it can be **published** for ad-hoc queries via **SQL external tables and views**

```python
# raw JSON → silver Parquet, partitioned by day
from pyspark.sql import functions as F
raw = spark.read.json("/raw/tfl/bikepoint/2026/10/15/")
(raw.selectExpr("id", "commonName AS name", "lat", "lon")
    .dropDuplicates(["id"])
    .withColumn("dt", F.lit("2026-10-15"))
    .write.mode("overwrite")
    .partitionBy("dt")
    .parquet("/std/mobility/bikepoint_status/"))
```

#### Curated zone (gold)

- **Cleansed, enriched and modeled** silver data, similar to the ETL that builds a warehouse (conformed dimensions, master data)
- Unlike a warehouse, **several versions** of the same data may exist for different projects: organised by project or domain
- Usually **managed by IT**, exposed with SQL tables and views, documented in a **data catalog**

```sql
-- gold: average bikes available per station and day
CREATE TABLE gold.daily_availability_by_station AS
SELECT dt, id, name, AVG(bikes) AS avg_bikes, MIN(bikes) AS min_bikes
FROM std.bikepoint_status
GROUP BY dt, id, name;
```

#### Work zone (sandbox, development)

Where most analysis and experimentation happens, organised by user, project or subject (`/work/users/ana`, `/work/projects/customer_churn`). A private space per user plus shared project spaces. When work becomes a product, it's **promoted to gold**.

#### Sensitive zone

Data protected for regulatory (**GDPR**) or business reasons. Only data stewards and authorised people or apps get access. Best practices: keep an **encrypted copy** here and a **redacted** copy in gold; grant access explicitly and temporarily; **de-identify** (anonymise or pseudonymise) for analytics. Modern catalogs can also **mask columns** or **filter rows** per user.

#### Try it in OSBDET: one bucket per zone

```python
import boto3, requests, datetime

s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="osbdet", aws_secret_access_key="osbdet123$")

for zone in ["raw", "std", "gold", "work", "sensitive"]:
    s3.create_bucket(Bucket=zone)

# land today's snapshot in raw, as-is
now = datetime.datetime.now()
key = now.strftime("tfl/bikepoint/%Y/%m/%d/%H%M.json")
body = requests.get("https://api.tfl.gov.uk/BikePoint").content
s3.put_object(Bucket="raw", Key=key, Body=body)
print("landed", key)
```

Run it a few times and check the MinIO console (`localhost:29001`).

---

## 3. From Data Lake to Lakehouse

```
Engines: Spark · SQL · BI · ML
Catalog: tables, permissions, lineage
Table format: Delta Lake · Apache Iceberg (transaction log, ACID, time travel)
Object storage: Parquet files in raw / std / gold
```

A **lakehouse** keeps the cheap, open files of the data lake and adds **ACID transactions**, **UPDATE/DELETE/MERGE** (challenge 3), **automatic compaction** (challenge 2) and **time travel**. The zones stay the same; bronze, silver and gold just become tables.

---

## 4. Summary

- Batch storage is **file-based**, in two families: **distributed file systems** and **object / blob stores**.
- Data engineers handle its challenges by **compacting small files**, **rewriting files** (or using a table format) and **copying data to a serving layer** for low latency.
- Batch storage hosts the **data lake**, which grows in stages: puddle → pond → lake → ocean.
- Data lakes are organised in **zones**: landing, raw (bronze), standardized (silver), curated (gold), work and sensitive.

---

## Readings and Bibliography

- [AWS: What is a data lake?](https://aws.amazon.com/what-is/data-lake/)
- [Google Cloud: What is a data lake?](https://cloud.google.com/learn/what-is-a-data-lake)
- [Databricks: Medallion architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Essential guide to data lakes (Matillion)](http://pages.matillion.com/rs/992-UIW-731/images/2019C2%20-%20Data%20Lakes%20eBook.pdf)

**Alex Gorelik — *The Enterprise Big Data Lake*. 1st edition, O'Reilly, 2019.**

---
