# Modern Data Architectures for Big Data I
## Lecture 9.1 — Data Processing

*Updated edition, October 2026*

---

## Agenda

1. What is it? · 2. Batch processing · 3. Stream processing · 4. Unified processing · 5. ETL processing · 6. SQL processing · 7. Graph processing · 8. Machine learning processing · 9. Workflow managers · 10. Cloud · 11. Summary

---

## 1. What Is It?

```
Sources ─► Ingestion ─► Storage ─► ┃ PROCESSING ┃ ─► Serving
           NiFi         HDFS        ┃ Spark (batch + streaming)
           Kafka Connect MinIO      ┃ Flink (streaming)
                        Kafka       ┃ MapReduce (batch, legacy)
```

The processing layer (aka **analytics** or **prep & train** layer) **reads data from storage** and processes it to **distill insights**: `raw data → clean · join · aggregate · train → tables · dashboards · models`.

We classify processing by **data dimensions** and **analytics type**.

| | 📦 Volume → **batch processing** | ⚡ Velocity → **stream processing** |
|---|---|---|
| Data | Large amounts | Small pieces, continuously |
| Latency | High (minutes to hours) | Low (milliseconds to seconds) |
| State | Data at rest | Data in motion |
| Mode | Offline, scheduled | Online, always running |
| Example | Nightly report over 5 years of orders | Alert when a bike station is empty |

**Analytics types (a map of this lecture):** batch (MapReduce, Spark) · stream (Spark, Flink, Kafka Streams) · unified (Spark, Flink, Beam) · ETL (Spark, Pig, dbt) · SQL (Spark SQL, Trino, Hive) · graph (GraphFrames, GraphX) · machine learning (MLlib) · workflows (Airflow, Dagster, Kestra) · and AI processing.

---

## 2. Batch Processing

Processing **large groups of records (a batch)** at once rather than one by one. Runs **offline** (minutes to days), **without human interaction**, usually on a schedule, reading from the **batch data store**. *Example:* every night at 02:00 a job reads yesterday's files from the lake and writes a daily report.

### MapReduce

Hadoop's one and only built-in processing component (next lecture). Main drawbacks:

- **Rigid** programming model, unsuitable for most applications
- **Slow**: writes to disk between every step
- Full API only for **Java** (JVM) languages
- Needs **extra technologies** for each workload type (SQL, ML, streaming…)

### Apache Spark

A **fast, multi-purpose** processing engine for large-scale datasets, born as a research project at **UC Berkeley's AMPLab**. Batch, stream, ETL, SQL, graph and machine learning processing, in Python, SQL, Scala or Java. Current series: **Spark 4**.

```
┌───────────┬────────────┬────────┬────────┬────────────┐
│ Spark SQL │ Structured │ MLlib  │ GraphX │ pandas API │
│ DataFrames│ Streaming  │        │        │ on Spark   │
├───────────┴────────────┴────────┴────────┴────────────┤
│                 Apache Spark core engine              │
└───────────────────────────────────────────────────────┘
```

---

## 3. Stream Processing

The **real-time** processing of data **continuously**, **concurrently** and **event by event**.

| Category | Latency | Example |
|---|---|---|
| Real time | < 100 ms | Card fraud check |
| Near real time (NRT) | 100 ms – 2 min | Live bike map, crypto ticker |
| Micro batch | 2 – 15 min | Dashboard refresh |
| Macro batch | > 15 min | Nightly report |

**Batch vs stream:** batch waits for the whole bucket and processes it at once (throughput first); stream processes each item as it passes on a conveyor belt (latency first). **Why stream?** Sometimes it's critical to react as soon as possible: block a fraudulent payment, alert when a sensor overheats, rebalance bikes before a station empties.

| Technology | What it is | Origin | Status 2026 |
|---|---|---|---|
| Spark Structured Streaming | Stream processing written like batch | UC Berkeley / Databricks | widely used |
| Apache Flink | True event-by-event engine, very low latency | TU Berlin (Stratosphere) | widely used |
| Kafka Streams | Client library for apps whose input and output are Kafka topics | Confluent | widely used |
| Apache Storm | Early distributed processor for unbounded streams | BackType / Twitter | legacy |
| Apache Samza | Stream processing on Kafka + YARN | LinkedIn | niche |

The old "Spark Streaming" (DStreams) API is legacy: use **Structured Streaming**. See also [an overview of Apache streaming technologies](https://databaseline.bitbucket.io/an-overview-of-apache-streaming-technologies/).

---

## 4. Unified Processing

In the early days, batch and stream needed different technologies (e.g. MapReduce + Storm): two codebases for the same logic. Today **unified** engines handle bounded (batch) and unbounded (stream) data with one model.

```python
# Spark: the same transformation, batch or streaming
batch  = spark.read.json("s3a://raw-binance/")                                   # files in MinIO
stream = spark.readStream.format("kafka").option("subscribe", "binance.bitcoin")… # live events
avg_price = lambda df: df.groupBy("s").agg(F.avg("c"))                            # one piece of logic
```

| Engine | Origin | In short |
|---|---|---|
| [Apache Spark](https://spark.apache.org/) | UC Berkeley AMPLab | Fast, multi-purpose; streaming in micro-batches. Best all-rounder, the focus of this course |
| [Apache Flink](https://flink.apache.org) | TU Berlin | Stream-first; distributed, high-performing, always-available, accurate streaming apps |
| [Apache Beam](https://beam.apache.org/) | Google | A unified programming model and SDKs that run on a **runner** (Flink, Spark, Google Dataflow). Portable pipelines, not an engine |

---

## 5. ETL Processing

```
ETL:  Extract ─► Transform ─► Load      (classic BI: transform before loading)
ELT:  Extract ─► Load ─► Transform      (modern lakes and warehouses: land raw data first)
```

- ETL tools let you build pipelines in a **high-level language** instead of full programs; ETL is a key term in traditional **business intelligence**
- **Spark** is replacing most ETL technologies, or is the engine under them
- **dbt** runs the "T" as SQL models inside warehouses and lakehouses

**Apache Pig vs Spark: the same ETL**

```pig
-- Pig Latin (Yahoo!, legacy): translated into MapReduce jobs
ratings = LOAD '/datalake/raw/movielens/ratings' USING PigStorage(',') AS (user, movie, rating:float);
good    = FILTER ratings BY rating >= 4.0;
by_mv   = GROUP good BY movie;
counts  = FOREACH by_mv GENERATE group, COUNT(good);
STORE counts INTO '/datalake/std/movielens/good_ratings';
```

```python
# PySpark (today)
(spark.read.csv("/datalake/raw/movielens/ratings", header=True, inferSchema=True)
      .filter("rating >= 4.0")
      .groupBy("movieId").count()
      .write.parquet("/datalake/std/movielens/good_ratings"))
```

---

## 6. SQL Processing

Most processing technologies require programming. Many companies lack programmers but have **many people who know SQL**. SQL engines let you write a query; the engine decides how to run it across the cluster.

```sql
-- top 3 movies with at least 100 ratings (MovieLens)
SELECT m.title, ROUND(AVG(r.rating), 2) AS avg, COUNT(*) AS n
FROM ratings r JOIN movies m ON r.movieId = m.movieId
GROUP BY m.title HAVING COUNT(*) >= 100
ORDER BY avg DESC LIMIT 3;
-- Shawshank Redemption 4.43 · Godfather 4.29 · Fight Club 4.27
```

| Engine | In short | Origin |
|---|---|---|
| Spark SQL | SQL inside Spark jobs; same engine as DataFrames | Spark project |
| [Trino](https://trino.io/) | Distributed SQL for interactive queries over many sources; new name (2020) of PrestoSQL | Presto's creators |
| [Presto](https://prestodb.io/) | The original engine, now under the Linux Foundation | Facebook |
| [Apache Hive](http://hive.apache.org) | SQL data warehouse over distributed storage; high-latency, batch-style queries (Hive 4 on Tez) | Facebook |
| Apache Impala | Low-latency, massively parallel SQL over HDFS, HBase and object storage | Cloudera |
| [Apache Drill](http://drill.apache.org) | Schema-free SQL; one query can join several data stores | MapR, based on Google's Dremel |

Also worth knowing: **DuckDB** (fast SQL on one machine), **BigQuery** (Dremel as a service), **Amazon Athena** (serverless, Trino-based).

---

## 7. Graph Processing

Creating, storing and running **algorithms over distributed graphs**: structures modelling **relations between entities**.

- **Vertices** (nodes): entities, e.g. people or accounts
- **Edges** (arcs) connect vertices: **directed** ("follows") or **undirected** ("is friends with")
- Both can carry data

*Example:* Ana follows Ben and Carla; Ben follows Carla; Carla and Dan are friends; Dan follows Eva; Eva follows Ana.

| Technology | Notes |
|---|---|
| Spark **GraphX** / **GraphFrames** | Graph algorithms ready to use in Spark (RDD-based / DataFrame-based) |
| Apache Giraph | Open-source counterpart of Google's **Pregel**; retired to the Apache Attic |
| Neo4j (graph DBs) | Store and query graphs interactively (serving layer) |

```python
from graphframes import GraphFrame
v = spark.createDataFrame([("a","Ana"),("b","Ben"),("c","Carla")], ["id","name"])
e = spark.createDataFrame([("a","b"),("a","c"),("b","c")], ["src","dst"])
g = GraphFrame(v, e)
g.pageRank(resetProbability=0.15, maxIter=10).vertices.orderBy("pagerank", ascending=False).show()
```

---

## 8. Machine Learning Processing

Creating models and applying ML algorithms over **large datasets**.

- **Spark MLlib**: use the DataFrame-based `pyspark.ml`; the RDD-based API is in maintenance
- **Apache Mahout**: early scalable ML on Hadoop; its MapReduce algorithms are legacy
- Today also: XGBoost and LightGBM on Spark, **Ray** for distributed Python and deep learning

```python
# recommend movies with ALS (MovieLens)
from pyspark.ml.recommendation import ALS
ratings = spark.read.csv("/datalake/raw/movielens/ratings", header=True, inferSchema=True)
train, test = ratings.randomSplit([0.8, 0.2], seed=42)
als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(train)
model.recommendForAllUsers(3).show(3, truncate=False)   # 3 movies per user
```

---

## 9. Workflow Managers

- Most big data jobs have **several steps** to execute and coordinate
- Workflow managers (schedulers, orchestrators) are **not processing technologies**: they decide *when* others run
- Needed when a job depends on another's output, or must run at a **time** or on an **event**

```
🗓 every day at 02:00
ingest ─► clean ratings ┐
       └► clean movies  ┴─► join ─► train model
                                └─► refresh dashboard      (a DAG: directed acyclic graph of tasks)
```

| Tool | In short | Created at | Status 2026 |
|---|---|---|---|
| [Apache Airflow](https://airflow.apache.org/) | Author, schedule and monitor workflows as Python code (Airflow 3) | Airbnb | industry standard |
| [Dagster](https://dagster.io/) | Asset-centric orchestrator for ML, analytics and ETL | Elementl (now Dagster Labs) | growing |
| Kestra | Declarative YAML workflows; included in OSBDET 26r1 | Kestra | growing |
| Luigi | Python package for pipelines of batch jobs | Spotify | mature |
| Apache Oozie | Workflow scheduler for Hadoop jobs | Yahoo! | retired (Attic) |
| [Azkaban](https://azkaban.github.io/) | Batch workflow scheduler for Hadoop jobs | LinkedIn | legacy |

---

## 10. Cloud

| | Azure | AWS | Google Cloud |
|---|---|---|---|
| Managed Spark / Hadoop | HDInsight · Microsoft Fabric (Spark) · Azure Databricks | Amazon EMR (also Serverless) · AWS Glue | Dataproc (also Serverless) |
| Stream processing | Azure Stream Analytics | Managed Service for Apache Flink (formerly Kinesis Data Analytics) | Dataflow (Apache Beam) |
| SQL at scale | Fabric Warehouse · Synapse | Athena · Redshift | BigQuery |
| Workflow managers | Data Factory | MWAA (Airflow) · Step Functions | Cloud Composer (Airflow) · Workflows |

**Databricks:** a data and AI platform built around an optimised Apache Spark, available on all three clouds.

---

## 11. Summary

- **Hadoop only supports batch processing** (MapReduce).
- Many processing technologies exist for different applications: **batch, stream, ETL, SQL, graph, ML**.
- **Unified engines** (Spark, Flink, Beam) handle batch and streaming with one model; Spark covers almost every type.
- **Workflow managers** don't process data: they schedule and coordinate the jobs that do.
- We only covered some technologies, and there are many more!

| | Storage | Processing | Workflow managers |
|---|---|---|---|
| **Open source** | HDFS · MinIO · Kafka | Spark · Flink · MapReduce · Trino | Airflow · Dagster · Kestra |
| **Azure** | ADLS · Event Hubs | HDInsight · Fabric · Databricks · Stream Analytics | Data Factory |
| **AWS** | S3 · Kinesis · MSK | EMR · Glue · Managed Flink · Athena | MWAA · Step Functions |
| **Google Cloud** | Cloud Storage · Pub/Sub | Dataproc · Dataflow · BigQuery | Cloud Composer |

---
