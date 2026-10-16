# Modern Data Architectures for Big Data I
## Lecture 11.1 — Apache Spark

*Updated edition, October 2026 · Spark 4*

---

## Agenda

1. **Introduction** — Hadoop vs Spark, what is Spark, its history
2. **Spark's architecture** — driver, executors, cluster managers, SparkSession
3. **A Spark application in action** — step by step, with code
4. **Course environment** — Spark in OSBDET and your first notebook
5. **Summary**

---

## 1. Introduction

### 1.1 Hadoop MapReduce vs Spark

| | MapReduce drawbacks | Spark advantages |
|---|---|---|
| API | Only two phases (map, reduce), one abstraction (key-value), a data flow unsuitable for most apps | No forced data flow; rich abstractions (DataFrames, Datasets, SQL) |
| Languages | Java (JVM) only | Python, SQL, Scala, Java (and R) |
| Speed | Uses HDFS (disk) between every step | Keeps data **in memory**; spills to local disk only when needed |
| Workloads | Extra tools per type: Pig, Hive, Mahout, Giraph, Storm… | Bundled libraries: SQL, streaming, ML, graph |

**Same job, very different code (word count):** about 60 lines of Java in Hadoop (imports, Mapper, Reducer, driver) vs 3 lines in PySpark:

```python
lines  = spark.read.text("/user/osbdet/wordcount/input")
words  = lines.select(F.explode(F.split(F.lower("value"), "[^a-z']+")).alias("word"))
counts = words.filter("word != ''").groupBy("word").count()
counts.show()
```

Iterative work (ML, graphs) benefits most from memory: MapReduce reads and writes HDFS every iteration; Spark reads once and keeps data in memory.

### 1.2 What is Apache Spark?

An **open-source unified computing engine** and a **set of libraries** for **parallel data processing** on computer clusters. It runs anywhere, from a laptop to thousands of servers.

- **Unified engine:** SQL, streaming, ML and graph processing; faster than MapReduce thanks to in-memory computing and other optimisations, and fast on disk too
- **Set of libraries:** easy APIs in Python, SQL, Scala, Java and R
- **Parallel processing:** Spark handles distribution, node communication and recovery

**What companies use it for:** importing data via APIs or connectors · cleaning malformed data · aggregating data into warehouses and lakehouses · exploring large datasets interactively · finding hidden patterns · training ML models · processing streams · preparing data for AI and LLM applications.

### Spark's history

| Period | Milestones |
|---|---|
| 2009–2010 | Research project at UC Berkeley AMPLab; paper [*Spark: Cluster Computing with Working Sets*](https://www.usenix.org/legacy/event/hotcloud10/tech/full_papers/Zaharia.pdf) |
| < 1.0 (2009–2013) | Functional API, in-memory data sharing, batch only; Shark for SQL (2011) |
| 1.x (2014–2015) | Spark SQL; MLlib, Streaming and GraphX grow; DataFrames (1.3) |
| 2.x (2016) | ANSI SQL parser (all 99 TPC-DS queries), Structured Streaming, SparkSession |
| 3.x (2020) | Kubernetes, adaptive query execution, pandas API on Spark, Spark Connect (3.4) |
| 4.x (2025–) | ANSI SQL mode by default, VARIANT type, Python data sources, Java 17; 4.2 released July 2026 |

The 2010 paper showed MapReduce's limits: iterative jobs reloaded data from disk every iteration, and interactive analysis was too slow.

---

## 2. Spark's Architecture

**A Spark application = driver process + executor processes**, running on resources granted by a **cluster manager**, which controls the physical machines (hundreds of them in large deployments).

```
┌─ Driver process ─┐        ┌─ Executors ─┐
│  SparkSession    │  ───►  │ [ ] [ ]     │
│  User code       │        │ [ ] [ ]     │
└──────────────────┘        └─────────────┘
           Cluster manager
```

### Three layers, three cluster managers

| Application | Cluster manager | Storage | Notes |
|---|---|---|---|
| Spark | **Spark Standalone** | HDFS | ★ this course |
| Spark | Hadoop YARN | HDFS | Spark on a Hadoop cluster |
| Spark | Kubernetes | HDFS / S3 | Cloud-native, containers |
| ~~Spark~~ | ~~Mesos~~ | | Deprecated in 3.2, no longer supported in Spark 4 |

### Driver and executors

| Driver process (the heart) | Executor processes (the workers) |
|---|---|
| Keeps information about the application | Execute the code assigned by the driver, as **tasks** |
| Responds to your program and inputs | Keep data in memory (**cache**) |
| Analyses, distributes and schedules work across executors | Report the state of the computation back to the driver |
| Keeps relevant state for the app's lifetime | |

**The full picture:** the driver program (with its SparkSession) submits jobs split into stages and tasks; the cluster manager starts executors on worker nodes; each executor has a cache and runs tasks in threads.

### SparkSession

- A Spark application is controlled through a **SparkSession**, created in the driver (one app ↔ one session)
- The **unified entry point since Spark 2.0**, replacing SparkContext, SQLContext, HiveContext and StreamingContext

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("my-first-app")
         .getOrCreate())          # reuses one if it exists

print(spark.version)              # e.g. 3.5.7 in OSBDET
df = spark.range(1_000_000)       # a DataFrame of 1M rows
print(df.count())                 # 1000000
spark.stop()                      # free the cluster's resources
```

### Languages

Scala (Spark's own language) · Java · **Python** (PySpark, what we use) · SQL · R (SparkR is deprecated since Spark 4; sparklyr is the alternative). Every API keeps the same core concepts. From Python, you don't write JVM code: PySpark translates your DataFrame operations into instructions for the JVM, which plans the job and sends tasks to executor JVMs. With DataFrames, Python is about as fast as Scala.

### Spark Connect (Spark 3.4+ and 4)

A thin client (notebook, IDE or app; `pyspark-client` is about 1.5 MB) sends **query plans** over gRPC to a remote **Spark Connect server**, which runs the driver and returns results as Arrow batches. Use Spark without installing it locally; upgrade servers without breaking clients; a misbehaving client can't crash the driver.

```python
spark = SparkSession.builder.remote("sc://host:15002").getOrCreate()
```

---

## 3. A Spark Application in Action

**Question:** in a table with millions of customer rows, how many customers use Yahoo for email? (Example with 10 rows and 2 machines.)

| last_name | phone | email |
|---|---|---|
| Burks | (916) 342-8003 | burks@yahoo.com |
| Todd | (917) 234-5004 | todd@yahoo.com |
| Fisher | (917) 234-22234 | tameka.fisher@aol.com |
| Spence | (912) 234-0001 | daryl.spence@aol.com |
| Rice | NULL | crcise@msn.com |
| Bean | NULL | lbean@hotmail.com |
| Hays | (917) 234-5004 | bobhays@hotmail.com |
| Duncan | NULL | duncan@yahoo.com |
| Baldwin | (912) 234-0001 | dbaldwin@msn.com |
| Newmann | NULL | pnewmann@gmail.com |

1. **Read** — the driver asks executors (machines 1 and 2) to load 5 rows each into memory
2. **withColumn** — each executor adds a `provider` column (the domain after "@")
3. **count** — each counts yahoo.com locally: 2 on machine 1, 1 on machine 2
4. **Combine** — executors send local counts to the driver: 2 + 1 = **3**

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("yahoo").getOrCreate()

customers = spark.createDataFrame([
    ("Burks", "burks@yahoo.com"), ("Todd", "todd@yahoo.com"),
    ("Fisher", "tameka.fisher@aol.com"), ("Spence", "daryl.spence@aol.com"),
    ("Rice", "crcise@msn.com"), ("Bean", "lbean@hotmail.com"),
    ("Hays", "bobhays@hotmail.com"), ("Duncan", "duncan@yahoo.com"),
    ("Baldwin", "dbaldwin@msn.com"), ("Newmann", "pnewmann@gmail.com"),
], ["last_name", "email"]).repartition(2)          # ① 2 partitions

with_provider = customers.withColumn(              # ② new column
    "provider", F.split("email", "@").getItem(1))

print(with_provider.filter("provider = 'yahoo.com'").count())  # ③④ → 3
```

`withColumn` and `filter` are **transformations** (lazy, only recorded); `count()` is an **action** that makes the driver send tasks.

### It's all about libraries and the API

- **High-level (structured) APIs:** DataFrames, Datasets and SQL. Use these: Spark optimises them
- **Low-level APIs:** RDDs and distributed variables, for legacy apps or total control

```
Structured Streaming | Advanced analytics | Libraries & ecosystem
Structured APIs: Datasets · DataFrames · SQL
Low-level APIs: RDDs · Distributed variables
```

---

## 4. Course Environment

```
Sources ─► NiFi ─► HDFS / Kafka ─► Spark (Spark SQL · Structured Streaming · MLlib) ◄─ Jupyter + Python
```

### Your first Spark notebook in OSBDET

```python
# Turn Hadoop and Spark on in localhost:2026, then in Jupyter:
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("movielens").getOrCreate()

# the files you ingested with NiFi in lab 8.1
movies = spark.read.csv("hdfs:///datalake/raw/movielens/movies", header=True, inferSchema=True)
movies.printSchema()
movies.show(3, truncate=False)

# how many movies per genre?
(movies.select(F.explode(F.split("genres", "\\|")).alias("genre"))
       .groupBy("genre").count()
       .orderBy(F.desc("count"))
       .show(5))
```

```
Drama 4361 · Comedy 3756 · Thriller 1894 · Action 1828 · Romance 1596
```

While it runs, open the **Spark UI** at `localhost:24040`. OSBDET ships Spark 3.5; everything here also works on Spark 4.

---

## 5. Summary

- Apache Spark is an open-source **unified processing** technology for big data: many processing types in **one API**, with clear advantages over Hadoop MapReduce.
- Spark relies on **cluster managers** (Standalone, YARN, Kubernetes) for memory and CPUs to run code in **executors**.
- A Spark application = **driver** (controls the lifecycle, asks for resources, schedules tasks) + **executors** (run the tasks).
- The **SparkSession** is the first thing you create in any Spark application.

[spark.apache.org](https://spark.apache.org/)

---
