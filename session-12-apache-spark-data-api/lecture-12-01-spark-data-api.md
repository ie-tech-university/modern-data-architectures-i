# Modern Data Architectures for Big Data I
## Lecture 12.1 — Apache Spark Data API

*Updated edition, November 2026. Spark 3.5 in OSBDET; Spark 4 notes included.*

All examples use the **MovieLens** files you ingested into HDFS in lab 8.1 (`hdfs://localhost:9000/datalake/raw/movielens/{movies,ratings}`): **100,836 ratings**, **9,742 movies**, **610 users**. Every output shown below is real: it was produced by running the code on that data with PySpark 3.5.7 (and 4.1.1 for the Spark 4 section).

---

## Agenda

1. **Spark APIs**: what an API is, building blocks, entry points, functional style
2. **Core concepts**: partitions, transformations (lazy) and actions (eager)
3. **Low-level API**: RDDs and distributed shared variables
4. **High-level API**: data types, DataFrames, Datasets and Spark SQL
5. **RDDs vs DataFrames**: why DataFrames win, and what's new in Spark 4
6. **Summary**, plus three exercises on MovieLens

---

## 1. Spark APIs

### What is an API?

**A**pplication **P**rogramming **I**nterface: a software interface that lets two applications or technologies **talk to each other**. In practice it is one of two things:

| | How you use it | Spark example |
|---|---|---|
| **A library or module** | Import it in your language and call its functions | `from pyspark.sql import SparkSession` (**how we use Spark**) |
| **A remote service (REST API)** | Send HTTP requests, get JSON back | Spark's monitoring API: `GET http://localhost:24040/api/v1/applications` |

```
Your code (notebook · script · app)  →  PySpark API (DataFrames · SQL · RDDs)  →  Spark engine (driver + executors)
```

### Building blocks: low-level vs high-level

Spark APIs give us **data structures** and **functionality** to process data **independently of its location and format**. They come in two layers:

```
┌──────────────────────────────────────────────────────────────┐
│ Structured Streaming │ Advanced analytics │ Libraries & ecosystem │
├──────────────────────────────────────────────────────────────┤
│ STRUCTURED (HIGH-LEVEL) APIs: Datasets · DataFrames · SQL    │  entry point: SparkSession
├──────────────────────────────────────────────────────────────┤
│ LOW-LEVEL APIs: RDDs · Distributed variables                 │  entry point: SparkContext
└──────────────────────────────────────────────────────────────┘
```

- **High-level (Structured) APIs**: DataFrames, Datasets, SQL. What you'll use 95% of the time.
- **Low-level APIs**: RDDs and distributed shared variables.
- **High-level APIs rely on low-level APIs**: every DataFrame operation finally runs as RDD operations.

### Two entry points, one inside the other

```python
from pyspark.sql import SparkSession

# one session per application (since Spark 2.0)
spark = (SparkSession.builder
         .appName("data-api")
         .getOrCreate())

# you don't create a SparkContext: it lives inside the session
sc = spark.sparkContext
print(sc.defaultParallelism)   # 4  (cores available)
```

| Entry point | Level | Main methods |
|---|---|---|
| `SparkSession` (`spark`) | high | `spark.read`, `spark.sql()`, `spark.createDataFrame()`, `spark.table()`, `spark.catalog` |
| `SparkContext` (`spark.sparkContext`) | low | `textFile()`, `parallelize()`, `broadcast()`, `accumulator()` |

Older notebooks call `findspark.init()` and build a `SparkContext` or `SQLContext` by hand. Today, **always start from `SparkSession`**.

### Spark and functional programming

Spark is written in **Scala**, a functional language on the **JVM** where **functions are first-class citizens**: you can pass them around like any other value. Spark uses this paradigm to run transformations on datasets held in the **cluster's memory**. Python is **multi-paradigm**, so PySpark (Python + Spark) can use the same style.

```python
# 1. functions are values: pass one to Spark
def is_good(rating):
    return rating >= 4.0

ratings_rdd.filter(is_good)

# 2. ...or an anonymous function (lambda)
ratings_rdd.map(lambda r: r * 2)

# 3. data is never modified: each step returns a NEW dataset
good = ratings_df.filter(F.col("rating") >= 4)
```

Spark ships your function to the executors and runs it on every record, in parallel.

---

## 2. Core Concepts

Three ideas apply to **every** Spark API (RDDs, DataFrames and SQL):

| Idea | In one sentence |
|---|---|
| **Partitions** | Data is split into chunks across the cluster; each executor core works on one partition at a time. |
| **Transformations** (lazy) | Describe *what* to do; Spark only records them in a plan. |
| **Actions** (eager) | Run the plan and return or save a result. |

### 2.1 Partitions

Massive parallel processing is possible because **data is partitioned** and each executor works on **a single partition at a time** for each task.

```
ratings (DataFrame, 100,836 rows) → repartition(4)

 Partition 0 · 25,209 rows ──► Worker node 1 · Executor · core 1 → task 0
 Partition 1 · 25,209 rows ──► Worker node 1 · Executor · core 2 → task 1
 Partition 2 · 25,209 rows ──► Worker node 2 · Executor · core 1 → task 2
 Partition 3 · 25,209 rows ──► Worker node 2 · Executor · core 2 → task 3
```

- **Partition**: a chunk of data physically distributed across the nodes of a cluster. With DataFrames, a chunk is a subset of the rows.
- **Parallelism**: the balance between the **number of partitions** and the **number of cores** (executors × cores per executor).

```python
ratings = spark.read.csv("hdfs://localhost:9000/datalake/raw/movielens/ratings",
                         header=True, inferSchema=True)

ratings.rdd.getNumPartitions()        # 1 (one 2.4 MB file)

r4 = ratings.repartition(4)           # shuffle into 4 parts
r4.rdd.getNumPartitions()             # 4
r4.rdd.glom().map(len).collect()      # [25209, 25209, 25209, 25209] rows per partition

r4.coalesce(1).rdd.getNumPartitions() # 1 (merge, no shuffle)

spark.conf.get("spark.sql.shuffle.partitions")   # '200'
```

- **Too few** partitions → idle cores, huge tasks, out-of-memory errors.
- **Too many** → thousands of tiny tasks; scheduling overhead wins.
- Rule of thumb: **2–4 partitions per core**, roughly **100–200 MB** each. Files are split at **128 MB** by default (`spark.sql.files.maxPartitionBytes`).
- After a shuffle Spark uses **200** partitions, but **Adaptive Query Execution** (on by default since Spark 3.2) merges the small ones for you.

### Two types of operations

| | Transformations | Actions |
|---|---|---|
| Evaluation | **Lazy** | **Eager** |
| What they do | Return a **new dataset**; add a step to the plan. The **T in ETL**: where business logic lives | Ask for a **result**: Spark optimises the plan and runs **jobs → stages → tasks** |
| Examples | `select`, `filter/where`, `withColumn`, `groupBy`, `join`, `orderBy`, `map`, `flatMap` | `show`, `count`, `collect`, `take`, `first`, `foreach`, `reduce`, `write…` |

Three facts about transformations: **① data structures are immutable · ② transformations are lazily evaluated · ③ transformations are triggered by actions.**

### 2.2 Transformations

**Data structures are immutable.** Every transformation on an RDD or DataFrame returns a **new** one:

```python
good = ratings.filter(F.col("rating") >= 4.0)

ratings.count()   # 100836  ← the original is untouched
good.count()      # 48580
```

> **Common mistake:** `ratings.filter(...)` on its own line does nothing useful. Write `good = ratings.filter(...)`.

**Transformations are lazily evaluated.** Spark keeps track of the chain of transformations and only computes it when an **action** requires a result. Measured on MovieLens (local mode, 4 cores):

| Line | Time |
|---|---|
| `good = ratings.filter(...)` (transformation: just a plan) | 0.03 s |
| `good.count()` (action: real work) | 0.50 s |

> **Think of a restaurant order.** The waiter writes down starter, main and dessert (transformations). The kitchen only starts cooking when you say *"that's all"* (the action). Because Spark sees the **whole order first**, it can optimise it: drop unused columns, filter early, skip work nobody asked for.

**Narrow vs wide transformations**, based on how many partitions each output partition depends on:

```
NARROW (1 → 1)                         WIDE / SHUFFLE (1 → N)
filter · select · withColumn · map     groupBy · join · orderBy · distinct

P0 ──► P0'                             P0 ─┬─► movieId A
P1 ──► P1'                             P1 ─┼─► movieId B
P2 ──► P2'                             P2 ─┴─► movieId C   (every input feeds every output)

✓ no data movement, same stage         ⇄ data crosses the network, new stage
```

**See it yourself with `explain()`** (read plans bottom-up; real output, trimmed):

```python
ratings.filter("rating >= 4").select("movieId", "rating").explain()
```
```
== Physical Plan ==
*(1) Filter (isnotnull(rating) AND (rating >= 4.0))
+- FileScan csv [movieId,rating]
     PushedFilters: [IsNotNull(rating), GreaterThanOrEqual(rating,4.0)]
```
One stage, no `Exchange`. Bonus: the filter is **pushed** into the file scan.

```python
ratings.groupBy("movieId").agg(F.avg("rating")).explain()
```
```
== Physical Plan ==
AdaptiveSparkPlan
+- HashAggregate(keys=[movieId], avg(rating))
   +- Exchange hashpartitioning(movieId, 200)
      +- HashAggregate(keys=[movieId], partial_avg(rating))
         +- FileScan csv [movieId,rating]
```
`Exchange` = **shuffle**. Spark first pre-aggregates inside each partition (`partial_avg`) so it moves less data.

### 2.3 Actions

Transformations only build up the **logical plan**; the real work happens when an **action** is triggered. Three kinds of actions:

| Kind | Examples |
|---|---|
| **View data in the console** | `show(5)` |
| **Collect data to the driver** (Python values or lists of rows) | `count()`, `first()`, `take(n)`, `collect()`, `reduce()` |
| **Write to storage** | `df.write.parquet(…)`, `.saveAsTable(…)`, `rdd.saveAsTextFile(…)` |

> **Each action launches a job.** Watch them in the Spark UI at `localhost:24040` → Jobs. Two actions on the same DataFrame = the plan runs twice (unless you cache it).

### Partitions, transformations and actions together

```
ratings.csv ─read─► DataFrame ─groupBy─► GroupedData ─agg(avg)─► DataFrame ─withColumnRenamed─► DataFrame
                                └──── one operation ────┘                                          │ orderBy(desc)
                                                                                                    ▼
                         🖥 console ◄══ show() ══ DataFrame ◄── limit(5) ────────────────────── DataFrame
                                       (ACTION)
  ── transformations (lazy)     ══ action (eager)
```

```python
from pyspark.sql import functions as F

base    = "hdfs://localhost:9000/datalake/raw/movielens"
opts    = dict(header=True, inferSchema=True)
ratings = spark.read.csv(f"{base}/ratings", **opts)
movies  = spark.read.csv(f"{base}/movies", **opts)

top = (ratings
       .groupBy("movieId")              # transformation
       .agg(F.count("*").alias("n"),
            F.round(F.avg("rating"), 2).alias("avg"))
       .filter("n >= 50")               # popular only
       .join(movies, "movieId")         # add the title
       .orderBy(F.desc("avg"), F.desc("n"))
       .select("title", "n", "avg"))
# …still nothing has run
top.show(5, truncate=False)             # ← ACTION
```
```
+---------------------------------------------------------------------------+---+----+
|title                                                                      |n  |avg |
+---------------------------------------------------------------------------+---+----+
|Shawshank Redemption, The (1994)                                           |317|4.43|
|Godfather, The (1972)                                                      |192|4.29|
|Fight Club (1999)                                                          |218|4.27|
|Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1964)|97 |4.27|
|Cool Hand Luke (1967)                                                      |57 |4.27|
+---------------------------------------------------------------------------+---+----+
```

### Best practices: shrink the data early

1. Apply **filtering** transformations first (`filter`, `where`).
2. Apply **select** transformations: keep only the columns you need (`select`, `drop`).
3. Apply **narrow** transformations (`withColumn`, `cast`).
4. Apply **wide** transformations (shuffles: `groupBy`, `join`, `orderBy`) on as little data as possible.
5. **Cache** only if you're going to use the DataFrame **more than once**.

Also: prefer **built-in functions** (`pyspark.sql.functions`) over Python UDFs, and never `collect()` a big dataset.

**Good news: Spark's Catalyst optimizer already does some of this for you.** Below, the filter is written *after* the join and no columns are chosen:

```python
q = (ratings.join(movies, "movieId")
            .filter(F.col("rating") == 5.0)
            .select("title"))
q.explain(mode="formatted")
```
```
Project [title]
+- BroadcastHashJoin [movieId]
   :- Project [movieId]
   :  +- Filter (rating = 5.0)          ← moved below the join
   :     +- Scan csv ratings   ReadSchema: movieId, rating
   +- BroadcastExchange
      +- Scan csv movies       ReadSchema: movieId, title
```

- **Predicate pushdown**: the filter runs before the join.
- **Column pruning**: `userId`, `timestamp` and `genres` are never read.
- **Broadcast join**: `movies` is small, so it's copied to every executor; no shuffle.

This works only with DataFrames and SQL. With RDDs, Spark can't look inside your Python functions, so **you** are the optimizer.

### Caching: reuse instead of recompute

```python
joined = ratings.join(movies, "movieId").cache()   # lazy too!

joined.count()        # 100836 ← 1st action: compute + store
joined.is_cached      # True
joined.storageLevel   # Disk Memory Deserialized 1x Replicated

# 2nd, 3rd… actions read from memory, not from HDFS
joined.groupBy("genres").count().show(3)
joined.filter("rating = 5").count()

joined.unpersist()    # free the memory when you're done
```

- `cache()` is a **transformation**: nothing is stored until the first action.
- For DataFrames it means `MEMORY_AND_DISK`; use `persist(StorageLevel…)` to choose another level.
- See cached data in the Spark UI → **Storage** tab.
- **Don't cache everything**: memory is shared with execution.

---

## 3. Low-Level API

Structured APIs rely on the low-level APIs, so these are Spark's **fundamental primitives**. You won't normally use them unless:

- You need **functionality not available** in the high-level APIs (very custom partitioning, raw unstructured records…).
- You **maintain legacy code** written with RDDs.
- You want to understand what Spark does under the hood.

> **⚠ 2026 reality check:** the RDD API and `SparkContext` are **not available on Spark Connect**, and therefore not on Databricks serverless compute or Databricks Free Edition. They work in OSBDET, which runs classic Spark.

### RDD: Resilient Distributed Dataset

The **only data abstraction available in Spark 1.x**: an **immutable, partitioned collection of records** that can be operated on in parallel.

- **Resilient**: Spark remembers the **lineage** (the steps that built each partition) and recomputes a lost partition from it.
- **Distributed**: partitions live on many executors.
- **Dataset**: each **record** is a plain **Python object** chosen by the programmer (string, tuple, dict…). **No rows, no columns, no schema.**

```
textFile        filter          map
  P0 ──────────► P0 ──────────► P0
  P1 ──────────► P1 ─ ─ ─ ─ ─ ► ✗   ↻ executor lost P1? recompute it from the lineage
  P2 ──────────► P2 ──────────► P2
```

RDDs provide **transformations**, lazily evaluated when **actions** are performed, to manipulate data in a distributed way. But individual records are raw Python objects, manipulated **manually** rather than through higher-level functions. **`SparkContext`** is the entry point.

### Records are raw Python objects

```python
sc = spark.sparkContext
lines = sc.textFile("hdfs://localhost:9000/datalake/raw/movielens/movies")

lines.take(3)
# ['movieId,title,genres',
#  '1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy',
#  '2,Jumanji (1995),Adventure|Children|Fantasy']

header = lines.first()
rows   = lines.filter(lambda l: l != header)
rows.count()                          # 9742

# naive parsing: split each line on commas
bad = rows.map(lambda l: l.split(",")) \
          .filter(lambda r: len(r) != 3)
bad.count()                           # 2079 😱
bad.first()
# ['11', '"American President', ' The (1995)"', 'Comedy|Drama|Romance']
```

**2,079 of the 9,742 titles contain commas.** With RDDs you parse every record yourself; `l.rsplit(",", 1)` safely separates the last field (genres). The DataFrame CSV reader understands quotes and gives you typed columns for free.

### RDD transformations

| Transformation | What it does | Narrow / wide |
|---|---|---|
| `map(f)` | Maps each record to something else through a function (1 → 1) | narrow |
| `filter(f)` | Selects the records where the function returns `True` | narrow |
| `flatMap(f)` | Maps one record to multiple records, then flattens (1 → N) | narrow |
| `sample(False, 0.1)` | Samples a fraction of the data | narrow |
| `distinct()` | Returns the distinct records | wide |
| `reduceByKey(f)` | Combines the values of each key in `(key, value)` pairs | wide |
| `sortBy(f)` | Sorts the records by the value `f` returns | wide |

```
map      'Comedy|Romance' → ['Comedy', 'Romance']        (1 → 1)
flatMap  'Comedy|Romance' → 'Comedy', 'Romance'          (1 → N)
filter   4.0, 2.5, 5.0    → 4.0, 5.0   (rating >= 4)
```

### RDDs in action: movies per genre

```python
genres = (rows
  .map(lambda l: l.rsplit(",", 1)[1].strip())   # last field
  .flatMap(lambda g: g.split("|")))             # 1 → N

top5 = (genres
  .map(lambda g: (g, 1))                        # (key, value)
  .reduceByKey(lambda a, b: a + b)              # shuffle!
  .sortBy(lambda kv: -kv[1])
  .take(5))                                     # ACTION

print(top5)
print(genres.distinct().count())
```
```
[('Drama', 4361), ('Comedy', 3756), ('Thriller', 1894), ('Action', 1828), ('Romance', 1596)]
20
```

Same numbers as the DataFrame version in lecture 11.1, and the same **map → shuffle → reduce** pattern as the MapReduce lab, in a few lines. Note the `(key, value)` tuples: you manage the structure by hand.

### RDD actions

| Action | Returns | On MovieLens |
|---|---|---|
| `first()` | The first record | `'movieId,title,genres'` |
| `count()` | The number of records | `9742` |
| `take(n)` | A list with the first *n* records | `['1,Toy Story (1995),…', …]` |
| `collect()` | A list with **all** records, in the driver | ⚠ very expensive and problematic |
| `reduce(f)` | Aggregates all records with a function | sum of ratings: `353083.0` |
| `foreach(f)` | Runs a function on each record (on the executors) | used with accumulators |

> **`collect()` brings everything to the driver.** 100,000 rows is fine; 10 billion will crash it. Use `take(n)`, or write the result to storage.

### Distributed shared variables

| | Broadcast variable | Accumulator |
|---|---|---|
| Direction | Driver → executors | Tasks → driver |
| Purpose | Save a **large read-only value** on every worker and reuse it across tasks and actions without re-sending it | **Consolidate** data from all tasks into a shared result (e.g. a global counter) |
| Who reads | Every task (`bt.value`) | Only the driver (`acc.value`) |

```python
# Accumulator: count movies with no genres while processing
no_genres = sc.accumulator(0)

def check(line):
    if line.strip().endswith("(no genres listed)"):
        no_genres.add(1)

rows.foreach(check)        # action, runs on the executors
print(no_genres.value)     # 34
```

```python
# Broadcast: a {movieId: title} lookup table on every node
def parse_title(line):
    movie_id, rest = line.split(",", 1)
    title = rest.rsplit(",", 1)[0].strip('"')
    return int(movie_id), title

titles = dict(rows.map(parse_title).collect())   # 9,742 entries: small
bt = sc.broadcast(titles)

ratings_rdd = (sc.textFile("hdfs://localhost:9000/datalake/raw/movielens/ratings")
                 .filter(lambda l: not l.startswith("userId"))
                 .map(lambda l: l.split(",")))

ratings_rdd.map(lambda x: (bt.value[int(x[1])], float(x[2]))).take(3)
# [('Toy Story (1995)', 4.0), ('Grumpier Old Men (1995)', 4.0), ('Heat (1995)', 4.0)]
```

With DataFrames you rarely need broadcast variables: Spark **broadcasts small tables in joins automatically** (under 10 MB by default, `spark.sql.autoBroadcastJoinThreshold`), or you can force it with `F.broadcast(movies)`.

---

## 4. High-Level API

### Structured APIs: three doors, one engine

```
SQL ───────┐                ┌───────────────── Catalyst optimizer ──────────────────┐
DataFrame ─┼─► Unresolved ─analysis (Catalog)─► Logical ─optimise─► Optimized ─plan─► Physical plans
Dataset ───┘   logical plan                      plan                logical plan        │ cost model
(Scala/Java)                                                                             ▼
                 Tasks on executors ◄── RDDs ◄── Tungsten code generation ◄── Selected physical plan
```

SQL and DataFrames compile to **the same plan**, so they are **equally fast**. Pick whichever reads better, and mix them freely.

### Spark data types

Structured APIs know the **type** of every column (numbers, text, dates…). Spark keeps this type information **internally** and maps it to the types of each language API. Most manipulations operate on **Spark types**, not Python types. All of them are in `pyspark.sql.types`.

| Spark type | SQL name | Python value | Notes |
|---|---|---|---|
| ByteType · ShortType | `TINYINT` · `SMALLINT` | `int` | −128…127 · −32,768…32,767 |
| IntegerType · LongType | `INT` · `BIGINT` | `int` | 32-bit · 64-bit |
| FloatType · DoubleType | `FLOAT` · `DOUBLE` | `float` | approximate |
| DecimalType(p, s) | `DECIMAL(10,2)` | `decimal.Decimal` | exact: use for money |
| StringType | `STRING` | `str` | |
| BooleanType | `BOOLEAN` | `bool` | |
| DateType · TimestampType | `DATE` · `TIMESTAMP` | `date` · `datetime` | timestamps use the session time zone |
| ArrayType · MapType | `ARRAY<…>` · `MAP<…>` | `list` · `dict` | e.g. a list of genres |
| StructType | `STRUCT<…>` | `Row` | nested record; also used as a schema |
| **VariantType** | `VARIANT` | `VariantVal` | **Spark 4**: fast semi-structured (JSON) data |

### DataFrames

The most common Structured API: a **table of data with rows and columns**, potentially spanning **thousands of computers**.

- **Schema**: the definition of the columns and their data types.
- Same concept as DataFrames in **pandas** (Python) or **R**, but those live on **one machine**; a Spark DataFrame is **partitioned across a cluster**.

```
pandas / R                     Spark DataFrame
one machine, limited by RAM    userId · movieId · rating
┌──────────┐                   ├ rows 1 – 33,612        ──► node 1
│ ▦▦▦▦▦▦▦▦ │                   ├ rows 33,613 – 67,224   ──► node 2
└──────────┘                   └ rows 67,225 – 100,836  ──► node 3
```

Remember: when you call a DataFrame transformation, it is actually a set of **RDD transformations** under the hood.

### Schemas: let Spark guess, or tell it

```python
from pyspark.sql.types import (StructType, StructField,
                               IntegerType, DoubleType, LongType)

# Option 1 · explicit StructType
schema = StructType([
    StructField("userId",    IntegerType()),
    StructField("movieId",   IntegerType()),
    StructField("rating",    DoubleType()),
    StructField("timestamp", LongType())])

# Option 2 · the same schema as a DDL string
schema = "userId INT, movieId INT, rating DOUBLE, timestamp LONG"

ratings = spark.read.csv(path, header=True, schema=schema)
ratings.printSchema()
```
```
root
 |-- userId: integer (nullable = true)
 |-- movieId: integer (nullable = true)
 |-- rating: double (nullable = true)
 |-- timestamp: long (nullable = true)
```

- `inferSchema=True`: easy, but Spark reads the data **an extra time** to guess, and may guess wrong.
- **Explicit schema**: faster and safer. Use it in production pipelines.

### pandas ⇄ Spark

pandas DataFrames and Spark DataFrames can be converted into each other:

```python
# Spark → pandas: the result must fit in the DRIVER's memory
pdf = top.limit(3).toPandas()
print(pdf)
#                               title    n   avg
# 0  Shawshank Redemption, The (1994)  317  4.43
# 1             Godfather, The (1972)  192  4.29
# 2                 Fight Club (1999)  218  4.27

# pandas → Spark
sdf = spark.createDataFrame(pdf)

# pandas syntax, Spark engine (pandas API on Spark, since Spark 3.2)
import pyspark.pandas as ps
psdf = ps.read_csv("hdfs://localhost:9000/datalake/raw/movielens/ratings")
psdf["rating"].mean()      # 3.501556983616962
```

> ⚠ **In OSBDET the pandas API on Spark fails:** with Spark 3.5.7 and NumPy 2, `import pyspark.pandas` raises ``AttributeError: `np.NaN` was removed in the NumPy 2.0 release``. It works on Spark 4; the rest of the cell works in OSBDET.

| pandas | Spark DataFrame |
|---|---|
| One machine, in memory | Many machines, partitions |
| Eager: runs line by line | Lazy: runs on an action |
| Mutable: `df["x"] = …` | Immutable: `withColumn` returns a new one |
| Has a row index | No index, no guaranteed order |
| Great up to a few GB | Built for GB → PB |

(R data frames convert in the same way with SparkR or sparklyr. SparkR is deprecated since Spark 4.)

### Creating DataFrames: the Data Source API

DataFrames can be created by hand, but the most common way is the **Data Source API**, when data comes from external sources. The Data Source API is **not available for RDDs**. We'll cover it in depth in future sessions.

```python
movies = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv("hdfs://localhost:9000/datalake/raw/movielens/movies"))
movies.show(3, truncate=False)

# other formats, same API
spark.read.json(…) · spark.read.parquet(…) · spark.read.jdbc(…)

# or by hand, for tests and small lookups
spark.createDataFrame([(1, "Toy Story")], "movieId INT, title STRING")
```
```
+-------+-----------------------+-------------------------------------------+
|movieId|title                  |genres                                     |
+-------+-----------------------+-------------------------------------------+
|1      |Toy Story (1995)       |Adventure|Animation|Children|Comedy|Fantasy|
|2      |Jumanji (1995)         |Adventure|Children|Fantasy                 |
|3      |Grumpier Old Men (1995)|Comedy|Romance                             |
+-------+-----------------------+-------------------------------------------+
```

The quoted titles with commas are parsed correctly.

### DataFrame transformations

| Transformation | What it does | Example on MovieLens |
|---|---|---|
| `select` | Choose or compute columns | `ratings.select("movieId", "rating")` |
| `withColumn` | Add or replace a column | `.withColumn("year", F.year("rated_at"))` |
| `filter / where` | Keep rows matching a condition | `.filter(F.col("rating") >= 4)` |
| `distinct` | Unique values | `ratings.select("rating").distinct()` → 10 values (0.5 … 5.0) |
| `sample` | A random fraction of rows | `ratings.sample(fraction=0.01, seed=42)` → 1,055 rows |
| `sort / orderBy` | Sort the rows | `.orderBy(F.desc("avg"))` |
| `groupBy + agg` | Aggregate per group | `.groupBy("movieId").agg(F.avg("rating"))` |
| `join` | Combine two DataFrames | `ratings.join(movies, "movieId")` |
| `withColumnRenamed · drop` | Rename or remove columns | `.withColumnRenamed("avg(rating)", "avg")` |

### Transformations in action: when do people rate?

```python
r2 = (ratings
      .withColumn("rated_at", F.to_timestamp(F.from_unixtime("timestamp")))  # epoch → timestamp
      .withColumn("year", F.year("rated_at")))

r2.select("userId", "movieId", "rating", "rated_at", "year").show(3)

(r2.groupBy("year").count()
   .orderBy(F.desc("count"))
   .show(3))
```
```
+------+-------+------+-------------------+----+
|userId|movieId|rating|           rated_at|year|
+------+-------+------+-------------------+----+
|     1|      1|   4.0|2000-07-30 20:45:03|2000|
|     1|      3|   4.0|2000-07-30 20:20:47|2000|
|     1|      6|   4.0|2000-07-30 20:37:04|2000|
+------+-------+------+-------------------+----+

+----+-----+
|year|count|
+----+-----+
|2000|10061|
|2017| 8198|
|2007| 7114|
+----+-----+
```
Times are shown in the session time zone, here Europe/Madrid (UTC+2 in July): `964982703` is 18:45:03 UTC. Set `spark.sql.session.timeZone` (e.g. `"UTC"`) to control it.

### DataFrame actions

| Action | Returns |
|---|---|
| `show(n)` | Prints the first *n* rows to the console (default 20) |
| `count()` | Number of rows |
| `first()` | The first `Row` |
| `take(n)` | A list with the first *n* rows |
| `collect()` | A list with **all** rows, in the driver (very expensive) |
| `foreach(f)` | Runs a function on each row |
| `toPandas()` | A pandas DataFrame, in the driver |
| `write.…` | Saves to storage (Parquet, tables…) |

```python
movies.count()     # 9742
movies.first()
# Row(movieId=1, title='Toy Story (1995)', genres='Adventure|Animation|Children|Comedy|Fantasy')

row = movies.first()
row.title          # 'Toy Story (1995)'
row["genres"]      # access by name or position

(top.write.mode("overwrite")
    .parquet("hdfs://localhost:9000/datalake/curated/top_movies"))
# _SUCCESS  part-00000-….snappy.parquet
```

Unlike RDD records, DataFrame results come back as `Row` objects with named fields.

### DataFrames vs Datasets

Within the Structured APIs we differentiate between **DataFrames** and **Datasets**. The difference is **when Spark checks data types**:

- **DataFrames**: at **runtime**. A type mismatch only shows up when the code runs.
- **Datasets**: at **compile time**. The compiler warns you about a type mismatch **before** you execute anything.

**Datasets exist only in the Scala and Java APIs**, which have a compiler and static types. They are not available in PySpark, so we won't use them in this course. In Scala, a DataFrame is simply a `Dataset[Row]`.

```scala
// Scala, for reference only
case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Long)

val ds = spark.read.option("header", "true")
  .schema("userId INT, movieId INT, rating DOUBLE, timestamp LONG")
  .csv(path).as[Rating]

ds.filter(r => r.rating >= 4.0)       // ✓ compiles
ds.filter(r => r.rating == "high")    // ✗ compile error: Double vs String
```

### So, when does PySpark tell you something is wrong?

A more precise picture than "only at the action":

| Error | When you see it | Real output |
|---|---|---|
| **Wrong column name** `ratings.select("ratin")` | **Right away**, when you define the transformation: Spark analyses the plan as you build it (on Spark Connect, this waits until the action) | `AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name 'ratin' cannot be resolved. Did you mean one of the following? ['rating', 'movieId', …]` |
| **Bad data in a column** `F.col("r").cast("double")` on the value `"five"` | **Only at the action** (`show()`), when the data is actually processed | Spark 4 (ANSI mode): `[CAST_INVALID_INPUT] The value 'five' of the type "STRING" cannot be cast to "DOUBLE" because it is malformed… Use 'try_cast' to tolerate malformed input and return NULL instead.` Spark 3.5: silently `NULL` |

```python
bad = spark.createDataFrame([("4.5",), ("five",)], ["r"])
x = bad.select(F.col("r").cast("double"))   # fine…
x.show()                                    # 💥 now (Spark 4)
```

### Spark SQL

Spark SQL integrates directly into the DataFrame (and Dataset) API, so you can express data manipulations in **SQL**. Two ways to use it:

- **Spark SQL CLI**: queries from the command line, with the `spark-sql` command.
- **Structured APIs**: the `sql()` method of the `SparkSession` object.

**Spark SQL CLI** (real session; in OSBDET open a terminal with Spark started):

```
$ spark-sql
Spark Web UI available at http://localhost:4040
Spark master: local[*], Application Id: local-17940…
spark-sql (default)> CREATE TEMPORARY VIEW ratings
                   > USING csv
                   > OPTIONS (path '/datalake/raw/movielens/ratings',
                   >   header 'true', inferSchema 'true');
Time taken: 4.499 seconds
spark-sql (default)> SELECT COUNT(*) FROM ratings WHERE rating >= 4;
48580
Time taken: 1.093 seconds, Fetched 1 row(s)
spark-sql (default)> quit;
```

**`spark.sql()` from Python** lets you reuse reports and queries from other tools and projects:

```python
# 1. register DataFrames as temporary views
ratings.createOrReplaceTempView("ratings")
movies.createOrReplaceTempView("movies")

# 2. query them: the result is a DataFrame
best = spark.sql("""
    SELECT m.title, COUNT(*) AS n,
           ROUND(AVG(r.rating), 2) AS avg
    FROM ratings r
    JOIN movies m ON r.movieId = m.movieId
    GROUP BY m.title
    HAVING COUNT(*) >= 50
    ORDER BY avg DESC, n DESC
    LIMIT 5
""")
best.show(truncate=False)

# 3. mix: keep going with the DataFrame API
best.filter(F.col("n") > 100).count()   # 3
```

Exactly the same result as the DataFrame version: **same plan, same speed**.

### Spark SQL, Hive and catalogs

- Since Spark 2.0, the SQL parser supports both **ANSI SQL** and **HiveQL**.
- Spark SQL can connect to the **Hive metastore** (where Hive keeps table metadata: schema, location, partitions) and read the data underneath.
- Moving **Hive workloads to Spark** is one of the classic ways of **modernising a legacy Hadoop environment**.
- Today the same role is also played by **lakehouse catalogs**: Unity Catalog, AWS Glue Data Catalog, Iceberg REST catalogs.

```
spark.sql("SELECT * FROM movies") ── where? ──► Catalog (Hive metastore · Unity · Glue)
        │                         ◄── schema + location ──┘
        └── reads the files directly ──► HDFS / S3 / MinIO: part-00000.parquet, part-00001.parquet…
```

---

## 5. RDDs vs DataFrames

### Why DataFrames are faster from Python

```
🐍 Python process ──Py4J──► ☕ JVM driver (SparkSession · Catalyst · DAG scheduler)
   (your notebook)                    │
                    ┌─────────────────┴─────────────────┐
     DataFrame / SQL path                     RDD path (and Python UDFs)
     ☕ Executor JVM: Tungsten compiled        ☕ Executor JVM ⇄ pickle ⇄ 🐍 Python worker
     code, compact binary rows                 (your lambda, row by row)
     ✓ rows never leave the JVM               ✗ every row: serialise → Python → serialise back
```

Catalyst can't look inside a Python `lambda`, and every row has to cross between the JVM and Python. With DataFrames, PySpark is about as fast as Scala.

### Same question, two APIs

**RDD**: record = string (line) → list of strings → tuples you manage by hand.

```python
lines = sc.textFile(".../ratings")
recs  = (lines.filter(lambda l: not l.startswith("userId"))
              .map(lambda l: l.split(",")))
(recs.map(lambda x: (int(x[1]), (float(x[2]), 1)))
     .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
     .filter(lambda kv: kv[1][1] >= 50)
     .mapValues(lambda s: round(s[0] / s[1], 2))
     .sortBy(lambda kv: -kv[1])
     .take(3))
# [(318, 4.43), (858, 4.29), (750, 4.27)]
```

**DataFrame**: just `Row`s with named, typed columns, optimised by Catalyst.

```python
ratings = spark.read.csv(".../ratings", header=True, inferSchema=True)
(ratings.groupBy("movieId")
        .agg(F.count("*").alias("n"),
             F.round(F.avg("rating"), 2).alias("avg"))
        .filter("n >= 50")
        .orderBy(F.desc("avg"))
        .take(3))
# [Row(movieId=318, n=317, avg=4.43),
#  Row(movieId=858, n=192, avg=4.29),
#  Row(movieId=1276, n=57, avg=4.27)]
```

(Several movies tie at 4.27, so the third one can differ between runs.)

| | RDD | DataFrame |
|---|---|---|
| Record | Any Python object (string, tuple, dict…) | `Row` with named, typed columns (schema) |
| You describe | **How** to compute (functions) | **What** you want (expressions, SQL) |
| Optimisation | None: Spark runs your code as written | Catalyst + Tungsten: pushdown, pruning, join choice, code generation |
| Speed from Python | Slow: rows cross into Python workers | Same as Scala: work stays in the JVM |
| Reading data | `textFile`, parse it yourself | Data Source API: CSV, JSON, Parquet, JDBC, Kafka… |
| Entry point | `SparkContext` | `SparkSession` |
| Spark Connect / serverless | ✗ not supported | ✓ supported |
| Use it for | Legacy code, very custom low-level logic | **Everything else** |

### What's new in the Data API since Spark 4

| Version | Feature | Why it matters |
|---|---|---|
| 4.0 | **ANSI SQL mode by default** | Invalid casts, overflows and division by zero raise errors instead of returning NULL |
| 4.0 | **VARIANT type** (GA and on by default in 4.1) | Efficient storage and querying of JSON-like semi-structured data |
| 4.0 | **SQL pipe syntax `\|>`** | Write SQL top-down, step by step, like a DataFrame chain |
| 4.0 | **Python Data Source API** | Write custom `spark.read.format(…)` connectors in pure Python |
| 4.0 | **Native plotting** | `df.plot.bar(…)` directly on a PySpark DataFrame (Plotly backend) |
| 4.0 | **Spark Connect everywhere** | Lightweight `pyspark-client`; DataFrame and SQL only, no RDDs |
| 4.1 | **Spark Declarative Pipelines** | Declare datasets and flows; Spark handles ordering, retries and checkpoints |
| 4.1 | **SQL scripting GA**, recursive CTEs | Variables, loops and error handlers in SQL |
| 4.2 | **Arrow by default** for Python UDFs; geospatial types; `QUALIFY` | Faster Python UDFs; new SQL capabilities |

Releases: Spark 4.0 (May 2025) · 4.1 (December 2025) · 4.2 (July 2026) · 4.3 (October 2026, the latest). OSBDET ships Spark 3.5, so these are a preview of what you'll meet at work.

**Spark 4 in code** (run on PySpark 4.1.1):

```python
spark.sql("""
  FROM ratings
  |> WHERE rating >= 4
  |> AGGREGATE COUNT(*) AS n GROUP BY movieId
  |> ORDER BY n DESC
  |> LIMIT 3
""").show()
# movieId 318 → 274 (Shawshank Redemption)
# movieId 356 → 249 (Forrest Gump)
# movieId 296 → 244 (Pulp Fiction)

spark.conf.get("spark.sql.ansi.enabled")              # 'true'
bad.select(F.expr("try_cast(r AS DOUBLE)")).show()    # 4.5 · NULL

v = spark.sql("""SELECT parse_json('{"user":414,"device":{"os":"android"}}') AS v""")
v.selectExpr("v:device.os::string AS os").show()      # android
```

---

## 6. Summary

**Spark has two APIs:**

| | Low-level API | High-level (Structured) API |
|---|---|---|
| Data abstraction | Distributed collection: **RDD** | Distributed tables: **DataFrames**, **SQL**, Datasets (Scala and Java only) |
| Also | Broadcast variables, accumulators | |
| Entry point | `SparkContext` | `SparkSession` |
| Records | Raw Python objects, no schema | `Row`s with a schema of Spark data types |
| Under the hood | | Catalyst plan that relies on the low-level API |
| Use for | Legacy code and special cases | **Almost everything** |

**Both APIs share the same two types of operations:**

- **Transformations** are **lazy**: they create a new "virtual" version of the data (the original is immutable). They are **narrow** (1 → 1, no data movement) or **wide** (shuffle, new stage).
- **Actions** **fire the computation** of all the transformations applied to a data abstraction, and show, return or write the result. Each action = one or more jobs in the Spark UI.
- **Partitions** drive parallelism: one task per partition, one task per core at a time.
- Filter and select **early**, shuffle **little**, cache only what you **reuse**, and avoid `collect()` on big data.

---

## Try It Yourself on MovieLens

| # | API | Question | Hints | Check your answer |
|---|---|---|---|---|
| ① | RDD | Which **rating value** is the most common? | `textFile`, `map` to `(rating, 1)`, `reduceByKey`, `sortBy` | **4.0**, given 26,818 times |
| ② | DataFrame | Which **genre** has the best average rating? | `join` ratings and movies, `explode(split(genres, "\\|"))`, `groupBy` + `agg` | **Film-Noir**, 3.92 (870 ratings). Lowest: Horror, 3.26 |
| ③ | Spark SQL | How many movies have **never been rated**? | `LEFT ANTI JOIN` or `NOT EXISTS` in `spark.sql()` | **18** movies (9,742 movies, 9,724 rated) |

**Bonus:** run `explain()` on your answer to ② and find the `Exchange` (shuffle) and the `BroadcastHashJoin`. Then open `localhost:24040` and count the jobs.

---

## Links & References

- [PySpark documentation](https://spark.apache.org/docs/latest/api/python/index.html): API reference for `DataFrame`, `functions`, `RDD`
- [Spark SQL, DataFrames and Datasets guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Spark SQL data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
- [RDD programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html) (includes shared variables)
- [Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Spark 4.1 release notes](https://spark.apache.org/releases/spark-release-4-1-0.html) · [Spark 4.2 release notes](https://spark.apache.org/releases/spark-release-4-2-0.html)
- Bill Chambers and Matei Zaharia, *Spark: The Definitive Guide*, chapters 4–7 (Structured APIs) and 12–14 (RDDs and shared variables)
- Jules S. Damji et al., *Learning Spark*, 2nd edition, chapters 3–4

---
