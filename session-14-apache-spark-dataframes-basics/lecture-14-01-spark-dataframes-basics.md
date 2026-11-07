# Modern Data Architectures for Big Data I
## Lecture 14.1 — Spark DataFrames, the Basics (DataFrames API I)

*Updated edition, November 2026. Spark 3.5 in OSBDET; Spark 4 differences noted.*

All examples use the **MovieLens** files you landed in HDFS in lab 8.1 (`hdfs:///datalake/raw/movielens/`): **9,742 movies**, **100,836 ratings**, **3,683 tags**, **610 users**. Every output below is real, produced with PySpark 3.5.7 (Spark 4.1.1 where noted), with the session time zone set to UTC.

---

## Agenda

1. **Creation**: programmatically and from data sources
2. **Inspection**: schema, contents and quick statistics
3. **Transformation**: importing functions, columns and expressions, column manipulation, row filtering, sorting
4. **Summary**: cheat sheet and exercises

---

## Where Are We?

```
┌───────────────────────────────────────────────────────────┐
│ Structured Streaming │ Advanced analytics │ Libraries      │
├───────────────────────────────────────────────────────────┤
│ Structured APIs:  Datasets · ★ DataFrames ★ · SQL          │  ← today
├───────────────────────────────────────────────────────────┤
│ Low-level APIs:   RDDs · Distributed variables             │  ← lecture 12.1
└───────────────────────────────────────────────────────────┘
```

- **11.2**: Spark's APIs, partitions, transformations and actions.
- **13.1 (today)**: create, inspect and transform DataFrames: columns, filters, sorting.
- **13.2**: column functions, aggregations, joins, sets and nulls.

### The DataFrame API at a glance

| 1 · Create | 2 · Inspect | 3 · Transform (lazy) | 4 · Act (eager) |
|---|---|---|---|
| `spark.read.csv()`, `spark.read.parquet()`, `createDataFrame()`, `spark.range()` | `schema`, `dtypes`, `columns`, `printSchema()`, `show()`, `describe()`, `summary()` | `select`, `withColumn`, `drop`, `withColumnRenamed`, `cast`, `filter/where`, `distinct`, `limit`, `orderBy` | `show()`, `count()`, `take()`, `collect()`, `toPandas()`, `write…` |

---

## 1. Creation

### What is a DataFrame?

A **distributed collection of data grouped into named columns**. It is equivalent to a **relational table** in Spark SQL, and can be created with various functions of the `SparkSession`.

```
            ┌──── a Column: F.col("title") ────┐
            │                                  │
movieId │ title                    │ genres
──────────┼──────────────────────────┼─────────────────────
1       │ Toy Story (1995)         │ Adventure|Animation|…   ┐ Partition 0
2       │ Jumanji (1995)           │ Adventure|Children|…    ┘   ← a Row
3       │ Grumpier Old Men (1995)  │ Comedy|Romance          ┐ Partition 1
4       │ Waiting to Exhale (1995) │ Comedy|Drama|Romance    ┘
…       │ … 9,742 rows

Schema = StructType([StructField('movieId', IntegerType(), True),
                     StructField('title',   StringType(),  True),
                     StructField('genres',  StringType(),  True)])   # name · type · nullable
```

### Two ways to create a DataFrame

| From data sources (most of the time) | Programmatically (tests, lookups) |
|---|---|
| Files, tables and databases through `SparkSession.read`, the Data Source API | From Python data with `SparkSession.createDataFrame(data, schema)` |

```python
ratings = spark.read.csv("hdfs:///datalake/raw/movielens/ratings",
                         header=True, inferSchema=True)
spark.read.json(…) · spark.read.parquet(…) · spark.read.table(…) · spark.read.jdbc(…)

genres = spark.createDataFrame([("Drama", "🎭"), ("Comedy", "😂")],
                               "genre STRING, emoji STRING")
spark.range(1_000_000)       # a quick test DataFrame
```

### `SparkSession.createDataFrame(data, schema)`

Creates a DataFrame from a **list** (of tuples, `Row`s or dicts), a **pandas DataFrame** or an **RDD**, with an optional schema.

```python
movies_df = spark.createDataFrame(
    [(1, "Toy Story", 1995, 8.3),
     (2, "Jumanji",   1995, 7.1),
     (3, "Heat",      1995, None)],
    "movieId INT, title STRING, year INT, imdb DOUBLE")

movies_df.show()
movies_df.printSchema()
```
```
+-------+---------+----+----+
|movieId|    title|year|imdb|
+-------+---------+----+----+
|      1|Toy Story|1995| 8.3|
|      2|  Jumanji|1995| 7.1|
|      3|     Heat|1995|NULL|
+-------+---------+----+----+

root
 |-- movieId: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- imdb: double (nullable = true)
```

`None` becomes `NULL`.

### Other inputs, and what Spark infers

```python
from pyspark.sql import Row
import pandas as pd

spark.createDataFrame([Row(movieId=1, title="Toy Story"), Row(movieId=2, title="Jumanji")])
spark.createDataFrame([{"movieId": 1, "title": "Toy Story"}])
spark.createDataFrame(pd.DataFrame({"movieId": [1, 2], "title": ["Toy Story", "Jumanji"]}))
spark.createDataFrame([(1, "a", 2.5)]).printSchema()
```

| Input | Inferred schema |
|---|---|
| `Row`s, dicts or pandas | `movieId: long`, `title: string` |
| tuples, no schema | `_1: long`, `_2: string`, `_3: double` |
| tuples + DDL schema | exactly what you wrote |

- A Python `int` is inferred as **long** (64-bit), not int.
- Without names you get `_1, _2, _3…`
- **Pass a schema** when the types matter.

### From data sources: the MovieLens files

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("dataframes").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")   # outputs match the slides anywhere

base = "hdfs:///datalake/raw/movielens"
opts = dict(header=True, inferSchema=True)

movies  = spark.read.csv(f"{base}/movies",  **opts)
ratings = spark.read.csv(f"{base}/ratings", **opts)
tags    = spark.read.csv(f"{base}/tags",    **opts)

movies.count(), ratings.count(), tags.count()     # (9742, 100836, 3683)
```

| DataFrame | Columns | Rows |
|---|---|---|
| movies | `movieId, title, genres` | 9,742 |
| ratings | `userId, movieId, rating, timestamp` | 100,836 |
| tags | `userId, movieId, tag, timestamp` | 3,683 |

---

## 2. Inspection

Once created, DataFrames can be inspected with functions and properties of the `DataFrame` object. Three families:

| Family | Members | Cost |
|---|---|---|
| **Schema** (metadata) | `schema`, `dtypes`, `columns`, `printSchema()` | No Spark job: answered from the plan |
| **Contents** (actions) | `show()`, `take()`, `head()`, `first()`, `tail()`, `collect()`, `toPandas()`, `count()` | Run a job and bring rows to the driver |
| **Statistics** (actions) | `describe()`, `summary()` | Computed in the cluster |

DataFrames are collections of **`Row`** objects, so every "contents" method returns (or prints) `Row`s.

### 2.1 Schema

```python
ratings.schema
# StructType([StructField('userId', IntegerType(), True), StructField('movieId', IntegerType(), True),
#             StructField('rating', DoubleType(), True), StructField('timestamp', IntegerType(), True)])

ratings.dtypes
# [('userId', 'int'), ('movieId', 'int'), ('rating', 'double'), ('timestamp', 'int')]

ratings.columns
# ['userId', 'movieId', 'rating', 'timestamp']

movies.printSchema()
# root
#  |-- movieId: integer (nullable = true)
#  |-- title: string (nullable = true)
#  |-- genres: string (nullable = true)
```

| Property / method | Returns |
|---|---|
| `schema` | a `StructType` (reuse it with `spark.read.schema(…)`) |
| `dtypes` | a list of `(name, type)` tuples |
| `columns` | a list of names |
| `printSchema()` | prints the schema as a tree (nested types included) |

### 2.2 Contents

**Rows.** Fields in a `Row` can be accessed like attributes (`row.key`), like dictionary values (`row["key"]`) or by position (`row[0]`):

```python
r = movies.first()
# Row(movieId=1, title='Toy Story (1995)', genres='Adventure|Animation|Children|Comedy|Fantasy')
r.title          # 'Toy Story (1995)'
r["genres"]      # 'Adventure|Animation|Children|Comedy|Fantasy'
r[0]             # 1
r.asDict()       # {'movieId': 1, 'title': 'Toy Story (1995)', 'genres': '…'}
```

Use `r["name"]` when the name has spaces or clashes with a `Row` method (`count`, `index`).

**`show(n=20, truncate=True, vertical=False)`** prints the first *n* rows to the console:

- `n`: number of rows to show (default 20).
- `truncate`: `True` cuts strings longer than **20** characters; a number cuts at that length and right-aligns cells; `False` shows everything.
- `vertical`: `True` prints one line per column value (great for wide tables).

```
movies.show(3)                              movies.show(3, truncate=12)
+-------+--------------------+--------------------+   +-------+------------+------------+
|movieId|               title|              genres|   |movieId|       title|      genres|
+-------+--------------------+--------------------+   +-------+------------+------------+
|      1|    Toy Story (1995)|Adventure|Animati...|   |      1|Toy Story...|Adventure...|
|      2|      Jumanji (1995)|Adventure|Childre...|   |      2|Jumanji (...|Adventure...|
|      3|Grumpier Old Men ...|      Comedy|Romance|   |      3|Grumpier ...|Comedy|Ro...|
+-------+--------------------+--------------------+   +-------+------------+------------+

movies.show(2, vertical=True, truncate=False)
-RECORD 0----------------------------------------------
 movieId | 1
 title   | Toy Story (1995)
 genres  | Adventure|Animation|Children|Comedy|Fantasy
-RECORD 1----------------------------------------------
 movieId | 2
 title   | Jumanji (1995)
 genres  | Adventure|Children|Fantasy
```

**Bringing rows to the driver:**

| Action | Returns |
|---|---|
| `take(n)` | the first *n* rows as a list of `Row` |
| `head()` / `first()` | the first `Row` (equivalent to `take(1)[0]`); `head(n)` = `take(n)` |
| `tail(n)` | the last *n* rows: `[Row(movieId=193587, title='Bungo Stray Dogs: Dead Apple (2018)', …), Row(movieId=193609, title='Andrew Dice Clay: Dice Rules (1991)', …)]` |
| `count()` | the total number of rows: `9742` |
| `collect()` | **all** rows as a list of `Row` ⚠ |
| `toPandas()` | **all** rows as a pandas DataFrame ⚠ |

> **Note:** `collect()` and `toPandas()` load all the data into the **driver's memory**. Use them only when the result is expected to be small, e.g. `df.limit(1000).toPandas()`.

### 2.3 Statistics

**`describe(*cols)`** computes count, mean, stddev, min and max for numeric and string columns (all columns if none are given):

```
ratings.describe().show()
+-------+------------------+----------------+------------------+--------------------+
|summary|            userId|         movieId|            rating|           timestamp|
+-------+------------------+----------------+------------------+--------------------+
|  count|            100836|          100836|            100836|              100836|
|   mean|326.12756356856676|19435.2957177992| 3.501556983616962|1.2059460873684695E9|
| stddev| 182.6184914635004|35530.9871987003|1.0425292390606342|2.1626103599513078E8|
|    min|                 1|               1|               0.5|           828124615|
|    max|               610|          193609|               5.0|          1537799250|
+-------+------------------+----------------+------------------+--------------------+

movies.describe().show()
+-------+------------------+--------------------+------------------+
|summary|           movieId|               title|            genres|
+-------+------------------+--------------------+------------------+
|  count|              9742|                9742|              9742|
|   mean|42200.353623485935|                NULL|              NULL|
| stddev| 52160.49485443825|                NULL|              NULL|
|    min|                 1|"11'09""01 - Sept...|(no genres listed)|
|    max|            193609|À nous la liberté...|           Western|
+-------+------------------+--------------------+------------------+
```

For strings, mean and stddev are `NULL` and min/max are **alphabetical**. Read the numbers with care: the mean of an ID means nothing.

**`summary(*stats)`** computes the given statistics: count, mean, stddev, min, max and any **approximate percentile** written as `"NN%"`. Without arguments: count, mean, stddev, min, 25%, 50%, 75% and max.

```
ratings.select("rating", "timestamp").summary().show()     ratings.select("rating").summary(
+-------+------------------+--------------------+              "count","min","10%","50%","90%","max").show()
|summary|            rating|           timestamp|          +-------+------+
+-------+------------------+--------------------+          |summary|rating|
|  count|            100836|              100836|          +-------+------+
|   mean| 3.501556983616962|1.2059460873684695E9|          |  count|100836|
| stddev|1.0425292390606342|2.1626103599513078E8|          |    min|   0.5|
|    min|               0.5|           828124615|          |    10%|   2.0|
|    25%|               3.0|          1018535155|          |    50%|   3.5|
|    50%|               3.5|          1186086516|          |    90%|   5.0|
|    75%|               4.0|          1435993828|          |    max|   5.0|
|    max|               5.0|          1537799250|          +-------+------+
+-------+------------------+--------------------+
```

---

## 3. Transformation

Once created, a DataFrame is transformed with functions defined in the **`DataFrame`** and **`Column`** objects, plus the functions in **`pyspark.sql.functions`** (428 in Spark 3.5, 507 in Spark 4.1). Most of them **mimic SQL**:

| SQL | DataFrame API |
|---|---|
| `SELECT a, b` | `df.select("a", "b")` |
| `SELECT *, x * 2 AS y` | `df.withColumn("y", F.col("x") * 2)` |
| `CAST(x AS INT)` | `F.col("x").cast("int")` |
| `WHERE x >= 4 AND y = 1` | `df.filter((F.col("x") >= 4) & (F.col("y") == 1))` |
| `SELECT DISTINCT a` | `df.select("a").distinct()` |
| `ORDER BY x DESC` | `df.orderBy(F.desc("x"))` |
| `LIMIT 10` | `df.limit(10)` |
| `GROUP BY`, `JOIN` | `groupBy().agg()`, `join()` (lecture 15.1) |

Every transformation returns a **new** DataFrame and runs nothing until an action.

### Importing functions

| Way | Code | Verdict |
|---|---|---|
| 1 · All import | `from pyspark.sql.functions import *` | **Avoid.** Imports ~430 names and overwrites Python built-ins (`sum`, `max`, `min`, `round`, `abs`, `filter`…) |
| 2 · One by one | `from pyspark.sql.functions import col, avg, desc` | Fewer collisions, but you keep editing the import line (and `max` still collides) |
| 3 · Alias the module | `import pyspark.sql.functions as F` | **Best practice.** One short alias; use functions as `F.max`, `F.col`… |

What the star import does to plain Python (real output):

```python
from pyspark.sql.functions import *
sum([1, 2, 3])   # PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got list.
round(3.7)       # PySparkTypeError: [NOT_COLUMN_OR_STR] … got float.
max(3, 7)        # TypeError: max() takes 1 positional argument but 2 were given
```

### 3.1 Columns

**References.** All four return `Column<'rating'>`:

```python
ratings.rating   ·   ratings["rating"]   ·   F.col("rating")   ·   F.column("rating")
```

| Form | Use it when… |
|---|---|
| `F.col("x")` | **Default.** Works before the DataFrame exists (e.g. in reusable functions) |
| `df["x"]` | The name has spaces or dots, or you must say **which** DataFrame (after a join) |
| `df.x` | Quick typing; fails if the name clashes with a method (`df.count`) |
| `"x"` | Most methods also accept plain names: `select("x")` |

A `Column` is not data: it is a **description** of a value Spark will compute for every row.

**`Column.alias(name)`** renames a column in the result:

```python
ratings.select(F.col("rating").alias("stars")).show(2)
# +-----+
# |stars|
# +-----+
# |  4.0|
# |  4.0|
```

**Column expressions** combine columns and operators (and functions). You pass them as arguments to most DataFrame operations:

```python
ratings.select("rating",
               (F.col("rating") * 2).alias("out_of_10"),
               (F.col("rating") >= 4).alias("liked")).show(3)
```
```
+------+---------+-----+
|rating|out_of_10|liked|
+------+---------+-----+
|   4.0|      8.0| true|
|   4.0|      8.0| true|
|   4.0|      8.0| true|
+------+---------+-----+
```

An expression is a small **tree** in the plan, `Column<'(rating * 2) AS out_of_10'>`:

```
        Alias "out_of_10"
              │
          Multiply (*)
           ┌──┴──┐
   Column rating   Literal 2
```

Catalyst can read and optimise it, unlike a Python lambda.

**`F.expr("string")`** parses a SQL snippet into a Column; **`selectExpr()`** is `select` + `expr` for every argument. Same logical plan, same speed:

```python
ratings.select(F.expr("rating * 2 AS out_of_10"), F.expr("rating >= 4 AS liked")).show(3)
ratings.selectExpr("userId", "rating * 2 AS out_of_10").show(2)
```

### 3.2 Column manipulation

```
select("movieId","rating")        withColumn("liked", rating >= 4)
userId│movieId│rating│timestamp   userId│movieId│rating│timestamp│+liked
 (out)│   1   │ 4.0  │  (out)       1   │   1   │ 4.0  │964982703│ true

drop("timestamp")                 withColumnRenamed("rating","stars")
userId│movieId│rating│−timestamp  userId│movieId│stars│timestamp
```

**`select(*cols)`** projects a set of expressions: column names (strings) or `Column`s; `"*"` expands to all current columns. A list works too.

```python
ratings.select("*", F.lit("movielens").alias("source")).show(2)
```
```
+------+-------+------+---------+---------+
|userId|movieId|rating|timestamp|   source|
+------+-------+------+---------+---------+
|     1|      1|   4.0|964982703|movielens|
|     1|      3|   4.0|964981247|movielens|
+------+-------+------+---------+---------+
```

Selecting fewer columns early means Spark reads less data: with Parquet it skips the other columns on disk.

**`withColumn(colName, colExpression)`** adds a column, or **replaces** an existing one with the same name. **`withColumns({…})`** (Spark 3.3+) adds several at once.

```python
r = ratings.withColumn("rated_at", F.to_timestamp(F.from_unixtime("timestamp")))
r.show(3)
ratings.withColumns({"out_of_10": F.col("rating") * 2, "liked": F.col("rating") >= 4})
ratings.withColumn("rating", F.col("rating") * 2)     # same name = replace
```
```
+------+-------+------+---------+-------------------+
|userId|movieId|rating|timestamp|           rated_at|
+------+-------+------+---------+-------------------+
|     1|      1|   4.0|964982703|2000-07-30 18:45:03|
|     1|      3|   4.0|964981247|2000-07-30 18:20:47|
|     1|      6|   4.0|964982224|2000-07-30 18:37:04|
+------+-------+------+---------+-------------------+
```

> Avoid long loops of `withColumn`: each call adds a projection to the plan, and hundreds of them make planning slow. Use `withColumns` or a single `select`.

**`drop(*cols)`** and **`withColumnRenamed(existing, new)`** (or `withColumnsRenamed({…})`, Spark 3.4+), or `select(…, F.col(x).alias(new))`:

```python
ratings.drop("timestamp").columns                     # ['userId', 'movieId', 'rating']
ratings.drop("timestamp", "userId").columns           # ['movieId', 'rating']
ratings.withColumnRenamed("rating", "stars").columns  # ['userId', 'movieId', 'stars', 'timestamp']
ratings.withColumnsRenamed({"rating": "stars", "movieId": "movie_id"}).columns
# ['userId', 'movie_id', 'stars', 'timestamp']
ratings.select("userId", F.col("rating").alias("stars"))
```

> ⚠ **Silent typos:** both methods **ignore names that don't exist**: `ratings.drop("timestamps")` and `ratings.withColumnRenamed("ratting", "x")` change nothing and raise no error. Check `df.columns`. Column names are **case-insensitive** by default: `drop("Rating")` does remove `rating`.

**Change a column's type** with `cast(dataType)` or its alias `astype`:

```python
c = (ratings.withColumn("rating_int", F.col("rating").cast("int"))
            .withColumn("ts_str", F.col("timestamp").astype("string")))
```
```
+------+-------+------+---------+----------+---------+
|userId|movieId|rating|timestamp|rating_int|   ts_str|
+------+-------+------+---------+----------+---------+
|     1|      1|   4.0|964982703|         4|964982703|
|     1|      3|   4.0|964981247|         4|964981247|
+------+-------+------+---------+----------+---------+
```

- Type names: `"int"`, `"bigint"`, `"double"`, `"decimal(10,2)"`, `"string"`, `"date"`, `"timestamp"`, `"boolean"`, or type objects such as `IntegerType()`.
- `4.5 → int` gives `4`: casting **truncates**, it doesn't round.
- Invalid values (`"five" → int`) give `NULL` in Spark 3.5 and an **error** in Spark 4 (ANSI mode). Use `try_cast` to get `NULL` on purpose.

### 3.3 Row filtering

**`filter(condition)`**, and its alias **`where(condition)`**, keeps the rows where the condition (a Column expression or a SQL string) is true. Rows where it is **false or NULL** are removed.

```python
ratings.filter(F.col("rating") == 5).count()     # 13211
ratings.filter("rating = 5").count()             # 13211
ratings.where(F.col("rating") == 5).count()      # 13211

(movies.filter(F.col("title").contains("Star Wars"))
       .select("movieId", "title").show(3, False))
```
```
+-------+-----------------------------------------------------+
|movieId|title                                                |
+-------+-----------------------------------------------------+
|260    |Star Wars: Episode IV - A New Hope (1977)            |
|1196   |Star Wars: Episode V - The Empire Strikes Back (1980)|
|1210   |Star Wars: Episode VI - Return of the Jedi (1983)    |
+-------+-----------------------------------------------------+
```

| Column method | Count on MovieLens |
|---|---|
| `isin(0.5, 1.0)` | ratings: 4,181 |
| `between(2, 3)` (inclusive) | ratings: 33,148 |
| `contains("Star Wars")` | movies: 13 |
| `like("%Matrix%")` | movies: 3 |
| `startswith("Sci-Fi")` (genres) | movies: 62 |

**Combining conditions** with `&` (and), `|` (or), `~` (not), **each condition in parentheses**:

```python
ratings.where((F.col("rating") >= 4) & (F.col("userId") == 1)).count()        # 200
ratings.filter((F.col("rating") == 0.5) | (F.col("rating") == 5)).count()     # 14581
ratings.filter(~(F.col("rating") >= 1)).count()                               # 1370
```

> **The classic trap.** `F.col("rating") >= 4 and F.col("userId") == 1` and `F.col("rating") >= 4 & F.col("userId") == 1` both fail:
> `PySparkValueError: [CANNOT_CONVERT_COLUMN_INTO_BOOL] Cannot convert column into bool: please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions.`
> Python's `and/or/not` don't work on Columns, and `&` binds tighter than `>=` and `==` (Python reads `4 & F.col(…)` first). (Spark 3.5 shows the second one as a raw `Py4JError`.)

**Unique rows.** `distinct()` removes rows identical in **every** column; `dropDuplicates([cols])` keeps one row per value of the given columns (which one is not guaranteed). Both are wide transformations.

```python
ratings.select("rating").distinct().count()    # 10
ratings.select("userId").distinct().count()    # 610
movies.select("genres").distinct().count()     # 951 genre combinations
ratings.dropDuplicates(["userId"]).count()     # 610
```

**`limit(num)`** returns a new DataFrame with only *num* rows. Without `orderBy`, which rows you get is not guaranteed.

### 3.4 Sorting

**`sort(*cols)`**, and its alias **`orderBy(*cols)`**, return a new DataFrame sorted by the given columns, **ascending by default**. Direction: `F.asc/F.desc` or the Column methods `asc()`, `desc()`, `asc_nulls_first()`, `asc_nulls_last()`, `desc_nulls_first()`, `desc_nulls_last()`. Sorting is a wide transformation (Spark samples the data, splits it into ranges and shuffles).

```python
ratings.sort("rating").show(3)                         # 0.5-star ratings of user 3
ratings.sort(F.desc("rating"), "userId").show(3)       # 5.0, user 1
ratings.orderBy(F.col("rating").desc(), F.col("timestamp").asc()).show(3)
# userId 429, movieIds 588/595/590, 5.0, timestamp 828124615: the oldest 5-star ratings (March 1996)
```

**Where do NULLs go?** Default: **first** in ascending order, **last** in descending order.

```python
n = spark.createDataFrame([("Heat", None), ("Toy Story", 8.3), ("Jumanji", 7.1)],
                          "title STRING, imdb DOUBLE")
```

| `sort("imdb")` | `sort(F.desc("imdb"))` | `F.col("imdb").desc_nulls_first()` | `F.asc_nulls_last("imdb")` |
|---|---|---|---|
| Heat NULL · Jumanji 7.1 · Toy Story 8.3 | Toy Story 8.3 · Jumanji 7.1 · Heat NULL | Heat NULL · Toy Story 8.3 · Jumanji 7.1 | Jumanji 7.1 · Toy Story 8.3 · Heat NULL |

### Putting it all together

What did our most active user (414, with 2,698 ratings) rate last?

```python
(ratings
   .filter(F.col("userId") == 414)
   .withColumn("rated_at", F.to_timestamp(F.from_unixtime("timestamp")))
   .join(movies, "movieId")                     # joins: lecture 15.1
   .select("title", "rating", "rated_at")
   .orderBy(F.desc("rated_at"))
   .show(5, truncate=False))
```
```
+------------------------------+------+-------------------+
|title                         |rating|rated_at           |
+------------------------------+------+-------------------+
|The Greatest Showman (2017)   |3.5   |2018-06-02 22:21:12|
|Solo: A Star Wars Story (2018)|3.5   |2018-06-02 22:18:40|
|The Hitman's Bodyguard (2017) |3.0   |2018-06-02 22:18:29|
|Black Panther (2017)          |4.0   |2018-06-02 22:18:12|
|Kings of Summer, The (2013)   |4.0   |2018-05-05 23:30:46|
+------------------------------+------+-------------------+
```

---

## 4. Summary: Cheat Sheet

| Area | Methods |
|---|---|
| 🏗 Create | `spark.read.csv(path, header=True, inferSchema=True)` · `spark.createDataFrame(data, "a INT, b STRING")` · `spark.range(n)` |
| 🧬 Schema | `df.schema` (StructType) · `df.dtypes` · `df.columns` · `df.printSchema()` |
| 📄 Contents (actions) | `show(n, truncate, vertical)` · `take(n)` · `head()` · `first()` · `tail(n)` · `count()` · `collect()` / `toPandas()` (small data only) · `describe()` · `summary()` |
| 🔧 Columns | `import pyspark.sql.functions as F` · `F.col("x")` · `df["x"]` · `.alias("y")` · `F.expr("x * 2")` · `selectExpr()` · `.cast("int")` |
| 🛠 Column manipulation | `select(*cols)` · `withColumn` · `withColumns` · `drop(*cols)` · `withColumnRenamed` · `withColumnsRenamed` |
| 🔎 Rows | `filter(cond)` = `where(cond)` · `(a) & (b)`, `(a) \| (b)`, `~a` · `isin`, `between`, `contains`, `like` · `distinct()` · `dropDuplicates([cols])` · `limit(n)` |
| ↕ Sorting | `sort(*cols)` = `orderBy(*cols)` · `F.desc("x")` · `F.col("x").asc()` · `asc_nulls_last`, `desc_nulls_first` |
| 💡 Remember | Transformations are lazy · each returns a NEW DataFrame · parenthesise every condition · `drop`/rename ignore typos |

### Try it yourself on MovieLens

| # | Task | Check your answer |
|---|---|---|
| ① Inspect | When was the **first** and the **last** rating given? `F.min` / `F.max` on `timestamp` with `F.from_unixtime` (UTC) | 1996-03-29 18:36:55 · 2018-09-24 14:27:30 |
| ② Filter | How many titles contain "(1995)"? How many movies contain **Comedy** in their genres? | 259 · 3,756 |
| ③ Columns + sort | List user 1's **5-star** movies with title and a `rated_at` column, newest first. How many are there? | 124 |

**Bonus:** run `describe()` on `tags`. Which statistics make sense for each column, and which don't?

---

## Links & References

- [PySpark API: DataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [PySpark API: Column](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html)
- [PySpark API: functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Quickstart: DataFrame](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)
- [Spark SQL data types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
- Bill Chambers and Matei Zaharia, *Spark: The Definitive Guide*, chapter 5 (Basic Structured Operations)

---
