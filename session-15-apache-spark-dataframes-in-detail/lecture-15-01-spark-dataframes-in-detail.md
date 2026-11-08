# Modern Data Architectures for Big Data I
## Lecture 15.1 — Spark DataFrames in Detail (DataFrames API II)

*Updated edition, November 2026. Spark 3.5 in OSBDET; Spark 4 differences noted.*

Same setup as 14.1: `movies`, `ratings`, `tags` (and `links`) read from `hdfs://localhost:9000/datalake/raw/movielens/`, `import pyspark.sql.functions as F`, session time zone UTC. Every output below is real (PySpark 3.5.7; UDF timings on Spark 4.1.1).

---

## Agenda

1. **Column functions**: literals, booleans, numbers, strings, dates, arrays, UDFs
2. **Aggregation**: summaries, groups, pivots (and a peek at windows)
3. **Joins**: shuffle vs broadcast, join types, gotchas
4. **Sets and sampling**: union, intersect, except, sample, randomSplit
5. **Null handling**: detect, fill, drop, compare
6. **Summary**: cheat sheet and exercises

**Where are we?** 13.1 covered *create → inspect → basic transformations* (select, filter, orderBy…). Today: the rest of the transformation toolbox.

---

## 3.5 Column Functions

Beyond column references and operators, `pyspark.sql.functions` has **428 functions in Spark 3.5** and **507 in Spark 4.1**: strings, dates, maths, arrays, JSON… What you can do depends on the **column type**. We cover the most popular ones; browse the rest in the API reference.

> **Rule of thumb:** if a built-in function exists, use it. It runs inside the JVM and Catalyst can optimise it; a Python UDF can't.

| Family | Functions |
|---|---|
| Literals & booleans | `lit`, `when`, `otherwise`, `isin`, `between` |
| Numeric | `round`, `bround`, `abs`, `sqrt`, `pow`, `floor`, `ceil`, `corr`, `monotonically_increasing_id` |
| String | `concat`, `concat_ws`, `lower`, `upper`, `initcap`, `trim`, `lpad`, `substring`, `length`, `split` |
| Date & time | `current_date`, `year`…, `date_add`, `date_sub`, `to_date`, `to_timestamp` |
| Array | `getItem`, `[i]`, `size`, `array_contains`, `explode` |
| UDFs | `F.udf`, `pandas_udf`, `spark.udf.register` |

### A column from a literal: `lit(value)`

Creates a Column with the **same value in every row**; its type is inferred from the Python value.

```python
ratings.select("rating", F.lit(10).alias("scale"), F.lit("movielens").alias("src")).show(2)
# +------+-----+---------+
# |rating|scale|      src|
# |   4.0|   10|movielens|

ratings.select(F.lit(10), F.lit("x"), F.lit(True), F.lit(None)).dtypes
# [('10', 'int'), ('x', 'string'), ('true', 'boolean'), ('NULL', 'void')]
```

Needed wherever a function expects a Column (`F.concat("title", F.lit(" · "), "genres")`). `F.lit(None)` has type `void`: add `.cast("string")` to give it a real type.

### 3.5.1 Boolean columns: `when` · `otherwise`

Booleans are the foundation of filtering and conditional logic. `when(condition, value)` evaluates conditions **in order** and returns the first matching value; without `otherwise`, unmatched rows get **NULL**. Equivalent to SQL `CASE WHEN … ELSE … END`.

```
rating ─► ≥ 4 ? ── yes ─► liked
            │ no
            ▼
          ≤ 2 ? ── yes ─► disliked
            │ no
            ▼
          otherwise("meh")  (or NULL)
```

```python
opinion = (F.when(F.col("rating") >= 4, "liked")
            .when(F.col("rating") <= 2, "disliked")
            .otherwise("meh"))
ratings.withColumn("opinion", opinion).groupBy("opinion").count().show()
```

| with `otherwise` | | without `otherwise` | |
|---|---|---|---|
| liked | 48,580 | liked | 48,580 |
| meh | 38,733 | **NULL** | 38,733 |
| disliked | 13,523 | disliked | 13,523 |

### 3.5.2 Numeric columns

Typical maths functions, all column to column: `abs`, `sqrt`, `pow`, `exp`, `log`, `log10`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `round`, `bround`, `floor`, `ceil`, plus the operators `+ - * / %`.

```python
ratings.select("rating", F.sqrt("rating").alias("sqrt"), F.pow("rating", 2).alias("pow2"),
               (F.col("rating") - 3.5).alias("diff"), F.abs(F.col("rating") - 3.5).alias("abs_diff")).show(3)
# |   4.0| 2.0|16.0| 0.5|     0.5|
```

**`round` vs `bround`.** The original slides described `bround` as "round down". That's not right:

| Function | Rule |
|---|---|
| `round(c, d)` | **HALF_UP**: .5 goes away from zero |
| `bround(c, d)` | **HALF_EVEN** ("banker's rounding"): .5 goes to the even neighbour |
| `floor(c)` / `ceil(c)` | always down / always up |

```
+----+-----+------+-------+--------+-----+----+
|   v|round|bround|round_1|bround_1|floor|ceil|
+----+-----+------+-------+--------+-----+----+
| 0.5|  1.0|   0.0|    0.5|     0.5|    0|   1|
| 1.5|  2.0|   2.0|    1.5|     1.5|    1|   2|
| 2.5|  3.0|   2.0|    2.5|     2.5|    2|   3|
|3.45|  3.0|   3.0|    3.5|     3.4|    3|   4|
|-2.5| -3.0|  -2.0|   -2.5|    -2.5|   -3|  -2|
+----+-----+------+-------+--------+-----+----+
```

On large sums, HALF_EVEN avoids the upward bias of always rounding .5 up, which is why finance uses it.

**`corr(col1, col2)`**: the Pearson correlation coefficient. Are popular movies rated higher?

```python
per_movie = ratings.groupBy("movieId").agg(F.count("*").alias("n"), F.avg("rating").alias("avg"))
per_movie.select(F.corr("n", "avg")).show()     # 0.12725857359560658: weak positive
```

| Ratings per movie | Movies | Average rating |
|---|---|---|
| 1 | 3,446 | 3.23 |
| 2–9 | 4,009 | 3.19 |
| 10–49 | 1,819 | 3.37 |
| 50–99 | 312 | 3.59 |
| 100+ | 138 | 3.82 |

**`monotonically_increasing_id()`** generates 64-bit IDs that are **unique and increasing but not consecutive**: `id = (partition_index << 33) + row_in_partition`. No shuffle needed.

```python
mid = movies.repartition(3).withColumn("id", F.monotonically_increasing_id())
mid.agg(F.min("id"), F.max("id")).show()     # 0 · 17179872430 for 9,742 rows
# partition 0: 0, 1, 2…  partition 1: 8,589,934,592…  partition 2: 17,179,869,184…
```

For consecutive numbers use `F.row_number().over(window)`, at the cost of a sort.

### 3.5.3 String columns

String manipulation appears in almost every analysis: log files (regular expressions), text modifications (case), removing spaces…

**Case and concatenation:**

```python
tags.select("tag", F.initcap("tag").alias("initcap"), F.lower("tag").alias("lower"),
            F.upper("tag").alias("upper")).show(5)
```
```
+---------------+---------------+---------------+---------------+
|            tag|        initcap|          lower|          upper|
+---------------+---------------+---------------+---------------+
|          funny|          Funny|          funny|          FUNNY|
|Highly quotable|Highly Quotable|highly quotable|HIGHLY QUOTABLE|
|   will ferrell|   Will Ferrell|   will ferrell|   WILL FERRELL|
|   Boxing story|   Boxing Story|   boxing story|   BOXING STORY|
|            MMA|            Mma|            mma|            MMA|
+---------------+---------------+---------------+---------------+
```

`initcap` uppercases the first letter of each word and lowercases the rest (MMA → Mma).

```python
movies.select(F.concat("title", F.lit(" · "), "genres"),
              F.concat_ws(" | ", "movieId", "title")).show(2, truncate=False)
# Toy Story (1995) · Adventure|Animation|Children|Comedy|Fantasy   |   1 | Toy Story (1995)
```

- `concat(*cols)` joins strings, binary or arrays; if **any** value is NULL, the result is NULL.
- `concat_ws(sep, *cols)` uses a separator and **skips NULLs**.

**Trim and pad:**

```python
t = spark.createDataFrame([("  sci-fi  ",)], "tag STRING")
# raw "[  sci-fi  ]" · ltrim "[sci-fi  ]" · rtrim "[  sci-fi]" · trim "[sci-fi]" · length 10

movies.select(F.lpad(F.col("movieId").cast("string"), 6, "0"), F.rpad("title", 12, "."))
# 000001 · "Toy Story (1"   (rpad also CUTS longer strings)
```

Found in MovieLens: **11 titles end with a trailing space** (`'96 Minutes (2011) '`), and tags drop from **1,589 to 1,475 distinct values** after `lower(trim(tag))`. Clean before you compare or group: "Sci-Fi ", "sci-fi" and "SCI-FI" are three different keys.

**`substring(col, pos, len)` and `length(col)`.** Positions start at **1**; negative positions count from the end. The year hides in the title:

```
 T  o  y  ␣  S  t  o  r  y  ␣  (  1  9  9  5  )      length = 16
 1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16
                                 −5 −4 −3 −2 −1
substring(title, -5, 4) → "1995"
```

```python
clean = F.trim("title")            # trim first!
movies.select("title", F.length("title"), F.substring(clean, -5, 4).cast("int").alias("year"))
```

Without `trim`, the 11 titles with a trailing space give values like `"011)"`; 14 titles have no year at all. Check your edge cases.

**`split(col, pattern)`** splits a string into an **array**. The pattern is a **regular expression**: `|` means "or" in regex, so escape it (`r"\|"`).

```python
g = movies.select("title", F.split("genres", r"\|").alias("genres"))
g.printSchema()
#  |-- genres: array (nullable = true)
#  |    |-- element: string (containsNull = false)
# Toy Story (1995) | [Adventure, Animation, Children, Comedy, Fantasy]
```

Other text functions worth knowing: `contains`, `startswith`, `regexp_replace`, `regexp_extract`, `translate`.

### 3.5.4 Date and timestamp columns

```
964982703 ──from_unixtime──► "2000-07-30 18:45:03" ──to_timestamp──► 2000-07-30 18:45:03 ──to_date──► 2000-07-30
(IntegerType, epoch s)        (StringType)                            (TimestampType)                  (DateType)
                                                                       │
                     year 2000 · month 7 · dayofmonth 30 · dayofweek 1 · dayofyear 212 · hour 18
```

`dayofweek`: 1 = Sunday … 7 = Saturday. Also `minute`, `second`, `weekofyear`, `quarter`.

```python
d = (ratings.withColumn("rated_at", F.to_timestamp(F.from_unixtime("timestamp")))
            .select("rated_at", F.to_date("rated_at").alias("date"), F.year("rated_at").alias("year"),
                    F.month("rated_at").alias("month"), F.dayofmonth("rated_at").alias("day"),
                    F.dayofweek("rated_at").alias("dow"), F.hour("rated_at").alias("hour")))
# |2000-07-30 18:45:03|2000-07-30|2000|7|30|1|18|

d.select("date", F.date_add("date", 30), F.date_sub("date", 7),
         F.datediff(F.current_date(), "date"), F.date_format("date", "EEEE")).show(1)
# |2000-07-30|2000-08-29|2000-07-23|9596|Sunday|   (days_ago computed on 2026-11-07)

spark.range(1).select(F.current_date(), F.current_timestamp())
# 2026-11-07 | 2026-11-07 19:15:30.535748

sd = spark.createDataFrame([("2026-11-07", "07/11/2026 19:35")], "iso STRING, es STRING")
sd.select(F.to_date("iso"), F.to_date("es", "dd/MM/yyyy HH:mm"),
          F.to_timestamp("es", "dd/MM/yyyy HH:mm")).show()
# 2026-11-07 | 2026-11-07 | 2026-11-07 19:35:00
```

| Pattern | Meaning | Example |
|---|---|---|
| `yyyy` | year | 2026 |
| `MM` · `MMM` | month | 11 · Nov |
| `dd` | day of month | 07 |
| `HH` · `mm` · `ss` | hour (0–23), minute, second | 19:35:00 |
| `EEEE` · `E` | day name | Saturday · Sat |

- Without a format, `to_date` expects ISO `yyyy-MM-dd`. Careful: `MM` = month, `mm` = minutes.
- Unparseable strings give `NULL` in Spark 3.5 and an error in Spark 4 (use `try_to_timestamp`).

**Dates in action: when do people rate movies?**

| Day (dow) | Ratings | | Hour (UTC) | Ratings |
|---|---|---|---|---|
| 1 · Sunday | 16,209 | | 20 | 6,533 |
| 2 · **Monday** | **17,583** | | 17 | 6,347 |
| 3 · Tuesday | 16,411 | | 21 | 6,271 |
| 4 · Wednesday | 14,014 | | | |
| 5 · **Thursday** | **10,706** | | | |
| 6 · Friday | 14,455 | | | |
| 7 · Saturday | 11,458 | | | |

Peak hours are in UTC; users live in different time zones, so the "local" hour is unknown: a real data-quality caveat.

### 3.5.5 Array columns

A column type that stores a **collection of elements of the same type** in each cell (`ArrayType(StringType())`).

```python
g.select("title",
  F.col("genres").getItem(0).alias("first"),          # position (0-based)
  F.col("genres")[1].alias("second"),                 # operator []
  F.size("genres").alias("n"),
  F.array_contains("genres", "Comedy").alias("comedy")).show(3, truncate=False)
g.select(F.expr("genres[0]"), F.element_at("genres", -1))   # Adventure · Fantasy (element_at is 1-based)
```
```
+-----------------------+---------+---------+---+------+
|title                  |first    |second   |n  |comedy|
+-----------------------+---------+---------+---+------+
|Toy Story (1995)       |Adventure|Animation|5  |true  |
|Jumanji (1995)         |Adventure|Children |3  |false |
|Grumpier Old Men (1995)|Comedy   |Romance  |2  |true  |
+-----------------------+---------+---------+---+------+
```

- Out-of-range positions return `NULL`. The maximum is 10 genres in one movie (`F.max(F.size("genres"))`).
- **Update:** the original slides said arrays have a fixed size and you can't add or remove elements. Like every column value they're immutable, but functions return **new** arrays: `array_append` (3.4+), `array_remove`, `array_sort`, `array_distinct`…

**`explode`** creates **one row per array element**, the key to "per genre" questions:

```python
genre_rows = movies.withColumn("genre", F.explode(F.split("genres", r"\|")))
genre_rows.select("title", "genre").show(4)
# Toy Story (1995) | Adventure / Animation / Children / Comedy …
```

Rows with empty or NULL arrays disappear; `explode_outer` keeps them.

### 3.5.6 User-defined functions (UDFs)

UDFs let you write custom transformations in Python (or Scala), even using external libraries. They take and return one or more columns. Two steps: **write** the function, then **register** it so every executor can run it.

```python
import re
from pyspark.sql.types import StringType

def fix_title(title):
    """Move a trailing ', The/A/An' to the front."""
    if title is None: return None
    title = title.strip()
    m = re.match(r"^(.*), (The|A|An) (\(\d{4}\))$", title)
    return f"{m.group(2)} {m.group(1)} {m.group(3)}" if m else title

fix_title_udf = F.udf(fix_title, StringType())                 # DataFrames
spark.udf.register("fix_title", fix_title, StringType())       # SQL: SELECT fix_title(title) FROM movies

movies.select("title", fix_title_udf("title").alias("fixed")) \
      .where("title != fixed").show(4, truncate=False)
```
```
|American President, The (1995)    |The American President (1995)    |
|Usual Suspects, The (1995)        |The Usual Suspects (1995)        |
|Big Green, The (1995)             |The Big Green (1995)             |
|Indian in the Cupboard, The (1995)|The Indian in the Cupboard (1995)|
```

1,554 titles change: 1,543 get their article moved to the front and 11 only lose trailing spaces.

> **Harmless noise in OSBDET:** a Python UDF followed by `show()` or `limit()` may print `Exception ignored … BrokenPipeError` from `pyspark/daemon.py`. Spark stopped reading as soon as it had the rows it needed, and the Python worker found the pipe closed (Spark 3.5 with Python 3.13). The result is correct: ignore it.

**How a Python UDF runs.** Spark serialises the function on the driver and sends it to all executors, then:

1. starts a **Python process** on each executor involved,
2. **serialises all the data** (pickle) and sends it to Python,
3. executes the function **row by row** in that process,
4. returns the results to the JVM, serialised again.

Catalyst sees the UDF as a **black box**: no pushdown, no optimisation inside it.

**Measured** (`x + 1` on 5,000,000 rows, Spark 4.1 local[4], best of 3):

| Approach | Time |
|---|---|
| Built-in expression | 0.20 s |
| Plain Python UDF | 2.87 s |
| pandas UDF | 0.90 s |
| Arrow-optimised UDF | 0.88 s |

```python
import pandas as pd

@F.pandas_udf("double")              # vectorised: one pandas Series per batch
def plus_one(s: pd.Series) -> pd.Series:
    return s + 1.0

F.udf(lambda x: x + 1.0, "double", useArrow=True)   # Arrow-optimised plain UDF (3.5+; default in 4.2)
```

1. **Built-in functions first.** The title fix can be written as `F.regexp_replace(F.trim("title"), r"^(.*), (The|A|An) (\(\d{4}\))$", "$2 $1 $3")`, with identical results on all 9,742 titles and no Python.
2. **pandas / Arrow UDFs** when you need Python: data moves in batches via Apache Arrow (~3× faster than a plain UDF here).
3. **Plain Python UDFs** last.

Always declare the return type, and handle `None` inside the function.

---

## 3.6 Aggregation

Group rows by some criteria (e.g. the same value in a column) and apply **aggregation functions** to one or more columns: **one result per group**. All of them are in `pyspark.sql.functions`.

```
ratings ─ groupBy("userId") ─► GroupedData (not a DataFrame!) ─ agg(count, avg…) ─► DataFrame, one row per group
                                 user 1: 4.0 · 5.0 · 3.0 …                          userId │   n │  avg
                                 user 2: 3.0 · 4.5 …                                     1 │ 232 │ 4.37
                                 user 3: 0.5 …                                           2 │  29 │ 3.95
                                                                                         3 │  39 │ 2.44
```

Grouping is a wide transformation: rows with the same key must meet in one partition (shuffle), after a partial aggregation in each partition.

**Grouping options:** summarise (no grouping) · summarise groups (`groupBy`) · multi-level (**pivot**, rollup, cube) · windows · user-defined aggregate functions. Rollup, cube, windows and UDAFs aren't covered in depth (a quick peek at the end of this section).

### Aggregation functions

```python
ratings.select(
    F.count("*").alias("rows"), F.countDistinct("userId").alias("users"),
    F.approx_count_distinct("movieId").alias("movies_approx"), F.countDistinct("movieId").alias("movies_exact"),
    F.min("rating").alias("min"), F.max("rating").alias("max"), F.round(F.avg("rating"), 3).alias("avg"),
    F.round(F.stddev("rating"), 3).alias("stddev"), F.round(F.variance("rating"), 3).alias("var"),
    F.sum("rating").alias("sum")).show()
```
```
+------+-----+-------------+------------+---+---+-----+------+-----+--------+
|  rows|users|movies_approx|movies_exact|min|max|  avg|stddev|  var|     sum|
+------+-----+-------------+------------+---+---+-----+------+-----+--------+
|100836|  610|         9420|        9724|0.5|5.0|3.502| 1.043|1.087|353083.0|
+------+-----+-------------+------------+---+---+-----+------+-----+--------+
```

| Function | Returns |
|---|---|
| `count(col)` | number of items in a group; `"*"` counts rows |
| `countDistinct(*cols)` | exact number of distinct values |
| `approx_count_distinct(col)` | approximate (HyperLogLog), much cheaper on huge data; default max error 5% (here 3%: 9,420 vs 9,724) |
| `min`, `max`, `sum` | as expected |
| `avg` = `mean` | the average (equivalent names) |
| `stddev`, `variance` | **sample** versions (n − 1); `stddev_pop`, `var_pop` also exist |
| `collect_list`, `collect_set` | the values as an array, with / without duplicates |

**Two details that bite:**

```python
n = spark.createDataFrame([(1, 8.3), (2, None), (3, 7.1)], "id INT, imdb DOUBLE")
n.select(F.count("*"), F.count("imdb"), F.avg("imdb")).show()      # 3 · 2 · 7.7
```

- `count("*")` counts **rows** (NULLs included); `count("col")` counts **non-NULL** values; `avg`, `sum`… **ignore NULLs** ((8.3 + 7.1) / 2 = 7.7).
- `collect_list` keeps duplicates, `collect_set` removes them, **in no guaranteed order**: Star Wars IV has 26 tags in the list and 18 in the set. Both bring a whole group into one row: careful with huge groups.

### Summarising groups: `groupBy` + `agg`

Grouping is a two-step process: `DataFrame.groupBy(cols)` returns a **`GroupedData`** object (not a DataFrame); `GroupedData.agg(functions)` returns a DataFrame that can be chained with more transformations and actions.

```python
(ratings.groupBy("userId")
        .agg(F.count("*").alias("n"), F.round(F.avg("rating"), 2).alias("avg"),
             F.min("rating").alias("min"), F.max("rating").alias("max"))
        .orderBy(F.desc("n")).show(5))
# 414 | 2698 | 3.39 · 599 | 2478 | 2.64 · 474 | 2108 | 3.4 · 448 | 1864 | 2.85 · 274 | 1346 | 3.24
```

`ratings.groupBy("rating").count()`: 0.5 → 1,370 · 1.0 → 2,811 · 1.5 → 1,791 · 2.0 → 7,551 · 2.5 → 5,550 · 3.0 → 20,047 · 3.5 → 13,136 · **4.0 → 26,818** · 4.5 → 8,551 · 5.0 → 13,211. Whole stars are far more popular than half stars.

**Groups + explode + join: statistics per genre**

```python
genre_rows = movies.withColumn("genre", F.explode(F.split("genres", r"\|")))
(ratings.join(genre_rows, "movieId").groupBy("genre")
   .agg(F.count("*").alias("ratings"), F.countDistinct("movieId").alias("movies"),
        F.round(F.avg("rating"), 2).alias("avg"))
   .orderBy(F.desc("ratings")).show(6))
```
```
|    Drama|  41928|  4349|3.66|
|   Comedy|  39053|  3753|3.38|
|   Action|  30635|  1828|3.45|
| Thriller|  26452|  1889|3.49|
|Adventure|  24161|  1262|3.51|
|  Romance|  18124|  1591|3.51|
```

Put several aggregations in the same `agg()` (one pass over the data); `groupBy("a", "b")` groups by several columns.

### Multi-level aggregation: `pivot`

Pivot is an aggregation where the **distinct values** of a grouping column are transposed into **individual columns**. It returns a `GroupedData`, so any aggregation can follow.

```python
top4 = ["Drama", "Comedy", "Action", "Thriller"]
(ratings.join(genre_rows, "movieId")
   .withColumn("year", F.year(F.from_unixtime("timestamp")))
   .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
   .groupBy("decade").pivot("genre", top4)
   .agg(F.round(F.avg("rating"), 2)).orderBy("decade").show())
```
```
+------+-----+------+------+--------+
|decade|Drama|Comedy|Action|Thriller|
+------+-----+------+------+--------+
|  1990| 3.76|  3.53|  3.51|    3.59|
|  2000| 3.63|  3.35|  3.38|    3.42|
|  2010| 3.66|   3.4|   3.5|    3.55|
+------+-----+------+------+--------+
```

(Decade in which the rating was given.) Passing the list of values skips an extra job that finds the distinct values.

**Pivot + `when`: opinions per year**

```python
(ratings.withColumn("year", F.year(F.from_unixtime("timestamp")))
        .withColumn("opinion", F.when(F.col("rating") >= 4, "liked")
                                .when(F.col("rating") <= 2, "disliked").otherwise("meh"))
        .groupBy("year").pivot("opinion", ["liked", "meh", "disliked"]).count()
        .orderBy(F.desc("year")).show(5))
# 2018 | 2879 | 2388 | 1151 · 2017 | 3224 | 3810 | 1164 · 2016 | 3702 | 2351 | 650 · …
```

### A peek beyond: rollup and windows

```python
(ratings.withColumn("liked", F.col("rating") >= 4).rollup("liked").count().orderBy("liked").show())
# NULL → 100836 (grand total) · false → 52256 · true → 48580

from pyspark.sql import Window
w = Window.partitionBy("userId").orderBy(F.desc("rating"), "movieId")
(ratings.withColumn("rank", F.row_number().over(w))
        .filter("rank <= 2 AND userId IN (1, 2)").join(movies, "movieId")
        .select("userId", "rank", "title", "rating").show())
# 1 | 1 | Seven (a.k.a. Se7en) (1995) · 1 | 2 | Usual Suspects, The (1995)
# 2 | 1 | Step Brothers (2008)        · 2 | 2 | Inside Job (2010)
```

Windows aggregate **without collapsing rows**: rankings, running totals, "top N per group".

---

## 3.7 Joins

### Data federation

A join combines data from a **left** DataFrame with a **right** one, wherever each comes from (CSV in HDFS, Parquet in MinIO/S3, a PostgreSQL table over JDBC…). This is possible because every source is abstracted as a DataFrame, which gives a **common format** (relational) and one **set of functions** for transformations and aggregations. Rows are combined by comparing **keys** on both sides and evaluating a **join expression**, usually an equality.

### Shuffle join vs broadcast join

| | Shuffle join (big ⋈ big) | Broadcast join (big ⋈ small) |
|---|---|---|
| How | Both sides are shuffled by key so matching rows meet; every node talks to every other node | The small table is copied once to every executor; each joins its own partitions locally |
| Cost | Network-heavy (can congest the network, e.g. with badly partitioned data) | No shuffle of the big table |

```python
ratings.join(movies, "movieId").explain()
# BroadcastHashJoin [movieId], [movieId], Inner, BuildRight
#   :- FileScan csv ratings …
#   +- BroadcastExchange …
#      +- FileScan csv movies …
```

`movies` (~0.5 MB) is under `spark.sql.autoBroadcastJoinThreshold` (**10 MB** by default), so Spark broadcasts it automatically. With `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)` the plan becomes a **SortMergeJoin** with an `Exchange hashpartitioning(movieId, 200)` and a `Sort` on each side. Force a broadcast with `ratings.join(F.broadcast(movies), "movieId")`. Adaptive Query Execution can also switch to broadcast at runtime.

### Join types

`DataFrame.join(other, on, how)`:

- `other`: the right side of the join.
- `on`: a column name, a list of names, a join expression or a list of Columns.
- `how`: `inner` (default), `cross`, `outer` / `full` / `full_outer`, `left` / `left_outer`, `right` / `right_outer`, `left_semi`, `left_anti`.

| Type | Result |
|---|---|
| inner | matches only |
| left | all of A, NULLs where B is missing |
| right | all of B, NULLs where A is missing |
| full | everything from both |
| left_semi | rows of A **with** a match, A's columns only |
| left_anti | rows of A **without** a match, A's columns only |
| cross | every row with every row (`crossJoin()`) |

> **Correction:** the original diagram showed a "Right Anti" join. Spark has no `right_anti`: swap the DataFrames and use `left_anti`.

**Every join type on two tiny DataFrames** (`m`: movies 1 Toy Story, 2 Jumanji, 3 Heat, 4 Casino; `r`: ratings (1, 4.0), (1, 5.0), (3, 3.5), (9, 2.0)):

| how | rows |
|---|---|
| inner | (1, Toy Story, 4.0) · (1, Toy Story, 5.0) · (3, Heat, 3.5) |
| left | inner rows + (2, Jumanji, NULL) · (4, Casino, NULL) |
| right | inner rows + (9, NULL, 2.0) |
| full | inner rows + (2, Jumanji, NULL) · (4, Casino, NULL) · (9, NULL, 2.0) |
| left_semi | (1, Toy Story) · (3, Heat) |
| left_anti | (2, Jumanji) · (4, Casino) |
| cross | 4 × 4 = 16 rows |

### Join gotchas and real questions

```python
j = m.join(r, m.movieId == r.movieId)      # join expression
j.columns       # ['movieId', 'title', 'movieId', 'rating']
j.select("movieId")
# AnalysisException: [AMBIGUOUS_REFERENCE] Reference `movieId` is ambiguous, could be: [`movieId`, `movieId`].
```

Join on the **name** (`"movieId"` or a list) to get a single key column, or say which one: `j.select(m.movieId, "title", "rating")`, or use `m.alias("a")` with `F.col("a.movieId")`.

```python
ratings.join(movies, "movieId").count()                  # 100836
movies.join(ratings, "movieId", "left_anti").count()     # 18 movies nobody rated
# 1076 Innocents, The (1961) · 2939 Niagara (1953) · 3338 For All Mankind (1989) …
movies.join(tags, "movieId", "left_semi").count()        # 1572 movies with at least one tag
```

An inner join with `tags` would repeat each movie once per tag; `left_semi` just filters.

---

## 3.8 Sets and Sampling

### Union

- `union(other)` (alias `unionAll`) stacks rows **by position** and **keeps duplicates** (SQL `UNION ALL`). Add `.distinct()` for SQL `UNION`.
- `unionByName(other)` matches columns **by name**; `allowMissingColumns=True` fills missing columns with NULL.

```python
x = spark.createDataFrame([(1, "Toy Story")], "movieId INT, title STRING")
y = spark.createDataFrame([("Jumanji", 2)], "title STRING, movieId INT")
x.union(y).show()          # movieId = "Jumanji", title = "2"   😱
x.unionByName(y).show()    # movieId = 2, title = "Jumanji"      ✓
```

Different numbers of columns with `union` raise `[NUM_COLUMNS_MISMATCH]`. Prefer `unionByName` whenever the DataFrames come from different sources.

### Intersect and except

What do user 1 (232 movies) and user 5 (44 movies) have in common?

```python
a = ratings.filter("userId = 1").select("movieId")
b = ratings.filter("userId = 5").select("movieId")
a.union(b).count()             # 276 (with duplicates)
a.union(b).distinct().count()  # 263
a.intersect(b).count()         # 13
a.exceptAll(b).count()         # 219
```

| Method | Rows in… | Duplicates |
|---|---|---|
| `intersect` | both | removed |
| `intersectAll` | both | kept (minimum count) |
| `exceptAll` | left, not right | kept: [1,1,2] − [1] = [1,2] |
| `subtract` | left, not right | removed: [1,1,2] − [1] = [2] |

### Sampling

`sample(withReplacement=False, fraction, seed=None)` returns a sampled subset. `fraction` is a **probability per row**, not an exact size:

```python
ratings.sample(fraction=0.1, seed=1).count()   # 10017
ratings.sample(fraction=0.1, seed=2).count()   # 10181
ratings.sample(fraction=0.1, seed=3).count()   # 10064
ratings.sample(True, 0.1, seed=42).count()     # 10140 (with replacement: rows can repeat)
```

`randomSplit(weights, seed)` splits **every row into exactly one** part, typically for **train / validation / test** sets in machine learning. Weights should sum to 1 (otherwise they're normalised).

```python
train, valid, test = ratings.randomSplit([0.7, 0.15, 0.15], seed=42)
# (70549, 15090, 15197) → 100836 in total
```

---

## 3.9 Null Handling

**NULL** represents missing or empty data. Spark optimises it, so prefer NULL over empty strings or magic values like −1. Handling nulls is especially important in **machine learning**, as not every algorithm supports them. They come from **missing values** in the source and from **outer joins**. In MovieLens: `links.tmdbId` has 8 NULLs; `movies` left-joined with average ratings has 18.

**Comparisons with NULL give NULL, not false:**

```python
q = spark.createDataFrame([(None,), (1.0,)], "v DOUBLE")
q.select("v", (F.col("v") == F.lit(None)).alias("eq_null"),
         F.col("v").isNull().alias("isNull"), F.col("v").eqNullSafe(None).alias("eqNullSafe")).show()
```
```
+----+-------+------+----------+
|   v|eq_null|isNull|eqNullSafe|
+----+-------+------+----------+
|NULL|   NULL|  true|      true|
| 1.0|   NULL| false|     false|
+----+-------+------+----------+
```

`q.filter(F.col("v") != 1.0).count()` returns **0**: `NULL != 1.0` is NULL, so that row is dropped too. Never test with `== None`: use `isNull()` / `isNotNull()` / `F.isnull(col)`, or `eqNullSafe` (SQL `<=>`).

### Detect NULLs

```python
avg = ratings.groupBy("movieId").agg(F.round(F.avg("rating"), 2).alias("avg"), F.count("*").alias("n"))
mv = movies.join(avg, "movieId", "left")                 # creates NULLs

mv.filter(F.isnull("avg")).select("title", "avg", "n").show(3)
# Innocents, The (1961) | NULL | NULL · Niagara (1953) | NULL | NULL · …

mv.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in mv.columns]).show()
# movieId 0 · title 0 · genres 0 · avg 18 · n 18
```

The NULL-count line is worth keeping as a snippet for every new dataset.

### Fix NULLs

**`fillna(value, subset)`** ("fill Not Available") replaces NULLs. The value must be an int, float, boolean or string, and it only fills columns of a matching type. A dict sets one value per column. `df.na.fill` is the same; `df.na.replace` replaces non-NULL values.

```python
mv.fillna(0, subset=["n"]).fillna({"avg": -1.0}).filter("n = 0").show(3)
# Innocents, The (1961) | -1.0 | 0 …
mv.select("title", F.coalesce("avg", F.lit(0.0)))      # coalesce: first non-NULL value, per row
```

**`dropna(how="any", thresh=None, subset=None)`** removes rows with NULLs:

| Call on `mv` (9,742 rows) | Rows left |
|---|---|
| `dropna()`: any NULL | 9,724 |
| `dropna(how="all")`: only if every value is NULL | 9,742 |
| `dropna(subset=["avg"])` | 9,724 |
| `dropna(thresh=4)`: keep rows with ≥ 4 non-NULL values (overrides `how`) | 9,724 |

> **Fill or drop?** Dropping loses rows; filling with 0 can bias averages. Decide per column, and document it.

---

## Summary: Cheat Sheet

| Area | Functions |
|---|---|
| 🔣 Booleans & literals | `F.lit(v)` · `F.when(c, v).when(…).otherwise(v)` |
| 🔢 Numbers | `round` (HALF_UP) · `bround` (HALF_EVEN) · `floor` · `ceil` · `abs` · `sqrt` · `pow` · `corr(a, b)` · `monotonically_increasing_id()` |
| 🔤 Strings | `concat` · `concat_ws(sep, …)` · `lower` · `upper` · `initcap` · `trim`/`ltrim`/`rtrim` · `lpad`/`rpad` · `substring(c, pos, len)` · `length` · `split(c, regex)` |
| 📅 Dates | `current_date` · `current_timestamp` · `year` · `month` · `dayofweek` · `hour` · `date_add` · `date_sub` · `datediff` · `to_date` / `to_timestamp(c, fmt)` |
| 📚 Arrays & UDFs | `getItem(i)` · `c[i]` · `size` · `array_contains` · `explode` · `F.udf(f, type)` · `pandas_udf` (built-in first!) |
| Σ Aggregation | `count` · `countDistinct` · `approx_count_distinct` · `min` · `max` · `sum` · `avg` · `stddev` · `collect_list` · `collect_set` · `groupBy(…).agg(…)` · `.pivot(c, values)` |
| 🔗 Joins & sets | `join(other, on, how)`: inner, left, right, full, left_semi, left_anti, cross · `F.broadcast(small)` · `unionByName` · `intersect` · `exceptAll` |
| ∅ Nulls & sampling | `isNull` · `isnull` · `eqNullSafe` · `fillna(v, subset)` · `coalesce` · `dropna(how, thresh, subset)` · `sample(fraction, seed)` · `randomSplit([w…], seed)` |

### Try it yourself on MovieLens

| # | Task | Check your answer |
|---|---|---|
| ① Aggregate + join | Which movie has the **most tags**? `groupBy` on `tags`, `join` with `movies`, then sort (**after** the join) | Pulp Fiction (1994), 181 tags |
| ② Strings | Which **release year** has the most movies? `trim` + `substring(…, -5, 4)` + `cast` | 2002 with 310 (then 2006: 295, 2001: 294) |
| ③ Sets | How many users rated **both** Toy Story (movieId 1) and Jumanji (movieId 2)? | 68 (215 rated Toy Story, 110 Jumanji) |

**Bonus:** build a table with every movie, its average rating and number of ratings, where never-rated movies show `n = 0` and `avg = NULL`. Which join type do you need, and which NULLs do you fill?

---

## Links & References

- [PySpark API: functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [PySpark API: DataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Datetime patterns](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
- [Performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) (broadcast joins, AQE)
- [Apache Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html) (pandas UDFs)
- Bill Chambers and Matei Zaharia, *Spark: The Definitive Guide*, chapters 6 (Working with Different Types of Data), 7 (Aggregations) and 8 (Joins)

---
