# Modern Data Architectures for Big Data I
## Optional Lab 9.1 (Databricks edition) — MovieLens Data Processing with PySpark

*Databricks Free Edition · serverless · Unity Catalog · Updated October 2026*

Companion notebook: **`lab-9-1-movielens-pyspark-databricks.ipynb`**

---

## Agenda

1. **Introduction** — same questions, new platform
2. **Setup** — workspace, notebook, data lake storage
3. **Number of movies by genre** — MapReduce thinking in PySpark
4. **Average rating by movie** — aggregations and business insight
5. **Hadoop vs Databricks** — what changed, and why it matters
6. **Wrap-up** — challenges and troubleshooting

---

## 1. Introduction

### The same lab, a different stack

| | MapReduce lab (OSBDET) | This lab (Databricks) |
|---|---|---|
| Storage | HDFS: `/datalake/raw/movielens` | Unity Catalog volume: `/Volumes/workspace/datalake/raw/movielens` |
| Processing | Hadoop MapReduce via Hadoop Streaming | Apache Spark (PySpark DataFrames and SQL) |
| Code | 4 Python scripts (mapper + reducer × 2) | One notebook, a few lines per question |
| Resources | YARN on your laptop's VM | Serverless compute, managed by Databricks |
| Results | `/jobs/…/part-00000` | CSV files in `…/jobs/` + Unity Catalog tables |

Same questions (movies per genre, average rating per movie) and the same answers.

### Databricks and its Free Edition

**Databricks** is a data and AI platform built by the creators of Apache Spark, on AWS, Azure and Google Cloud: notebooks for PySpark and SQL, **Unity Catalog** to organise and govern data (catalogs, schemas, tables, volumes) and **serverless compute**.

**Free Edition** (what we use): no-cost, for learning and non-commercial use (successor of the old Community Edition); **serverless compute only**, with limited size and usage; **outbound internet restricted to trusted domains** (so we upload the data); one workspace with the default catalog `workspace`.

### The lab's data value chain

```
CSV files ─upload→ UC volume raw (/Volumes/workspace/datalake/raw/movielens) ◄─read/write─► PySpark (notebook, serverless)
                                                                                             └─► jobs volume + UC tables
```

### Same data lake layout, mirrored in Unity Catalog

| OSBDET (HDFS) | Databricks (Unity Catalog) |
|---|---|
| `/datalake` | catalog `workspace` → schema `datalake` |
| `/datalake/raw/movielens/movies/movies.csv` | volume `raw` → `/Volumes/workspace/datalake/raw/movielens/movies/movies.csv` |
| `/datalake/raw/movielens/ratings/ratings.csv` | `/Volumes/workspace/datalake/raw/movielens/ratings/ratings.csv` |
| `/jobs/movies-genre`, `/jobs/movie-rating` | volume `jobs` → `/Volumes/workspace/datalake/jobs/movies-genre`, `…/movie-rating` |

```python
RAW  = "/Volumes/workspace/datalake/raw/movielens"   # = /datalake/raw/movielens
JOBS = "/Volumes/workspace/datalake/jobs"            # = /jobs
```

---

## 2. Setup

### Steps 1–3 — Workspace and notebook

1. **Sign up for Databricks Free Edition** (search "Databricks Free Edition"; sign in with your IE email)
2. **Import the notebook:** **Workspace** → your folder → **⋮ → Import** → `lab-9-1-movielens-pyspark-databricks.ipynb`
3. **Connect to serverless:** top-right **Connect** → **Serverless**

### Step 4 — Create the data lake storage

```python
from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "datalake"
RAW  = f"/Volumes/{CATALOG}/{SCHEMA}/raw/movielens"
JOBS = f"/Volumes/{CATALOG}/{SCHEMA}/jobs"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")        # the "datalake" folder
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")    # raw zone
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.jobs")   # job outputs

for dataset in ["movies", "ratings", "tags", "links"]:
    dbutils.fs.mkdirs(f"{RAW}/{dataset}")                     # one folder per dataset
```

A **volume** is Unity Catalog's place for **files**; **tables** are for structured data. Both live inside a schema.

### Step 5 — Put MovieLens in the raw zone

**Option A · upload (recommended):** **Catalog** → workspace → datalake → **raw** → open `movielens/movies` → **Upload to this volume** → `movies.csv`; same for `ratings.csv` into `movielens/ratings` (files from `movielens-resources.zip`, lab 7.1).

**Option B · download in code:** the notebook tries to fetch `ml-latest-small.zip` from GroupLens; Free Edition may block it (trusted domains only), in which case use option A.

### Step 6 — Verify and load

```python
# = hdfs dfs -ls in the Hadoop lab
for dataset in ["movies", "ratings"]:
    files = [f for f in dbutils.fs.ls(f"{RAW}/{dataset}") if f.name.endswith(".csv")]
    assert files, f"Upload {dataset}.csv first"

# read every file in the folder, like -input in Hadoop
movies  = spark.read.csv(f"{RAW}/movies",  header=True, inferSchema=True)
ratings = spark.read.csv(f"{RAW}/ratings", header=True, inferSchema=True)
print(f"{movies.count():,} movies · {ratings.count():,} ratings")   # 9,742 movies · 100,836 ratings
```

- `header=True` skips the header (the mapper did it by hand)
- Spark's CSV reader handles **titles with commas in quotes**
- `inferSchema=True` gives real types (`movieId` is an integer)

---

## 3. Number of Movies by Genre

| MapReduce | PySpark |
|---|---|
| **map()**: one `(genre, 1)` per genre | `F.explode(F.split("genres", r"\|"))`: one row per (movie, genre) |
| **sort / shuffle** | `.groupBy("genre")` |
| **reduce()**: count | `.agg(F.count("*"))` |

```python
# "map": one row per (movie, genre)
movie_genre  = movies.select("movieId", F.explode(F.split("genres", r"\|")).alias("genre"))
# "sort" + "reduce": group by genre and count
genre_counts = (movie_genre.groupBy("genre")
                .agg(F.count("*").alias("movies"))
                .orderBy(F.desc("movies")))
display(genre_counts)
```

**Result** (identical to the Hadoop job): Drama 4,361 · Comedy 3,756 · Thriller 1,894 · Action 1,828 · Romance 1,596 · Adventure 1,263 · Crime 1,199 · Sci-Fi 980 · … · (no genres listed) 34. In Databricks, `display()` → **+ → Visualization → Bar** draws the chart with no code.

### The same with SQL

```python
movies.createOrReplaceTempView("movies")
display(spark.sql("""
  SELECT genre, COUNT(*) AS movies
  FROM (SELECT explode(split(genres, '\\|')) AS genre FROM movies)
  GROUP BY genre
  ORDER BY movies DESC
"""))
```

### Where the "sort" hides

`genre_counts.explain()` prints the physical plan (bottom to top):

```
Sort by movies
HashAggregate · count                         ← "reduce"
Exchange hashpartitioning(genre)              ← the shuffle = MapReduce's sort
HashAggregate · partial_count                 ← like a combiner
Generate explode(split(genres))               ← "map"
FileScan csv · …/raw/movielens/movies
```

### Save the results: files and a table

```python
# like /jobs/movies-genre in HDFS
(genre_counts.coalesce(1)                  # one file = one reducer
    .write.mode("overwrite")
    .option("header", True)
    .csv(f"{JOBS}/movies-genre"))

# better: a governed table anyone can query with SQL
genre_counts.write.mode("overwrite").saveAsTable("workspace.datalake.movies_genre")
```

Same `_SUCCESS` marker and `part-…csv` file as Hadoop; `mode("overwrite")` makes re-runs safe; the table is a **Delta** table visible in **Catalog**.

---

## 4. Average Rating by Movie

```python
movie_rating = (ratings
    .groupBy("movieId")                              # key = movie
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"),
         F.count("*").alias("num_ratings"))
    .orderBy("movieId"))

print(f"{movie_rating.count():,} movies rated")      # 9,724
display(movie_rating.limit(5))                       # 1 → 3.92 (215) · 2 → 3.43 (110) · 3 → 3.26 (52) …

movie_rating.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{JOBS}/movie-rating")
movie_rating.write.mode("overwrite").saveAsTable("workspace.datalake.movie_rating")
```

`movieId` is a number now, so it sorts numerically (Hadoop sorted text: 1, 10, 100…).

### Business insight: the best-rated movies

```python
top_movies = (movie_rating
    .filter("num_ratings >= 100")      # enough ratings to trust
    .join(movies, "movieId")           # add titles
    .orderBy(F.desc("avg_rating"))
    .select("title", "avg_rating", "num_ratings"))
display(top_movies.limit(5))
```

| title | avg | n |
|---|---|---|
| Shawshank Redemption, The (1994) | 4.43 | 317 |
| Godfather, The (1972) | 4.29 | 192 |
| Fight Club (1999) | 4.27 | 218 |
| Godfather: Part II, The (1974) | 4.26 | 129 |
| Goodfellas (1990) | 4.25 | 126 |

A **join** is one line in Spark; in MapReduce it needs another job or complex code.

---

## 5. Hadoop vs Databricks

| | Hadoop Streaming | PySpark on Databricks |
|---|---|---|
| Code | ≈ 50 lines in 4 files | ≈ 10 lines in one notebook |
| Parsing | By hand: skip header, split lines, handle commas | CSV reader with header and types |
| Grouping | Write the "same key" logic in the reducer | `groupBy` |
| Joins | Another job (or complex code) | `.join()` |
| Re-runs | Delete the output folder first | `mode("overwrite")` |
| Results | Text files | Files and tables with SQL, charts and permissions |
| Infrastructure | Your VM: HDFS + YARN | Serverless: starts and stops for you |

### The RDD version is MapReduce, literally

```python
# runs in OSBDET's Spark or a classic Databricks cluster,
# NOT on serverless compute (no RDD API there)
sc = spark.sparkContext
counts = (sc.textFile("hdfs:///datalake/raw/movielens/movies")
            .filter(lambda l: not l.startswith("movieId"))
            .flatMap(lambda l: l.rsplit(",", 1)[-1].split("|"))  # map
            .map(lambda g: (g, 1))                               # (genre, 1)
            .reduceByKey(lambda a, b: a + b))                   # sort + reduce
print(sorted(counts.collect(), key=lambda kv: -kv[1])[:3])
# [('Drama', 4361), ('Comedy', 3756), ('Thriller', 1894)]
```

`flatMap` + `map` = your mapper; `reduceByKey` = Hadoop's sort + your reducer. Databricks serverless (and Spark Connect) support only **DataFrames and SQL**, which is why the notebook uses them.

---

## 6. Wrap-up

### Challenges (cells ready in the notebook)

- **A · Average rating by user:** group by `userId`, keep users with ≥ 20 ratings. The toughest critic: user 442, 1.28 average
- **B · Ratings per year:** `F.year(F.from_unixtime("timestamp"))`, then count per year
- **C · Best genre:** join `movie_genre` with `movie_rating` and compute a rating per genre weighted by `num_ratings`

**Shutdown?** Nothing to stop: serverless compute shuts itself down when idle. Volumes and tables stay in Unity Catalog.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `CATALOG_NOT_FOUND` / `SCHEMA_NOT_FOUND` | Catalog isn't `workspace`, or setup not run | Check **Catalog** in the sidebar; set `CATALOG`; run the setup cell |
| "Upload movies.csv first" assertion fails | File in the wrong folder | It must be `raw/movielens/movies/movies.csv`, all lowercase |
| The download cell says "not possible" | Free Edition's restricted internet | Use option A (upload) |
| `[NOT_SUPPORTED_WITH_SERVERLESS]` on `sparkContext` | RDD code on serverless | Use DataFrames (or a classic cluster) |
| Genres look wrong | `\|` is regex "or" | Escape it: `r"\|"` in Python, `'\\|'` in SQL |

---

## Congrats, we're done!

Same questions, same answers, a fraction of the code.

---

<details>
<summary><strong>Instructor notes</strong></summary>

- **New lab** derived from Optional Lab 9.1 (MapReduce). Same data, questions and data lake layout: the HDFS paths `/datalake/raw/movielens/<dataset>/` and `/jobs/<job>/` map to Unity Catalog volumes `raw` and `jobs` in schema `workspace.datalake`.
- **Tested:** all DataFrame, SQL and RDD code was run with PySpark against the real `ml-latest-small` files; the results shown are real and match the Hadoop jobs exactly.
- **Free Edition constraints** (from Databricks docs): serverless compute only; outbound internet restricted to trusted domains (hence the upload-first approach); no RDD API on serverless.
- **Not tested inside Databricks:** the Databricks-specific calls (`dbutils`, `display`, `CREATE VOLUME`, `saveAsTable`) were stubbed locally. Run the notebook once in a Free Edition workspace before class.
- If your workspace's default catalog isn't `workspace` (e.g. a university account), change `CATALOG` in the first cell.

</details>
