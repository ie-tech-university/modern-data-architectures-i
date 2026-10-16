# Modern Data Architectures for Big Data I
## Lab 7.1 — MovieLens Batch Data Ingestion

*NiFi 2 + HDFS on OSBDET 2026 · Updated October 2026*

---

## Agenda

1. **Introduction to the lab** — the dataset and what we'll build
2. **Raw zone preparation** — a data lake folder in HDFS
3. **Landing zone preparation** — drop the files where NiFi can find them
4. **Batch data ingestion** — import, understand and run the NiFi flow
5. **Course environment shutdown** — plus challenges and troubleshooting

---

## Time to turn OSBDET on!

1. **Start the VM** — VirtualBox: *Start without GUI* · UTM: *Play*
2. **Open the WebUI** — `http://localhost:2026`
3. **Download the lab files** — `movielens-resources.zip` and the NiFi flow (`Batch_Data_Ingestion.json`) from Blackboard

---

## 1. Introduction to the Lab

### The MovieLens dataset

Movie ratings from [MovieLens](https://movielens.org/), a movie recommendation service run by [GroupLens](https://grouplens.org/datasets/movielens/) (University of Minnesota). We use the **small** version (`ml-latest-small`): **100,836 ratings** and **3,683 tags** across **9,742 movies** by **610 users**.

| File | Columns | Rows | Size |
|---|---|---|---|
| `movies.csv` | movieId, title, genres | 9,742 | 483 KB |
| `ratings.csv` | userId, movieId, rating, timestamp | 100,836 | 2.4 MB |
| `tags.csv` | userId, movieId, tag, timestamp | 3,683 | 116 KB |
| `links.csv` | movieId, imdbId, tmdbId | 9,742 | 193 KB |
| `ml-youtube.csv` | youtubeId, movieId, title (trailers) | — | — |

```
movieId,title,genres
1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
2,Jumanji (1995),Adventure|Children|Fantasy
```

### The lab's data value chain

```
SOURCES          INGESTION              STORAGE                      PROCESSING
CSV files  ───►  Apache NiFi      ───►  HDFS                         (next labs)
                 List → Fetch → Put     /datalake/raw/movielens
```

### From the landing zone to the raw zone

```
LANDING ZONE (VM disk, Jupyter)               RAW ZONE (HDFS, data lake)
/home/osbdet/notebooks/landing/               /datalake/raw/
└── movielens/                  ── NiFi ──►   └── movielens/
    ├── movies/movies.csv     (moves each         ├── movies/movies.csv
    ├── ratings/ratings.csv    file, deletes      ├── ratings/ratings.csv
    ├── tags/tags.csv          it from landing)   ├── tags/ …
    ├── links/links.csv                           ├── links/ …
    └── trailers/ml-youtube.csv                   └── trailers/ …
                                              pattern: /datalake/raw/<source>/<dataset>/
```

Exactly the zones from lecture 7.1: data arrives in **landing** and is stored **as-is** in **raw**.

### Lab outline

1. **Raw zone** — create the data lake folder in HDFS
2. **Landing zone** — copy the MovieLens files into the VM
3. **Ingestion** — import the NiFi flow, understand it, run it
4. **Shut down** — stop everything cleanly

**What you'll run:** `ListFile → FetchFile → UpdateAttribute → PutHDFS`

---

## 2. Raw Zone Preparation

### Step 1 — Start Hadoop

HDFS is the **storage side of Hadoop**. In `localhost:2026`, turn the **Hadoop** switch on. The **HDFS Web UI** lets you manage files and folders from your browser.

### Steps 2–3 — Create the data lake folder

1. Open the HDFS Web UI: **`localhost:50070`** → *Utilities → Browse the file system*
2. Click the "create folder" icon and type **`datalake/raw/movielens`** (without the first slash)

Or from a terminal:

```bash
hdfs dfs -mkdir -p /datalake/raw/movielens
```

---

## 3. Landing Zone Preparation

### Step 1 — Extract movielens-resources.zip on your laptop

```
movielens-resources/
├── links/links.csv
├── movies/movies.csv
├── ratings/ratings.csv
├── tags/tags.csv
└── trailers/ml-youtube.csv
```

**Why one folder per file?** The folder name becomes the **dataset** name in the data lake, and each dataset can later receive many files (one per day, for example).

### Steps 2–4 — Build the landing zone in Jupyter

1. In OSBDET's Jupyter (`localhost:28888/lab`), create the **`landing`** folder
2. Inside it, create **`movielens`**
3. **Drag & drop** the 5 folders from your laptop into `landing/movielens`

> **All lowercase!** NiFi looks for exactly `landing`, and folder names become part of the data lake paths.

### Step 5 — Validate your landing zone

Jupyter's root is `/home/osbdet/notebooks` inside the VM. From a Jupyter terminal:

```bash
$ find ~/notebooks/landing -type f
…/landing/movielens/links/links.csv
…/landing/movielens/movies/movies.csv
…/landing/movielens/ratings/ratings.csv
…/landing/movielens/tags/tags.csv
…/landing/movielens/trailers/ml-youtube.csv
```

5 files? You're ready for NiFi.

---

## 4. Batch Data Ingestion

### Steps 1–3 — Start NiFi and import the flow

1. **Start NiFi** in the WebUI (it can take a few minutes)
2. **Open the NiFi UI** — `localhost:29090/nifi`
3. **Create a Process Group from the flow definition**: drag the Process Group icon, click the upload icon, pick `Batch_Data_Ingestion.json`, and name it **Batch Data Ingestion** (no underscores)

### Step 4 — What's inside the Process Group

```
[List Files In "Landing Zone"] ─success─► [Fetch & Delete File From Landing] ─success─►
        ListFile                                   FetchFile

[Add Dataset & Source Name] ─success─► [Put File in "Raw Zone"]
        UpdateAttribute                        PutHDFS
```

**Find** files → **read** them → **label** them → **store** them.

### Step 5 — ListFile: discover files in landing

| Property | Value |
|---|---|
| Input Directory | `/home/osbdet/notebooks/landing` |
| Listing Strategy | Tracking Timestamps |
| Recurse Subdirectories | `true` |
| Input Directory Location | Local |
| File Filter | `[^\.].*` |

- Scans landing and all its subfolders
- Creates **one FlowFile per file** with **attributes only** (no content yet): `filename`, `path`, `absolute.path`…
- **Tracking Timestamps** remembers what it already listed, so each file is listed once
- `[^\.].*` skips hidden files

#### Why List + Fetch, not just "get"?

A common NiFi pattern: **list** cheaply (metadata only), then **fetch** the heavy content. Listing is fast and stateful; fetching can run in parallel, and in a cluster one node lists while all nodes fetch. Same pattern: ListS3 → FetchS3Object, ListSFTP → FetchSFTP.

### Step 6 — FetchFile: read (and delete) each file

| Property | Value |
|---|---|
| File to Fetch | `${absolute.path}/${filename}` |
| Completion Strategy | **Delete File** |
| Move Conflict Strategy | Rename |
| File not Found Log Level | ERROR |

`${…}` is the **NiFi Expression Language**: it reads the FlowFile's attributes.

- `${absolute.path}` → `/home/osbdet/notebooks/landing/movielens/movies/`
- `${filename}` → `movies.csv`

> **Delete File:** once fetched, the file disappears from landing. That's intended: an empty landing zone means everything was ingested.

### Step 7 — UpdateAttribute: add dataset and source

| New attribute | Expression |
|---|---|
| `dataset` | `${path:substringBeforeLast('/'):substringAfterLast('/')}` |
| `source` | `${path:substringBefore('/')}` |

Worked example with `path = movielens/movies/` (set by ListFile):

```
dataset:  "movielens/movies/" ─substringBeforeLast('/')→ "movielens/movies" ─substringAfterLast('/')→ "movies"
source:   "movielens/movies/" ─substringBefore('/')→ "movielens"
```

Functions chain left to right with `:`.

### Step 8 — PutHDFS: write to the raw zone

| Property | Value |
|---|---|
| Hadoop Configuration Resources | `/opt/hadoop3/etc/hadoop/core-site.xml` |
| Directory | `/datalake/raw/${source}/${dataset}` → `/datalake/raw/movielens/movies` |
| Conflict Resolution Strategy | replace |
| Writing Strategy | Write and rename |

- **core-site.xml** tells NiFi where HDFS is
- **replace**: re-ingesting a file overwrites it
- **Write and rename**: write to a temporary name first, so nobody reads half a file

### Follow one FlowFile: movies.csv

| After… | Attributes | Content |
|---|---|---|
| **ListFile** | `filename=movies.csv`, `path=movielens/movies/`, `absolute.path=…` | none |
| **FetchFile** | same | 483 KB (`movieId,title,genres…`); file deleted from landing |
| **UpdateAttribute** | + `dataset=movies`, `source=movielens` | unchanged |
| **PutHDFS** | — | stored at `/datalake/raw/movielens/movies/movies.csv` |

Content is never transformed: the raw zone keeps data **as-is**; only attributes decide *where* it goes.

### Step 9 — Start the ingestion

Press **Start ▶** in the Operation palette (or right-click the Process Group → **Start**). **5 files** flow through each processor in a few seconds; refresh Jupyter and the landing folders are **empty**.

### Steps 10–11 — Validate the data lake

In the HDFS Web UI, browse `/datalake/raw/movielens`: you'll see `links`, `movies`, `ratings`, `tags`, `trailers`. Open `movies` → `movies.csv` → **Head the file**. Or:

```bash
$ hdfs dfs -ls -R /datalake/raw/movielens
$ hdfs dfs -cat /datalake/raw/movielens/movies/movies.csv | head -3
movieId,title,genres
1,Toy Story (1995),Adventure|Animation|…
2,Jumanji (1995),Adventure|Children|Fantasy
```

The file in HDFS is 494,431 bytes, exactly the original.

### Bonus — query the raw zone from Jupyter

```python
# in OSBDET's Jupyter (Spark on): top-rated movies
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()

raw = "hdfs:///datalake/raw/movielens"   # default HDFS
movies  = spark.read.csv(f"{raw}/movies",  header=True)
ratings = spark.read.csv(f"{raw}/ratings", header=True, inferSchema=True)

(ratings.groupBy("movieId")
        .agg(F.avg("rating").alias("avg"), F.count("*").alias("n"))
        .filter("n >= 100")
        .join(movies, "movieId")
        .orderBy(F.desc("avg"))
        .select("title", F.round("avg", 2).alias("avg"), "n")
        .show(3, truncate=False))
```

```
Shawshank Redemption, The (1994)  4.43  317
Godfather, The (1972)             4.29  192
Fight Club (1999)                 4.27  218
```

---

## 5. Course Environment Shutdown

1. **Stop the Process Group** — right-click → *Stop* (otherwise NiFi restarts it next time it boots)
2. **Stop NiFi** — WebUI switch
3. **Stop Hadoop** — WebUI switch (your HDFS files are kept)
4. **Stop OSBDET** — power button in the WebUI (VirtualBox: *Stop → Shut Down*)

---

## Challenges

- **A · Partition by ingestion date:** change PutHDFS's Directory to `/datalake/raw/${source}/${dataset}/${now():format('yyyy/MM/dd')}`
- **B · Ingest again:** copy `movies.csv` into landing again. Does it arrive? Why does Tracking Timestamps matter?
- **C · A new source:** create `landing/tfl/bikepoint/`, drop a BikePoint JSON there. Where does it land in HDFS, without touching the flow?

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| ListFile finds nothing | Folder not called exactly `landing`, or files in the wrong place | `find ~/notebooks/landing -type f` |
| PutHDFS errors / files stuck in a queue | Hadoop isn't running | Start Hadoop; files retry automatically |
| Files end up in `/datalake/raw/movielens/movielens/` | CSVs copied without their dataset folder | Keep `landing/movielens/<dataset>/file` |
| Landing is empty after the run | Expected: FetchFile deletes files | Look in HDFS |
| A re-copied file isn't ingested | Older timestamp than what ListFile remembers | Touch the file, or clear ListFile's state (right-click → View State) |

---

## Congrats, we're done!

Your first data lake raw zone, filled automatically by NiFi.

---
