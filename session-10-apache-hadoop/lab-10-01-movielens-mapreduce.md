# Modern Data Architectures for Big Data I
## Optional Lab 10.1 — MovieLens MapReduce Data Processing

*Hadoop Streaming + Python on OSBDET 2026 · Updated October 2026*

---

## Agenda

1. **Introduction to the lab** — the business case and what we'll compute
2. **Data set availability verification** — is MovieLens in the data lake?
3. **Number of movies by genre** — your first MapReduce job
4. **Average rating by movie** — a MapReduce job that computes averages
5. **Wrap-up** — shutdown, challenges, troubleshooting

---

## Time to turn OSBDET on!

1. **Start the VM** — VirtualBox: *Start without GUI* · UTM: *Play*
2. **Open the WebUI** — `http://localhost:2026`
3. **Have lab 8.1 done** — MovieLens must already be in HDFS

---

## 1. Introduction to the Lab

### The MovieLens dataset

Movie ratings from [MovieLens](https://movielens.org/), maintained by [GroupLens](https://grouplens.org/datasets/movielens/). Small version: **100,836 ratings**, **3,683 tags**, **9,742 movies**, **610 users**, already ingested into HDFS in lab 8.1.

```
movies.csv                                    ratings.csv
movieId,title,genres                          userId,movieId,rating,timestamp
1,Toy Story (1995),Adventure|Animation|…      1,1,4.0,964982703
2,Jumanji (1995),Adventure|Children|Fantasy   1,3,4.0,964981247
```

### The business case

We want to identify **new opportunities to increase revenue**, which needs a better understanding of the data. Two first questions:

1. **How many movies are there in each genre?** (crowded vs niche genres)
2. **What's the average rating of each movie?** (and how many people rated it)

### The lab's data value chain

```
(lab 8.1)  CSV files ─► NiFi ─► HDFS /datalake/raw/movielens ◄──► Hadoop MapReduce (Hadoop Streaming + Python, YARN)
                                HDFS /jobs/…   ◄── results
```

### Hadoop Streaming: MapReduce in Python

- Hadoop MapReduce's native API is **Java**
- [Hadoop Streaming](https://hadoop.apache.org/docs/current/hadoop-streaming/HadoopStreaming.html) runs **any program** that reads stdin and writes stdout, such as a Python script
- Each printed line is a key-value pair: `key<TAB>value`
- Hadoop **sorts by key** between map and reduce, so the reducer gets each key's lines together

```
HDFS input ─stdin→ mapper.py ─key⇥value→ Hadoop sorts by key ─stdin→ reducer.py ─→ HDFS output
```

### Lab outline

1. **Verify** MovieLens is in HDFS → 2. **Genres job** → 3. **Ratings job** → 4. **Shut down**

Files you'll create: `genres-mapper.py`, `genres-reducer.py`, `movie-rating-mapper.py`, `movie-rating-reducer.py` (provided with this lab).

---

## 2. Data Set Availability Verification

**Steps 1–2.** Start **Hadoop** in `localhost:2026` (HDFS + YARN) and open the HDFS UI at `localhost:50070` → *Utilities → Browse the file system*.

**Steps 3–5.** These must exist:

```
/datalake/raw/movielens/
├── links/
├── movies/movies.csv      ✓
├── ratings/ratings.csv    ✓
├── tags/
└── trailers/
```

From a Jupyter terminal:

```bash
$ hdfs dfs -ls /datalake/raw/movielens/movies /datalake/raw/movielens/ratings
… 494431  /datalake/raw/movielens/movies/movies.csv
… 2483723 /datalake/raw/movielens/ratings/ratings.csv
```

Missing? Run lab 8.1 again, or upload them: `hdfs dfs -put movies.csv /datalake/raw/movielens/movies/`

---

## 3. Number of Movies by Genre

### Design the job

```
input line:  1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
   ↓ map()     one pair per genre
Adventure⇥1  Animation⇥1  Children⇥1  Comedy⇥1  Fantasy⇥1
   ↓ sort()    Hadoop groups all movies' pairs by genre
Adventure ⇥ [1, 1, 1, … 1]
   ↓ reduce()  count them
Adventure ⇥ 1263
```

In SQL: `SELECT genre, COUNT(*) FROM movie_genres GROUP BY genre`.

### The code

```python
#!/usr/bin/env python3
# genres-mapper.py: line → (genre, 1)
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("movieId"):
        continue                          # skip header
    genres = line.rsplit(",", 1)[-1]      # last column
    for genre in genres.split("|"):
        print(f"{genre}\t1")
```

```python
#!/usr/bin/env python3
# genres-reducer.py: sorted pairs → (genre, total)
import sys

current, total = None, 0
for line in sys.stdin:
    genre, count = line.rstrip("\n").split("\t")
    if genre != current:                  # a new key starts
        if current is not None:
            print(f"{current}\t{total}")
        current, total = genre, 0
    total += int(count)
if current is not None:
    print(f"{current}\t{total}")
```

> **Why `rsplit(",", 1)`?** Titles can contain commas, e.g. `11,"American President, The (1995)",Comedy|Drama|Romance`. Genres are always the **last** column.

### Step 1 — Upload and test locally

Drag both files into Jupyter's root (`localhost:28888/lab`), open a terminal and test without Hadoop (the Unix pipe is MapReduce on one machine):

```bash
# map only: the first movie
$ hdfs dfs -cat /datalake/raw/movielens/movies/movies.csv | head -2 | python3 genres-mapper.py
Adventure 1 · Animation 1 · Children 1 · Comedy 1 · Fantasy 1

# the whole job, locally
$ hdfs dfs -cat /datalake/raw/movielens/movies/movies.csv \
    | python3 genres-mapper.py | sort | python3 genres-reducer.py
```

### Step 2 — Run it on Hadoop

```bash
hadoop jar /opt/hadoop3/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files genres-mapper.py,genres-reducer.py \
  -mapper  "python3 genres-mapper.py" \
  -reducer "python3 genres-reducer.py" \
  -input   /datalake/raw/movielens/movies \
  -output  /jobs/movies-genre
```

| Option | Meaning |
|---|---|
| `-files` | Ship the scripts to every node that runs a task |
| `-mapper` / `-reducer` | The commands Hadoop runs for each map and reduce task |
| `-input` | HDFS folder (or files) to read |
| `-output` | HDFS folder to create; **must not exist** yet |

Re-running? `hdfs dfs -rm -r /jobs/movies-genre` first.

### Step 3 — Watch the job in YARN

Open `localhost:28088`: your `streamjob…` application should end as **FINISHED / SUCCEEDED**. Click it to see its map and reduce tasks and logs.

### Step 4 — Read the results

```bash
$ hdfs dfs -ls /jobs/movies-genre
… _SUCCESS
… part-00000
$ hdfs dfs -cat /jobs/movies-genre/part-00000
(no genres listed) 34 · Action 1828 · Adventure 1263 · Animation 611 · Children 664 · Comedy 3756
Crime 1199 · Documentary 440 · Drama 4361 · Fantasy 779 · Film-Noir 87 · Horror 978 · IMAX 158
Musical 334 · Mystery 573 · Romance 1596 · Sci-Fi 980 · Thriller 1894 · War 382 · Western 167
```

- **_SUCCESS**: empty marker file, the job finished
- **part-00000**: one file per reducer, sorted by key
- A movie with 3 genres counts 3 times, so totals add up to more than 9,742

---

## 4. Average Rating by Movie

### Design the job

```
input line:  1,1,4.0,964982703          (userId, movieId, rating, timestamp)
   ↓ map()     keep movieId and rating
1⇥4.0
   ↓ sort()    all ratings of movie 1 together
1 ⇥ [4.0, 4.0, 4.5, 2.5, …]   (215 ratings)
   ↓ reduce()  average and count
1 ⇥ 3.92 ⇥ 215
```

> **Note:** the original lab called this "average movie rating *by user*", but the key is the **movie**: it computes the average rating **of each movie**. Averaging by user is challenge A.

### The code

```python
#!/usr/bin/env python3
# movie-rating-mapper.py: line → (movieId, rating)
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("userId"):
        continue
    user_id, movie_id, rating, ts = line.split(",")
    print(f"{movie_id}\t{rating}")
```

```python
#!/usr/bin/env python3
# movie-rating-reducer.py → (movieId, avg, n)
import sys

def emit(movie, total, n):
    print(f"{movie}\t{total / n:.2f}\t{n}")

current, total, n = None, 0.0, 0
for line in sys.stdin:
    movie, rating = line.rstrip("\n").split("\t")
    if movie != current:
        if current is not None: emit(current, total, n)
        current, total, n = movie, 0.0, 0
    total += float(rating)
    n += 1
if current is not None: emit(current, total, n)
```

The reducer keeps a running **sum** and **count**, never the whole list, so memory stays tiny.

### Steps 1–3 — Upload, run, watch

```bash
# local test first
hdfs dfs -cat /datalake/raw/movielens/ratings/ratings.csv \
  | python3 movie-rating-mapper.py | sort | python3 movie-rating-reducer.py | head -3

# then on Hadoop
hadoop jar /opt/hadoop3/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files movie-rating-mapper.py,movie-rating-reducer.py \
  -mapper  "python3 movie-rating-mapper.py" \
  -reducer "python3 movie-rating-reducer.py" \
  -input   /datalake/raw/movielens/ratings \
  -output  /jobs/movie-rating
```

ratings.csv (2.4 MB) is 5× bigger than movies.csv but far below one 128 MB block, so you'll only see one or two map tasks in YARN.

### Step 4 — Read the results

```bash
$ hdfs dfs -cat /jobs/movie-rating/part-00000 | head -5
1        3.92   215
10       3.50   132
100      2.79   14
100044   4.00   1
100068   3.50   1
$ hdfs dfs -cat /jobs/movie-rating/part-00000 | wc -l
9724
```

- **9,724** movies have at least one rating (18 of the 9,742 have none)
- Movie **1** (Toy Story): average **3.92** from **215** ratings
- Keys are sorted as **text**, not numbers: `1, 10, 100, 100044…`

> **Business insight:** averages from 1 rating are meaningless. With ≥ 100 ratings, the top movie is **Shawshank Redemption (4.43, 317 ratings)**.

---

## 5. Wrap-up

### Shut down

1. **Stop Hadoop** in `localhost:2026` (HDFS files and job results are kept)
2. **Stop OSBDET** — power button in the WebUI (VirtualBox: *Stop → Shut Down*)

### Challenges

- **A · Average rating by user:** emit `userId` as the key. Who is the toughest critic? (user 3 averages 2.44)
- **B · Ratings by year:** convert `timestamp` to a year in the mapper and count ratings per year
- **C · Same lab, in Spark:** see the **Databricks version** of this lab

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Output directory … already exists` | You ran the job before | `hdfs dfs -rm -r /jobs/movies-genre` |
| `PipeMapRed.waitOutputThreads(): subprocess failed` | A Python error in your script | Run the local pipe test; check task logs in YARN |
| A genre called `genres` appears | Header not skipped | Keep the `startswith("movieId")` check |
| Strange genres like `The (1995)"` | Splitting titles with commas | Use `rsplit(",", 1)` |
| Connection refused to port 9000 / 8032 | HDFS or YARN not running | Start Hadoop and wait a minute |
| `No such file` for the jar | Different Hadoop version | `ls /opt/hadoop3/share/hadoop/tools/lib/` |

---

## Congrats, we're done!

Two MapReduce jobs, real insights, all on Hadoop.

---

<details>
<summary><strong>Instructor notes — what changed from the original deck</strong></summary>

- **Kept:** the full flow (business case, value chain, verification in HDFS, two Hadoop Streaming jobs with their design, execution, YARN and results, shutdown).
- **Corrected:** dataset figures (100,836 ratings, 3,683 tags, 9,742 movies, 610 users); the second job is the average rating **by movie**, not by user (the key is movieId); example outputs replaced with real results (Adventure 1263, not 1219; movie 1 = 3.92 from 215 ratings, while the slide showed movie 3 with 215); deprecated `-file` replaced by `-files`; `-mapper "python3 …"` avoids depending on file permissions; the jar is referenced with a wildcard (`hadoop-streaming-*.jar`) instead of `3.4.0`; `-input` points to the folder.
- **Added:** full, tested mapper/reducer code (handles header, CRLF and quoted commas), local pipe testing, an option table, the meaning of `_SUCCESS` and `part-00000`, text vs numeric key sorting, business insight, challenges and troubleshooting.
- **Logos** are not reproduced.

</details>
