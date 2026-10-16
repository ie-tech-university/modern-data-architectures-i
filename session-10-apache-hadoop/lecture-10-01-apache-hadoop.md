# Modern Data Architectures for Big Data I
## Lecture 9.2 — Apache Hadoop

*Updated edition, October 2026*

---

## Agenda

1. **Hadoop** — what it is and where it comes from
2. **HDFS** — distributed storage
3. **YARN** — distributed resources
4. **MapReduce** — the model, word count and a hands-on in Python
5. **Summary**

---

## 1. Hadoop

**What is Hadoop?** A technology that gives companies the ability to **store and process huge amounts of data**. It spans two layers of our architecture: storage (HDFS) and processing (MapReduce).

### History: copied from Google's papers

| Google (papers) | → | Hadoop (open source) |
|---|---|---|
| **Google File System** (2003): store files across many computers | Doug Cutting & Mike Cafarella reimplemented them | **HDFS**: Hadoop's counterpart of GFS |
| **MapReduce** (2004): run distributed apps over those files | | **Hadoop MapReduce**: Hadoop's equivalent |

### Hadoop 1 → 2 → 3

```
Hadoop 1.0                 Hadoop 2.x                       Hadoop 3.x
Pig | Hive | Others        MR | Tez | Spark | Streaming     MR | Tez | Spark | …
MapReduce                  YARN (resource management)       YARN (+ federation, timeline v2)
(resources + processing)   HDFS 2                           HDFS 3 (erasure coding)
HDFS
single use: batch          multi-purpose                    up to 50% less storage, more scalable
```

Current release: **Hadoop 3.5**. OSBDET ships Hadoop 3.4.

### Three components (since 2.x)

| Component | Role |
|---|---|
| **HDFS** (Hadoop Distributed File System) | Distributed **storage** of files |
| **YARN** (Yet Another Resource Negotiator) | Distributed **resources** (memory & CPUs) for running applications |
| **MapReduce** | A framework (libraries & tools) to develop **batch** applications |

---

## 2. HDFS

An implementation of the **Google File System** paper: a **file system** designed for **very large files** in **clusters of computers**.

- Organises data in **directories** and stores it in **files**
- How you lay out files **affects scalability and performance** (zones, partitions: lecture 7.1)
- Very large files: **hundreds of MB each** (128 MB blocks by default). Prefer few big files to many small ones

**What HDFS is not for:**

- **Low-latency access:** apps needing answers in tens of milliseconds use **NoSQL** datastores (serving layer). HDFS is optimised for **throughput**, at the expense of latency.
- **Lots of small files:** every file, directory and block costs memory in the **NameNode** (about 150 bytes each) and a separate task to process.

### How it works: blocks and replicas

- Files are stored on many computers: the **DataNodes**
- Each file is split into **blocks**, stored on different DataNodes
- Each block is **replicated 3 times** by default
- The **NameNode** keeps the map of blocks and locations

```
trips.csv (300 MB) = [A 128 MB][B 128 MB][C 44 MB]
NameNode: A → 1,2,3 · B → 2,3,4 · C → 1,3,4
DataNode 1: A C   DataNode 2: A B   DataNode 3: A B C   DataNode 4: B C
```

Blocks let you store **files bigger than any disk** and give **fault tolerance**: lose a DataNode and every block still has 2 copies.

### Hadoop 3: erasure coding

| Scheme | Blocks for 6 blocks of data | Overhead |
|---|---|---|
| 3× replication | 18 | +200% |
| Erasure coding RS(6,3) | 9 (6 data + 3 parity) | +50% |

Same fault tolerance for **about half the storage**, the "up to 50% lower storage cost" of Hadoop 3. Best for cold data.

---

## 3. YARN

- Hadoop's cluster **resource management** system (memory and CPUs)
- An **operating system at cluster level**, dedicated component since **Hadoop 2**
- Lets **other engines** run on a Hadoop cluster: interactive, streaming, real-time, batch (Spark, Tez, Flink…)
- Focuses only on **executing applications** and sharing the cluster's power between them

```
Batch (MapReduce) | Interactive (Tez) | SQL (Hive) | Streaming (Flink) | Spark | Others
YARN · data operating system
HDFS · distributed storage
```

### How YARN runs an application

1. The client submits the application to the **ResourceManager** (one per cluster)
2. It starts an **ApplicationMaster** for that app in a **container** (a slice of memory and CPU) on a **NodeManager**
3. The ApplicationMaster asks for more containers; NodeManagers launch them, ideally where the data lives

---

## 4. MapReduce

### The model

A way of **designing a solution** for a large-scale data processing problem:

```
INPUT ─► MAP ─► SORT ─► REDUCE ─► OUTPUT
```

- **Key-value pairs**: the basic data structure (any type: number, text, bytes, boolean, null…)
- **map** selects, filters or transforms input and emits key-value pairs
- **sort** groups pairs by key (aka shuffle)
- **reduce** aggregates the values of each key (count, sum, max, avg…)
- map and reduce may return none, one or many pairs; developers only code **map** and **reduce**

**MapReduce is like SQL's GROUP BY:**

```sql
SELECT key, reduce([value]) FROM map(input) GROUP BY key
```

### Word count

**Problem:** given some text files, return every word and how often it appears. *Hint:* think backwards: which intermediate table makes the result easy?

```
input                                   map(): (word, 1)
Hamlet, "To be or not to be"        ─►  hamlet 1, to 1, be 1, or 1, not 1, to 1, be 1
Juliet, "Romeo, Romeo, why not      ─►  juliet 1, romeo 1, romeo 1, why 1, not 1, to 1, wait 1
         to wait?"
sort(): group by word                   reduce(): sum
be [1,1] · not [1,1] · romeo [1,1]  ─►  be 2 · not 2 · romeo 2
to [1,1,1] · wait [1] · why [1] …   ─►  to 3 · wait 1 · why 1 …
```

In SQL: `SELECT word, SUM(value) FROM intermediary GROUP BY word`.

### Hadoop MapReduce

- The **model** says nothing about parallelism; **Hadoop MapReduce** runs it distributed thanks to **YARN**
- It's an **API**: you code map and reduce; Hadoop splits and runs the work
- **Parallel + data locality**: each node processes the blocks it stores
- **Fault tolerance**: failed or slow tasks are re-executed on other nodes

### Hands-on: word count in Python

**mapper.py**

```python
import sys, re
for line in sys.stdin:
    for word in re.findall(r"[a-z']+", line.lower()):
        print(f"{word}\t1")        # emit (word, 1)
```

**reducer.py**

```python
import sys
current, total = None, 0
for line in sys.stdin:              # already sorted by word
    word, count = line.rstrip("\n").split("\t")
    if word != current:
        if current: print(f"{current}\t{total}")
        current, total = word, 0
    total += int(count)
if current: print(f"{current}\t{total}")
```

**Test it without Hadoop** (the Unix pipe *is* MapReduce on one machine):

```bash
$ cat hamlet.txt juliet.txt | python3 mapper.py | sort | python3 reducer.py
be 2   hamlet 1   juliet 1   not 2   or 1   romeo 2   to 3   wait 1   why 1
```

**Run it on Hadoop (OSBDET)** with Hadoop Streaming:

```bash
hdfs dfs -mkdir -p /user/osbdet/wordcount/input
hdfs dfs -put hamlet.txt juliet.txt /user/osbdet/wordcount/input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input  /user/osbdet/wordcount/input \
  -output /user/osbdet/wordcount/output

hdfs dfs -cat /user/osbdet/wordcount/output/part-*
```

Watch it in the YARN UI (`localhost:28088`). The output folder must not exist before the run.

### Why MapReduce was overtaken

Hadoop MapReduce writes to HDFS (disk) between every iteration; Spark keeps intermediate data in memory. It has been overtaken by **Apache Spark** and **Apache Flink**. *Benchmark (sorting 100 TB):* Hadoop 72 min on 2,100 machines (2013) vs Spark 23 min on 206 machines (2014).

### The Hadoop ecosystem in 2026

| Piece | Still widely used? | Typical replacement |
|---|---|---|
| HDFS | On-premises clusters | Cloud object storage, MinIO-like stores |
| YARN | On-premises clusters | Kubernetes, Spark standalone, cloud services |
| MapReduce | Legacy | Spark, Flink |
| Hive | Yes, as SQL and metastore | Spark SQL, Trino; Iceberg / Delta catalogs |
| Pig, Oozie, Mahout | Legacy / retired | Spark, Airflow, MLlib |

---

## 5. Summary

- Hadoop is a technology for **storing and batch processing** huge amounts of data
- It's based on **ideas from Google's papers**
- **BIG DATA ⇔ SCALABILITY ⇔ DISTRIBUTED**
- Three components: **HDFS** (storage), **YARN** (resources), **MapReduce** (processing)

**Two great videos:** [Understanding HDFS using Legos](http://www.youtube.com/watch?v=4Gfl0WuONMY) · [Understanding MapReduce using playing cards](http://www.youtube.com/watch?v=bcjSe0xCHbE)

---
