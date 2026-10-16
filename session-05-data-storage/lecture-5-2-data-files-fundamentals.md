# Modern Data Architectures for Big Data I
## Lecture 5.2 — Data & Files Fundamentals

*Updated edition, October 2026*

---

## Agenda

1. **Data classification** — structured, semi-structured, unstructured
2. **File formats** — text files, binary files, encodings
3. **Big data storage formats** — Parquet, ORC, Avro: a real comparison
4. **Summary**

---

## 1. Data Classification

Data is classified by its **internal organisation**:

```
 STRUCTURED            SEMI-STRUCTURED           UNSTRUCTURED
 rows and columns      self-describing, nested   no predefined format
 tables · CSV          JSON · XML                images · audio · video · text
 ◄── more organised, easier to query        less organised, harder to query ──►
```

### Structured vs unstructured

| | Structured | Unstructured |
|---|---|---|
| Shape | Fits rows and columns | Doesn't fit rows and columns |
| Typical content | Numbers, dates, short strings | Images, audio, video, documents, posts |
| Share of data | About 10–20% | About 80–90% |
| Storage needed | Less | More |
| Management | Easier, mature tools | Harder; AI now helps extract structure |

*Shares are widely cited industry estimates, not exact measurements. Picture an iceberg: structured data is the visible tip.*

---

### 1.1 Structured data

Information with a **high degree of organisation**, in **two dimensions** (rows and columns). Every record of the same table has **exactly the same attributes, in the same order**. *Examples:* relational database tables, CSV files, spreadsheets.

| customer_id | first_name | last_name | email | create_date |
|---|---|---|---|---|
| 1 | John | Doe | jd@gmail.com | 2004-05-07 |
| 2 | Jane | Doe | jd2@gmail.com | 2002-09-24 |
| 3 | John | Smith | js@gmail.com | 2010-03-12 |
| 4 | Jane | Smith | js2@gmail.com | 2008-12-31 |

#### CSV (comma-separated values)

```
customer_id,first_name,last_name,email,create_date     ← HEADER
1,John,Doe,jd@gmail.com,2004-05-07                      ┐
2,Jane,Doe,jd2@gmail.com,2002-09-24                     │ CONTENTS
3,John,Smith,js@gmail.com,2010-03-12                    │
4,Jane,Smith,js2@gmail.com,2008-12-31                   ┘
```

#### Why CSVs are tricky

- **No data types:** is `00123` a number or a postcode? Is `2004-05-07` a date?
- **The header is optional:** the file doesn't say whether line 1 is data.
- **The delimiter varies:** in Spain, Excel often uses `;` because the comma is the decimal separator (`3,14;Madrid`).
- **Commas inside values:** real TfL data, `BikePoints_1,"River Street , Clerkenwell",51.529163`, needs quotes.

```python
import pandas as pd
df = pd.read_csv("customers.csv", sep=";", decimal=",",      # Spanish-style CSV
                 dtype={"postcode": str}, parse_dates=["create_date"])
```

---

### 1.2 Semi-structured data

Information with a **self-describing structure**: each value travels with its name. Often **more than two dimensions** (nested elements and collections); records of the same kind may have **different attributes, in a different order**. *Examples:* JSON, XML, YAML.

```json
[
  { "customer_id": 1, "first_name": "John", "last_name": "Doe",
    "email": "jd@gmail.com", "create_date": "2004-05-07T00:00:00.000Z" },
  { "customer_id": 2, "first_name": "Jane", "last_name": "Doe",
    "email": "jd2@gmail.com", "create_date": "2002-09-24T00:00:00.000Z" }
]
```

#### JSON's flexibility, in four rules

1. **Field order doesn't matter:** `{"customer_id": 1, "first_name": "John"}` = `{"first_name": "John", "customer_id": 1}`
2. **Records may have different fields:** `{"customer_id": 2, "first_name": "Jane", "gender": "female"}` is also valid
3. **Collections with `[ ]`:** `{"customer_id": 1, "emails": ["jd@gmail.com", "john.doe@hotmail.com"]}`
4. **Nested objects with `{ }`:**

```json
{"customer_id": 1,
 "address": {"street": "María de Molina", "number": 11,
             "coordinates": {"lat": 40.437, "lon": -3.686}}}
```

#### Real-world JSON: a TfL bike point

```json
{
  "id": "BikePoints_1",
  "commonName": "River Street , Clerkenwell",
  "additionalProperties": [                     // ← collection
    { "key": "NbBikes", "value": "7", … },      // ← nested objects
    { "key": "NbEmptyDocks", "value": "11", … }
  ],
  "children": [],                                // ← empty collection
  "lat": 51.529163,                              // ← numbers
  "lon": -0.10997
}
```

The data from the **TfL lab** uses every JSON feature. Notice `"value": "7"` is a **string**: JSON keeps types, but only the ones the producer chose.

#### JSON Lines: JSON for big data

| JSON array | JSON Lines (`.jsonl`, `.ndjson`) |
|---|---|
| One big document wrapped in `[ ]` | One complete JSON object per line |
| Must read the whole file before parsing | Easy to split, stream and append |

```
{"id": "BikePoints_1", "NbBikes": 7}
{"id": "BikePoints_2", "NbBikes": 2}
```

Spark, Kafka consumers and log tools prefer JSON Lines: `spark.read.json()` expects one object per line by default.

#### XML (Extensible Markup Language)

```xml
<?xml version="1.0"?>
<customers>
  <customer>
    <customer_id>1</customer_id>
    <first_name>John</first_name>
    <last_name>Doe</last_name>
    <email>jd@gmail.com</email>
  </customer>
  …
</customers>
```

More verbose than JSON (every name appears twice); still common in banking, government, office documents and enterprise integrations. New APIs mostly use JSON.

---

### 1.3 Unstructured data

Data with **no set record format**: it may come in any shape or form. *Examples:* images, audio, video, emails, raw text, logs, web pages, PDFs.

```
logo.png in a text editor:  ‰PNG IHDR ¼ szzô IDATxÚìýÉ$YEO¢²ÜÓ±íô™ÙY¶XÛ»U`m+¸öÿ¿…

server.log:
2026-10-15 21:09:12 INFO  NiFi started in 94 s
2026-10-15 21:10:40 WARN  InvokeHTTP retry: timeout
```

Logs are text, but each application writes them its own way.

> **2026 note:** AI models can now pull structure out of unstructured data: text from PDFs, labels from images, embeddings for search.

---

## 2. File Formats

Data files fall into two families according to how their contents are encoded:

| Text files | Binary files |
|---|---|
| Can only store **text**: letters, numbers, punctuation | Can store **any data** as raw bytes, mostly not human-readable |
| Open with **any text editor** | Meant for a **specific application** |

### Text files depend on encodings

An **encoding** maps characters to bytes:

| Character | UTF-8 | Latin-1 (ISO-8859-1) |
|---|---|---|
| `é` | `c3 a9` | `e9` |

Read UTF-8 bytes as Latin-1 and "José" becomes "JosÃ©" (*mojibake*). **Rule of thumb:** write and read **UTF-8**.

```python
open("clientes.csv", encoding="utf-8")
pd.read_csv("old_export.csv", encoding="latin-1")
```

### Text or binary? Ask the extension…

`<filename>.<extension>` usually tells you the type:

| Common text formats | | Type of data |
|---|---|---|
| `.csv` `.tsv` `.psv` | Comma / tab / pipe separated | structured |
| `.json` `.jsonl` | JSON, JSON Lines | semi-structured |
| `.xml` `.yaml` | XML, YAML | semi-structured |
| `.txt` `.html` `.log` | Plain text, web pages, logs | unstructured |

| Common binary formats | | Type of data |
|---|---|---|
| `.parquet` `.orc` `.avro` | Big data storage formats | structured |
| `.xlsx` `.docx` `.pptx` | Office documents | unstructured* |
| `.pdf` | Portable document format | unstructured |
| `.png` `.jpg` `.gif` `.webp` | Images | unstructured |
| `.mp3` `.ogg` `.mp4` `.webm` | Audio and video | unstructured |

\* A spreadsheet can hold a structured table, but the file itself is a binary office document.

### …or ask the file itself: magic numbers

Extensions can lie. Most binary formats start with a fixed signature:

| Format | First bytes |
|---|---|
| Parquet | `PAR1` (also the last 4 bytes) |
| PDF | `%PDF` |
| PNG | `89 50 4E 47` (‰PNG) |
| gzip | `1F 8B` |
| ZIP, `.xlsx`, `.docx` | `PK` — Office files are ZIP archives! |

```
$ file day.parquet day.csv day.csv.gz day.jsonl
day.parquet: Apache Parquet
day.csv:     CSV ASCII text
day.csv.gz:  gzip compressed data, was "day.csv"
day.jsonl:   New Line Delimited JSON text data
```

---

## 3. Big Data Storage Formats

Binary files come in two types:

- **Data formats:** one kind of content for a specific app (images, audio, PDF, office documents)
- **Storage formats:** tables stored efficiently to improve the **storage and performance** of big data applications (Parquet, ORC, Avro)

### Row vs columnar layout

```
Row-oriented (CSV, Avro):   [1 John 7][2 Jane 2][3 Ana 9]   → great to write/read whole records
Columnar (Parquet, ORC):    [1 2 3][John Jane Ana][7 2 9]   → read only needed columns, compress similar values
```

| Format | Layout | Highlights |
|---|---|---|
| **Apache Parquet** | Columnar | The default analytics format (Spark, pandas, DuckDB, BigQuery, Snowflake, Databricks); basis of Iceberg and Delta tables |
| **Apache ORC** | Columnar | Born in the Hive/Hadoop world; similar ideas to Parquet |
| **Apache Avro** | Row | Compact rows with an embedded, evolvable schema; very common with Kafka |

All three are **self-describing**: the schema travels inside the file.

### A real comparison: one day of TfL bike data

799 stations × a snapshot every 5 minutes × 24 h = **230,112 rows**, 10 columns:

| Format | Size |
|---|---|
| JSON | 46.1 MB |
| JSON Lines | 46.1 MB |
| CSV | 22.4 MB |
| CSV + gzip | 5.7 MB |
| **Parquet** | **0.9 MB** |

Parquet is about **24× smaller than CSV** and **50× smaller than JSON**: station names and coordinates repeat every 5 minutes, and columnar compression loves repetition. Reading **2 columns** from Parquet took **0.01 s** versus **0.24 s** to load the whole CSV on the same laptop.

*Simulated from one real BikePoint response (October 2026) with small random changes per snapshot.*

### Try it yourself: CSV to Parquet

```python
import pandas as pd

df = pd.read_csv("day.csv", parse_dates=["ts"])
df.to_parquet("day.parquet")                       # needs pyarrow

# read only the columns you need: Parquet skips the rest of the file
bikes = pd.read_parquet("day.parquet", columns=["name", "NbBikes"])

# the schema travels inside the file
import pyarrow.parquet as pq
print(pq.read_schema("day.parquet"))
# ts: timestamp[ns]   id: string   name: string   lat: double   NbBikes: int64 …
```

In Spark: `spark.read.csv(...).write.parquet("s3a://datalake/curated/tfl/")`

---

## 4. Summary

- Data has different degrees of organisation: **structured, semi-structured and unstructured**.
- Big data solutions store data in **files** (text or binary) in distributed file systems and object storage.
- **Text files depend on encodings**, which define the characters a file can contain. Use UTF-8.
- **Binary files** come in two types: **data formats** (images, audio, PDF…) and **storage formats** (Parquet, ORC, Avro) that improve storage and performance.
- Rule of thumb for the data lake: land raw data as it arrives (often JSON/CSV), then store curated data as **Parquet**.

---

## Interesting Readings

- [Apache Parquet documentation](https://parquet.apache.org/docs/)
- [Apache ORC documentation](https://orc.apache.org/docs/)
- [Apache Avro documentation](https://avro.apache.org/docs/)
- [JSON Lines](https://jsonlines.org/)
- [Big data formats (original reading)](https://techmagie.wordpress.com/category/big-data/data-formats/)

---
