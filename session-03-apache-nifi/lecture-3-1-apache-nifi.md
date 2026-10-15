# Modern Data Architectures for Big Data I
## Lecture 4.2 — Apache NiFi

*Updated edition, October 2026 — NiFi 2*

---

## Agenda

1. **What is NiFi?**
2. **Core components**
3. **NiFi architecture**
4. **Summary**
5. **Alternatives to NiFi**: including Python pipelines with Apache Airflow

---

## 1. What Is NiFi?

> Apache NiFi is an **easy-to-use**, **powerful** and **reliable** system to **ingest and distribute data**, addressing the challenges of **enterprise data flow at scale**.

**NiFi** stands for **Ni**agara**Fi**les. It was created at the US National Security Agency (NSA) and donated to Apache in 2014. The current major version is **NiFi 2** (2.12 in September 2026).

| | |
|---|---|
| **Easy to use** | Flows are designed, configured, controlled and monitored from a **web UI**: drag components onto a canvas and connect them. |
| **Powerful** | Highly configurable, scales to demanding throughputs (>100,000 events/sec), built-in security and **data provenance**, extensible in Java or **Python**. |
| **Reliable** | **Guaranteed delivery** when needed, plus failover and recovery mechanisms. |

**Typical sources:** server logs, clickstreams, application events, IoT, CDC (change data capture), files and APIs.

---

## 2. Core Components

| Concept | In one sentence |
|---|---|
| **FlowFile** | A unit of data moving through the system: content + attributes |
| **Processor** | Does the work: extracts, filters, transforms and loads data |
| **Connection** | A queue linking processors, buffering FlowFiles between them |
| **Flow Controller** | The scheduler that brokers the exchange of FlowFiles between processors |
| **Process Group** | A set of processors and connections bundled together as a new component |

### FlowFile

A FlowFile is **a unit of data moving through the system**: a small record or an entire file. It has two parts:

```
┌───────────────────────────────────┐
│ ATTRIBUTES (key → value metadata) │
│   uuid      = 9f3c…               │
│   filename  = update-1042.json    │
│   path      = ./                  │
│   mime.type = application/json    │
├───────────────────────────────────┤
│ CONTENT (the payload, bytes)      │
│   {"message": {"text": "…"}}      │
└───────────────────────────────────┘
```

**Example:** a Telegram message ingested into NiFi becomes a FlowFile. The message JSON is the **content**; its ID or chat type can be stored as **attributes**.

- **Content** stores the data itself and rarely changes. Transforming the original data is **not** good practice: **enrich** it instead.
- **Attributes** can be added, altered or removed, and are used for **routing** and extra information. Every FlowFile has `uuid`, `filename` and `path`.

### Processor

Processors **perform the work**: some combination of **extraction, filtering, transformation and loading**, with access to both attributes and content. NiFi ships with **almost 300 processors**.

| Verb | Meaning | Examples |
|---|---|---|
| **Get / Fetch / List** | Extract data | GetFile, FetchS3Object, ListSFTP |
| **Consume / Listen** | Receive messages or events | ConsumeKafka, ListenHTTP |
| **Put / Publish** | Load data | PutS3Object, PutDatabaseRecord, PublishKafka |
| Other verbs | Other actions | InvokeHTTP, EvaluateJsonPath, RouteOnAttribute, ConvertRecord |

**Example:** an **InvokeHTTP** processor ("Give me latest updates") calls the Telegram Bot API and creates a FlowFile for each response. Each processor shows live statistics (**In, Read/Write, Out, Tasks/Time**) and routes its output through named **relationships** (Response, Failure, Retry…).

### Connection

Connections **route FlowFiles between processors**, one per relationship. They **act as queues**, so processors can work at **different rates** (buffering). When a queue fills up, NiFi applies **back pressure** (by default at 10,000 FlowFiles or 1 GB).

```
EvaluateJsonPath "Extract information from update"
   │ matched                    │ unmatched
   ▼
RouteOnAttribute "Identify type of chat"
   │ group_chat         │ private_chat
   ▼                    ▼
```

### Flow Controller

The **broker** that schedules processors and hands FlowFiles between them.

```
┌──────────────────────── Flow Controller ────────────────────────┐
│  [P1] ──►  ▢ ▢ ▢ ▢  connection (queue)  ──►  [P2]               │
└─────────────────────────────────────────────────────────────────┘
```

### Process Group

A **set of processors and connections bundled together**, creating a new component by **composition**:

```
P1 → P2 → P3   +   Input port   +   Output port
=  [ P4:  Input → P1 → P2 → P3 → Output ]
```

**Example:** "Extract and classify updates" receives Telegram updates and sends them out through three ports: *group chat*, *private chat* and *private chat command*.

---

## 3. NiFi Architecture

### Standalone: everything on one server

Inside one **JVM**: a **web server** (UI and REST API), the **Flow Controller** (processors and extensions), and three repositories on local storage:

- **FlowFile repository**: the state of every FlowFile (a write-ahead log)
- **Content repository**: the actual bytes
- **Provenance repository**: the history of everything that happened to each FlowFile

Because state lives on disk, NiFi recovers after a crash.

### Multi-node cluster: many servers working together

- Every node runs the **same flow** on a different share of the data.
- A client can talk to **any node**; changes are replicated to all.
- A coordination service elects the **cluster coordinator** (manages membership) and the **primary node** (runs tasks that must happen only once).
- That service is **Apache ZooKeeper** or, with NiFi 2, **Kubernetes**.

---

## 4. Summary

- NiFi is an **easy-to-use, powerful and reliable** system for **enterprise data ingestion at scale**.
- It **protects** ingestion with **buffering** and **back pressure** when data arrives faster than it can be written.
- It provides **guaranteed delivery** and **data provenance**.
- It **scales horizontally** by adding nodes or clusters.

### What's new in NiFi 2

- **New web UI**
- **Python processors**
- **Java 21** required
- **Flow definitions (JSON) and Git-based registry clients** replace XML templates
- **Kubernetes-native clustering**

---

## 5. Alternatives to NiFi

NiFi is not the only way to ingest data. Depending on the team and the scenario, these are common choices:

| Alternative | Style | Best for | Mode |
|---|---|---|---|
| **Python + Apache Airflow** | Code (Python DAGs) | Scheduled or event-triggered batch ingestion, full control in code | Batch |
| **Kafka Connect** | Configuration (JSON connectors) | Moving data in and out of Kafka | Streaming |
| **Airbyte** | UI + ready-made connectors | Copying data from SaaS apps and databases (ELT) | Batch / CDC |
| **Kestra** | Declarative YAML flows | Event-driven and scheduled workflows (included in OSBDET 26r1) | Batch / events |
| **Managed cloud services** | UI / serverless | AWS Glue, Azure Data Factory, Google Dataflow | Batch / streaming |

### Python + Airflow: ingest on a schedule or when data appears

**Apache Airflow** (version 3 since 2025) orchestrates workflows written in **Python** as **DAGs** (directed acyclic graphs of tasks). For ingestion, a DAG typically:

1. **Starts** on a **schedule** (`@hourly`, a cron expression) or when a **sensor** detects something: a file landing in a folder (`FileSensor`), an object in a bucket (`S3KeySensor`), an API becoming available (`HttpSensor`). Airflow 3 can also trigger DAGs when an upstream **asset** is updated.
2. **Extracts** the data with Python (an API call, a database query, reading files).
3. **Loads** it as raw files into **file/object storage**: MinIO, Amazon S3, Azure Data Lake Storage or HDFS.

```
 schedule (@hourly)                 ┌──────────┐    ┌──────────┐    ┌───────────────────┐
 or sensor (file/API/object) ──►    │ extract  │ ─► │  load    │ ─► │ MinIO / S3 / HDFS │
                                    │ (Python) │    │ (Python) │    │  raw/telegram/... │
                                    └──────────┘    └──────────┘    └───────────────────┘
```

**Example DAG (Airflow 3, TaskFlow API):** every hour, wait until the Telegram API answers, fetch the latest updates and store them as a JSON file in a MinIO bucket.

```python
from datetime import datetime
import json, requests

from airflow.sdk import dag, task, Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dag(schedule="@hourly", start_date=datetime(2026, 10, 1), catchup=False)
def telegram_to_minio():

    # Sensor: wait until the API is reachable (checks every 60 s, up to 10 min)
    api_ready = HttpSensor(
        task_id="api_ready",
        http_conn_id="telegram_api",
        endpoint="",
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    @task
    def extract() -> list:
        token = Variable.get("telegram_token")
        r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=30)
        r.raise_for_status()
        return r.json()["result"]

    @task
    def load(updates: list, ts_nodash=None) -> str:
        key = f"raw/telegram/{ts_nodash}.json"
        S3Hook(aws_conn_id="minio").load_string(
            json.dumps(updates), key=key, bucket_name="datalake", replace=True
        )
        return key

    api_ready >> load(extract())


telegram_to_minio()
```

> **Good practice:** Airflow is an **orchestrator**. Pass small values (such as file paths) between tasks, not large datasets, and keep raw files unchanged in storage, the same "enrich, don't transform" idea as with NiFi.

### NiFi or Airflow?

| | **Apache NiFi** | **Python + Airflow** |
|---|---|---|
| How you build | Visual canvas | Python code, versioned in Git |
| Data handling | Continuous flow of FlowFiles | Batches, run by run |
| Triggers | Always running, processors poll or listen | Schedules, sensors, assets |
| Built-in safety | Back pressure, provenance, guaranteed delivery | Retries, backfills, run history |
| Sweet spot | Many sources, near real-time routing | Scheduled batch ingestion with custom logic |

They are often **combined**: NiFi or Kafka moves data continuously, and Airflow orchestrates the batch jobs that come after.

---

## Links & References

- [NiFi project](https://nifi.apache.org/)
- [NiFi 2 documentation](https://nifi.apache.org/documentation/v2/)
- [NiFi user guide](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html)
- [All NiFi components](https://nifi.apache.org/components/)
- [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [Airflow sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html)

---
