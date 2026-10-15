# Modern Data Architectures for Big Data I
## Lecture 4.1 — Data Ingestion

*Updated edition, October 2026*

---

## Agenda

1. **Data sources**
2. **Data ingestion**
3. **Ecosystem**: open source, cloud and third party
4. **Summary**
5. **Available options depending on requirements**: including Python + Airflow and Databricks Lakeflow Jobs

---

## 1. Data Sources

Data is out there, being created or already stored somewhere else. These places are called **data sources**. We classify them in two ways: **internal vs external** (who owns the data) and **batch vs streaming** (at rest or in motion).

| | **Batch (data at rest)** | **Streaming (data in motion)** |
|---|---|---|
| **Internal** (company data) | ERP and CRM tables, internal documents | Website and app logs, sensors, call-center events |
| **External** (third-party data) | Official statistics, public datasets for ML | Social media, weather feeds, partner APIs |

**Data at rest** is already stored in files or database tables and is moved to the platform **in bulk**.

**Data in motion** (in transit, in flight) is a **continuous stream of records** travelling between two places: clickstreams, IoT devices, social networks.

**Sources are far from the platform.** Most data sources live outside of or far from the big data platform. Something has to bridge that gap: the **ingestion layer**.

```
┌─────────────┐       ┌──────────────── Big data platform ────────────────┐
│ Data sources│ ───►  │ INGESTION │ Storage │ Processing │ Serving        │
└─────────────┘       └───────────────────────────────────────────────────┘
```

---

## 2. Data Ingestion

**Data ingestion** (also called data integration, collection or capture) is about **extracting or capturing data from its source and transporting it to a centralized store**, such as a data lake or lakehouse. The ingestion layer onboards datasets in **batch and streaming mode** from many sources.

### Four challenges

1. **Batch sources:** how do we ingest from databases and mainframes?
2. **Streaming sources:** how do we ingest from apps, social networks or IoT devices?
3. **Scalability:** how do we grow as much as required?
4. **Reliability:** how do we avoid losing data on the way?

**Solution:** different technologies suit different ingestion scenarios.

### How ingestion is done today

| Pattern | Idea | Example tools |
|---|---|---|
| **Batch / ELT** | Extract and load on a schedule, transform later in the platform | Airbyte, Fivetran, AWS Glue, Data Factory |
| **Change data capture (CDC)** | Stream every insert, update and delete from a database's log | Debezium, Datastream, AWS DMS, Qlik Replicate |
| **Event streaming** | Apps publish events continuously to a durable log | Kafka, Kinesis, Event Hubs, Pub/Sub |
| **Zero-ETL** | The cloud replicates data between its own services automatically | Aurora → Redshift, Fabric mirroring |

---

## 3. Ecosystem

### 3.1 Open source

| Technology | What it does | Data |
|---|---|---|
| **Apache NiFi** | Visual flows for routing, transforming and moving data between systems | At rest and in motion |
| **Kafka Connect** | Framework of connectors between Kafka and external systems | At rest and in motion |
| **Debezium** | Change data capture from databases, built on Kafka Connect | In motion (CDC) |
| **Airbyte** | Hundreds of ready-made connectors for batch ELT | At rest |

> **Legacy tools:** Apache **Sqoop** (databases → Hadoop) and Apache **Flume** (log streaming), both created at Cloudera, were the first big data ingestion tools. Sqoop was retired in 2021 and Flume is no longer developed.

**Ingestion vs stream processing**

```
 DATA MOVING ◄───────────────────────────────────────────► STREAM PROCESSING
 Kafka Connect · Debezium · Airbyte    NiFi      Spark Structured Streaming · Flink
 (ingestion)                       (in between)             (processing)
```

---

### 3.2 Cloud

Cloud providers offer managed services for four scenarios: **onboarding/offloading** (very large one-off transfers), **batch data**, **streaming data** and **IoT**.

| Scenario | AWS | Azure | Google Cloud |
|---|---|---|---|
| **Onboarding** | AWS DataSync, Data Transfer Terminal | Azure Data Box | Storage Transfer Service, Transfer Appliance |
| **Batch / CDC** | AWS Glue, AWS DMS | Azure Data Factory (also in Microsoft Fabric) | Datastream, Dataflow |
| **Streaming** | Kinesis Data Streams, Amazon Data Firehose, Amazon MSK | Event Hubs | Pub/Sub |
| **IoT** | AWS IoT Core | Azure IoT Hub | Partner platforms → Pub/Sub |

> **Retired since the last edition:** Google Cloud IoT Core (2023), AWS Data Pipeline (closed to new customers, 2024), AWS Snowball Edge (existing customers only, 2025) and Azure Data Box Heavy.

---

### 3.3 Third party

| Product | Notes |
|---|---|
| **Fivetran** | Managed ELT connectors, the modern SaaS approach |
| **Informatica** | Enterprise data integration; part of Salesforce since November 2025 |
| **Qlik (Talend, Qlik Replicate)** | Integration and real-time CDC |
| **Oracle GoldenGate** | Real-time database replication and CDC |
| **Confluent Cloud** | Managed Kafka and connectors |

---

## 4. Summary

- **Data ingestion is one of the toughest parts of big data**: tens of sources, arriving at variable speed and in great variety.
- We need **different technologies** depending on the **source type** and the **data velocity**.
- Four scenarios: **onboarding, batch, streaming and IoT**. Today, **CDC** and **managed connectors (ELT)** dominate batch, and **event streaming** dominates real time.

### Which tool for which scenario?

| Scenario | AWS | Azure | Google Cloud | Open source | Third party |
|---|---|---|---|---|---|
| **Onboarding** | DataSync | Data Box | Transfer Appliance | — | — |
| **Batch / CDC** | Glue, DMS | Data Factory | Datastream | NiFi, Airbyte, Debezium | Fivetran, Informatica, Qlik |
| **Streaming** | Kinesis, MSK | Event Hubs | Pub/Sub | Kafka, NiFi | Confluent Cloud |
| **IoT** | IoT Core | IoT Hub | Partners + Pub/Sub | NiFi, MQTT brokers | — |

---

## 5. Available Options Depending on Requirements

There is no single best ingestion tool. Start from the **requirements**:

- **Latency:** do we need data every day, every hour, or within seconds?
- **Source type:** files, databases, SaaS apps, APIs, events, devices?
- **Volume:** megabytes per day, or terabytes in a one-off migration?
- **Platform:** are we committed to one cloud or to a platform such as Databricks?
- **Team skills:** Python developers, SQL analysts, or low-code users?
- **Operations and cost:** run it ourselves (open source) or pay for a managed service?

### If you need… choose…

| If you need… | Option | Examples |
|---|---|---|
| Custom sources (APIs, scripts) on a **schedule** or when a **file appears**, full control in code | **Python + Apache Airflow** | Airflow DAGs with sensors, writing to MinIO/S3 |
| Ingestion **inside a Databricks lakehouse**, triggered by schedules or file arrival | **Databricks Lakeflow Jobs** (formerly Workflows) | Auto Loader, Lakeflow Connect, Declarative Pipelines |
| Copy data from **many SaaS apps and databases** with minimal code | **Managed connectors (ELT)** | Fivetran, Airbyte |
| Every **database change** in near real time | **Change data capture** | Debezium + Kafka, Datastream, AWS DMS |
| **High-volume real-time events** from apps or devices | **Event streaming** | Kafka, Kinesis, Event Hubs, Pub/Sub |
| Many heterogeneous sources, **visual routing**, often on-premises | **Visual flow tool** | Apache NiFi |
| Low operations, already **committed to one cloud** | **Cloud-native services** | AWS Glue, Azure Data Factory / Fabric, Google Dataflow |
| A **one-off migration** of terabytes or petabytes | **Bulk transfer** | AWS DataSync, Azure Data Box, Google Transfer Appliance |

### Option: Python + Airflow

**Apache Airflow** (version 3) runs ingestion pipelines written in Python as **DAGs**. A run starts on a **schedule** (`@daily`, cron) or when a **sensor** sees something: a file in a folder (`FileSensor`), an object in a bucket (`S3KeySensor`), an API responding (`HttpSensor`). The DAG then extracts the data and writes it as raw files to **file/object storage** (MinIO, S3, ADLS, HDFS).

```
 schedule (@daily) ─┐
                    ├─► sensor: wait for file ─► upload task (Python) ─► MinIO / S3: raw/sales/dt=…/
 or file arrives ───┘
```

**Example:** every day, wait for the day's sales file to land in a folder, then upload it to the data lake.

```python
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dag(schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False)
def sales_files_to_lake():

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/data/landing/sales_{{ ds_nodash }}.csv",
        poke_interval=300,          # check every 5 minutes
        timeout=6 * 3600,           # give up after 6 hours
        mode="reschedule",
    )

    @task
    def upload(ds_nodash=None):
        S3Hook(aws_conn_id="minio").load_file(
            filename=f"/data/landing/sales_{ds_nodash}.csv",
            key=f"raw/sales/dt={ds_nodash}/sales.csv",
            bucket_name="datalake",
            replace=True,
        )

    wait_for_file >> upload()


sales_files_to_lake()
```

### Option: Databricks Lakeflow Jobs

On Databricks, ingestion is orchestrated with **Lakeflow Jobs** (formerly **Databricks Workflows**). A job can run on a **schedule**, **continuously**, or with a **file-arrival trigger** when new files land in cloud storage. Typical ingestion tasks are:

- **Auto Loader**: incrementally loads new files from cloud storage into Delta tables, tracking which files were already processed
- **Lakeflow Connect**: managed connectors for databases and SaaS apps (e.g. SQL Server, Salesforce, Workday)
- **Lakeflow Spark Declarative Pipelines** (formerly Delta Live Tables): declarative batch and streaming pipelines

```
 files land in cloud storage ─► file-arrival trigger ─► Lakeflow Job ─► Auto Loader task ─► Delta table (bronze)
```

```python
(spark.readStream.format("cloudFiles")                 # Auto Loader
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/main/raw/_schemas/telegram")
    .load("/Volumes/main/raw/landing/telegram/")
    .writeStream
    .option("checkpointLocation", "/Volumes/main/raw/_checkpoints/telegram")
    .trigger(availableNow=True)                       # process new files, then stop
    .toTable("main.bronze.telegram_updates"))
```

### Comparing the options

| Option | Latency | Build effort | Who runs it | Typical skills |
|---|---|---|---|---|
| Python + Airflow | Minutes to hours | Medium (code) | You (open source) or managed Airflow | Python |
| Databricks Lakeflow Jobs | Seconds to hours | Low to medium | Databricks (managed) | Python, SQL |
| Managed connectors | Minutes to hours | Low (configuration) | Vendor (SaaS) | Configuration |
| CDC | Seconds | Medium | You or the cloud | Databases, Kafka |
| Event streaming | Milliseconds to seconds | Medium to high | You or the cloud | Distributed systems |
| Apache NiFi | Seconds | Low (visual) | You | Low-code |
| Cloud-native services | Seconds to hours | Low | The cloud provider | Cloud |
| Bulk transfer | Days (one-off) | Low | The cloud provider | Operations |

---

## Interesting Readings

- [AWS: Data ingestion methods](https://docs.aws.amazon.com/whitepapers/latest/building-data-lakes/data-ingestion-methods.html)
- [Azure: Choosing a real-time message ingestion technology](https://learn.microsoft.com/en-us/azure/architecture/data-guide/technology-choices/real-time-ingestion)
- [Google Cloud: Data lifecycle](https://cloud.google.com/architecture/data-lifecycle-cloud-platform)
- [Debezium: change data capture](https://debezium.io/documentation/reference/stable/index.html)
- [Apache Airflow: sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html)
- [Databricks: streaming and incremental ingestion](https://docs.databricks.com/ingestion/streaming.html)

---