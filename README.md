# Modern Data Architectures for Big Data I

The course provides a practical and architectural overview of the Big Data lifecycle, including data ingestion, distributed storage, batch and streaming processing and large-scale analytics using technologies such as Apache NiFi, Apache Kafka and Apache Spark.

## Course environment

All labs run on **[OSBDET v26r1](https://github.com/raulmarinperez/osbdet/releases/tag/v26r1)** (OSBDET 2026 Release 1): NiFi 2.7, Hadoop 3.4, Spark 3.5, Kafka 4, MinIO and JupyterLab in one virtual machine. Installation steps are in [Lecture 2.1](session-02-osbdet-and-foundations/lecture-02-01-course-environment.md).

## Sessions

| Session | Material |
|---|---|
| 1 | [1.1 Course Introduction](session-01-big-data-primer/lecture-01-01-course-introduction.md) · [1.2 Big Data Primer](session-01-big-data-primer/lecture-01-02-big-data-primer.md) |
| 2 | [2.1 Course Environment](session-02-osbdet-and-foundations/lecture-02-01-course-environment.md) · [2.2 Linux Commands](session-02-osbdet-and-foundations/lecture-02-02-linux-commands.md) · [2.3 Git & GitHub Essentials](session-02-osbdet-and-foundations/lecture-02-03-git-github-essentials.md) · [Lab 2.1 notebook: Linux inside Jupyter](session-02-osbdet-and-foundations/lab-02-01-linux-in-jupyter.ipynb) |
| 3 | [3.1 Data Ingestion](session-03-data-ingestion/lecture-03-01-data-ingestion.md) |
| 4 | [4.1 Apache NiFi](session-04-apache-nifi/lecture-04-01-apache-nifi.md) |
| 5 | [Lab 5.1 TfL Data Ingestion with NiFi](session-05-data-ingestion-lab/lab-05-01-tfl-nifi-data-ingestion.md) |
| 6 | [6.1 Data Storage](session-06-data-storage/lecture-06-01-data-storage.md) · [6.2 Data & Files Fundamentals](session-06-data-storage/lecture-06-02-data-files-fundamentals.md) |
| 7 | [7.1 Batch Data Storage](session-07-data-storage-batch-streaming/lecture-07-01-batch-data-storage.md) · [7.2 Apache Kafka](session-07-data-storage-batch-streaming/lecture-07-02-apache-kafka.md) |
| 8 | [Lab 8.1 MovieLens Batch Ingestion](session-08-data-storage-lab/lab-08-01-movielens-batch-ingestion.md) · [Lab 8.2 Binance Dual Ingestion](session-08-data-storage-lab/lab-08-02-binance-dual-ingestion.md) |
| 9 | [9.1 Data Processing](session-09-data-processing/lecture-09-01-data-processing.md) |
| 10 | [10.1 Apache Hadoop](session-10-apache-hadoop/lecture-10-01-apache-hadoop.md) · [Optional Lab 10.1 MovieLens MapReduce](session-10-apache-hadoop/lab-10-01-movielens-mapreduce.md) |
| 11 | [11.1 Apache Spark](session-11-apache-spark/lecture-11-01-apache-spark.md) · [Optional Lab 11.1 PySpark on Databricks](session-11-apache-spark/lab-11-01-movielens-pyspark-databricks.md) |
| 12 | [12.1 Apache Spark Data API](session-12-apache-spark-data-api/lecture-12-01-spark-data-api.md) |
| 13 | Apache Spark Data Sources API *(coming soon)* |
| 14 | [14.1 Spark DataFrames, the Basics](session-14-apache-spark-dataframes-basics/lecture-14-01-spark-dataframes-basics.md) |
| 15 | [15.1 Spark DataFrames in Detail](session-15-apache-spark-dataframes-in-detail/lecture-15-01-spark-dataframes-in-detail.md) |
| 16 | [Lab 16.1 TfL Bike Points with Spark DataFrames](session-16-apache-spark-dataframes-lab/lab-16-01-tfl-bike-point-spark-dataframes.md) · [Lab 16.2 Madrid's Air Quality into HDFS](session-16-apache-spark-dataframes-lab/lab-16-02-madrid-air-quality-into-hdfs.md) · [Lab 16.3 Processing Madrid's Air Quality](session-16-apache-spark-dataframes-lab/lab-16-03-madrid-air-quality-spark-analysis.md) |
| 17 | [Lab 17.1 Flights EDA with Spark](session-17-apache-spark-exploratory-data-analysis-lab/lab-17-01-flights-eda-spark.md) |
| 18 | [Lab 18.1 Spark SQL API](session-18-apache-spark-sql-api-lab/lab-18-01-tfl-bike-point-spark-analysis.md) |
| 19 | [19.1 Course Outline](session-19-course-outline/lecture-19-01-course-outline.md) |
| 20 | Group presentations and final exam |

**Group assignment:** [From Raw Data to Insights](session-00-group-assignment/group-assignment.md).

## Repository layout

Each `session-NN-<topic>/` folder holds, for every lecture or lab `N.M`:

- `lecture-NN-MM-<topic>.md` or `lab-NN-MM-<topic>.md`: the full guide.
- `…-slides.html`: the slide deck (open it in a browser); `…-slides.zip` is the same deck, packed for upload.
- `lab-NN-MM-<topic>.ipynb`: the lab notebook, when there is one.
- NiFi flow definitions (`*.json`), scripts and datasets used by the lab.
