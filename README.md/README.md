# Near-Real-Time Flight Data Streaming Pipeline using OpenSky, AWS Kinesis, AWS Glue, Apache Iceberg and Airflow

## Project Overview

This project implements a near-real-time flight data pipeline using the OpenSky Network API and AWS cloud services.

The pipeline ingests live flight data through Amazon Kinesis, stores the raw data in a Bronze layer, processes and cleans the data using AWS Glue and Apache Spark, maintains the latest aircraft state in an Apache Iceberg Silver layer, and creates historical analytical summaries in the Gold layer using SCD Type 2.

Apache Airflow orchestrates the Silver and Gold processing as scheduled micro-batches.

The architecture intentionally separates streaming ingestion from downstream analytical processing. This provides frequent data updates while avoiding the additional infrastructure cost of continuously running downstream analytical jobs when sub-second Gold updates are not required.

The project demonstrates modern Data Engineering concepts including:

* Streaming ingestion
* Micro-batch processing
* Lakehouse architecture
* Apache Iceberg
* AWS Glue
* Amazon Kinesis
* Apache Airflow
* PySpark
* SCD Type 2
* Data quality and deduplication
* Cloud-based data processing

---

# Architecture

```text
OpenSky Network API
        ↓
Python Producer
        ↓
Amazon Kinesis Data Stream
        ↓
Bronze Layer
Raw Flight Data in S3
        ↓
Apache Airflow
        ↓
Silver AWS Glue Job
Iceberg MERGE / Upsert
        ↓
Gold AWS Glue Job
SCD Type 2
        ↓
Amazon Athena
```

## Processing Flow

```text
OpenSky API
     ↓
Python Producer
     ↓
Kinesis Data Stream
     ↓
Bronze Layer
     ↓
Airflow
     ↓
Silver Glue ETL
Iceberg MERGE / Upsert
     ↓
Gold Glue ETL
SCD Type 2
     ↓
Athena
```

Kinesis provides the streaming ingestion backbone, while Apache Airflow orchestrates the Silver and Gold processing as controlled micro-batches.

---

# Architecture Decision

The project intentionally separates **streaming ingestion** from **analytical processing**.

Kinesis continuously receives flight data from the producer, while Silver and Gold processing is performed through scheduled Airflow micro-batches.

This design was chosen to balance:

* Data freshness
* Processing requirements
* Infrastructure cost
* Analytical latency

The Silver layer maintains the latest aircraft state using Apache Iceberg `MERGE INTO`, while the Gold layer maintains historical changes in analytical aggregates using SCD Type 2.

The Gold layer does not require sub-second updates for the analytical use cases in this project, so micro-batch processing avoids unnecessary continuous downstream compute and Iceberg transaction overhead.

---

# Technology Stack

## Programming Languages

* Python
* SQL

## Streaming

* Amazon Kinesis Data Streams

## Data Processing

* AWS Glue Spark ETL
* Apache Spark
* PySpark

## Storage

* Amazon S3

## Lakehouse

* Apache Iceberg

## Metadata Management

* AWS Glue Data Catalog

## Orchestration

* Apache Airflow
* Docker
* Docker Compose

## Query Layer

* Amazon Athena

## Development Tools

* VS Code
* Git
* GitHub
* Python Virtual Environment

---

# Project Components

## 1. Producer Layer

The producer fetches live flight information from the OpenSky Network API and publishes records into Amazon Kinesis.

### Features

* Live flight data ingestion
* Automatic retry handling
* API rate-limit handling
* Configurable polling interval
* Configurable backoff mechanism
* Structured logging

### Technologies

* Python
* httpx
* tenacity
* boto3

---

# 2. Streaming Layer

Amazon Kinesis Data Streams acts as the streaming backbone of the project.

### Responsibilities

* Receive live flight data from the producer
* Provide a durable streaming buffer
* Decouple data ingestion from downstream processing
* Handle continuous incoming flight events

### Flow

```text
OpenSky API
     ↓
Python Producer
     ↓
Kinesis Data Stream
     ↓
Bronze Layer
```

---

# 3. Bronze Layer

The Bronze layer stores incoming flight data in raw form.

### Characteristics

* Raw data preservation
* Append-oriented storage
* Minimal transformations
* Historical data retention
* Processing and ingestion metadata

### Storage

* Amazon S3

The Bronze layer acts as the raw data layer and provides a persistent source for downstream Silver processing.

### Data Stored

* Original flight payload
* Source metadata
* Record identifiers
* Ingestion timestamps
* Processing timestamps

---

# 4. Silver Layer

The Silver layer performs cleansing, validation, enrichment and latest-state processing.

### Processing

The Silver layer is processed using an AWS Glue Spark ETL job.

Apache Airflow triggers the Silver Glue job as part of the pipeline's micro-batch workflow.

### Technologies

* AWS Glue Spark ETL
* Apache Spark
* PySpark
* Apache Iceberg
* AWS Glue Data Catalog

---

## Silver Transformations

* Null filtering
* Data validation
* Duplicate removal
* Callsign trimming
* Flight date derivation
* Speed conversion
* Flight status derivation
* Speed categorization
* Altitude categorization
* Day extraction

---

## Business Features

### Speed Category

* Low Speed
* Cruise
* High Speed

### Altitude Category

* Low
* Medium
* High

### Flight Status

* In Air
* On Ground

### Additional Columns

* `speed_kmh`
* `is_moving`
* `flight_day`

---

## Silver Deduplication

Records are deduplicated using aircraft identity and observation timestamp.

```text
icao24 + last_contact
```

For each processing batch, the latest observation for an aircraft is selected using Spark Window functions.

---

## Silver Upsert Strategy

The Silver layer uses Apache Iceberg `MERGE INTO` to maintain the latest known state of each aircraft.

```text
Incoming Bronze Data
        ↓
AWS Glue Silver Job
        ↓
Data Cleaning & Transformation
        ↓
Deduplication
        ↓
Iceberg MERGE INTO
        ↓
Silver Iceberg Table
```

The Silver layer does **not** use SCD Type 2.

It maintains the latest state of each aircraft using `icao24` as the matching key and `last_contact` to prevent stale observations from replacing newer observations.

### Merge Logic

```text
Incoming Aircraft
       ↓
Match icao24
       │
       ├── No Match
       │      ↓
       │    INSERT
       │
       └── Match
              ↓
       Compare last_contact
              │
        ┌─────┴─────┐
        ↓           ↓
     Newer       Older/Equal
        ↓           ↓
     UPDATE       Ignore
```

---

# 5. Gold Layer

The Gold layer contains analytical aggregate tables derived from Silver.

The project does **not** use a traditional dimension/fact star schema.

Instead, the Gold layer maintains analytical summaries using **SCD Type 2**.

---

## Gold Tables

### flights_by_country

Aggregates flight activity by origin country.

Contains:

* `origin_country`
* `total_flights`
* `avg_speed`
* `avg_altitude`
* `moving_flights`
* `grounded_flights`
* `effective_from`
* `effective_to`
* `is_current`
* `version`

---

### speed_summary

Aggregates aircraft by speed category.

Contains:

* `speed_category`
* `flight_count`
* `effective_from`
* `effective_to`
* `is_current`
* `version`

---

### altitude_summary

Aggregates aircraft by altitude category.

Contains:

* `altitude_category`
* `flight_count`
* `effective_from`
* `effective_to`
* `is_current`
* `version`

---

### flight_status

Aggregates aircraft by current flight status.

Contains:

* `flight_status`
* `flight_count`
* `effective_from`
* `effective_to`
* `is_current`
* `version`

---

### daily_summary

Aggregates flight activity by flight day.

Contains:

* `flight_day`
* `flight_count`
* `effective_from`
* `effective_to`
* `is_current`
* `version`

---

# Gold SCD Type 2 Strategy

The Gold layer uses SCD Type 2 to preserve historical changes in analytical aggregates.

For example:

```text
Version 1

India | 100 flights
effective_from = 10:00
effective_to   = 10:05
is_current     = false
version        = 1
```

After the aggregate changes:

```text
Version 2

India | 125 flights
effective_from = 10:05
effective_to   = NULL
is_current     = true
version        = 2
```

When an aggregate changes:

1. The existing current record is expired.
2. `effective_to` is populated.
3. `is_current` becomes `false`.
4. A new version is inserted.
5. The new version becomes the current record.

Unchanged aggregates do not create unnecessary new versions.

---

# Why Gold Uses Micro-Batch Processing

The project uses continuous streaming ingestion through Kinesis while downstream Silver and Gold processing is orchestrated through Airflow micro-batches.

```text
Kinesis
   ↓
Bronze
   ↓
Airflow
   ↓
Silver
   ↓
Gold
```

This is a deliberate cost-optimization decision.

Continuously executing downstream analytical processing for every incoming event could result in:

* Higher Glue compute consumption
* More frequent Iceberg commits
* Increased processing overhead
* Higher infrastructure cost

The Gold layer therefore runs at a controlled interval because the analytical use cases do not require sub-second aggregate updates.

The Silver layer remains the appropriate layer for accessing the latest aircraft state.

---

# 6. Airflow Orchestration

Apache Airflow orchestrates the **Silver and Gold Glue jobs**.

The Bronze ingestion layer is independent of the Airflow DAG.

## Pipeline Flow

```text
Kinesis
   ↓
Bronze
   ↓
Airflow
   ↓
Silver Glue Job
   ↓
Gold Glue Job
```

## DAG Dependency

```text
opensky_silver_iceberg
          ↓
opensky_gold_scd2
```

Airflow first triggers the Silver Glue job.

After Silver completes successfully, Airflow triggers the Gold Glue job.

---

## Scheduling

The DAG runs as a scheduled micro-batch workflow.

Example:

```text
Every 5 minutes

Silver
   ↓
Gold
```

The Airflow DAG uses:

```python
max_active_runs=1
```

to prevent overlapping pipeline executions.

This ensures that a new processing cycle does not start while the previous Silver → Gold cycle is still running.

---

## Airflow Responsibilities

* Job orchestration
* Dependency management
* Scheduling
* Failure handling
* Retry configuration
* Pipeline monitoring
* Controlled micro-batch processing

---

# 7. Docker Usage

Docker is used to run Apache Airflow locally.

### Components

* Airflow Webserver
* Airflow Scheduler
* Airflow Triggerer
* PostgreSQL Metadata Database

Docker provides a reproducible local Airflow environment without requiring a separate Airflow installation.

---

# Data Quality Rules

The pipeline applies several validation and data quality rules.

## Validation Rules

* `icao24` must not be null
* Latitude must not be null
* Longitude must not be null
* Invalid records are filtered
* Duplicate observations are removed
* Latest aircraft observation is selected

## Silver Data Quality

The Silver layer prevents stale aircraft observations from replacing newer observations through the Iceberg `MERGE INTO` logic.

---

# Challenges Solved

## Streaming Challenges

* Kinesis stream processing
* Streaming data ingestion
* Data buffering
* Continuous incoming flight data

## Glue Challenges

* Glue Spark ETL configuration
* Iceberg integration
* Iceberg `MERGE INTO`
* SCD Type 2 implementation
* Managing downstream processing dependencies

## OpenSky Challenges

* API rate limiting
* Retry management
* Exponential backoff
* Continuous data ingestion

## Airflow Challenges

* Local Docker deployment
* AWS Glue integration
* DAG scheduling
* Job dependency management
* Separating streaming ingestion from scheduled analytical processing

---

# AWS Services Used

* Amazon Kinesis Data Streams
* AWS Glue
* AWS Glue Data Catalog
* Amazon S3
* Amazon Athena
* Amazon CloudWatch
* AWS IAM

---

# Apache Iceberg Features Used

* `MERGE INTO`
* ACID transactions
* Snapshot-based table management
* Schema evolution
* Partition evolution
* Metadata management

---

# Skills Demonstrated

## Data Engineering

* Streaming data pipelines
* Micro-batch processing
* Lakehouse architecture
* ETL pipelines
* Data quality
* Data deduplication
* SCD Type 2
* Data orchestration

## AWS

* Amazon Kinesis
* AWS Glue
* Amazon S3
* Amazon Athena
* AWS IAM
* AWS Glue Data Catalog

## Big Data

* Apache Spark
* PySpark
* Apache Iceberg

## DevOps

* Docker
* Docker Compose
* Apache Airflow
* Git
* GitHub

---

# Future Enhancements

* Real-time dashboarding
* CloudWatch monitoring and alerting
* Data quality framework
* CI/CD automation
* Data governance
* Infrastructure as Code using Terraform
* Additional analytical use cases
* Performance optimization for large-scale Iceberg tables

---

# Repository Structure

```text
opensky-streaming-pipeline/

├── producer/
│
├── glue_jobs/
│   ├── bronze.py
│   ├── silver_iceberg.py
│   └── gold_scd2.py
│
├── airflow/
│   └── opensky_pipeline.py
│
├── screenshots/
├── architecture/
├── docs/
├── requirements.txt
├── .gitignore
└── README.md
```

---

# Author

**Nikhil Eshwar**

Assistant System Engineer | Data Engineering Enthusiast

Focused on building scalable cloud-native data pipelines using AWS, Apache Spark, Apache Iceberg, Kinesis and Airflow.
