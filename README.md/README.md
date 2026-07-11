# Real-Time Flight Data Streaming Pipeline using OpenSky, AWS Kinesis, AWS Glue, Apache Iceberg and Airflow

## Project Overview

This project implements a real-time streaming data pipeline for global flight tracking data using the OpenSky Network API and AWS services.

The pipeline continuously ingests live flight events, processes them through a Bronze-Silver-Gold lakehouse architecture, performs dimensional modeling, and orchestrates the complete workflow using Apache Airflow.

The project demonstrates modern Data Engineering concepts including streaming ingestion, micro-batch processing, data lakehouse architecture, Iceberg tables, dimensional modeling, orchestration, and cloud-native data processing.

---

## Architecture

```text
OpenSky API
    ↓
Python Producer
    ↓
Amazon Kinesis Data Stream
    ↓
AWS Glue Streaming Job
    ↓
Bronze Layer (Raw Data in S3)
    ↓
AWS Glue ETL Job
    ↓
Silver Layer (Apache Iceberg)
    ↓
AWS Glue ETL Jobs
    ↓
Gold Layer (Star Schema)
    ↓
Amazon Athena
```

---

## Technology Stack

### Programming Languages

* Python

### Streaming

* Amazon Kinesis Data Streams

### Data Processing

* AWS Glue Streaming ETL
* AWS Glue Spark ETL
* Apache Spark
* PySpark

### Storage

* Amazon S3

### Lakehouse

* Apache Iceberg

### Metadata Management

* AWS Glue Catalog

### Orchestration

* Apache Airflow
* Docker
* Docker Compose

### Query Layer

* Amazon Athena

### Development Tools

* VS Code
* Git
* GitHub
* Windows 11
* Python Virtual Environment

---

## Project Components

# 1. Producer Layer

The producer fetches live flight information from the OpenSky Network API and publishes records into Amazon Kinesis.

### Features

* Real-time flight data ingestion
* Automatic retry handling
* Rate limit handling
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

Amazon Kinesis acts as the streaming backbone of the project.

### Features

* Real-time ingestion
* Scalable event streaming
* Decouples producer and processing layers
* Fault tolerance

---

# 3. Bronze Layer

The Bronze layer stores raw incoming events exactly as received from Kinesis.

### Characteristics

* Raw immutable data
* Append-only storage
* Historical preservation
* Minimal transformations

### Storage

* Amazon S3

### Processing

* AWS Glue Streaming Job
* Glue forEachBatch processing

### Data Stored

* Original flight payload
* Metadata
* Processing timestamps
* Ingestion timestamps

---

# 4. Silver Layer

The Silver layer performs cleansing, validation and enrichment.

### Technologies

* Apache Iceberg
* AWS Glue Spark ETL
* Glue Catalog

### Transformations

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

### Business Features Added

#### Speed Category

* Low Speed
* Cruise
* High Speed

#### Altitude Category

* Low
* Medium
* High

#### Flight Status

* In Air
* On Ground

#### Additional Features

* speed_kmh
* is_moving
* flight_day

### Deduplication Strategy

Records are deduplicated using:

```text
icao24 + last_contact
```

Latest record per aircraft is selected using Spark Window functions.

---

# 5. Gold Layer

The Gold layer implements dimensional modeling using a Star Schema.

## Dimension Tables

### dim_date

Contains:

* date_key
* flight_date
* year
* month
* quarter
* week
* day

---

### dim_country

Contains:

* country_key
* origin_country

---

### dim_flight

Contains:

* flight_key
* callsign

---

### dim_aircraft

Contains:

* aircraft_key
* icao24

---

## Fact Table

### fact_aircraft

Contains:

* callsign
* icao24
* origin_country
* flight_date
* velocity
* altitude
* longitude
* latitude
* flight_status
* speed_kmh
* speed_category
* altitude_category
* timestamps

### Fact Table Strategy

* Event level storage
* Every flight event is preserved
* No aggregation
* Supports historical analysis

---

## Data Modeling Approach

### Dimension Strategy

* Slowly Changing Dimension Type 1
* Latest values overwrite previous values

### Fact Strategy

* Event-based fact table
* Historical event preservation

---

## Partition Strategy

### Bronze

Partitioned by ingestion date.

### Silver

Partitioned by:

* flight_date

### Gold

Partitioned based on analytical requirements.

---

## Apache Iceberg Features Used

* MERGE INTO
* Partition evolution
* ACID transactions
* Metadata management
* Snapshot support
* Schema evolution support

---

## Airflow Orchestration

Airflow orchestrates all Glue jobs.

### Pipeline Flow

```text
silver_stream_iceberg
        ↓
dim_date
        ↓
dim_flight
        ↓
dim_country
        ↓
dim_aircraft
        ↓
fact_aircraft
```

### Features

* Dependency management
* Retry mechanism
* Scheduling
* Failure tracking
* Monitoring

---

## Docker Usage

Docker was used to run Apache Airflow locally.

### Components

* Airflow Webserver
* Airflow Scheduler
* Airflow Triggerer
* PostgreSQL Metadata Database

---

## Data Quality Rules

### Validation Rules

* ICAO24 must not be null
* Latitude must not be null
* Longitude must not be null
* Duplicate aircraft events removed
* Invalid records filtered

---

## Challenges Solved

### Streaming Challenges

* Kinesis shard iterator issues
* Checkpoint management
* Stream recreation handling

### Glue Challenges

* Concurrent run handling
* Streaming job orchestration
* Iceberg integration

### OpenSky Challenges

* API rate limiting
* Retry management
* Exponential backoff

### Airflow Challenges

* Local Docker deployment
* Glue integration
* Scheduler configuration

---

## AWS Services Used

* Amazon Kinesis Data Streams
* AWS Glue
* AWS Glue Catalog
* Amazon S3
* Amazon Athena
* Amazon CloudWatch
* IAM

---

## Skills Demonstrated

### Data Engineering

* Streaming pipelines
* Lakehouse architecture
* Dimensional modeling
* ETL pipelines
* Data orchestration

### AWS

* Kinesis
* Glue
* S3
* Athena
* IAM

### Big Data

* Spark
* PySpark
* Iceberg

### DevOps

* Docker
* Airflow
* Git
* GitHub

---

## Future Enhancements

* Event-driven architecture
* Kafka integration
* CI/CD automation
* Monitoring and alerting
* Data governance
* Infrastructure as Code using Terraform
* Data quality framework
* Real-time dashboarding using QuickSight

---

## Repository Structure

```text
opensky-streaming-pipeline/

├── producer/
├── glue_jobs/
│   ├── bronze_streaming.py
│   ├── silver_stream_iceberg.py
│   ├── dim_date.py
│   ├── dim_country.py
│   ├── dim_flight.py
│   ├── dim_aircraft.py
│   └── fact_aircraft.py
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

## Author

Nikhil Eshwar

Assistant System Engineer | Data Engineering Enthusiast

Focused on building scalable data pipelines using modern cloud-native technologies.
