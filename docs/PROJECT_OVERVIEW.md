# Anomaly Detection System

Real-time anomaly detection and fraud analysis pipeline using Kafka, Spark Structured Streaming, MinIO, Redis, and Python.

---

# Table of Contents

- [1. Project Overview](#1-project-overview)
- [2. Business Problem](#2-business-problem)
- [3. System Objectives](#3-system-objectives)
- [4. Current Architecture](#4-current-architecture)
- [5. Technology Stack](#5-technology-stack)
- [6. System Flow](#6-system-flow)
- [7. Project Structure](#7-project-structure)
- [8. Infrastructure](#8-infrastructure)
- [9. Docker Services](#9-docker-services)
- [10. Kafka Configuration](#10-kafka-configuration)
- [11. Spark Streaming Pipeline](#11-spark-streaming-pipeline)
- [12. Event Schema](#12-event-schema)
- [13. Feature Engineering](#13-feature-engineering)
- [14. Watermarking Strategy](#14-watermarking-strategy)
- [15. Window Aggregation Strategy](#15-window-aggregation-strategy)
- [16. Current Functionalities](#16-current-functionalities)
- [17. Local Development Setup](#17-local-development-setup)
- [18. Running the Project](#18-running-the-project)
- [19. Example Event](#19-example-event)
- [20. Example Output](#20-example-output)
- [21. Problems Encountered](#21-problems-encountered)
- [22. Solutions Applied](#22-solutions-applied)
- [23. Engineering Decisions](#23-engineering-decisions)
- [24. Code Standards](#24-code-standards)
- [25. Logging Standards](#25-logging-standards)
- [26. Git Workflow](#26-git-workflow)
- [27. Environment Variables](#27-environment-variables)
- [28. Future Roadmap](#28-future-roadmap)
- [29. Planned ML Pipeline](#29-planned-ml-pipeline)
- [30. Team Responsibilities](#30-team-responsibilities)
- [31. Scalability Considerations](#31-scalability-considerations)
- [32. Reliability Considerations](#32-reliability-considerations)
- [33. Security Considerations](#33-security-considerations)
- [34. Performance Considerations](#34-performance-considerations)
- [35. Known Limitations](#35-known-limitations)
- [36. Long-Term Vision](#36-long-term-vision)

---

# 1. Project Overview

This project implements a real-time anomaly detection platform focused on transactional event processing.

The system is designed to ingest transaction events continuously, process them through a streaming pipeline, generate behavioral features in real time, and later apply anomaly detection and machine learning models.

The architecture follows modern distributed data engineering principles and was designed to simulate production-grade streaming systems used in:

- Fraud detection
- Financial monitoring
- Behavioral analysis
- Risk scoring
- Real-time observability systems

---

# 2. Business Problem

Traditional fraud detection systems often rely on batch processing.

Batch systems introduce several limitations:

- Delayed anomaly detection
- Slow reaction time
- Poor real-time visibility
- Limited operational responsiveness

This project addresses these issues through a streaming-first architecture.

The system continuously consumes transaction events and computes features in near real time.

---

# 3. System Objectives

Main objectives:

- Process transactional events in real time
- Generate streaming behavioral features
- Detect anomalous behavior patterns
- Build scalable streaming infrastructure
- Simulate production-grade data engineering systems
- Provide foundation for ML-based fraud detection

---

# 4. Current Architecture

```text
+-------------------+
| Python Producer   |
+-------------------+
          |
          v
+-------------------+
| Kafka Topic       |
| transactions      |
+-------------------+
          |
          v
+-------------------+
| Spark Structured  |
| Streaming         |
+-------------------+
          |
          v
+-------------------+
| Feature           |
| Engineering       |
+-------------------+
          |
          v
+-------------------+
| Console Output    |
| (temporary sink)  |
+-------------------+
```

Future architecture:

```text
Producer
   ↓
Kafka
   ↓
Spark Streaming
   ↓
Feature Pipeline
   ↓
MinIO / Data Lake
   ↓
ML Inference Layer
   ↓
Anomaly Detection
   ↓
Redis Cache
   ↓
Alert System / Dashboard
```

---

# 5. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Main programming language |
| Apache Kafka | Event streaming platform |
| Apache Spark Structured Streaming | Real-time stream processing |
| Docker | Infrastructure containerization |
| Docker Compose | Multi-service orchestration |
| MinIO | Object storage / Data Lake |
| Redis | Fast cache and state storage |
| Poetry | Dependency management |
| PySpark | Spark API for Python |

---

# 6. System Flow

## Step 1 — Event Generation

A Python producer generates transaction events.

Example:

```json
{
  "transaction_id": "tx_001",
  "user_id": 1,
  "timestamp": "2026-05-08T14:40:00",
  "amount": 120.5,
  "merchant": "Amazon",
  "category": "electronics",
  "device_id": "dev_abc",
  "latitude": -23.55052,
  "longitude": -46.633308
}
```

---

## Step 2 — Kafka Ingestion

The producer publishes events into the Kafka topic:

```text
transactions
```

Kafka acts as the event backbone of the architecture.

---

## Step 3 — Spark Consumption

Spark Structured Streaming continuously consumes Kafka messages.

---

## Step 4 — JSON Parsing

Raw Kafka byte payloads are converted into structured Spark DataFrames.

---

## Step 5 — Event Time Processing

The event timestamp is transformed into proper event-time semantics.

---

## Step 6 — Watermarking

Late-arriving data handling is enabled through watermarking.

---

## Step 7 — Feature Engineering

Behavioral aggregations are generated:

- transaction count
- average amount
- total amount
- rolling windows

---

## Step 8 — Output Sink

Currently:

```text
console sink
```

Future:

- MinIO
- Iceberg/Delta
- Feature Store
- ML Inference
- Dashboard

---

# 7. Project Structure

```text
anomaly-detection-system/
│
├── streaming/
│   ├── spark_stream.py
│   └── producer.py
│
├── docker/
│
├── data/
│
├── models/
│
├── notebooks/
│
├── tests/
│
├── docker-compose.yml
│
├── pyproject.toml
│
└── README.md
```

---

# 8. Infrastructure

Infrastructure is fully containerized using Docker.

Current services:

- Kafka
- MinIO
- Redis

All services are isolated and reproducible.

---

# 9. Docker Services

## Kafka

Purpose:

- Event streaming
- Decoupling producers and consumers
- Stream durability

Port:

```text
9092
```

---

## MinIO

Purpose:

- Object storage
- Data lake simulation
- Parquet persistence

Ports:

```text
9000 -> API
9001 -> Console
```

Credentials:

```text
user: admin
password: password123
```

---

## Redis

Purpose:

- Low-latency cache
- Real-time state access
- Fast anomaly lookup

Port:

```text
6379
```

---

# 10. Kafka Configuration

Current topic:

```text
transactions
```

Current strategy:

- Single broker
- Single partition
- Local development mode

Future improvements:

- Multi-broker cluster
- Replication
- Partition scaling
- Topic retention policies

---

# 11. Spark Streaming Pipeline

Main streaming responsibilities:

- Kafka consumption
- JSON parsing
- Event-time processing
- Watermarking
- Aggregation
- Feature engineering

Current processing model:

```python
window(event_time, "5 minutes")
```

---

# 12. Event Schema

```python
transaction_id: string
user_id: integer
timestamp: string
amount: double
merchant: string
category: string
device_id: string
latitude: double
longitude: double
```

---

# 13. Feature Engineering

Current features:

| Feature | Description |
|---|---|
| transaction_count_5min | Number of transactions in 5-minute window |
| avg_amount_5min | Average transaction amount |
| total_amount_5min | Total transaction amount |

Future features:

- velocity features
- geo anomalies
- merchant entropy
- device switching
- temporal anomalies
- rolling z-score
- user behavioral embeddings

---

# 14. Watermarking Strategy

Current watermark:

```python
.withWatermark("event_time", "10 minutes")
```

Purpose:

- Handle delayed events
- Avoid infinite state accumulation
- Improve streaming reliability

---

# 15. Window Aggregation Strategy

Current window:

```python
window(event_time, "5 minutes")
```

Purpose:

- Short-term behavioral aggregation
- Real-time fraud indicators
- Sliding behavioral analysis

Future:

- sliding windows
- session windows
- multi-scale aggregation

---

# 16. Current Functionalities

Implemented and working:

- Docker infrastructure
- Kafka broker
- Kafka topic creation
- Producer pipeline
- Spark Structured Streaming
- Kafka ingestion
- JSON parsing
- Watermarking
- Window aggregations
- Real-time feature generation
- Streaming console output

---

# 17. Local Development Setup

## Requirements

- Docker
- Docker Compose
- Python 3.11
- Poetry
- Java 17+

---

## Install dependencies

```bash
poetry install
```

---

# 18. Running the Project

## Terminal 1 — Start Infrastructure

```bash
docker compose up -d
```

---

## Terminal 2 — Run Producer

```bash
poetry run python streaming/producer.py
```

---

## Terminal 3 — Run Spark Streaming

```bash
poetry run python streaming/spark_stream.py
```

---

# 19. Example Event

```json
{
  "transaction_id": "tx_001",
  "user_id": 1,
  "timestamp": "2026-05-08T14:40:00",
  "amount": 120.5,
  "merchant": "Amazon",
  "category": "electronics",
  "device_id": "dev_abc",
  "latitude": -23.55052,
  "longitude": -46.633308
}
```

---

# 20. Example Output

```text
+-------------------+-------------------+-------+----------------------+---------------+-----------------+
|window_start       |window_end         |user_id|transaction_count_5min|avg_amount_5min|total_amount_5min|
+-------------------+-------------------+-------+----------------------+---------------+-----------------+
|2026-05-08 14:40:00|2026-05-08 14:45:00|1      |2                     |210.25         |420.5            |
+-------------------+-------------------+-------+----------------------+---------------+-----------------+
```

---

# 21. Problems Encountered

## Kafka Topic Resolution Error

Error:

```text
UnknownTopicOrPartitionException
```

Cause:

Mismatch between:

- existing Kafka topic
- Spark subscription topic
- Kafka advertised listeners

---

# 22. Solutions Applied

## Fixed Kafka advertised listeners

Correct configuration:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

---

## Standardized topic naming

Standard topic:

```text
transactions
```

---

## Updated Spark consumer configuration

Spark now consumes the correct topic.

---

# 23. Engineering Decisions

## Why Kafka?

- Industry standard
- Durable event streaming
- Horizontal scalability
- Strong ecosystem

---

## Why Spark Structured Streaming?

- Mature streaming engine
- Native Kafka integration
- Scalable stateful processing

---

## Why MinIO?

- S3-compatible
- Free
- Excellent local development solution

---

## Why Redis?

- Extremely low latency
- Useful for anomaly scoring and caching

---

# 24. Code Standards

## Python

- PEP8
- Type-safe where possible
- Modular architecture
- Functional decomposition

---

## Naming

### Variables

```python
snake_case
```

### Classes

```python
PascalCase
```

### Constants

```python
UPPER_CASE
```

---

# 25. Logging Standards

Current logging:

```python
spark.sparkContext.setLogLevel("WARN")
```

Future logging stack:

- structured JSON logging
- centralized logs
- observability pipeline

---

# 26. Git Workflow

Recommended:

```text
main
develop
feature/*
hotfix/*
```

Branch examples:

```text
feature/spark-feature-store
feature/minio-writer
feature/anomaly-detector
```

---

# 27. Environment Variables

Future recommendation:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transactions

MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123

REDIS_HOST=localhost
REDIS_PORT=6379
```

---

# 28. Future Roadmap

## Phase 1 — Streaming Infrastructure

- [x] Kafka
- [x] Spark
- [x] Docker
- [x] Streaming ingestion

---

## Phase 2 — Persistence Layer

- [ ] MinIO integration
- [ ] Parquet writing
- [ ] Data lake organization

---

## Phase 3 — ML Pipeline

- [ ] Offline training
- [ ] Feature store
- [ ] Model registry
- [ ] Online inference

---

## Phase 4 — Detection Layer

- [ ] Real-time scoring
- [ ] Threshold engine
- [ ] Alert system

---

## Phase 5 — Visualization

- [ ] Dashboard
- [ ] Monitoring
- [ ] Real-time metrics

---

# 29. Planned ML Pipeline

Possible models:

- Isolation Forest
- XGBoost
- Autoencoders
- One-Class SVM

Potential features:

- transaction velocity
- amount deviation
- geographic distance
- merchant risk
- device consistency

---

# 30. Team Responsibilities

## Data Engineering

Responsible for:

- Kafka
- Spark
- Data lake
- Streaming pipelines

---

## Machine Learning

Responsible for:

- feature engineering
- training
- evaluation
- inference

---

## Infrastructure

Responsible for:

- Docker
- deployment
- observability
- scalability

---

## Backend/API

Responsible for:

- alert APIs
- integrations
- dashboards

---

# 31. Scalability Considerations

Future scalability improvements:

- Kafka partition scaling
- Spark cluster deployment
- Distributed MinIO
- Kubernetes orchestration

---

# 32. Reliability Considerations

Future improvements:

- checkpointing
- retry strategies
- dead-letter queues
- exactly-once semantics

---

# 33. Security Considerations

Future improvements:

- TLS encryption
- Kafka authentication
- Secrets management
- Role-based access

---

# 34. Performance Considerations

Important future optimizations:

- partition tuning
- state store optimization
- adaptive microbatching
- serialization optimization

---

# 35. Known Limitations

Current limitations:

- single-node infrastructure
- no persistent checkpoints
- no ML inference yet
- console sink only
- local development only

---

# 36. Long-Term Vision

Long-term goal:

Build a production-grade real-time anomaly detection platform capable of:

- ingesting millions of events
- generating streaming behavioral intelligence
- detecting anomalies in real time
- serving low-latency predictions
- supporting scalable distributed processing

This project is intended to simulate modern real-world streaming data architectures used by:

- fintech companies
- payment processors
- fraud detection platforms
- observability systems
- large-scale event-driven applications