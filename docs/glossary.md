# Glossary

Key terms used in this project and in streaming data analytics.

---

## Kafka and Messaging

### Apache Kafka

A distributed event streaming platform used to publish, store, and consume
streams of records in real time. This project runs Kafka locally in KRaft mode.

### Bootstrap Servers

The host and port address used to connect to a Kafka broker.
Configured in `.env` as `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`.

### Broker

A Kafka server that receives, stores, and serves messages.
In this project one local broker handles all topics.

### Consumer

A Python program that reads messages from a Kafka topic.
See `kafka_consumer_case.py`.

### Consumer Group

A named group of consumers that share the work of reading a topic.
Each message is delivered to only one consumer in the group.
Configured in `.env` as `KAFKA_GROUP_ID`.

### KRaft Mode

Kafka's built-in consensus protocol that replaces ZooKeeper.
Used in Kafka 4.x and later. No separate ZooKeeper process is needed.

### Message / Event

A single record produced to and consumed from a Kafka topic.
In this project each message represents one sales transaction as JSON.

### Offset

The position of a message within a Kafka partition.
Offsets start at 0 and increase by 1 for each message.
Used to track which messages a consumer has already read.

### Partition

A subdivision of a Kafka topic that allows parallel processing.
This project uses one partition per topic for local development.

### Producer

A Python program that sends messages to a Kafka topic.
See `kafka_producer_case.py`.

### Topic

A named channel in Kafka where messages are published and consumed.
Configured in `.env` as `KAFKA_TOPIC`.

---

## Data and Validation

### Data Contract

A formal specification of the fields, types, and rules a valid message must follow.
Defined in `data_validation/data_contract_case.py`.

### Derived Fields

Fields calculated by the consumer from raw message values and reference data.
Examples include `subtotal`, `tax_amount`, and `total`.
See `data_engineering/derived_fields.py`.

### Reference Data

Lookup tables such as regions, products, and currencies loaded from CSV files.
Used during validation to confirm that field values are known and allowed.

### Rejected Record

A consumed message that failed one or more validation checks.
Rejected records are stored in DuckDB with a `validation_errors` field explaining why.

### Valid Record

A consumed message that passed all validation checks in the data contract.
Valid records are stored in DuckDB for analysis.

### Validation

The process of checking a consumed message against the data contract.
Invalid messages are separated from valid ones before storage.

---

## Storage and Analysis

### DuckDB

A fast, file-based analytical database that runs entirely in Python.
No server is required. This project stores consumed messages in a `.duckdb` file.

### SQL

Structured Query Language, used to create tables, insert records, and query results.
DuckDB uses standard SQL syntax.

### VARCHAR

A SQL column type for text values of variable length.
All fields in this project are stored as VARCHAR in DuckDB.

---

## Python and Tooling

### confluent-kafka

The official Python client for Apache Kafka, backed by Confluent.
Wraps the high-performance C library `librdkafka`.

### Data Class

A Python class decorated with `@dataclass` that automatically generates
`__init__`, `__repr__`, and other methods from field annotations.
Used for `KafkaSettings` and `ValidationResult` in this project.

### dotenv / .env

A file that stores environment variables such as Kafka connection settings.
Loaded at runtime by `python-dotenv`. Never committed to version control.

### Module

A single Python file. In this project, each `.py` file in `src/streaming/` is a module.

### Package

A directory of Python modules with an `__init__.py` file.
`datafun-streaming` is an installable package this project depends on.

### pyproject.toml

The single configuration file for a Python project.
Defines dependencies, tool settings, and build instructions.

### src Layout

A project structure where all importable code lives under `src/`.
Prevents accidental imports from the repo root during development.

### uv

A fast Python package and project manager used in place of pip and venv.
Run `uv sync` to install dependencies and `uv run` to execute scripts.

### Virtual Environment

An isolated Python installation for a project.
Created automatically by `uv` in the `.venv/` folder.
