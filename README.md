# Sales Pipeline POC

## Overview

A metadata-driven PySpark data pipeline framework for ingesting, cleansing, validating, transforming, and loading data across Bronze and Silver layers in a Medallion Architecture.
The framework supports batch ingestion, Auto Loader streaming ingestion, configurable transformations, rule-driven data quality, audit logging, and Delta Lake persistence.

---

## Architecture

```text
Source Files
    │
    ▼
Bronze Ingestion
├── Load File
├── COPY INTO
└── Auto Loader
    │   With added metadata for run_id, ingestion_timestamp, etc
    ▼
Bronze Tables
    │
    ▼
Data Processing
├── Metadata Enrichment
├── Cleansing Rules
├── Data Quality Validation
├── Joins
└── Transformations
    │   With valid and cleaned data into silver tables and invalid records into quarantine table
    ▼
Silver Tables
    │
    ▼
Gold View    
    │
    ▼
Audit & Monitoring
```

---

## Core Capabilities

### Ingestion

* Batch file ingestion
* Delta `COPY INTO`
* Databricks Auto Loader
* CSV and Excel support
* Config-driven execution

### Data Quality

* Rule csv based validation
* Minimum value validation
* Duplicate detection
* Conflicting key detection
* Referential integrity checks
* Quarantine support

### Data Cleansing

* Default value replacement
* Regex cleansing
* Case standardization
* Date normalization
* Decimal precision handling
* Datatype enforcement

### Transformations

* Column rename
* Expression-based derivation
* Type casting
* Column selection
* Literal value injection
* Rounding
* Join strategies (Broadcast/Shuffle)

### Governance

* Audit logging
* Metadata enrichment
* Run tracking
* Error handling
* Operational monitoring

---

## Project Structure

```text
sales_pipeline_poc
│
├── DDL/
│   ├── bronze/
│   └── silver/
│   └── gold/
|
├── src/
│   ├── ingestion/
│   ├── readers/
│   ├── writers/
│   ├── transformation/
│   ├── cleanser/
│   ├── validation/
│   ├── helpers/
│   ├── notebooks/
│   └── config/
│
└── tests/
    └── unit/
```

---

## Processing Flow

### Bronze Layer

Supported ingestion patterns:

| Type        | Description                            |
| ----------- | -------------------------------------- |
| Load File   | Batch ingestion using Reader framework |
| COPY INTO   | Incremental Delta ingestion            |
| Auto Loader | Streaming ingestion                    |

### Silver Layer

Processing pipeline:

1. Read Bronze source
2. Apply pre-processing
3. Execute joins
4. Execute transformations
5. Apply cleansing rules from csv
6. Apply validation rules from csv
7. Persist Silver table
8. Write audit records
9. Consume via gold view

---

## Configuration Driven Design

All pipelines are controlled through JSON configuration.

Example:

```json
{
  "source": {
    "path": "/mnt/raw/customers",
    "format": "csv"
  },
  "target": {
    "table": "bronze_customers"
  }
}
```

No code changes are required for onboarding new datasets.

---

## Notebook Schedule

| Notebook          | Schedule        | Justification                                                                                      |
| ----------------- | --------------- | -------------------------------------------------------------------------------------------------- |
| `bronze_customer` | On File Arrival | Reference data that changes infrequently and should be ingested when new files are received.       |
| `bronze_orders`   | Auto Loaded     | Core transactional dataset requiring immediate ingestion upon arrival.                             |
| `bronze_products` | On File Arrival | Reference data that changes infrequently and should be ingested when new files are received.       |
| `silver_customer` | Daily           | Customer dimension data refreshed daily to support analytical reporting and yearly trend analysis. |
| `silver_orders`   | Hourly          | High-volume transactional data requiring frequent processing to keep downstream datasets current.  |
| `silver_products` | Daily           | Product dimension data refreshed daily to support analytical reporting and yearly trend analysis.  |
| `silver_sales`    | Daily           | Sales fact dataset refreshed daily to support analytical reporting and yearly trend analysis.      |
| `gold_aggregate`  | View            | Aggregated reporting layer generated on demand from silver data for analytical consumption.        |


---
## Testing

Unit tests cover:

* Readers
* Writers
* Ingestion jobs
* Transformations
* Join processing
* Cleansing strategies
* Validation framework
* Metadata helpers
* Audit utilities

Run tests:

```bash
pytest -v
```

---

## Design Principles

* Configuration over code
* Strategy Pattern
* Factory Pattern
* Single Responsibility Principle
* Open-Closed Principle
* Extensible rule engine
* Test-first development
* Cloud-native architecture

---

## Technology Stack

* Python
* PySpark
* Delta Lake
* Databricks Auto Loader
* Pytest

---

## Future Enhancements

* Data quality scorecards
* Great Expectations integration
* Data lineage tracking
* CDC ingestion support
* Workflow orchestration
* Observability dashboards
* Unity Catalog integration

---

## Author
Sindhura Rao