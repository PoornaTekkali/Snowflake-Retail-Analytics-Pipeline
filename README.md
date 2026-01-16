
# SNOWFLAKE REAL TIME ANALYTICS PIPELINE

## Overview

An end-to-end retail analytics pipeline built using **Snowflake** and **Azure Data Lake**, following a **Bronze → Silver → Gold** architecture.
The project ingests raw retail data, transforms it, and delivers analytics-ready datasets for reporting.


## Architecture

* **Bronze**: Raw data ingestion from Azure Data Lake
* **Silver**: Cleaned and transformed data
* **Gold**: Analytics layer using a star schema

---
## Repository Structure

```
├── customer.csv
├── products.json
├── transaction.snappy.parquet
├── external_stage_creation.sql
├── silver_transform.sql
├── gold_star_schema.sql
├── goldlayer_view1.sql
├── goldlayer_view2.sql
├── pipeline_tasks.sql
```
---

## Data Sources

* **customer.csv** – Customer data
* **products.json** – Product catalog
* **transaction.snappy.parquet** – Sales transactions

---

## Key Components

* **External Stage** to ingest CSV, JSON, and Parquet files
* **Silver transformations** for data cleaning and standardization
* **Gold star schema**:

  * `DIM_CUSTOMER`
  * `DIM_PRODUCT`
  * `FACT_SALES`
* **Streams & Tasks** for automated data refresh
* Analytical views for daily sales and customer insights

---

## Data Flow

Azure Data Lake → Bronze → Silver → Gold → Analytics Views

---

## Technologies

* Snowflake
* Azure Data Lake Storage (ADLS Gen2)
* SQL

---

## Use Case

Retail analytics pipeline designed for reporting, BI consumption, and data engineering portfolio demonstration.

