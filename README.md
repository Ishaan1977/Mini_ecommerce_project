# E-Commerce Customer Analytics & Order Processing Pipeline

A distributed data engineering pipeline built with **PySpark** to ingest, clean, and profile e-commerce datasets. The project implements window functions, multi-dimensional relational joins, and advanced analytical aggregations to isolate premium customer tiers, audit fulfillment health, and track seasonal transaction trends.

## Project Architecture & Pipeline Workflow

The pipeline transitions raw, unstructured transactional records into optimized analytical outputs through a sequential multi-stage engineering workflow:
[Raw Customers & Orders Data]
│
▼
[Data Sanitization] ────► (Type Casting, Null Imputation & Feature Extraction)
│
▼
[Advanced Aggregations] ──► (Window Functions, Pivot Tables & State Partitions)
│
▼
[Relational Integration] ─► (Inner Join on Customer Lifecycle + Orders Transactions)
│
▼
[Business Intelligence] ──► (RFM Profiling, Segmentation & Seasonal Analysis)
│
▼
[Optimized Storage Layer] ─► (Parquet Compression & Flattened CSV Exports)

---

## Core Technical Features

### 1. Data Schema Engineering & Sanitization
* **Strict Type Inference:** Converted legacy string data structures to deterministic types, enforcing `DateType` schemas onto unstructured registration strings and casting text flags to operational binary `BooleanType` conditions.
* **Null Imputation Strategy:** Mitigated sparse data risks by implementing structural fallbacks across key geographic dimensions (`city`, `state`, `country`), enforcing data consistency prior to partition boundaries.
* **Granular Time-Series Feature Extraction:** Isolated structural registration patterns by decomposing lifecycle timestamps into analytical components (`reg_yr`, `reg_month`).

### 2. Multi-Dimensional Customer Profiling
* **Geographic & Behavior Segmentation:** Leveraged distributed aggregations to profile regional customer densities and mapped geographic active vs. inactive user ratios using multi-field pivot layouts.
* **Analytical Window Partitions:** Deployed advanced window specifications (`Window.partitionBy`) to apply comparative ranking metrics (`rank()`, `dense_rank()`, `row_number()`) across regional cohorts, separating customer acquisition velocities per state.
* **Cohort Identification:** Extracted chronological cohort baselines by computing extreme boundaries (`min` and `max` lifecycle timestamps) to isolate historic vs. newly acquired user segments across distinct urban centers.

### 3. Distributed E-Commerce Business Intelligence
* **Relational Core Joins:** Executed optimized inner joins between sanitized user profiles and core order transaction logs (`orders.csv`) to resolve unified lifecycle data matrices.
* **Revenue Valuation & Lifecycle Tiering:** Aggregated customer spending behaviors to identify top-tier premium users (e.g., highlighting accounts like Customer IDs `3336` and `3884` with maximum wallet share).
* **Fulfillment Operations Audit:** Categorized systemic fulfillment patterns by auditing discrete order state distributions (`Pending`, `Shipped`, `Delivered`, `Cancelled`) to isolate supply chain bottlenecks.
* **High-Frequency/Low-Value Matrix:** Constructed multi-dimensional ordering arrays (`orderBy(col('count').desc(), col('sum(total_amount)'))`) to successfully map high-frequency, low-ticket consumer behaviors.

---

## Production Technical Stack

* **Core Engine:** Apache Spark / PySpark (Structured DataFrames API)
* **Execution Environment:** Databricks Serverless Compute Runtime
* **Language:** Python 3.12
* **Storage Formats:** Apache Parquet (Snappy Compressed Columnar Layout), Flat CSV

---

## Deep-Dive Business Insights Discovered

* **Demographic Concentration:** Customer distribution across the 8 mapped cities remains highly uniform (averaging ~2,200 users per city), indicating successful, localized market penetration.
* **Fulfillment Disruptions:** The dataset reveals an elevated order cancellation trend (~4,469 records), which exceeds completed delivery metrics (~4,341 records). This represents a core operational vulnerability requiring deeper supply chain or payment gateway debugging.
* **Macro Transaction Stability:** Monthly ordering volumes show highly resilient purchasing habits, maintaining stable transaction frequencies throughout the calendar year with minor baseline fluctuations.

---

## Step-by-Step Execution Guide

### 1. Target Data Environment Setup
Ensure your underlying environment contains your source transactional schemas (`customers.csv` and `orders.csv`). Adjust the localized repository and file directory constants matching your setup:
```python
# Directory Variable Setup
customer_source = "/YourWorkspacePath/customers.csv"
order_source = "/YourWorkspacePath/orders.csv"
```
### 2. Run Pipeline Steps
Execute the notebook sequentially to trigger the parsing steps:

Initialize Session: Spins up the default SparkSession context.

Transform Customers: Ingests raw data, resolves date/boolean structures, strips missing cells, and calculates rank distributions.

Analyze Transactions: Evaluates transactional trends, groups regional cohorts, and builds analytical spend classifications.

Data Delivery Stage: Formats data matrices into efficient storage targets.

### 3. Storage Ingestion & Export Targets
To avoid workspace root write constraints on serverless architectures, the pipeline converts highly summarized Spark clusters into localized memory blocks to write clean output assets:
```python
# Convert processed metrics to native memory arrays
pandas_df = customer_orders_dfr.toPandas()

# Generate local persistent storage footprints
out_csv_path = "./Mini_ecommerce_project/customer_and_orders_output.csv"
pandas_df.to_csv(out_csv_path, index=False)
```
