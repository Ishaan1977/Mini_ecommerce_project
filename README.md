# E-Commerce Customer Analytics & Order Processing Pipeline (Synthetic Data)

A distributed data engineering pipeline built with **PySpark** on Databricks to practice data cleaning, schema enforcement, and analytical modeling. 

**Note on Dataset Context:** This project intentionally utilizes a synthetically generated, non-real-world dataset designed to simulate complex data-quality issues (such as structural anomalies, null records, and geographical mismatches like cities paired with incorrect states). The core focus of this project is demonstrating how to engineer robust programmatic guards to parse, clean, and extract business intelligence from highly flawed upstream data.

## Project Architecture & Pipeline Workflow

The pipeline transitions raw, unstructured transactional records into optimized analytical outputs through a sequential multi-stage engineering workflow:
```text
[Synthetic Customers & Orders Data] 
                 │
                 ▼
     [Data Cleansing Layer] ────► (Type Casting, Fault Mitigation & Feature Extraction)
                 │
                 ▼
     [Advanced Aggregations] ──► (Window Functions, Pivot Tables & State Partitions)
                 │
                 ▼
     [Relational Integration] ─► (Inner Join on Customer Lifecycle + Orders Transactions)
                 │
                 ▼
     [Business Intelligence] ──► (RFM Profiling, Behavioral Segmentation & Trend Analysis)
                 │
                 ▼
     [Optimized Storage Layer] ─► (Pandas-Assisted Local Workspace Write/Parquet)
```
---

## Core Technical Features

### 1. Handling Mock Data Anomalies & Sanitization
* **Strict Type Inference:** Cleaned raw string fields into deterministic data types, converting unstructured registration strings into explicit `DateType` schemas and text flags into operational `BooleanType` conditions.
* **Geographic Imputation & Anomaly Awareness:** Addressed structural simulation faults where key geographic tracking fields (`city`, `state`, `country`) contained synthetic mismatches or missing values, applying structural fallbacks to ensure logical partition boundaries could still be constructed.
* **Granular Feature Extraction:** Isolated time-series registration patterns by decomposing lifecycle timestamps into specific analytical target attributes (`reg_yr`, `reg_month`).

### 2. Multi-Dimensional Customer Profiling
* **Geographic & Behavior Segmentation:** Leveraged distributed aggregations to profile regional customer densities and mapped geographic active vs. inactive user ratios using multi-field pivot layouts.
* **Analytical Window Partitions:** Deployed advanced window specifications (`Window.partitionBy`) to apply comparative ranking metrics (`rank()`, `dense_rank()`, `row_number()`) across simulated regional cohorts, tracking customer acquisition distribution parameters.
* **Boundary Cohort Identification:** Tracked chronological cohort baselines by computing extreme boundaries (`min` and `max` lifecycle timestamps) to isolate early vs. newly generated user profiles within the mock layout.

### 3. Distributed E-Commerce Business Intelligence
* **Relational Core Joins:** Executed optimized inner joins between sanitized user profiles and transactional order history tables (`orders.csv`) to resolve unified analytics data matrices.
* **Revenue Valuation & Lifecycle Tiering:** Aggregated mock spending behaviors to identify top-tier premium users (e.g., isolating simulated accounts like Customer IDs `3336` and `3884` with maximum wallet share).
* **Fulfillment Operations Audit:** Audited discrete order state distributions (`Pending`, `Shipped`, `Delivered`, `Cancelled`) to isolate transactional flow trends built into the generation model.

---

## Production Technical Stack

* **Core Engine:** Apache Spark / PySpark (Structured DataFrames API)
* **Execution Environment:** Databricks Serverless Compute Runtime (Free / Community Tier Edition)
* **Language:** Python 3.12
* **Storage Formats:** Apache Parquet, Flat CSV

---

## Pipeline Insights & Data Observations

Because this dataset was generated artificially for testing and practice, the metrics reflect synthetic design rules rather than organic economic activity:
* **Uniform Demographic Distribution:** Customer distribution across the 8 mapped cities remains highly uniform (averaging ~2,200 users per city), indicating an evenly distributed random generator framework upstream.
* **High Synthetic Noise:** The dataset exhibits unexpected skewing, such as an elevated order cancellation trend (~4,469 records) outpacing completed delivery metrics (~4,341 records). In a real production system, this would point to severe fulfillment failures, but here it highlights the pipeline's capability to safely parse high-error mock data streams.
* **Macro Transaction Stability:** Monthly ordering volumes remain exceptionally stable across the calendar year, a clear sign of simulated baseline random distributions with zero organic holiday or seasonal spikes.

---

## Step-by-Step Execution Guide

### 1. Target Data Environment Setup
Ensure your underlying environment contains your source transactional schemas (`customers.csv` and `orders.csv`). Adjust the localized repository and file directory constants matching your setup:
```python
# Directory Variable Setup
customer_source = "file:/Workspace/Users/your_email/Mini_ecommerce_project/customers.csv"
order_source = "file:/Workspace/Users/your_email/Mini_ecommerce_project/orders.csv"
```
### 2. Run Pipeline Steps
* Execute the notebook sequentially to trigger the parsing steps:

* Initialize Session: Spins up the default SparkSession context.

* Transform Customers: Ingests raw mock data, resolves date/boolean structures, handles missing fields, and calculates rank distributions.

* Analyze Transactions: Evaluates transactional trends, groups regional cohorts, and builds analytical spend classifications.

* Data Delivery Stage: Formats data matrices into efficient storage targets.

3. Serverless Environment Storage Fix
To bypass Spark root storage write restrictions common in serverless sandbox environments, the pipeline aggregates data in memory and utilizes Python-native Pandas writers to save outputs directly into Git-tracked repository folders:
```python
# Convert processed metrics to native memory arrays to bypass DBFS restrictions
pandas_df = customer_orders_dfr.toPandas()

# Generate local persistent storage footprints directly in your Git repository folder
out_csv_path = "./Mini_ecommerce_project/customer_and_orders_output.csv"
pandas_df.to_csv(out_csv_path, index=False)
```
