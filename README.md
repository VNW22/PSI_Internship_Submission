# E-Commerce ETL Pipeline Submission (PySpark)

## Project Overview
This project implements a complete data engineering pipeline to process, clean, and analyze e-commerce data using **PySpark**. The pipeline handles data ingestion with strict schema enforcement, complex window-based transformations, and partitioned storage in Parquet format.

## Technical Choice: PySpark
This implementation uses **PySpark 4.1.1**. While we initially considered a Pandas-based approach due to the small dataset size and some early environment setup challenges with Java, we have successfully configured the environment and implemented the full solution in PySpark to meet the original assessment requirements.

## Task Implementation Summary

### Task 01: Data Ingestion & Schema Enforcement
- **Approach**: Used `StructType` to define explicit schemas for all four tables.
- **Handling**: Rows failing schema validation are moved to the `output/rejected/` directory using Spark's `subtract` and `dropna` logic.

### Task 02: Data Quality & Cleaning
- **Duplicates**: Removed using `dropDuplicates()`.
- **Date Normalization**: Used `F.coalesce` and `F.to_date` to handle multiple incoming date formats (`yyyy-MM-dd` and `dd/MM/yyyy`).
- **Lowercasing**: Standardized `customer_tier` values using Spark's native `lower()` function.

### Task 03: Joins & Enrichment
- **Orphan Items**: Isolated using a `left_anti` join between `order_items` and `orders`.
- **Enrichment**: Joined orders with customers and items. Added a calculated `net_amount` column.

### Task 04: Aggregations & Window Functions
- **Ranking**: Used `Window.partitionBy("country")` and `F.rank()` to rank customers by lifetime spend.
- **Rolling Window**: Implemented a 7-day rolling order count per customer using `rangeBetween` on unix timestamps.
- **Monthly Revenue Share**: Calculated category-wise revenue share per month using window partitions.

### Task 05: Return Analysis
- **Metrics**: Calculated return rates per category and tier.
- **Anomaly Detection**: Flagged returns where the refund amount exceeded the net order total.

### Task 06: Output & Partitioning
- **Storage**: Final enriched data is saved in **Parquet** format.
- **Partitioning**: Data is partitioned physically by `year` and `month` to optimize query performance.

## Bonus Challenges
- **B2 (Broadcast Join)**: Applied a broadcast hint to the `customers` table during the join to optimize performance.
- **B3 (DQ Gate)**: Added a check before the final write to ensure no NULL customer IDs exist in the enriched dataset.
- **B4 (Explain Plan)**: Included a `df.explain(mode="formatted")` call at the end of the pipeline to audit the execution plan.

---
## Setup & Execution
1. **Prerequisite**: Java Runtime (OpenJDK/Temurin) must be installed.
2. Install requirements: `pip install -r requirements.txt`
3. Run pipeline: `python pipeline.py`
