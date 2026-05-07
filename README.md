# E-Commerce ETL Pipeline Submission

## Project Overview
This project implements a complete data engineering pipeline to process, clean, and analyze e-commerce data. The pipeline handles data ingestion, quality enforcement, complex transformations, and partitioned storage.

## Technical Choice: Pandas vs. PySpark
While the initial assessment suggested PySpark, this implementation uses **Pandas** for the following reasons:
1. **Efficiency for Scale**: The current dataset contains approximately 2,000 rows. Pandas is significantly faster and more resource-efficient than PySpark for data of this scale, which requires significant overhead to initialize a SparkSession.
2. **Environment Compatibility**: During development, we encountered persistent issues with the local Java Runtime environment required by PySpark. To ensure a robust, runnable, and easily verifiable submission, we opted for the Pandas stack.
3. **Logic Parity**: Every complex operation requested (Window functions, joins, schema enforcement) has been implemented in Pandas with the exact same logical rigor as a Spark implementation.

## Task Implementation Summary

### Task 01: Data Ingestion & Schema Enforcement
- **Approach**: Used a custom loader that validates every row against expected types. 
- **Handling**: Any row containing NULL values in key fields or failed type casts is diverted to the `output/rejected/` directory to prevent silent data loss.

### Task 02: Data Quality & Cleaning
- **Duplicates**: Removed using `drop_duplicates()`.
- **Date Normalization**: Robust parsing that handles both `YYYY-MM-DD` and `DD/MM/YYYY` formats.
- **Categorical Cleaning**: Standardized `customer_tier` to lowercase for consistent grouping.

### Task 03: Joins & Enrichment
- **Orphan Identification**: Used an "anti-join" logic to find items referencing non-existent orders, saving them to `output/orphaned/`.
- **Derivations**: Calculated `net_amount` based on discounts at the row level.

### Task 04: Aggregations & Window Functions
- **Ranking**: Implemented country-wise spending ranks using dense ranking logic.
- **Rolling Windows**: Created a 7-day rolling order count per customer based on chronologically sorted order dates.

### Task 05: Return Analysis
- **Return Rates**: Computed the ratio of returns per category and tier relative to total unique orders.
- **Anomaly Detection**: Flagged instances where refund amounts exceeded the calculated net order amount.

### Task 06: Output & Partitioning
- **Storage**: The final dataset is saved as a **Parquet** file.
- **Organization**: Partitioned physically by `year` and `month` to simulate professional data lake structures.

## Bonus Challenges
- **B1 (Unit Tests)**: Created `test_pipeline.py` to verify date logic and calculations.
- **B3 (DQ Gate)**: Implemented a runtime check that raises an exception if NULL customers are detected after the join phase.
- **Optimized Windowing**: Utilized vectorized rolling functions for high-performance time-series analysis.

---
## Setup & Execution
1. Install requirements: `pip install -r requirements.txt`
2. Run pipeline: `python pipeline.py`
3. Verify tests: `pytest test_pipeline.py`
