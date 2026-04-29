# Personal Finance Data Pipeline & Dashboard

## Overview
This project builds an end-to-end data pipeline using Databricks (PySpark) to process personal bank transaction data and visualize insights in Power BI.

## Architecture
Raw CSV → Data Cleaning → Transformation → Aggregation → BI Dashboard

## Tech Stack
- Databricks (PySpark)
- Delta Tables
- SQL
- Power BI

## Data Pipeline

### 1. Ingestion
- Load CSV files from volume storage

### 2. Data Cleaning
- Handle malformed date formats
- Standardize transaction fields

### 3. Transformation
- Create transaction_date
- Calculate amount (deposit - withdrawal)
- Categorize transactions (income, transfer)

### 4. Aggregation
- Monthly income / expense
- Net cashflow
- Transaction count

## Dashboard

Key Insights:
- Monthly cashflow trend
- Income vs expense distribution
- Transaction activity patterns

## Sample Output
<img width="2559" height="1293" alt="databricks_code" src="https://github.com/user-attachments/assets/4e039483-4613-43ec-9050-de37825c03c7" />

<img width="2559" height="1297" alt="databricks_sample_output" src="https://github.com/user-attachments/assets/79fbdc29-4ad4-4730-99e6-feeff2173090" />

<img width="2559" height="1439" alt="BI_Dashboard" src="https://github.com/user-attachments/assets/21510386-860b-460f-8bef-80140f7f6061" />
