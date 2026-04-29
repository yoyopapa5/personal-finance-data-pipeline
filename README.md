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
<img width="2559" height="1439" alt="BI_Dashboard" src="https://github.com/user-attachments/assets/21510386-860b-460f-8bef-80140f7f6061" />
