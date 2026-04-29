# Databricks notebook source
from pyspark.sql import functions as F

# 1. Read raw CSV files
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("/Volumes/dbw_personal_finance_lab/default/personal-finance-vol/raw/*.csv")
)

# 2. Rename columns
df = (
    df.withColumnRenamed("Date", "transaction_date")
      .withColumnRenamed("Transaction", "transaction")
      .withColumnRenamed("Deposit", "deposit")
      .withColumnRenamed("Withdrawal", "withdrawal")
      .withColumnRenamed("Balance in Original Currency", "balance_original_currency")
)

# 3. Convert date column safely
df = df.withColumn(
    "transaction_date",
    F.coalesce(
        F.try_to_timestamp("transaction_date", F.lit("yyyy/MM/dd")).cast("date"),
        F.try_to_timestamp("transaction_date", F.lit("yyyy-MM-dd HH:mm:ss")).cast("date"),
        F.try_to_timestamp("transaction_date", F.lit("yyyy-MM-dd H:mm:ss")).cast("date"),
        F.try_to_timestamp("transaction_date", F.lit("yyyy-MM-dd")).cast("date")
    )
)

# 4. Clean numeric columns
df = (
    df.withColumn("deposit", F.regexp_replace("deposit", ",", "").cast("double"))
      .withColumn("withdrawal", F.regexp_replace("withdrawal", ",", "").cast("double"))
      .withColumn("balance_original_currency", F.regexp_replace("balance_original_currency", ",", "").cast("double"))
)

# 5. Build unified amount column
df = df.withColumn(
    "amount",
    F.when(F.col("deposit").isNotNull(), F.col("deposit"))
     .when(F.col("withdrawal").isNotNull(), -F.col("withdrawal"))
)

# 6. Filter invalid rows
df = df.filter(F.col("transaction_date").isNotNull())
df = df.filter(
    ~F.col("transaction").isin("Balance Brought Forward", "Balance Carried Forward")
)

# 7. Add statement month
df = df.withColumn("statement_month", F.date_format("transaction_date", "yyyy-MM"))

# 8. Categorize transactions
df = df.withColumn(
    "category",
    F.when(F.col("transaction") == "Interest", "interest_income")
     .when(F.col("deposit").isNotNull(), "cash_in")
     .when(F.col("withdrawal").isNotNull(), "expense")
     .otherwise("other")
)

# 9. Final cleaned transaction table
transactions_cleaned = df.select(
    "transaction_date",
    "statement_month",
    "transaction",
    "deposit",
    "withdrawal",
    "balance_original_currency",
    "amount",
    "category"
)

# 10. Monthly summary
monthly_summary = transactions_cleaned.groupBy("statement_month").agg(
    F.round(F.sum(F.when(F.col("amount") > 0, F.col("amount")).otherwise(0)), 2).alias("total_income"),
    F.round(F.sum(F.when(F.col("amount") < 0, -F.col("amount")).otherwise(0)), 2).alias("total_expense"),
    F.round(F.sum("amount"), 2).alias("net_cashflow"),
    F.count("*").alias("transaction_count")
)

# 11. Category summary
category_summary = transactions_cleaned.groupBy("statement_month", "category").agg(
    F.round(F.sum("amount"), 2).alias("total_amount"),
    F.count("*").alias("transaction_count")
)

# 12. Display final outputs
display(transactions_cleaned.orderBy("transaction_date"))
display(monthly_summary.orderBy("statement_month"))
display(category_summary.orderBy("statement_month", "category"))

# 13. Save outputs as parquet
transactions_cleaned.write.mode("overwrite").parquet(
    "/Volumes/dbw_personal_finance_lab/default/personal-finance-vol/curated/transactions_cleaned"
)

monthly_summary.write.mode("overwrite").parquet(
    "/Volumes/dbw_personal_finance_lab/default/personal-finance-vol/curated/monthly_summary"
)

category_summary.write.mode("overwrite").parquet(
    "/Volumes/dbw_personal_finance_lab/default/personal-finance-vol/curated/category_summary"
)

# 14. Publish
spark.sql("CREATE SCHEMA IF NOT EXISTS finance")

monthly_summary.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("finance.monthly_summary")

category_summary.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("finance.category_summary")
