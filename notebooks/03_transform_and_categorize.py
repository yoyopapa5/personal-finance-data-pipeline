# Databricks notebook source
from pyspark.sql import functions as F

# 1. Read raw CSV files from volume
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

# 3. Convert date column safely (supports multiple formats)
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

# 6. Remove rows where date failed to parse
df = df.filter(F.col("transaction_date").isNotNull())

# 7. Remove non-transaction balance rows
df = df.filter(
    ~F.col("transaction").isin("Balance Brought Forward", "Balance Carried Forward")
)

# 8. Add statement month
df = df.withColumn("statement_month", F.date_format("transaction_date", "yyyy-MM"))

# 9. Categorize transactions
df = df.withColumn(
    "category",
    F.when(F.col("transaction") == "Interest", "interest_income")
     .when(F.col("deposit").isNotNull(), "cash_in")
     .when(F.col("withdrawal").isNotNull(), "expense")
     .otherwise("other")
)

# 10. Show cleaned data
display(df.orderBy("transaction_date"))

# 11. Monthly summary
monthly_summary = df.groupBy("statement_month").agg(
    F.sum(F.when(F.col("amount") > 0, F.col("amount")).otherwise(0)).alias("total_income"),
    F.sum(F.when(F.col("amount") < 0, -F.col("amount")).otherwise(0)).alias("total_expense"),
    F.sum("amount").alias("net_cashflow"),
    F.count("*").alias("transaction_count")
)

display(monthly_summary.orderBy("statement_month"))

# 12. Category summary
category_summary = df.groupBy("statement_month", "category").agg(
    F.sum("amount").alias("total_amount"),
    F.count("*").alias("transaction_count")
)

display(category_summary.orderBy("statement_month", "category"))
print(df.count)
df.write.mode("overwrite")
