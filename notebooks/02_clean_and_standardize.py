# Databricks notebook source
from pyspark.sql import functions as F

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/dbw_personal_finance_lab/default/personal-finance-vol/raw/*.csv")
)

# rename columns
df = (
    df.withColumnRenamed("Date", "transaction_date")
      .withColumnRenamed("Transaction", "transaction")
      .withColumnRenamed("Deposit", "deposit")
      .withColumnRenamed("Withdrawal", "withdrawal")
      .withColumnRenamed("Balance in Original Currency", "balance_original_currency")
)

# convert date
df = df.withColumn("transaction_date", F.to_date("transaction_date"))

# build unified amount column
df = df.withColumn(
    "amount",
    F.when(F.col("deposit").isNotNull(), F.col("deposit"))
     .when(F.col("withdrawal").isNotNull(), -F.col("withdrawal"))
)

# remove non-transaction balance rows
df = df.filter(
    ~F.col("transaction").isin("Balance Brought Forward", "Balance Carried Forward")
)

df.show()
df.printSchema()
print(df.count)
df.write.mode("overwrite")
