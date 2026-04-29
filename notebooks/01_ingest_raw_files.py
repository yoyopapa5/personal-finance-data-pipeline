# Databricks notebook source
from pyspark.sql import functions as F

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/dbw_personal_finance_lab/default/personal-finance-vol/raw/*.csv")
)

df.show()
df.printSchema()
