# Databricks notebook source
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.getOrCreate()
 
for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {layer}")
print("Schemas created: bronze, silver, gold")

# COMMAND ----------


databases = spark.sql("SHOW DATABASES")
databases.show()