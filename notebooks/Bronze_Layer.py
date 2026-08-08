# Databricks notebook source
# Bronze Layer - Raw Data Ingestion
from pyspark.sql import functions as F
raw_path = "/Volumes/workspace/default/healthcare_data/patients_records.csv"
bronze_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(raw_path)
display(bronze_df)

# COMMAND ----------

print("Number of rows:", bronze_df.count())
print("Number of columns:", len(bronze_df.columns))

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

bronze_path = "/Volumes/workspace/default/healthcare_data/bronze/bronze_patients"
bronze_df.write \
    .format("delta") \
    .option("delta.columnMapping.mode", "name") \
    .option("delta.minReaderVersion", "2") \
    .option("delta.minWriterVersion", "5") \
    .mode("overwrite") \
    .save(bronze_path)
print("Bronze layer created successfully.")
print("Bronze location:", bronze_path)
