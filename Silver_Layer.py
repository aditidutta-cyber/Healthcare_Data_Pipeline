# Databricks notebook source
# Silver Layer - Read Bronze Data
from pyspark.sql import functions as F
bronze_path = "/Volumes/workspace/default/healthcare_data/bronze/bronze_patients"
silver_raw_df = spark.read.format("delta").load(bronze_path)
print("Bronze records loaded:", silver_raw_df.count())
print("Bronze columns:", len(silver_raw_df.columns))
display(silver_raw_df)

# COMMAND ----------

silver_df = silver_raw_df.select(
    F.col("Name").alias("name"),
    F.col("Age").cast("int").alias("age"),
    F.col("Gender").alias("gender"),
    F.col("Blood Type").alias("blood_type"),
    F.col("Medical Condition").alias("medical_condition"),
    F.to_date(F.col("Date of Admission"), "yyyy-MM-dd").alias("admission_date"),
    F.col("Doctor").alias("doctor"),
    F.col("Hospital").alias("hospital"),
    F.col("Insurance Provider").alias("insurance_provider"),
    F.col("Billing Amount").cast("double").alias("billing_amount"),
    F.col("Room Number").cast("int").alias("room_number"),
    F.col("Admission Type").alias("admission_type"),
    F.to_date(F.col("Discharge Date"), "yyyy-MM-dd").alias("discharge_date"),
    F.col("Medication").alias("medication"),
    F.col("Test Results").alias("test_results")
)
display(silver_df)

# COMMAND ----------

# DBTITLE 1,tttttttttttlhgzsxscvvvvvvvvv---------------------------
before_cleaning = silver_df.count()
silver_df = silver_df.dropDuplicates()
after_duplicates = silver_df.count()
silver_df = silver_df.dropna()
after_null_removal = silver_df.count()
print("Records before cleaning:", before_cleaning)
print("Records after duplicate removal:", after_duplicates)
print("Records after null removal:", after_null_removal)
print("Duplicates removed:", before_cleaning - after_duplicates)
print("Null-containing records removed:", after_duplicates - after_null_removal)

# COMMAND ----------

print("=== SILVER DATA QUALITY CHECK ===")
# Checking null values in every column
print("\nNull values by column:")
silver_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in silver_df.columns
]).show()
# Checking age range
print("\nAge statistics:")
silver_df.select(
    F.min("age").alias("minimum_age"),
    F.max("age").alias("maximum_age"),
    F.avg("age").alias("average_age")
).show()
# Checking the billing amount
print("\nBilling amount statistics:")
silver_df.select(
    F.min("billing_amount").alias("minimum_billing"),
    F.max("billing_amount").alias("maximum_billing"),
    F.avg("billing_amount").alias("average_billing")
).show()
# Checking invalid date relationships
invalid_dates = silver_df.filter(
    F.col("discharge_date") < F.col("admission_date")
).count()
print("Records with discharge date before admission date:", invalid_dates)

# COMMAND ----------

negative_billing_df = silver_df.filter(
    F.col("billing_amount") < 0
)
print("Number of records with negative billing:", negative_billing_df.count())
display(
    negative_billing_df.select(
        "name",
        "age",
        "hospital",
        "medical_condition",
        "billing_amount",
        "admission_date",
        "discharge_date"
    )
)

# COMMAND ----------

# Validate and remove invalid billing amounts
before_billing_cleaning = silver_df.count()
invalid_billing_count = silver_df.filter(
    F.col("billing_amount") < 0
).count()
silver_df = silver_df.filter(
    F.col("billing_amount") >= 0
)
after_billing_cleaning = silver_df.count()
print("Invalid negative billing records:", invalid_billing_count)
print("Records before billing validation:", before_billing_cleaning)
print("Records after billing validation:", after_billing_cleaning)

# COMMAND ----------

name_counts = silver_df.groupBy(
    F.lower(F.trim(F.col("name"))).alias("normalized_name")
).count()
print("Total unique normalized names:", name_counts.count())
print("Names appearing more than once:")
display(
    name_counts
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# Create a derived patient key from the normalized patient name
silver_df = silver_df.withColumn(
    "patient_key",
    F.sha2(
        F.lower(F.trim(F.col("name"))),
        256
    )
)
silver_df = silver_df.select(
    "patient_key",
    *[c for c in silver_df.columns if c != "patient_key"]
)
print("Derived patient key created.")
print("Total Silver records:", silver_df.count())
print("Unique patient keys:", silver_df.select("patient_key").distinct().count())

# COMMAND ----------

# Create a human-readable patient ID
from pyspark.sql.window import Window
silver_with_name = silver_df.withColumn(
    "normalized_name",
    F.lower(F.trim(F.col("name")))
)
patient_mapping = (
    silver_with_name
    .select("normalized_name")
    .distinct()
    .withColumn(
        "patient_number",
        F.row_number().over(
            Window.orderBy("normalized_name")
        )
    )
    .withColumn(
        "patient_key",
        F.concat(
            F.lit("P"),
            F.lpad(F.col("patient_number").cast("string"), 5, "0")
        )
    )
    .select("normalized_name", "patient_key")
)
silver_df = (
    silver_with_name
    .drop("patient_key")
    .join(patient_mapping, on="normalized_name", how="left")
    .drop("normalized_name")
)
silver_df = silver_df.select(
    "patient_key",
    *[c for c in silver_df.columns if c != "patient_key"]
)
print("Readable patient IDs created.")
print("Total Silver records:", silver_df.count())
print("Unique patient IDs:", silver_df.select("patient_key").distinct().count())
display(
    silver_df
    .select("patient_key", "name", "age", "hospital", "admission_date")
    .orderBy("patient_key")
    .limit(20)
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
#Normalize patient names
silver_df = silver_df.withColumn(
    "normalized_name",
    F.lower(F.trim(F.col("name")))
)
#Create one row per unique patient name
unique_patients = (
    silver_df
    .select("normalized_name")
    .distinct()
)
#Assign a readable patient key to each unique patient
patient_window = Window.orderBy("normalized_name")
patient_keys = (
    unique_patients
    .withColumn(
        "patient_key",
        F.format_string(
            "P%05d",
            F.row_number().over(patient_window)
        )
    )
)
#Attach the same patient key to all records
silver_df = (
    silver_df
    .drop("patient_key")
    .join(
        patient_keys,
        on="normalized_name",
        how="left"
    )
)
print("Readable patient keys created.")
print("Total Silver records:", silver_df.count())
print(
    "Unique patient keys:",
    silver_df.select("patient_key").distinct().count()
)
display(
    silver_df.select(
        "patient_key",
        "name",
        "age",
        "gender",
        "medical_condition"
    ).limit(20)
)

# COMMAND ----------

# Create initial SCD Type 2 records
from pyspark.sql.window import Window
from pyspark.sql import functions as F
scd_window = Window.partitionBy("patient_key").orderBy(
    F.col("admission_date").desc()
)
scd_df = (
    silver_df
    .withColumn(
        "row_num",
        F.row_number().over(scd_window)
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
scd_df = (
    scd_df
    .withColumn(
        "effective_start_date",
        F.col("admission_date")
    )
    .withColumn(
        "effective_end_date",
        F.to_date(F.lit("9999-12-31"))
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
)
print("Initial SCD records:", scd_df.count())
print(
    "Unique patient keys:",
    scd_df.select("patient_key").distinct().count()
)
display(
    scd_df.select(
        "patient_key",
        "name",
        "hospital",
        "medical_condition",
        "admission_date",
        "effective_start_date",
        "effective_end_date",
        "is_current"
    ).limit(20)
)

# COMMAND ----------

# Save the initial SCD Type 2 table as Delta
scd_path = "/Volumes/workspace/default/healthcare_data/silver/silver_patient_scd"
scd_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(scd_path)
print("Silver SCD table created successfully.")
print("Location:", scd_path)
print("Records:", scd_df.count())

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
# Load the existing Silver SCD Delta table
target = DeltaTable.forPath(spark, scd_path)
# Create two source records:
# 1. One record matches the existing patient and expires the old version
# 2. One record does not match and gets inserted as the new current version
expire_record = (
    changed_patient
    .withColumn("merge_key", F.col("patient_key"))
)
new_record = (
    changed_patient
    .withColumn("merge_key", F.lit(None).cast("string"))
)
merge_source = expire_record.unionByName(new_record)
# Perform SCD Type 2 MERGE
(
    target.alias("target")
    .merge(
        merge_source.alias("source"),
        """
        target.patient_key = source.merge_key
        AND target.is_current = true
        """
    )
    .whenMatchedUpdate(
        set={
            "effective_end_date":
                "date_sub(source.effective_start_date, 1)",
            "is_current":
                "false"
        }
    )
    .whenNotMatchedInsert(
        values={
            "patient_key": "source.patient_key",
            "name": "source.name",
            "age": "source.age",
            "gender": "source.gender",
            "blood_type": "source.blood_type",
            "medical_condition": "source.medical_condition",
            "admission_date": "source.admission_date",
            "doctor": "source.doctor",
            "hospital": "source.hospital",
            "insurance_provider": "source.insurance_provider",
            "billing_amount": "source.billing_amount",
            "room_number": "source.room_number",
            "admission_type": "source.admission_type",
            "discharge_date": "source.discharge_date",
            "medication": "source.medication",
            "test_results": "source.test_results",
            "effective_start_date": "source.effective_start_date",
            "effective_end_date": "source.effective_end_date",
            "is_current": "source.is_current"
        }
    )
    .execute()
)
print("SCD Type 2 MERGE completed successfully.")

# COMMAND ----------

# Verify SCD Type 2 history for P00001
scd_check = (
    spark.read
    .format("delta")
    .load(scd_path)
    .filter(F.col("patient_key") == "P00001")
    .select(
        "patient_key",
        "name",
        "billing_amount",
        "effective_start_date",
        "effective_end_date",
        "is_current"
    )
    .orderBy("effective_start_date")
)
display(scd_check)