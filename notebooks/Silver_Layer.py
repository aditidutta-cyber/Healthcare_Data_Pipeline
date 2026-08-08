# Databricks notebook source
from pyspark.sql import functions as F
silver_path = "/Volumes/workspace/default/healthcare_data/silver/silver_patient_scd"
silver_scd_df = spark.read.format("delta").load(silver_path)
gold_base_df = (
    silver_scd_df
    .filter(F.col("is_current") == True)
)
print("Silver SCD records:", silver_scd_df.count())
print("Current patient records:", gold_base_df.count())
display(gold_base_df)

# COMMAND ----------

patient_summary_df = (
    gold_base_df
    .select(
        "patient_key",
        "name",
        "age",
        "gender",
        "blood_type",
        "medical_condition",
        "admission_date",
        "hospital",
        "insurance_provider",
        "billing_amount",
        "room_number",
        "admission_type",
        "discharge_date",
        "medication",
        "test_results"
    )
)
print("Gold Patient Summary records:", patient_summary_df.count())
display(
    patient_summary_df,
    gold_base_df
    .filter(F.col("patient_key") == "P00001"))

# COMMAND ----------

# Gold Table 2: Hospital Analytics
hospital_analytics_df = (
    gold_base_df
    .groupBy("hospital")
    .agg(
        F.count("*").alias("total_patients"),
        F.round(F.avg("age"), 2).alias("average_age"),
        F.round(F.sum("billing_amount"), 2).alias("total_billing"),
        F.round(F.avg("billing_amount"), 2).alias("average_billing")
    )
    .orderBy(F.desc("total_patients"))
)
print("Number of hospitals:", hospital_analytics_df.count())
display(hospital_analytics_df)

# COMMAND ----------

# Gold Table 3: Medical Condition Analytics
condition_analytics_df = (
    gold_base_df
    .groupBy("medical_condition")
    .agg(
        F.count("*").alias("total_patients"),
        F.round(F.avg("age"), 2).alias("average_age"),
        F.round(F.sum("billing_amount"), 2).alias("total_billing"),
        F.round(F.avg("billing_amount"), 2).alias("average_billing")
    )
    .orderBy(F.desc("total_patients"))
)
print("Number of medical conditions:", condition_analytics_df.count())
display(condition_analytics_df)

# COMMAND ----------

# Gold Table 4: Insurance Analytics
insurance_analytics_df = (
    gold_base_df
    .groupBy("insurance_provider")
    .agg(
        F.count("*").alias("total_patients"),
        F.round(F.sum("billing_amount"), 2).alias("total_billing"),
        F.round(F.avg("billing_amount"), 2).alias("average_billing")
    )
    .orderBy(F.desc("total_patients"))
)
print("Number of insurance providers:", insurance_analytics_df.count())
display(insurance_analytics_df)

# COMMAND ----------

# Gold Table 5: Monthly Admission Analytics
monthly_admission_df = (
    gold_base_df
    .withColumn(
        "admission_month",
        F.date_format(F.col("admission_date"), "yyyy-MM")
    )
    .groupBy("admission_month")
    .agg(
        F.count("*").alias("total_admissions"),
        F.round(F.sum("billing_amount"), 2).alias("total_billing"),
        F.round(F.avg("billing_amount"), 2).alias("average_billing")
    )
    .orderBy("admission_month")
)
print("Number of admission months:", monthly_admission_df.count())
display(monthly_admission_df)

# COMMAND ----------

# Gold Table 6: Admission Type Analytics
admission_type_df = (
    gold_base_df
    .groupBy("admission_type")
    .agg(
        F.count("*").alias("total_patients"),
        F.round(F.sum("billing_amount"), 2).alias("total_billing"),
        F.round(F.avg("billing_amount"), 2).alias("average_billing")
    )
    .orderBy(F.desc("total_patients"))
)
print("Number of admission types:", admission_type_df.count())
display(admission_type_df)

# COMMAND ----------

from pyspark.sql.window import Window
# Gold Table 7: Hospital Ranking
hospital_ranking_df = (
    hospital_analytics_df
    .withColumn(
        "hospital_rank",
        F.dense_rank().over(
            Window.orderBy(F.desc("total_patients"))
        )
    )
    .select(
        "hospital_rank",
        "hospital",
        "total_patients",
        "total_billing",
        "average_billing",
        "average_age"
    )
    .orderBy("hospital_rank")
)
print("Hospital ranking created successfully.")

display(hospital_ranking_df)

# COMMAND ----------

# Save Gold tables as Delta tables

gold_base_path = "/Volumes/workspace/default/healthcare_data/gold"

# 1. Patient Summary
patient_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/patient_summary")

# 2. Hospital Analytics
hospital_analytics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/hospital_analytics")

# 3. Medical Condition Analytics
condition_analytics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/condition_analytics")

# 4. Insurance Analytics
insurance_analytics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/insurance_analytics")

# 5. Monthly Admission Analytics
monthly_admission_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/monthly_admission")

# 6. Admission Type Analytics
admission_type_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/admission_type")

# 7. Hospital Ranking
hospital_ranking_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/hospital_ranking")

print("All Gold tables saved successfully.")
print("Gold location:", gold_base_path)

# COMMAND ----------

# Final Gold Layer Validation
gold_tables = {
    "Patient Summary": patient_summary_df,
    "Hospital Analytics": hospital_analytics_df,
    "Condition Analytics": condition_analytics_df,
    "Insurance Analytics": insurance_analytics_df,
    "Monthly Admission": monthly_admission_df,
    "Admission Type": admission_type_df,
    "Hospital Ranking": hospital_ranking_df
}
print("=== GOLD LAYER VALIDATION ===")
for table_name, df in gold_tables.items():
    print(f"{table_name}: {df.count()} records")
print("\nAll Gold tables validated successfully.")
