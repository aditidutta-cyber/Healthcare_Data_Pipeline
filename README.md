# Healthcare Data Pipeline

## Overview

This project implements a healthcare data pipeline using the Medallion Architecture:

**Bronze → Silver → Gold**

The pipeline ingests raw healthcare records, cleans and validates the data, creates a readable patient key, maintains history using SCD Type 2, and produces business-ready analytical datasets.

## Architecture

```text
Raw Healthcare CSV
       |
       v
+-------------+
|   Bronze    |
| Raw Data    |
+-------------+
       |
       v
+-------------+
|   Silver    |
| Cleaning    |
| Validation  |
| Patient Key |
| SCD Type 2  |
+-------------+
       |
       v
+-------------+
|    Gold     |
| Analytics   |
+-------------+
```

## Technology Stack

- Databricks
- Python
- PySpark
- Spark SQL
- Delta Lake
- Medallion Architecture

## Bronze Layer

The Bronze layer ingests the raw healthcare CSV data and preserves the source data for downstream processing.

- Input file: `patients_records.csv`
- Raw records: **55,500**
- Storage: Delta
- Bronze path: `/Volumes/workspace/default/healthcare_data/bronze/bronze_patients`

## Silver Layer

The Silver layer prepares the data for reliable analysis.

### Data Cleaning

- Standardized column names
- Applied appropriate data types
- Checked for null values
- Removed duplicate records
- Validated billing amounts
- Validated admission/discharge dates
- Created a readable patient key

### Cleaning Results

| Metric | Result |
|---|---:|
| Raw records | 55,500 |
| Records after duplicate removal | 54,966 |
| Invalid negative billing records removed | 106 |
| Final Silver records | 54,860 |
| Unique patient keys | 40,167 |

### Patient Key

A readable patient identifier was created in the format:

`P00001`, `P00002`, `P00003`, ...

This allows repeated records belonging to the same normalized patient to be associated with one patient key.

### SCD Type 2

SCD Type 2 was implemented to preserve historical changes.

The SCD table contains:

- `patient_key`
- `effective_start_date`
- `effective_end_date`
- `is_current`

A Delta `MERGE` was used to update changed records while retaining the previous version.

Final SCD results:

- Total SCD records: **40,168**
- Current patient records: **40,167**

Silver SCD path:

`/Volumes/workspace/default/healthcare_data/silver/silver_patient_scd`

## Gold Layer

The Gold layer contains business-ready analytical datasets.

| Gold Table | Records/Groups | Purpose |
|---|---:|---|
| Patient Summary | 40,167 | Current patient-level information |
| Hospital Analytics | 32,786 | Hospital-level patient and billing analysis |
| Medical Condition Analytics | 6 | Condition-level analysis |
| Insurance Analytics | 5 | Insurance-provider analysis |
| Monthly Admission Analytics | 61 | Monthly admission trends |
| Admission Type Analytics | 3 | Admission-type analysis |
| Hospital Ranking | 32,786 | Hospital ranking analysis |

## Final Validation

All Gold tables were validated successfully.

```text
Patient Summary:             40,167
Hospital Analytics:          32,786
Condition Analytics:         6
Insurance Analytics:         5
Monthly Admission:           61
Admission Type:              3
Hospital Ranking:            32,786

All Gold tables validated successfully.
```

## Project Structure

```text
Healthcare_Data_Pipeline/
├── README.md
├── Bronze_Layer
├── Silver_Layer
└── Gold_Layer
```

## Future Scope

The pipeline can be extended with:

- Real-time ingestion using Kafka
- BI dashboards using Power BI, Tableau, or Databricks SQL
- Predictive analytics
- MLOps workflows for model training, deployment, monitoring, and retraining

## Conclusion

The completed pipeline demonstrates a healthcare data engineering workflow using Databricks, PySpark, Delta Lake, and the Medallion Architecture. Raw data is preserved in Bronze, cleaned and historized in Silver, and transformed into business-ready analytics in Gold.
