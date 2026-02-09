# Databricks notebook source
# MAGIC %md
# MAGIC Phase 2: Auto Loader Implementation (Section 2)
# MAGIC <br>
# MAGIC Step 2.1: Create Notebook - Bronze Layer Ingestion

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "/Users/subhras.mishra@gmail.com/Databricks lab/End-to-End Pipelines/Healthcare Patient Analytics Pipeline/2-Environment & Data Preparation"

# COMMAND ----------

# Configure Auto Loader for streaming ingestion from Unity Catalog Volumes
from pyspark.sql.functions import current_timestamp, input_file_name

# Define checkpoint location (using managed table location or create a checkpoints subdirectory)
# Option 1: Use a subdirectory in healthcare.raw volume
checkpoint_path = "/Volumes/endtoenddatasets/healthcare/raw/.checkpoints/patient_admissions"

# Option 2: Use Unity Catalog managed location (recommended)
# checkpoint_path will be managed automatically by Delta Live Tables or set explicitly

# Auto Loader - Stream patient admissions from Unity Catalog Volume
admissions_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.inferColumnTypes","true")
    .option("header", "true")
    .load("/Volumes/endtoenddatasets/healthcare/raw/admissions")
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_file", col("_metadata.file_name"))
)

# Write to Bronze Delta table (Unity Catalog managed table)
(
    admissions_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("endtoenddatasets.bronze.patient_admissions")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Step 2.2: Auto Loader for JSON Data

# COMMAND ----------

# Auto Loader for patient vitals (JSON format) from Unity Catalog Volume
checkpoint_path_vitals = "/Volumes/endtoenddatasets/healthcare/raw/.checkpoints/patient_vitals"

vitals_stream = (
  spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format","json")
  .option("cloudFiles.schemaLocation", f"{checkpoint_path_vitals}/schema")
  .option("cloudFiles.inferColumnTypes", "true")
  .load("/Volumes/endtoenddatasets/healthcare/raw/vitals/")
  .withColumn("ingestion_time", current_timestamp())
)

(
  vitals_stream.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path_vitals)
  .trigger(availableNow=True)
  .table("endtoenddatasets.bronze.patient_vitals")
)



# COMMAND ----------

# MAGIC %md
# MAGIC Step 2.3: Verify Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check tables created in bronze schema
# MAGIC SHOW TABLES IN endtoenddatasets.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Examine data
# MAGIC SELECT * FROM endtoenddatasets.bronze.patient_admissions LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED endtoenddatasets.bronze.patient_admissions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify data is managed by Unity Catalog
# MAGIC DESCRIBE DETAIL endtoenddatasets.bronze.patient_admissions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Examine data
# MAGIC SELECT * FROM endtoenddatasets.bronze.patient_vitals LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED endtoenddatasets.bronze.patient_vitals;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify data is managed by Unity Catalog
# MAGIC DESCRIBE DETAIL endtoenddatasets.bronze.patient_vitals;

# COMMAND ----------

# MAGIC %md
# MAGIC