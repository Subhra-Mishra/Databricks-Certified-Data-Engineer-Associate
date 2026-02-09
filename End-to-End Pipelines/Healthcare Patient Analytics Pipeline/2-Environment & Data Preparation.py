# Databricks notebook source
# MAGIC %md
# MAGIC Step 1.1: Enable Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Unity Catalog is enabled
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog for project
# MAGIC CREATE CATALOG IF NOT EXISTS endtoenddatasets;
# MAGIC USE CATALOG endtoenddatasets;
# MAGIC
# MAGIC -- Create schemas for medallion architecture
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1.2: Verify and Setup Unity Catalog Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the existing 'raw' volume in healthcare schema
# MAGIC SHOW VOLUMES IN endtoenddatasets.healthcare;
# MAGIC
# MAGIC -- Describe the raw volume
# MAGIC DESCRIBE VOLUME endtoenddatasets.healthcare.raw;
# MAGIC     
# MAGIC -- Optional: Create additional volumes if needed for checkpoints/staging
# MAGIC -- These are typically not needed as volumes, but included for completeness
# MAGIC CREATE VOLUME IF NOT EXISTS endtoenddatasets.healthcare.checkpoints;
# MAGIC -- CREATE VOLUME IF NOT EXISTS endtoenddatasets.healthcare.staging;

# COMMAND ----------

# Verify your raw volume path and create subdirectories for data organization
# Your data is already in: /Volumes/endtoenddatasets/healthcare/raw/

# List existing files in raw volume
display(dbutils.fs.ls("/Volumes/endtoenddatasets/healthcare/raw"))
# Create subdirectories if they don't exist (optional - organize by data type)
# Only create if you want to organize your existing data

subdirs = [
    "/Volumes/endtoenddatasets/healthcare/raw/vitals",
    "/Volumes/endtoenddatasets/healthcare/raw/admissions",
    "/Volumes/endtoenddatasets/healthcare/raw/billing",
    "/Volumes/endtoenddatasets/healthcare/raw/procedures"
]

for subdir in subdirs:
  try:
    dbutils.fs.ls(subdir)
  except:
    dbutils.fs.mkdirs(subdir)
    print(f"Created {subdir}")

print("/n Volume Structure Verified")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1.3: Create Sample Data Files

# COMMAND ----------

# MAGIC %run "/Users/subhras.mishra@gmail.com/Databricks lab/End-to-End Pipelines/Healthcare Patient Analytics Pipeline/1-Healthcare Data Creation"

# COMMAND ----------

