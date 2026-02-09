# Databricks notebook source
# MAGIC %md
# MAGIC Phase 4: Gold Layer - Analytics Ready (Section 3)
# MAGIC Step 4.1: Create Notebook - Gold Layer Aggregations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Gold table using SQL DDL in Unity Catalog
# MAGIC CREATE OR REPLACE TABLE endtoenddatasets.gold.daily_revenue(
# MAGIC   revenue_date DATE,
# MAGIC   department STRING,
# MAGIC   total_revenue DECIMAL(18,2),
# MAGIC   total_admissions INT,
# MAGIC   avg_length_of_stay DECIMAL(5,2),
# MAGIC   unique_patients INT,
# MAGIC   updated_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert aggregated data
# MAGIC INSERT INTO endtoenddatasets.gold.daily_revenue
# MAGIC SELECT
# MAGIC   admission_date as revenue_date,
# MAGIC   department,
# MAGIC   SUM(total_charges) as total_revenue,
# MAGIC   COUNT(DISTINCT admission_id) as total_admissions,
# MAGIC   AVG(length_of_stay) as avg_length_of_stay,
# MAGIC   COUNT(DISTINCT patient_id) as unique_patients,
# MAGIC   CURRENT_TIMESTAMP() as updated_at
# MAGIC FROM endtoenddatasets.silver.patient_admissions
# MAGIC GROUP BY admission_date, department;
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4.2: Create Patient Summary Fact Table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Patient 360 view in Gold layer
# MAGIC CREATE OR REPLACE TABLE endtoenddatasets.gold.patient_summary 
# MAGIC AS
# MAGIC SELECT
# MAGIC   p.patient_id,
# MAGIC   p.patient_name,
# MAGIC   p.age,
# MAGIC   p.risk_level,
# MAGIC   COUNT(a.admission_id) as total_admissions,
# MAGIC   SUM(a.total_charges) as lifetime_charges,
# MAGIC   AVG(a.length_of_stay) as avg_length_of_stay,
# MAGIC   MAX(a.admission_date) as last_admission_date
# MAGIC FROM endtoenddatasets.silver.patient_admissions as a
# MAGIC JOIN endtoenddatasets.silver.patient_vitals as p
# MAGIC ON a.patient_id = p.patient_id
# MAGIC GROUP BY p.patient_id, p.patient_name, p.age, p.risk_level;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM endtoenddatasets.gold.patient_summary

# COMMAND ----------

