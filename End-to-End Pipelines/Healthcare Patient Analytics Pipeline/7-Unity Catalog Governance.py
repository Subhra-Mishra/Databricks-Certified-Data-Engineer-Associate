# Databricks notebook source
# MAGIC %md
# MAGIC Phase 7: Unity Catalog Governance (Section 5)
# MAGIC <br>Step 7.1: Create Roles and Permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GROUPS;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG endtoenddatasets;
# MAGIC SELECT current_catalog();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG endtoenddatasets TO `subhras.mishra@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA endtoenddatasets.gold TO `subhras.mishra@gmail.com`;
# MAGIC GRANT SELECT ON SCHEMA endtoenddatasets.gold TO `subhras.mishra@gmail.com`;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 1
# %sql
# -- Create groups (if not already created)
# CREATE GROUP IF NOT EXISTS data_engineers;
# CREATE GROUP IF NOT EXISTS data_analysts;
# CREATE GROUP IF NOT EXISTS external_partners;

# -- Grant catalog-level permissions
# GRANT USE CATALOG ON CATALOG endtoenddatasets TO data_engineers;
# GRANT USE CATALOG ON CATALOG endtoenddatasets TO data_analysts;

# -- Schema-level permissions
# GRANT USE SCHEMA ON SCHEMA endtoenddatasets.bronze TO data_engineers;
# GRANT USE SCHEMA ON SCHEMA endtoenddatasets.silver TO data_engineers;
# GRANT USE SCHEMA ON SCHEMA endtoenddatasets.gold TO data_engineers;
# GRANT USE SCHEMA ON SCHEMA endtoenddatasets.healthcare TO data_engineers;
# GRANT USE SCHEMA ON SCHEMA endtoenddatasets.gold TO data_analysts;

# -- Table-level permissions
# GRANT SELECT ON ALL TABLES IN SCHEMA endtoenddatasets.gold TO data_analysts;
# GRANT ALL PRIVILEGES ON SCHEMA endtoenddatasets.bronze TO data_engineers;
# GRANT ALL PRIVILEGES ON SCHEMA endtoenddatasets.silver TO data_engineers;
# GRANT ALL PRIVILEGES ON SCHEMA endtoenddatasets.gold TO data_engineers;

# -- Volume-level permissions
# GRANT READ VOLUME ON VOLUME endtoenddatasets.healthcare.raw TO data_analysts;
# GRANT ALL PRIVILEGES ON VOLUME endtoenddatasets.healthcare.raw TO data_engineers;

# -- Verify permissions
# SHOW GRANTS ON CATALOG endtoenddatasets;
# SHOW GRANTS ON SCHEMA endtoenddatasets.gold;
# SHOW GRANTS ON VOLUME endtoenddatasets.healthcare.raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create groups (if not already created)
# MAGIC -- CREATE GROUP `subhras.mishra@gmail.com`;
# MAGIC -- CREATE GROUP `subhras.mishra@gmail.com`;
# MAGIC -- CREATE GROUP external_partners;
# MAGIC
# MAGIC
# MAGIC -- Grant catalog-level permissions
# MAGIC GRANT USE CATALOG ON CATALOG endtoenddatasets TO `subhras.mishra@gmail.com`;
# MAGIC GRANT USE CATALOG ON CATALOG endtoenddatasets TO `subhras.mishra@gmail.com`;
# MAGIC
# MAGIC -- Schema-level permissions
# MAGIC GRANT USE SCHEMA ON SCHEMA endtoenddatasets.bronze TO `subhras.mishra@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA endtoenddatasets.gold TO `subhras.mishra@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA endtoenddatasets.silver TO `subhras.mishra@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA endtoenddatasets.healthcare TO `subhras.mishra@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA endtoenddatasets.gold TO `subhras.mishra@gmail.com`;
# MAGIC
# MAGIC -- Table-level permissions
# MAGIC GRANT SELECT ON SCHEMA endtoenddatasets.gold TO `subhras.mishra@gmail.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA endtoenddatasets.bronze TO `subhras.mishra@gmail.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA endtoenddatasets.silver TO `subhras.mishra@gmail.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA endtoenddatasets.gold TO `subhras.mishra@gmail.com`;
# MAGIC
# MAGIC -- Volume-level permissions
# MAGIC GRANT READ VOLUME ON VOLUME endtoenddatasets.healthcare.raw TO `subhras.mishra@gmail.com`;
# MAGIC GRANT ALL PRIVILEGES ON VOLUME endtoenddatasets.healthcare.raw TO `subhras.mishra@gmail.com`;
# MAGIC
# MAGIC -- Verify permissions
# MAGIC SHOW GRANTS ON CATALOG endtoenddatasets;
# MAGIC SHOW GRANTS ON SCHEMA endtoenddatasets.gold;
# MAGIC SHOW GRANTS ON VOLUME endtoenddatasets.healthcare.raw;

# COMMAND ----------

# MAGIC %md
# MAGIC Step 7.2: Managed vs External Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create MANAGED table (data stored in Unity Catalog managed location)
# MAGIC CREATE OR REPLACE TABLE endtoenddatasets.gold.patient_metrics_managed (
# MAGIC   patient_id STRING,
# MAGIC   total_visits INT,
# MAGIC   total_revenue DECIMAL(18, 2)
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Create EXTERNAL table (data stored in Unity Catalog Volume)
# MAGIC -- Using your existing healthcare.raw volume
# MAGIC CREATE OR REPLACE TABLE endtoenddatasets.gold.patient_metrics_external (
# MAGIC   patient_id STRING,
# MAGIC   total_visits INT,
# MAGIC   total_revenue DECIMAL(18, 2)
# MAGIC ) USING DELTA
# MAGIC LOCATION "/Volumes/endtoenddatasets/healthcare/raw/external_metrics/";
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare properties
# MAGIC DESCRIBE EXTENDED endtoenddatasets.gold.patient_metrics_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED endtoenddatasets.gold.patient_metrics_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Key differences:
# MAGIC -- Managed: 
# MAGIC --   - Unity Catalog controls storage location
# MAGIC --   - DROP TABLE deletes both metadata and data
# MAGIC -- External: 
# MAGIC --   - You specify the location (Volume path in this case)
# MAGIC --   - DROP TABLE deletes only metadata, data remains in the Volume

# COMMAND ----------

# MAGIC %md
# MAGIC Step 7.3: Enable Audit Logging
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Audit logs are automatically stored by Unity Catalog
# MAGIC -- Query system tables to view access logs
# MAGIC
# MAGIC SELECT 
# MAGIC   event_time,
# MAGIC   user_identity.email as user_email,
# MAGIC   request_params.table_full_name as table_accessed,
# MAGIC   action_name
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name IN ('getTable', 'readTable')
# MAGIC   AND event_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

