# Databricks notebook source
# MAGIC %md
# MAGIC Phase 3: Silver Layer Transformations (Section 3)
# MAGIC <br>Step 3.1: Create Notebook - Silver Layer Processing

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

bronze_admissions = spark.table("endtoenddatasets.bronze.patient_admissions")

silver_admissions = (bronze_admissions
                     .dropDuplicates(["admission_id"])
                     .filter(col("patient_id").isNotNull())
                     .filter(col("admission_date") <= current_date())
                     .withColumn("length_of_stay", datediff(col("discharge_date"), col("admission_date")))
                     .withColumn("admission_year", year(col("admission_date")))
                     .withColumn("admission_month", month(col("admission_date")))
                     .withColumn("processed_time", current_timestamp())
                     )

(silver_admissions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("endtoenddatasets.silver.patient_admissions")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3.2: Create User-Defined Functions (UDF)
# MAGIC

# COMMAND ----------

# Define UDF for calculating risk score
from pyspark.sql.functions import udf, col, size
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def calculate_risk_level(age, chronic_conditions):
  if age > 65 and chronic_conditions > 2:
    return "HIGH"
  elif age > 50 or chronic_conditions > 1:
    return "MEDIUM"
  else:
    return "LOW"
  

silver_patients = (spark
                   .table("endtoenddatasets.bronze.patient_vitals")
                   .withColumn("risk_level", 
                               calculate_risk_level(col("age"), size(col("chronic_conditions")))
                               )
                   )

(silver_patients
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("endtoenddatasets.silver.patient_vitals")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3.3: Complex Aggregations with PySpark
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window

# Complex aggregations - Department metrics
department_metrics = (silver_admissions
                      .groupBy("department", "admission_year", "admission_month")
                      .agg(
                        count("admission_id").alias("total_admissions"),
                        avg("length_of_stay").alias("avg_length_of_stay"),
                        sum("total_charges").alias("total_revenue"),
                        countDistinct("patient_id").alias("unique_patients")
                        )
                      )

# Window functions - Running totals
windowSpec = (Window
              .partitionBy("department")
              .orderBy("admission_year", "admission_month")
              )

department_trends = (department_metrics
                     .withColumn("running_total_revenue",
                                 sum("total_revenue")
                                 .over(windowSpec))
                     .withColumn("prev_month_admissions",
                                 lag("total_admissions", 1)
                                 .over(windowSpec))
                     )

(department_trends.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("endtoenddatasets.silver.department_metrics"))


# COMMAND ----------

