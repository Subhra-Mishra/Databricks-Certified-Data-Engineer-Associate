# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Bronze layer - Declarative Pipelines style reading from Unity Catalog Volume
@dp.table(
    name="bronze.dp_bronze_admissions",
    comment="Raw patient admissions data reading from Unity Catalog Volume",
    table_properties = {"quality": "bronze"}
)
def bronze_admissions():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load("/Volumes/endtoenddatasets/healthcare/raw/admissions/")
            .withColumn("ingestion_time", current_timestamp())
        )
    

# COMMAND ----------

# Silver layer with expectations (data quality)
@dp.table(
    name = "silver.dp_silver_admissions",
    comment = "Cleaned and validated admissions",
    table_properties={"quality":"silver"}
)
@dp.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
@dp.expect_or_drop("valid_dates", "admission_date <= current_date()")
def silver_admissions():
    return (spark.readStream
            .table("bronze.dp_bronze_admissions")
            .dropDuplicates(["admission_id"])
            .withColumn("length_of_stay", datediff(col("discharge_date"), col("admission_date")))
        )




# COMMAND ----------

# Gold layer - aggregated metrics
@dp.table(
    name = "gold.dp_gold_department_metrics",
    comment = "Daily department performance metrics",
    table_properties={"quality":"gold"}
)
def gold_department_metrics():
    return (spark.readStream
            .table("silver.dp_silver_admissions")
            .groupBy("department", to_date("admission_date").alias("metric_date"))
            .agg(
                count("admission_id").alias("total_admissions"),
                sum("total_charges").alias("total_revenue"),
                avg("length_of_stay").alias("avg_length_of_stay")
            )
        )