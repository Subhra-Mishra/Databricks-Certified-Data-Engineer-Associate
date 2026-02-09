# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG endtoenddatasets;
# MAGIC USE SCHEMA healthcare;

# COMMAND ----------

# DBTITLE 1,Cell 2
import builtins
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from faker import Faker
import random
from datetime import timedelta

fake = Faker()
random.seed(42)


# COMMAND ----------

NUM_PATIENTS = 100
NUM_ADMISSIONS = 300
NUM_BILLINGS = 500
NUM_PROCEDURES = 400

departments = ["Cardiology", "Neurology", "Orthopedics", "Pediatrics", "Oncology", "Emergency"]
conditions = ["Diabetes", "Hypertension", "Asthma", "Cancer", "Heart Disease"]
procedures = ["X-Ray", "MRI", "CT Scan", "Blood Test", "Surgery", "Physical Therapy"]


# COMMAND ----------

def write_single_file(df, path, filename, filetype="csv", header=True):
    tmp_path = path + "_tmp"

    if filetype == "csv":
        df.coalesce(1).write.mode("overwrite").option("header", header).csv(tmp_path)
    else:
        df.coalesce(1).write.mode("overwrite").json(tmp_path)

    files = dbutils.fs.ls(tmp_path)
    part_file = [f.path for f in files if f.name.startswith("part-")][0]

    final_path = path + filename

    dbutils.fs.mv(part_file, final_path)
    dbutils.fs.rm(tmp_path, recurse=True)


# COMMAND ----------

vitals_data = []

for i in range(1, NUM_PATIENTS + 1):
    vitals_data.append((
        i,
        fake.name(),
        random.randint(1, 90),
        random.sample(conditions, random.randint(0, 3)),
        f"{random.randint(100,140)}/{random.randint(60,90)}",
        random.randint(60, 120)
    ))

vitals_schema = StructType([
    StructField("patient_id", IntegerType(), False),
    StructField("patient_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("chronic_conditions", ArrayType(StringType()), True),
    StructField("blood_pressure", StringType(), True),
    StructField("heart_rate", IntegerType(), True)
])

df_vitals = spark.createDataFrame(vitals_data, vitals_schema)

write_single_file(
    df_vitals,
    "/Volumes/endtoenddatasets/healthcare/raw/vitals/",
    "patient_vitals.json",
    filetype="json"
)


# COMMAND ----------

# DBTITLE 1,Cell 7
admissions_data = []

for i in range(1, NUM_ADMISSIONS + 1):
    pid = random.randint(1, NUM_PATIENTS)
    admit_date = fake.date_between(start_date="-1y", end_date="today")
    discharge_date = admit_date + timedelta(days=random.randint(1, 14))

    admissions_data.append((
        i,
        pid,
        random.choice(departments),
        admit_date,
        discharge_date,
        builtins.round(random.uniform(500, 20000), 2)
    ))

admissions_schema = StructType([
    StructField("admission_id", IntegerType(), False),
    StructField("patient_id", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("admission_date", DateType(), True),
    StructField("discharge_date", DateType(), True),
    StructField("total_charges", DoubleType(), True)
])

df_admissions = spark.createDataFrame(admissions_data, admissions_schema)

write_single_file(
    df_admissions,
    "/Volumes/endtoenddatasets/healthcare/raw/admissions/",
    "patient_admissions.csv",
    filetype="csv"
)


# COMMAND ----------

billing_data = []

for i in range(1, NUM_BILLINGS + 1):
    billing_data.append((
        i,
        random.randint(1, NUM_PATIENTS),
        random.choice(departments),
        fake.date_between(start_date="-1y", end_date="today"),
        builtins.round(random.uniform(50, 5000), 2),
        random.randint(1, 10)
    ))

billing_schema = StructType([
    StructField("billing_id", IntegerType(), False),
    StructField("patient_id", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("billing_date", DateType(), True),
    StructField("amount_billed", DoubleType(), True),
    StructField("quantity", IntegerType(), True)
])

df_billing = spark.createDataFrame(billing_data, billing_schema)

write_single_file(
    df_billing,
    "/Volumes/endtoenddatasets/healthcare/raw/billing/",
    "patient_billing.json",
    filetype="json"
)


# COMMAND ----------

procedure_data = []

for i in range(1, NUM_PROCEDURES + 1):
    procedure_data.append((
        i,
        random.randint(1, NUM_PATIENTS),
        random.choice(procedures),
        random.choice(departments),
        fake.date_between(start_date="-1y", end_date="today"),
        builtins.round(random.uniform(200, 15000), 2)
    ))

procedure_schema = StructType([
    StructField("procedure_id", IntegerType(), False),
    StructField("patient_id", IntegerType(), True),
    StructField("procedure_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("procedure_date", DateType(), True),
    StructField("cost", DoubleType(), True)
])

df_procedures = spark.createDataFrame(procedure_data, procedure_schema)

write_single_file(
    df_procedures,
    "/Volumes/endtoenddatasets/healthcare/raw/procedures/",
    "medical_procedures.csv",
    filetype="csv"
)


# COMMAND ----------

display(df_vitals)
display(df_admissions)
display(df_billing)
display(df_procedures)

# COMMAND ----------

