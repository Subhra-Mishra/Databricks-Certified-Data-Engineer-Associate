# Healthcare Patient Analytics Pipeline - Certification Practice Guide
## Project Overview

This project provides a hands-on experience in building an end-to-end data pipeline for healthcare patient analytics using Databricks. The pipeline ingests patient data, processes it, and generates insights to help healthcare providers make better decisions.

Build an end-to-end data engineering pipeline for hospital patient analytics covering all Databricks Certified Data Engineer Associate exam topics.


## Project Objectives

The project objectives are as follows:

Build an end-to-end data pipeline for healthcare patient analytics using Databricks.

Ingest patient data from a CSV file.

Process the data using PySpark.

Generate insights using machine learning models.

Deploy the pipeline using Databricks. 




## Prerequisites Setup

### 1. Databricks Workspace Configuration

Community Edition (free)

Access to Unity Catalog (available in all workspaces)

Serverless compute enabled (check workspace settings)

DBFS FileStore access (available in all workspaces)


#### Your Current Setup:

 Catalog: endtoenddatasets

 Schemas: bronze, silver, gold, healthcare, default, information_schema

 Volume: healthcare.raw (for source data files)

 Tables will be created in: bronze, silver, gold schemas


### 2. Sample Data Sources

Create sample healthcare datasets (CSV/JSON files):

patient_admissions.csv

patient_billing.json

medical_procedures.csv

patient_vitals.json



## Phase 1: Environment & Data Preparation (Section 1 & 2)

### Step 1.1: Enable Unity Catalog

### Step 1.2: Create Sample Data Files

[Create Sample Data Files](https://github.com/Subhra-Mishra/Databricks-Certified-Data-Engineer-Associate/blob/main/End-to-End%20Pipelines/Healthcare%20Patient%20Analytics%20Pipeline/2-Environment%20%26%20Data%20Preparation.py)

### Step 1.3: Setup Storage Location

[Setup Storage Location](1-Healthcare Data Creation.py)


#### Alternative: Upload via UI

- Navigate to EndtoEndDatasets->healthcare->raw

- Click Upload to this Volume

- Upload CSV/JSON files to UC(Upload files to a Volume in Unity Catalog)

- Browse or Drag and drop files to upload


## Phase 2: Auto Loader Implementation (Section 2)

### Step 2.1: Create Notebook - Bronze Layer Ingestion

Notebook Name: 3-Auto Loader Implementation

### Step 2.2: Auto Loader for JSON Data

### Step 2.3: Verify Bronze Tables

[Bronze Tables](3-Auto Loader Implementation.py)

## Phase 3: Silver Layer Transformations (Section 3)

### Step 3.1: Create Notebook - Silver Layer Processing

Notebook Name: 4-Silver Layer Transformations

### Step 3.2: Create User-Defined Functions (UDF)

### Step 3.3: Complex Aggregations with PySpark

## Phase 4: Gold Layer - Analytics Ready (Section 3)

### Step 4.1: Create Notebook - Gold Layer Aggregations

Notebook Name: 5-Gold Layer - Analytics Ready

### Step 4.2: Create Patient Summary Fact Table

## Phase 5: Lakeflow Spark Declarative Pipelines (Section 3)

### Step 5.1: Create Delta Live Tables Pipeline

Notebook Name: 6-Lakeflow Spark Declarative Pipelines

## Step 5.2: Create DLT Pipeline in UI

- Navigate to **Jobs & Pipelines**

- Click **ETL Pipeline** in create new drop down

- Configure:

    - Pipeline name: Healthcare_Patient_Analytics_DLT

    - Next step for your pipeline: Add existing assets

    - Add existing assets: Select 6-Lakeflow Spark Declarative Pipelines

    - Target: endtoenddatasets (catalog) + schema (default so that we can access other schemas in the code e.g., bronze, silver, gold)

    - Storage location: Leave blank (Unity Catalog managed)

    - Compute mode: Serverless (recommended) or Fixed

Click **Run Pipeline**

Note: Unity Catalog automatically manages table storage locations

