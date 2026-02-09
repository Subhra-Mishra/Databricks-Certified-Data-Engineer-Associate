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

![Create Sample Data Files](Databricks-Certified-Data-Engineer-Associate
/End-to-End Pipelines/Healthcare Patient Analytics Pipeline/2-Environment & Data Preparation.py)

### Step 1.3: Setup Storage Location

![Setup Storage Location](Databricks-Certified-Data-Engineer-Associate
/End-to-End Pipelines/Healthcare Patient Analytics Pipeline/1-Healthcare Data Creation.py)


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

![Bronze Tables](Databricks-Certified-Data-Engineer-Associate
/End-to-End Pipelines/Healthcare Patient Analytics Pipeline/3-Auto Loader Implementation.py)


