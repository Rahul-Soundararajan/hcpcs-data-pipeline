🚀 HCPCS Data Engineering Pipeline

A complete end-to-end data engineering project that extracts, processes, validates, and analyzes HCPCS Level II medical codes from the official source.

📌 Project Overview

This project builds a full data pipeline that ingests HCPCS data from:

🔗 https://www.hcpcsdata.com/Codes

It includes:

Web scraping of all 17 HCPCS categories

SCD-Type 2 data warehouse modeling

Python-based ETL pipeline (incremental + historical updates)

Data quality validation checks

Scheduled automation via cron

Email alerting on pipeline/validation failures

Analytical SQL queries for reporting

🧩 Architecture Components
1️⃣ Data Extraction (Web Scraping)

Scrapes all 17 category pages from hcpcsdata.com

Extracts:

group_code

category_name

hcpcs_code

long_description

Stores raw data in CSV format for ingestion

2️⃣ Data Modeling – SCD Type 2

Database schema used:

CREATE TABLE hcpcs_codes (
  id BIGSERIAL PRIMARY KEY,
  group_code CHAR(1) NOT NULL,
  category_name VARCHAR(255) NOT NULL,
  hcpcs_code VARCHAR(10) NOT NULL,
  long_description TEXT NOT NULL,
  effective_date DATE DEFAULT CURRENT_DATE,
  end_date DATE,
  is_current CHAR(1) DEFAULT 'Y'
);


Historical Tracking Logic (SCD-2):

✔ Same code + same description → skip

✔ Same code, new description → expire old, insert new

✔ New code, same description → insert as new

✔ Completely new record → insert

This allows full change history over time.

⚙️ 3️⃣ Orchestration & Monitoring

Linux cron job triggers ETL every 3 minutes

Post-load validation script ensures data consistency

Logs maintained for debugging and audit

Email alerts sent when:

ETL script fails

Validation checks fail

🔍 4️⃣ Data Quality Validation

The pipeline validates:

✔ Active duplicate codes

✔ Missing required fields

✔ Invalid date (effective_date > end_date)

✔ Future-dated records

✔ Incorrect SCD2 versioning

✔ Records without proper end_date closures

Pipeline stops + triggers alert on any failure.

📊 5️⃣ Analytical SQL Outputs

Includes SQL answers for:

Count of codes per group

Top 5 categories with highest codes

Codes whose descriptions changed over time

Codes active in 2022 but expired before 2024
