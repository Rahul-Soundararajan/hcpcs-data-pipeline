🚀 HCPCS Data Engineering Pipeline
A complete end-to-end data engineering pipeline that extracts, processes, validates, and analyzes HCPCS Level II medical codes from the official website.

📌 Project Overview
This project builds a full data workflow that ingests HCPCS codes from:
🔗 https://www.hcpcsdata.com/Codes

It covers:
✔ Web scraping 17 HCPCS categories
✔ SCD-Type 2 historical data modeling
✔ Python ETL to MySQL
✔ Automated data validation
✔ Scheduling + email alerts
✔ Analytical SQL for insights
The pipeline demonstrates real-world skills in orchestration, data modeling, monitoring, and analytics.

🧩 Architecture Components
1️. Data Extraction (Web Scraping)

   Scrapes all 17 HCPCS categories
      Extracts:
       group_code
       category_name
       hcpcs_code
       long_description
   
  --Saves raw/extracted data as CSV for downstream ETL
  --Error-handling added for network and parsing failures

2️. Data Modeling — SCD Type-2

    Create Database HCPCS;

    Use HCPCS;

    CREATE TABLE hcpcs_codes (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      group_code CHAR(1) NOT NULL,
      category_name VARCHAR(255) NOT NULL,
      hcpcs_code VARCHAR(10) NOT NULL,
      long_description TEXT NOT NULL,
      effective_date DATE DEFAULT (CURRENT_DATE),
      end_date DATE,
      Is_current CHAR(1) NOT NULL
    );
	
  SCD-Type 2 handling includes:
    ✔ If code + description match → skip
    ✔ If same code, new description → close old record, insert new version
    ✔ If same description, different code → insert new
    ✔ If completely new → insert new
    ✔ Ensures accurate history tracking for each HCPCS code
	
3️. Orchestration & Monitoring

  --Crontab used for pipeline scheduling
	
  --HCPCS_ETL_SCD2.py → Loads data into MySQL
        Master runner shell script automates:
            ETL execution
            Validation
            Failure detection
            Email notifications
					
  Alerting:
                Email is triggered if:
                ❌ ETL fails
                ❌ Data validation fails
				
  --HCPCS_Post_Validation.py → Runs Data Quality Validation checks
	
  ✔ Duplicate active records
  ✔ Missing mandatory fields
  ✔ Incorrect effective/end-date relationships
  ✔ Future-dated effective dates
  ✔ SCD2 versioning issues
  ✔ Orphan codes/descriptions
  ✔ Historical inconsistencies
		
  --Centralized log folder created for ETL + validation logs
	  
  4️ Data Quality Validation
    Post-load validation queries check for:
    ✔ Duplicate active records
    ✔ Missing mandatory fields
    ✔ Incorrect effective/end-date relationships
    ✔ Future-dated effective dates
    ✔ SCD2 versioning issues
    ✔ Orphan codes/descriptions
    ✔ Historical inconsistencies
    If any check fails, the pipeline sends an automated alert email.

 5️. Analysis (SQL Queries Included)
    📊 Count of codes per group
    🏆 Top 5 categories by number of codes
    🔄 Codes that changed descriptions over time
    ⏳ Codes active in 2022 but expired before 2024
