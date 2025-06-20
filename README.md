# 🚕 AWS Serverless Data Lake Jumpstart – NYC Taxi Data

This project was built as part of the **AWS Workshop Studio**: *Serverless Data Lake Jumpstart*. It showcases how to build a fully serverless, end-to-end data pipeline on AWS using real-world NYC Taxi trip data.

The pipeline enables data ingestion, transformation, querying, and visualization — all without provisioning servers.

---

## 🛠️ Tools & Services Used

- **Amazon S3** – Central data lake storage
- **AWS Glue** – ETL transformation and Data Catalog
- **Amazon Athena** – Serverless SQL querying over S3
- **Amazon QuickSight** – Data visualization and dashboarding
- **AWS CloudFormation** – Infrastructure provisioning

---

## ✅ What I Built

### 1. Data Ingestion
Raw NYC Taxi trip data was ingested and stored in Amazon S3 in both CSV and optimized Parquet formats.

### 2. Data Transformation
Used AWS Glue ETL jobs to:
- Convert raw CSV data into partitioned Parquet
- Store transformed data in S3 under curated folders
- Automatically register tables in the AWS Glue Data Catalog

### 3. Querying in Athena
- Connected Athena to the Glue Catalog
- Wrote SQL queries to analyze revenue trends, top zones, payment types, etc.
- Compared query performance between CSV and Parquet formats

### 4. Visualization in QuickSight
- Connected QuickSight to Athena datasets
- Created interactive dashboards showing:
  - Revenue by borough
  - Top pickup zones by fare

---
## 📈 Outcome

This project helped me:

- Understand the end-to-end lifecycle of data in a cloud-native architecture  
- Gain hands-on experience with serverless ETL, SQL-on-S3, and cloud BI tools  
- Build a reusable pipeline for structured, scalable analytics on real-world data  

---

## 📎 Credits

This project is based on the **AWS Serverless Data Lake Jumpstart** workshop hosted via AWS Workshop Studio

---







