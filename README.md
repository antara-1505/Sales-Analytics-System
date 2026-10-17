# 📊 Sales Analytics System (AWS Glue • Athena • QuickSight)

## 📌 Project Overview

This project implements an **end-to-end cloud-based ETL and analytics pipeline** for large-scale sales transaction data using **AWS services**. Raw CSV sales data is ingested from Amazon S3, transformed and validated using AWS Glue (PySpark), queried through Amazon Athena, and visualized using Amazon QuickSight dashboards to deliver actionable business insights.

The pipeline focuses on **data quality, scalability, and analytics readiness**, converting raw transactional data into a clean, trusted fact table suitable for BI and reporting.

## [video demonstration ](https://youtu.be/qfkWtwcRjI0?si=dJVohddN3ZhphWyd)
---

## 🏗️ Architecture Overview

```
Amazon S3 (Raw Sales Data)
        ↓
AWS Glue ETL (PySpark Transformations)
        ↓
Amazon S3 (Processed Parquet Data)
        ↓
AWS Glue Data Catalog
        ↓
Amazon Athena
        ↓
Amazon QuickSight Dashboard
```

---

## 🔧 Tech Stack

* **Storage:** Amazon S3
* **ETL & Processing:** AWS Glue (PySpark, DynamicFrames)
* **Metadata Management:** AWS Glue Data Catalog
* **Query Engine:** Amazon Athena (SQL)
* **Visualization:** Amazon QuickSight (SPICE)
* **Language:** Python (PySpark)

---

## 📥 Data Ingestion

* Input format: CSV
* Raw data size: ~934,000 rows
* Source: Sales transaction data (orders, products, pricing, timestamps, addresses)

---

## 🔄 Data Transformation Logic

Key transformations implemented in AWS Glue:

* Removed blank rows, repeated headers, and invalid transactions
* Handled empty strings vs null values safely
* Casted numeric fields (`Quantity Ordered`, `Price Each`)
* Parsed timestamps from order dates
* Derived analytical features:

  * Total sales per transaction
  * Order month and hour
  * City, state, and zip code from address
* Validated row counts and schema before loading

After cleaning and validation, **186,305 high-quality sales records** were retained for analytics.

---

## 📦 Data Output

* Output format: **Parquet**
* Optimized for Athena and QuickSight
* Stored in Amazon S3 (processed layer)
* Registered in AWS Glue Data Catalog as a fact table

---

## 🔍 Analytics with Amazon Athena

Example analytical query:

```sql
SELECT city, SUM(sales) AS total_sales
FROM sales_fact
GROUP BY city
ORDER BY total_sales DESC;
```

Athena enables fast, serverless SQL analytics directly on Parquet data stored in S3.

---

## 📈 Dashboarding with Amazon QuickSight

An interactive BI dashboard was created featuring:

* KPI tiles: Total Sales, Total Orders, Average Order Value
* Monthly sales trend analysis
* Top products by revenue
* Sales distribution by city and state
* Hourly sales patterns
* Interactive filters for time, product, and location

Data is imported into **SPICE** for high performance and low latency.

---

## 🎯 Key Outcomes & Learnings

* Built a **production-grade ETL pipeline** using AWS Glue
* Enforced strong data quality and validation checks
* Converted raw data into analytics-ready datasets
* Delivered business insights using cloud-native BI tools
* Gained hands-on experience with scalable AWS analytics architecture

---

## 🚀 Future Enhancements

* Implement star schema (fact and dimension tables)
* Add incremental loading using Glue bookmarks
* Partition data by year and month for cost optimization
* Automate orchestration using AWS Step Functions or Airflow
* Extend pipeline for near real-time ingestion

---

## 👩‍💻 Author

**Antara Shaw**
Data Analytics & Cloud Enthusiast
B.Tech in Biotechnology | Aspiring Data Engineer / Analyst

---

⭐ This project demonstrates practical experience in building scalable, cloud-based analytics systems suitable for enterprise use cases.

