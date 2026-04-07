# Privia Health Data Engineering Pipeline

[![Interactive Data Story](https://img.shields.io/badge/View-Interactive_Data_Story-00D4B4?style=for-the-badge)](https://tolani007.github.io/Data-Engineering-vault/privia-health-challenge/)

## 🚀 Overview

This repository demonstrates an enterprise-grade data engineering solution for processing and transforming messy, wide-format medical records into a queryable, scalable Delta Lake architecture. Built using **PySpark and Databricks**, this project showcases my ability to untangle complex real-world data constraints, enforce data quality, and optimize for downstream operational analytics.

**👉 [Experience the Interactive Data Story here](https://tolani007.github.io/Data-Engineering-vault/privia-health-challenge/)**

---

## 🛠 Business Problem

Healthcare datasets often arrive in non-standardized, heavily denormalized formats. In this scenario, I ingested a raw Excel deliverable containing patient demographics and longitudinally locked risk scores (Q1 & Q2). 

The challenges were:
- Critical metadata (provider group, date) was embedded within the filename rather than the rows.
- Temporal data (Quarters) were spread across columns (wide format) making it impossible to perform automated trend analysis.
- Nulls, non-standardized flags (0/1 for binary Sex), and unstructured text required programmatic normalization.

---

## 🏗 Technical Architecture & Implementation

I developed a robust ETL pipeline consisting of two core stages and a comprehensive unit-testing suite.

### 1. Ingestion & Demographic Standardization (`01_demographics_etl.py`)
- Used `pandas` and `regex` extractors to parse operational metadata from the physical filename.
- Standardized text formats (middle initials) and mapped categorical variables gracefully preventing brittle pipeline failures on missing data.
- Loaded the clean data frame cleanly into a Delta table on Databricks.

### 2. Temporal Unpivoting & Risk Calculation (`02_risk_quarters_etl.py`)
- Sliced the wide format dataset and utilized `pandas.melt()` to unpivot the temporal quarters into a scalable, long-format truth.
- Joined the isolated risk and attribute halves back together. 
- Isolated the signal: mathematically filtered for patients whose risk scores *deteriorated* from Q1 to Q2, providing immediate actionable clinical intelligence.

### 3. Test-Driven Data Engineering (`03_tests.py`)
- Validated all core logic locally using `pytest` against mocked DataFrames to ensure absolute confidence before cluster execution.
- Covered filename parsing, unpivots, sex transformations, and risk isolations without needing a heavy Spark cluster for fast CI/CD feedback loops.

---

## 🔮 Future Architecture State

To scale this MVP to a fully autonomous enterprise pipeline, I would integrate:
1. **Event-Driven Execution:** Utilizing Databricks Auto Loader or Workflows to trigger the pipeline the exact second a new data payload arrives.
2. **Idempotent Upserts:** Moving from `overwrite` to `MERGE INTO`, ensuring that re-processed files update records but never duplicate clinical data.
3. **Strict Schema Validation:** Adding Delta Live Tables (`CONSTRAINT`) expectations to trap malformed records before they contaminate downstream BI.
4. **Comprehensive Auditing:** A centralized metadata table tracking every ingested file, row counts, and exceptions for 100% operational transparency.

---

## 💻 Tech Stack
- **Compute & Architecture:** Databricks (Unity Catalog, Delta Lake) 
- **Languages:** Python, PySpark 3.5, SQL
- **Libraries:** pandas 2.0 (melting/unpivoting), pytest (TDD/validation), openpyxl

---

## 📬 Let's Connect
I am actively seeking roles where I can architect robust data pipelines and drive operational efficiency. 
Feel free to reach out to discuss data engineering, analytics, and infrastructure!
- [LinkedIn](https://www.linkedin.com/in/tolani-akinola)
- [Twitter / X](https://x.com/tolaniakinola_)
- [GitHub](https://github.com/tolani007)
