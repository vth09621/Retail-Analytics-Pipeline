<div align="center">

# 🛒 Retail Analytics Pipeline

### End-to-End Retail Data Engineering Platform

A production-style data pipeline that transforms raw retail data into
clean, analytics-ready datasets using **AWS S3, PySpark, Apache Airflow, DuckDB, and dbt**.

<p>
  <img src="https://img.shields.io/badge/AWS-S3-FF9900?logo=amazonaws&logoColor=white" alt="AWS S3"/>
  <img src="https://img.shields.io/badge/PySpark-4.2.0-E25A1C?logo=apachespark&logoColor=white" alt="PySpark"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-3.3.2-017CEE?logo=apacheairflow&logoColor=white" alt="Airflow"/>
  <img src="https://img.shields.io/badge/DuckDB-Analytics-FFF000?logo=duckdb&logoColor=black" alt="DuckDB"/>
  <img src="https://img.shields.io/badge/dbt-1.12.5-FF694B?logo=dbt&logoColor=white" alt="dbt"/>
  <img src="https://img.shields.io/badge/Python-3.14-3776AB?logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Git-GitHub-181717?logo=github&logoColor=white" alt="GitHub"/>
</p>

</div>

---

## 📌 Project Overview

This project implements a complete **retail data engineering pipeline** starting
from raw CSV files and ending with analytics-ready data models.

The pipeline follows a **Medallion Architecture**:

```text
                    ┌─────────────────────┐
                    │   12 Raw CSV Files  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      AWS S3 Raw     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       PySpark       │
                    │      Ingestion      │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    🥉 Bronze Layer  │
                    │      S3 / Parquet   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    🥈 Silver Layer  │
                    │ Cleaning & Transform │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     🥇 Gold Layer   │
                    │ Business Analytics  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       DuckDB        │
                    │ Analytical Warehouse│
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │         dbt         │
                    │ Models + Data Tests │
                    └─────────────────────┘

             Apache Airflow orchestrates the workflow

