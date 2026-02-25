# Modern Lakehouse Engineering Project — NYC TLC Yellow Taxi

## Overview

This project implements a production-grade Lakehouse architecture using:

- PySpark
- Delta Lake
- Medallion Architecture (Bronze / Silver / Gold)
- Data Quality Gate
- BI-ready Star Schema Marts

Dataset: **NYC TLC Yellow Taxi (January 2024)**  
Source: Official NYC Taxi & Limousine Commission public dataset.

---

# Architecture

Bronze → Silver → Gold → Data Quality → BI Marts

## Bronze
- Raw ingestion from official TLC parquet files
- Stored in Delta format
- Append-friendly
- No business filtering applied

## Silver
- Curated, validated dataset
- Enforced temporal window (January 2024)
- Removed invalid trips:
  - Null pickup/dropoff timestamps
  - Negative fare
  - Zero/negative duration
- Derived column: `pickup_date`
- Partitioned by `pickup_date`

## Gold
- Business-level daily KPIs
- Deterministic aggregations
- Null-safe revenue normalization
- Daily grain
- Validated against Silver row counts

## Data Quality Gate
- Reconciliation: Bronze → Silver → Gold
- Completeness (null rate per column)
- Validity rules (duration, fare, distance, passenger count)
- Schema drift detection
- Deterministic monthly snapshot stored in Delta

## BI-Ready Marts
### fact_trips
- Grain: 1 trip
- Partitioned by pickup_date
- Derived duration metrics
- Date surrogate key

### mart_daily_revenue
- Grain: 1 day
- Business KPIs ready for dashboard consumption

### dim_date
- Calendar dimension
- Surrogate date_key (yyyyMMdd)
- Weekend flag
- Day/month names

---

# Key Engineering Principles Demonstrated

- Deterministic pipelines
- Idempotent notebook execution
- No implicit state dependency
- Physical cleanup of Delta paths
- Partition-aware modeling
- Data validation before serving
- Separation of concerns between layers
- BI-friendly star schema modeling

---

# Row-Level Reconciliation

All downstream layers are validated:

- Silver row count matches fact_trips
- Gold KPI sum(trips) matches Silver
- dim_date covers full monthly window
- No orphan keys in star schema joins

---

# Why This Project Matters

This repository demonstrates:

- Modern Data Engineering best practices
- Lakehouse architecture design
- Data quality-first mindset
- Production-ready modeling decisions
- Analytics Engineering concepts without dbt dependency

---

# Next Improvements (Planned)

- Schema contract enforcement
- Automated DQ thresholds
- Incremental processing strategy
- CI validation simulation
- Performance optimization (clustering strategy)

---

# How To Run Locally

```bash
./.venv/bin/python orchestration/local/run_pipeline.py run-all \
  --input-parquet data/raw/yellow_tripdata_2024-01.parquet \
  --year 2024 --month 1 --strict-quality
```

Useful shortcuts:

- `make lint`
- `make contracts`
- `make smoke`
- `make run-all INPUT_PARQUET=data/raw/yellow_tripdata_2024-01.parquet YEAR=2024 MONTH=1 STRICT_QUALITY=1`

---

# Author

Data Engineering Portfolio Project  
Built for professional-level demonstration.
