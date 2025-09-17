# Test Suite for Customer Segmentation Pipeline

This folder contains **testing utilities** to simulate a production-like workflow for the `customer_segmentation_pipeline`.  
Instead of pulling the dataset from KaggleHub (production mode), these scripts generate **synthetic unseen data monthly** and automatically re-run the pipeline every cycle.

---

## 📂 Files Overview

### 1. `test_ingest_synthetic.py`
- Generates **synthetic sales data** for a given year and month.
- Inserts the data into **Postgres** (`raw.sales_2023_2024`) without overwriting existing rows.
- Ensures **50% old customers** (from existing IDs) and **50% new customers** (new IDs).

### 2. `customer_segmentation_pipeline_test.py`

 - A test variant of the main pipeline.
 - Skips the KaggleHub ingest step and instead works directly on synthetic data.
Runs:
 - DBT models (stg → int → mart → modelling_features → BI Tables)
 - Training & clustering (train_cluster_model.py)
 - DBT post-analysis models (customer_clustered_information, dim_customer_clustered, etc.)


### 3. `test_synthetic.py`

Orchestrates the end-to-end simulation:
Calls test_ingest_synthetic.py → generates new unseen data for the next month.
Runs customer_segmentation_pipeline_test.py on the updated dataset.
Waits 3 minutes before repeating (to simulate a monthly refresh).

Usage:
```bash
python test_synthetic.py
```

## 🔄 Workflow Simulation

Real world: new customer sales are ingested monthly → pipeline re-runs automatically.
Test mode: this is accelerated — every 3 minutes = 1 month.
This setup allows you to:
 - Validate that the pipeline handles incremental unseen data.

 - Ensure customer clustering updates dynamically with new sales.

## ⚠️ Notes

These scripts do not touch your production ingest (01-ingest/ingest-data.py).

They only append to the raw.sales_2023_2024 table.


