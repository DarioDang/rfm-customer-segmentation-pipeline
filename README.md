# 📊 RFM Customer Segmentation Pipeline

This project implements a **production-ready customer segmentation pipeline** using RFM (Recency, Frequency, Monetary) analysis, DBT transformations, and ML clustering.  
It simulates a **real-world retail analytics workflow**: ingesting sales data, transforming with DBT, training clustering models, and serving outputs for analytics.

---

## 🚀 Project Overview

Businesses often want to segment customers into meaningful groups (e.g., loyal, at-risk, new customers) for **targeted marketing** and **lifetime value optimization**.  

This pipeline automates that process:
1. **Ingest raw sales data** → from KaggleHub or synthetic generator.  
2. **Transform into RFM features** → using **dbt** (staging → intermediate → marts).  
3. **Train K-Means clustering** → with metrics tracked in **MLflow**.  
4. **Store customer segments** → in Postgres for downstream analytics.  
5. **Simulate production streaming** → by appending monthly unseen data and re-running the pipeline every cycle.  


## 📂 Repository Structure

```
├── 01-ingest/                     # Load sales data into Postgres (from KaggleHub)
│   └── load_from_kagglehub_to_postgres.py
├── 02-EDA/                        # Exploratory data analysis in Jupyter
├── 03-model/                      # Train and log clustering model
│   └── train_cluster_model.py
├── electronic_rfm_dbt/            # DBT project (staging, intermediate, marts, analytics)
├── test/                          # Test suite simulating production streaming
│   ├── test_ingest_synthetic.py
│   ├── customer_segmentation_pipeline_test.py
│   └── test_synthetic.py
├── docker-compose.yaml            # Postgres + pgAdmin setup
└── README.md                      # Project documentation
```

## Dataset 
## 📂 Dataset

The dataset used in this project represents **retail electronic sales transactions** between **September 2023 – September 2024** (sourced from KaggleHub).  
It includes **customer demographics, product details, purchase behavior, and transactional metadata**, which are later transformed into **RFM features**.

| Column              | Type       | Description                                                                 |
|---------------------|------------|-----------------------------------------------------------------------------|
| `customer_id`       | Integer    | Unique identifier for each customer.                                         |
| `age`               | Integer    | Customer age (18–70).                                                        |
| `gender`            | Text       | Gender of the customer (`Male`, `Female`).                                   |
| `loyalty_member`    | Text       | Loyalty program status (`Yes` = enrolled, `No` = not enrolled).              |
| `product_type`      | Text       | Product category (`Smartphone`, `Laptop`, `Tablet`, `Accessories`).          |
| `sku`               | Text       | Unique product code (e.g., `SMA-1234`).                                      |
| `rating`            | Integer    | Customer rating of product (1–5 stars).                                      |
| `order_status`      | Text       | Order outcome (`Completed`, `Cancelled`).                                    |
| `payment_method`    | Text       | Payment channel (`Cash`, `Credit Card`, `Paypal`).                           |
| `total_price`       | Numeric    | Total transaction amount (unit price × quantity + add-ons).                  |
| `unit_price`        | Numeric    | Price per product unit.                                                      |
| `quantity`          | Integer    | Number of units purchased.                                                   |
| `purchase_date`     | Date       | Date of purchase (YYYY-MM-DD).                                               |
| `shipping_type`     | Text       | Shipping method (`Standard`, `Overnight`, `Express`).                        |
| `add_ons_purchased` | Text       | Extra items purchased (`None`, `Accessories`, `Extended Warranty`).          |
| `add_on_total`      | Numeric    | Total value of add-ons purchased.                                            |

---

### 🔑 Why This Dataset Matters
- **Rich Features** → Enables both **transactional analysis** and **customer segmentation**.  
- **Realistic Schema** → Includes demographics, order lifecycle, product-level, and revenue-level features.  
- **Supports RFM Modeling** → Directly feeds into **Recency, Frequency, Monetary** analysis.  
- **Synthetic Data Extension** → Monthly unseen data is **simulated** to test pipeline automation.  

## ⚙️ Setup Instructions

#### 1. Clone and Install

```bash
git clone https://github.com/<your-username>/rfm-customer-segmentation-pipeline.git
cd rfm-customer-segmentation-pipeline
pip install -r requirements.txt
```

#### 2. Start Database
```bash
docker-compose up -d
```

This runs:
   -  Postgres 15 → stores raw, staging, marts, and analytics tables.
   - pgAdmin → UI to explore your database.

#### 3. Ingest Data
```bash
python 01-ingest/load_from_kagglehub_to_postgres.py
```

#### 4. Run DBT Models
```bash
cd electronic_rfm_dbt
dbt run
```

Creates transformations:

- staging → cleaned raw data

- intermediate → aggregated metrics

- marts → RFM features

- analytics → clustered outputs ready for BI Analysis

#### 5. Train Clustering Model
```bash
python 03-model/train_cluster_model.py
```

Scales features

Runs K-Means for multiple k values

Logs silhouette score + parameters into MLflow

#### 6. End - to - End Pipeline (Prefect)
```bash
python customer_segmentation_pipeline.py
```

This will:
- Ingest
- Run dbt models
- Train clustering model
- Update analytics tables

### 🧪 Test Mode (Simulating Production)

- To simulate real-world streaming:

- Every 3 minutes = 1 month

- New synthetic data is appended

- Pipeline is re-run automatically

```bash
python test/test_synthetic.py
```

This validates that the pipeline adapts to incremental unseen data and updates customer clusters continuously.

## 📚 Key Learnings & Skills Applied

| **Category**           | **Skills & Tools**                                                                 |
|-------------------------|------------------------------------------------------------------------------------|
| **Data Engineering**    | - Dockerized Postgres + pgAdmin <br> - Automated data ingestion from KaggleHub <br> - Synthetic data generation to simulate real-world pipelines |
| **Data Transformation** | - DBT: schema-based transformations (staging → marts → analytics) <br> - Incremental models, schema overrides (public_analytics, public_segmentation) |
| **Machine Learning**    | - RFM feature engineering <br> - K-Means clustering <br> - Model evaluation with silhouette score <br> - Model and metric tracking with MLflow |
| **Orchestration**       | - Prefect for pipeline orchestration <br> - Task-based design (ingest, dbt, train, post-analysis) <br> - Scheduled runs simulating production |
| **Testing & Simulation**| - Synthetic data generator <br> - Automated streaming simulation (`test_synthetic.py`) |
| **Visualization & BI**  | - Fact & dimension tables prepared for BI dashboards <br> - Indexed Postgres tables for efficient queries |


## 📈 Why This Project Matters
This pipeline demonstrates end-to-end MLOps and data engineering skills:

- Building data pipelines (ETL → feature store → ML model → analytics).

- Automating workflows to mimic real production environments.

- Using industry-standard tools: DBT, Prefect, MLflow, Docker, Postgres.

- Delivering business value through customer segmentation insights.

## 👤 Author

Dario Dang

Applied Data Scientist | MLOps & Data Engineering Enthusiast