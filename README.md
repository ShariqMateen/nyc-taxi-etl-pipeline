# NYC Taxi ETL Pipeline

## 📌 Overview  
A complete end‑to‑end ETL pipeline for NYC Yellow Taxi data (Jan–Jun 2023).  
This project extracts raw Parquet files, transforms them into clean and analytics‑ready datasets, loads them into PostgreSQL, and includes a Jupyter Notebook for exploratory data analysis (EDA).  
It also supports workflow orchestration using Apache Airflow.

---

## 🚀 Features  
- Automated extraction of 6 months of raw taxi data  
- File validation, size checks & logging  
- Data cleaning, validation & feature engineering  
- Conversion from Parquet → CSV  
- Loading into PostgreSQL (raw + clean tables)  
- EDA notebook with visual insights  
- Optional Airflow scheduling with Docker  
- Clean, modular folder structure  

---

## 📁 Project Structure  
```
NYC_TAXI_ETL/
│
├── airflow_project/              # Airflow DAG & Docker setup  
├── data/                         # Raw, clean & CSV datasets  
├── NYC-Database-creation/        # SQL scripts for tables  
├── scripts/                      # ETL scripts + log files  
├── Visualization_jupyterNB/      # Jupyter Notebook EDA  
└── requirement.txt               # All dependencies  
```

---
## 🚀 Pipeline Overview

### **1. Extract**
- Downloads 6 monthly Parquet files from TLC open cloud.
- Saves them into `data/data_raw/`.
- Logs:
  - File size
  - Number of records
  - Download success/failure

### **2. Transform**
- Cleans & validates raw data:
  - Converts timestamps and numeric types
  - Fixes `store_and_fwd_flag` (Y/N → boolean)
  - Removes invalid trips, negative values, impossible durations
  - Validates payment types
  - Computes derived features:
    - trip_duration_minutes  
    - average_speed_mph  
    - pickup date/hour/weekday
- Saves output to:
  - `data/data_clean/` (Parquet)
  - `data/data_csv/` (CSV)
- Logs cleaned row counts, CSV conversion time, size differences.

### **3. Load**
- Loads both **raw** and **cleaned** datasets into PostgreSQL tables:
  - `trips_raw`
  - `trips_clean`
- Uses high-performance `COPY` operation.
- Automatically creates missing columns.
- Logs insertion counts & errors.

---

## 📊 Jupyter Notebook Analysis
The notebook (`Analysis.ipynb`) performs:
- Trip volume by hour, weekday, and month  
- Distribution of:
  - Trip distance  
  - Payment types  
  - Passenger count  
  - Duration  
- Correlation heatmaps  
- Revenue-based insights  
- Peak taxi demand detection  
- Speed & congestion analysis  

This helps understand demand patterns, revenue behavior, and city mobility trends.


---

## ⚙️ How to Run ETL Locally  

### 1️⃣ Install Dependencies  
```
pip install -r requirement.txt
```

### 2️⃣ Run Extraction  
```
python scripts/extract.py
```

### 3️⃣ Run Transformation  
```
python scripts/Transform.py
```

### 4️⃣ Run Load (Update DB credentials first)  
To run this pipeline on your machine, update database credentials in:
```
Load.py
```
```
return psycopg2.connect(
    dbname="YOUR_DATABASE_NAME",
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    host="YOUR_HOST",
    port="YOUR_PORT"
)

```
and then run using
```

python scripts/Load.py
```

---

## 🌀 Running with Apache Airflow (Optional)

### Step 1 — Install Docker Desktop  

### Step 2 — Open terminal inside `airflow_project/`  

### Step 3 — Start Airflow  
```
docker-compose up -d
```

### Step 4 — Access Airflow UI  
URL:  
```
http://localhost:8080
```  
Default Login:  
- **Username:** airflow  
- **Password:** airflow  

### Step 5 — Trigger DAG  
Run DAG named:  
```
etl_pipeline
```

---

## 📊 EDA Insights  
The notebook `Visualization_jupyterNB/Analysis.ipynb` includes:  
- Hourly trip trends  
- Weekday travel patterns  
- Average speed analysis  
- Revenue & fare breakdown  
- Passenger count distribution  
- Duration & distance insights  

---

## 🗄️ Database Setup (PostgreSQL)

### Create Tables  
Use SQL file:  
```
NYC-Database-creation/NYC-Data-table-queries.sql
```

### Configure Connection  
Update credentials inside:  
```
scripts/Load.py
```

---


## 👨‍💻 Author  
**Shariq**  
Data Engineer | ETL Pipelines | Workflow Automation  
