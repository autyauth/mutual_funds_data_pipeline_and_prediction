# 🌀 mutual funds Data Pipeline with Airflow (MAFIA PROJECT)

This project is an Airflow-based pipeline designed to automate the daily data collection, prediction, and transmission of mutual fund data.

---
## 📁 Project Structure

The directory structure is organized into 6 main parts:

```text
.
├── config/                 # Airflow configuration files
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── requirements.txt    # Python packages with versions
├── dags/                   # Airflow DAGs and backend-style logic
│   ├── service/            # Business logic (e.g., tax calculation, recommendations)
│   ├── adapter/            # Connectors to external APIs and services
│   ├── repository/         # Database queries and interfaces
│   ├── model/              # ORM models mapped to MySQL tables
│   ├── database/           # Engine and session configuration (MySQL)
│   ├── dto/                # Data Transfer Objects (for HTTP/JSON communication)
│   └── utils/              # Reusable utility functions
├── data/                   # Initial or required static data for DAGs
├── plugins/                # Custom Airflow plugins (if any)

```
📁 config/backend_api.json  
Open this file and update the following field:  
- `"password"`: Replace with your **admin password**
## 🔧 API Configuration (config.json)

The `config.json` file defines the API endpoints used to communicate with the MAFIA Backend system.

It includes:
- Login credentials
- Destination URLs for sending NAV and prediction data
- Fund project mappings for each API

> 🛠️ **If you want to change where the data is sent (e.g., to a different server or environment), simply update the URL fields in this file.**
---

📁 data/data_invalid_dates.csv  
Open this file and update the following content inside the `json_config` column:

- `"Ocp-Apim-Subscription-Key"`: Replace with your **API subscription key**

- `"YTM name"`: This refers to **bond yield** — please insert your own **Bond Yield API endpoint**.  
  *(Sorry, I don't provide mine 😄)*

---
---

## 🧰 Installation & Usage Guide

Follow these steps to deploy and run the MAFIA Data Pipeline using Docker Compose.

### 📦 1) Clone & Setup Source Code on Server

Place the source code inside your server under a directory named `mutual_funds_data_pipeline_and_prediction`, as shown below:

```
/your/server/path/
└── mutual_funds_data_pipeline_and_prediction/
```

---

### 🐳 2) Install & Launch with Docker Compose

Navigate to the project directory and build the containers:

```bash
cd mutual_funds_data_pipeline_and_prediction

docker-compose up -d --build
```
if no network before 


```bash
docker network create shared_network 
```
---

### ✅ 3) Verify Running Containers

Check that all 6 containers are running:

```bash
docker ps
```

Expected services include:
- airflow-webserver
- airflow-scheduler
- airflow-worker
- airflow-triggerer
- airflow-init
- postgres / redis / etc

---

### 🗃 4) Create Initial Tables for Data Fetching

Access the Airflow webserver container:

```bash
docker exec -it <airflow-webserver-container-name> bash
```

Inside the container, run:

```bash
airflow dags unpause create_table_dag
airflow dags trigger create_table_dag
```

Verify the run:

```bash
airflow dags list-runs -d create_table_dag
```

- If `state = success`, table creation was successful.
- If `state = failed`, check Docker container logs and fix any issues, then try again.

---

### 📥 5) Start Daily Data Fetching DAG

Unpause the main DAG to start pulling daily mutual fund data:

```bash
airflow dags unpause fund_daily_send_data_dag
```

This will:
- Fetch data from 2025-01-01 to the current date
- Continue fetching daily moving forward

To stop the process:

```bash
airflow dags pause fund_daily_send_data_dag
```

---

### 📊 6) Check DAG Run Status

Monitor execution results:

```bash
airflow dags list-runs -d fund_daily_send_data_dag
```

This shows:
- Run status (success/failure)
- Execution dates
- Queued dates for the next fetch

---

## ⚙️ System Process Flow

### 1. **Data Fetching**
- Query `invalid_date` entries from the `data_invalid_date` table.
- Pull daily NAV data and market data (SET, Bond, YTM) from third-party APIs.
- Store fetched data in the Airflow database.
- Register the next date to fetch data.

### 2. **Marking Prediction Schedule**
- Create entries in `send_predict_invalid_date` to identify which dates require NAV prediction.

### 3. **NAV Data Transmission**
- Send NAV data (`is_data_send = False`) to MAFIA Backend via API.
- On success, update `is_data_send = True`.

### 4. **Prediction Process**
- Select all unsent prediction entries (`is_predict = False`).
- Run ML model to generate trend forecasts.
- Store results in the `prediction_ml` table.
- Update `is_predict = True` after prediction.

### 5. **Prediction Result Transmission**
- Send unsent prediction data (`is_predict_send = False`) to the MAFIA Backend.
- On success, update `is_predict_send = True`.

---

## 🛠 Tech Stack

- Apache Airflow
- Python 3.x
- MySQL
- Docker & Docker Compose
- RESTful API
- Machine Learning Models

---

