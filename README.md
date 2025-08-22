# 🚀 Dockerized Stock Market Data Pipeline (Airflow + PostgreSQL)

This project is a **data pipeline** that automatically fetches stock market data from the **Alpha Vantage API**, 
parses it and stores it inside a **PostgreSQL database**.  
The pipeline is orchestrated using **Apache Airflow** and runs entirely inside Docker containers.

---

## ✨ What this project does
- Fetches stock data (like AAPL, MSFT, GOOGL) from Alpha Vantage
- Cleans and parses the JSON response
- Stores the data in PostgreSQL with UPSERT logic (no duplicates)
- Runs on a schedule (hourly/daily) using Airflow DAGs
- Handles errors gracefully (API limits, invalid symbols, network errors)
- Uses Docker Compose for **one command setup**

---

## 🗂️ Project Structure
```
.
├─ dags/
│  └─ stock_pipeline_dag.py     # Airflow DAG (workflow definition)
├  └─ fetch_and_upsert.py       # Python script to fetch & insert data 
├─ sql/
│  └─ init.sql                  # Creates stock_prices table
├─ docker-compose.yml           # Docker Compose setup
├─ .env.example                 # Example config (copy to .env)
└─ README.md                    # Documentation
```

---

## ⚙️ Requirements
- Docker Desktop / Docker Engine with Docker Compose
- Alpha Vantage API key (free): https://www.alphavantage.co/support/#api-key

---

## 🚀 How to Run

### 1. Clone this repo
```bash
git clone https://github.com/Yuvarajprince/Dockerized-Data-Pipeline-with-Airflow/
cd Dockerized Stock Market Data Pipeline
```

### 2. Configure environment variables
```bash
cp .env.example .env
```
Edit `.env` and set:
- `ALPHAVANTAGE_API_KEY=your_api_key_here`
- Other values if needed (Postgres user, password, stock symbols)

### 3. Start everything with Docker Compose
```bash
docker compose up -d airflow-init
docker compose up -d
```

### 4. Access the services
- **Airflow UI:** http://localhost:8080 (use username/password from `.env`)
- **Adminer UI:** http://localhost:8081 (DB client for PostgreSQL)

### 5. Enable and run the pipeline
1. In Airflow UI → turn ON `stock_pipeline_dag`
2. Trigger it manually (▶️ button) or wait for the schedule
3. Data will appear in the `stock_prices` table

---

## 🗄️ Check the Data in DB
In Adminer (or psql), run:
```sql
SELECT * FROM stock_prices ORDER BY ts DESC LIMIT 10;
```

Example output:
| symbol | ts                  | open   | high   | low    | close  | volume |
|--------|---------------------|--------|--------|--------|--------|--------|
| AAPL   | 2025-08-22 10:00:00 | 225.50 | 228.30 | 224.90 | 227.80 | 120300 |
| MSFT   | 2025-08-22 10:00:00 | 340.00 | 345.20 | 339.10 | 344.70 | 98000  |

---

## 🛡️ Error Handling
- Network / API limit issues → retried automatically
- Wrong symbol or function → skipped without breaking pipeline
- Bad datapoints → ignored, but rest is inserted

---

## 🏗️ Scalability
- Add more symbols in `.env` → tasks created automatically
- Airflow can scale from a single machine to a distributed cluster
- UPSERT keeps data clean even with retries

---

## 📸 For Assignment Submission
Please include screenshots of:
1. Airflow DAG with green (success) tasks
2. Adminer showing rows in `stock_prices`

---

## 🙌 Credits
Developed for an internship joining assignment given by **8byte**.
