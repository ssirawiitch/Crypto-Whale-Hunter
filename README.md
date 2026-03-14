# 🐋 Crypto Whale Hunter: Real-time Data Pipeline

A high-performance, real-time data pipeline designed to ingest, process, and analyze "Whale" transactions (large-scale trades) from the cryptocurrency market. This project demonstrates the **ELT (Extract, Load, Transform)** pattern using modern cloud data warehousing practices on Google Cloud Platform.

---

## 🏗️ System Architecture

The pipeline follows a robust 4-stage process:

1.  **Ingestion (Extract):** A Python-based service connects to the **Binance WebSocket API** to stream live trade data for major assets.
2.  **Storage (Load):** Raw data is filtered for high-value transactions and batch-inserted into **Google BigQuery** using the **Batch Load (Job)** API to bypass Sandbox/Free Tier limitations.
3.  **Transformation (Transform):** Analytical logic is handled within the data warehouse using **SQL Views** to calculate market momentum and net flows.
4.  **Visualization:** A live **Looker Studio** dashboard connects to the BigQuery views to provide actionable insights.

---

## 🛠️ Tech Stack

| Component          | Technology                               |
| :----------------- | :--------------------------------------- |
| **Language** | Python 3.11+                             |
| **Data Warehouse** | Google BigQuery                          |
| **Authentication** | Google Service Account (JSON Key)        |
| **Database Ops** | BigQuery Batch Load API                  |
| **Visualization** | Google Looker Studio                     |

---

## 📋 Database Schema & Design

The table is designed for cost-efficiency and performance, utilizing **Partitioning** on the `timestamp` field to reduce query costs and increase scan speeds.

![Database Schema](result/image.png)
*Figure 1: Table schema definition and data types in BigQuery.*

---

## 🚀 Execution & Real-time Tracking

The ingestion engine monitors market events in real-time. When a "Whale" transaction is detected, it is immediately prepared and uploaded to the cloud with automated job status verification.

<!-- ![Terminal Output](result/image-1.png) -->
![Terminal Output](result/image-3.png)
*Figure 2: Real-time terminal output showing detection and successful cloud upload.*

---

## 📊 Final Results in BigQuery

Data is stored in a structured format, enabling deep-dive SQL analysis and seamless integration with Business Intelligence (BI) tools.

```
SELECT * FROM `crypto_ds.raw_whale_trades` ORDER BY timestamp DESC LIMIT 10
```

<!-- ![BigQuery Results](result/image-2.png) -->
![BigQuery Result](result/image-4.png)
*Figure 3: Verified whale trade records successfully stored in BigQuery.*

---

## 🔄 Orchestration & Analytics Layer (New!)

To transform raw data into actionable market insights, an **Orchestration Layer** powered by **Apache Airflow** has been integrated. This addition shifts the project from a simple ingestion script to a production-grade data pipeline.

### 🧩 Key Enhancements
* **Workflow Automation:** Managed by Apache Airflow 3.x running in **Docker**, utilizing the **LocalExecutor** for optimal resource management on local environments.
* **Daily Market Summary:** A specialized DAG (`daily_crypto_whale_summary`) is scheduled to trigger every day at **01:00 UTC**. It automatically aggregates and analyzes whale activities from the previous 24 hours.
* **Market Momentum Logic:** Implemented automated SQL-based analysis to calculate the "Net Flow" and determine if **Bulls (Buyers)** or **Bears (Sellers)** dominated the whale-tier transactions.
* **Error Handling & Monitoring:** Built-in retry mechanisms and task state monitoring ensure the pipeline's reliability against API timeouts or cloud connectivity issues.

### ⚙️ Orchestration Tech Stack

| Component          | Technology                               |
| :----------------- | :--------------------------------------- |
| **Orchestrator** | Apache Airflow 3.x (Dockerized)          |
| **Executor** | LocalExecutor                            |
| **GCP Integration**| Airflow `BigQueryHook` & `GCP Connection` |
| **Scheduling** | Cron-based (`0 1 * * *`)                 |

### 📊 Downstream Workflow (DAG)

The Airflow DAG follows a structured sequence to process data stored in BigQuery:

1.  **Extract & Aggregate:** Executes a parameterized SQL query to fetch whale trades specifically from `CURRENT_DATE() - 1`.
2.  **Market Sentiment Calculation:** Processes the results to compare total volume and transaction counts between `BUY` and `SELL` sides.
3.  **Insight Logging:** Outputs a structured summary of market momentum, providing a clear "Daily Winner" (Bulls vs. Bears).
4.  **Extensibility:** The architecture is designed to easily integrate future tasks like **LINE/Slack Notifications** or triggering **ML Prediction Models**.

---