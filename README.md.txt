# Real-Time Fraud Detection Pipeline

An end-to-end Real-Time Fraud Detection Data Engineering Project built using Apache Kafka, PySpark, MySQL, Databricks, Delta Lake, and Tableau.

---

# Project Overview

This project simulates a real-time banking fraud detection system where transaction data is streamed through Kafka, processed using PySpark, stored in MySQL, transformed in Databricks using Medallion Architecture (Bronze, Silver, Gold layers), and visualized using Tableau dashboards.

The pipeline detects suspicious transactions such as:

* High Amount Transactions
* International Transactions
* Fraudulent Patterns

---

#  Architecture

Kafka Producer → Kafka Topic → PySpark Streaming Consumer → MySQL → Databricks Bronze Layer → Silver Layer → Gold Layer → Tableau Dashboard

---

#  Technologies Used

| Category               | Technology           |
| ---------------------- | -------------------- |
| Programming Language   | Python               |
| Streaming Platform     | Apache Kafka         |
| Stream Processing      | PySpark              |
| Database               | MySQL                |
| Lakehouse              | Delta Lake           |
| Cloud Platform         | Databricks           |
| Workflow Orchestration | Databricks Workflows |
| Data Visualization     | Tableau              |
| Version Control        | Git & GitHub         |

---

#  Project Structure

```bash
fraud_project/

├── kafka/
│   ├── producer/
│   └── consumer/
│
├── databricks/
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   └── gold_layer.py
│
├── tableau/
│   └── fraud_dashboard
│
├── sql/
│   └── fraud_tables.sql
│
├── screenshots/
│
├── requirements.txt
│
└── README.md
```

---

#  Features

* Real-time transaction streaming using Kafka
* Fraud detection using PySpark Structured Streaming
* MySQL integration for fraud alert storage
* Medallion Architecture implementation
* Delta Lake storage in Databricks
* Workflow orchestration using Databricks Workflows
* Interactive Tableau Dashboard
* End-to-end data engineering pipeline

---

# Fraud Detection Rules

The system flags transactions based on:

* Amount greater than 100000
* Transactions outside India
* Suspicious transaction behavior

---

# Tableau Dashboard

The Tableau dashboard provides:

* Total Transactions KPI
* Fraud Cases KPI
* Fraud Amount KPI
* Top Fraud Country
* Fraud Distribution Analysis
* Merchant Fraud Analysis
* Transaction Type Analysis

---

#  Workflow Pipeline

1. Kafka Producer generates transaction data
2. Kafka Topic streams messages
3. PySpark Consumer processes transactions
4. Fraud rules identify suspicious transactions
5. Alerts stored in MySQL
6. Databricks processes Bronze → Silver → Gold layers
7. Tableau visualizes business insights

---

#  Screenshots

Add screenshots here after upload:

* Kafka Producer Running
* PySpark Streaming Output
* Databricks Workflow DAG
* Bronze/Silver/Gold Tables
* Tableau Dashboard

---

#  How to Run

## Clone Repository

```bash
git clone https://github.com/soni-saumya/real-time-fraud-detection-pipeline.git
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Run Kafka Producer

```bash
python producer.py
```

## Run PySpark Consumer

```bash
python fraud_consumer.py
```

---

#  Future Enhancements

* Real-time dashboard integration
* Machine Learning based fraud detection
* Cloud deployment on AWS/Azure
* Kafka Connect integration
* CI/CD pipeline implementation

---

#  Author

Saumya Soni

GitHub:
https://github.com/soni-saumya
