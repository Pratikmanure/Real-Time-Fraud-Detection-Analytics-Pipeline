# 🚨 Real-Time Fraud Detection Analytics Pipeline

## 📌 Overview

This project simulates a real-time fraud detection system that processes streaming financial transaction data, detects anomalies using SQL window functions, and visualizes suspicious activity through an interactive dashboard.

The system mimics real-world data engineering and analytics workflows used in fintech and banking systems.

---

## ⚙️ Key Features

* 🔄 **Real-Time Data Simulation**

  * Generates continuous credit card transaction data (amount, location, merchant)
  * Mimics real-world streaming data pipelines

* 🗄️ **Database Architecture**

  * Stores transaction data in a relational SQL database
  * Designed normalized schema for efficient querying

* 🔍 **Fraud Detection (SQL)**

  * Uses advanced SQL window functions to detect anomalies
  * Example:

    * Same card used in multiple locations within short time
    * Unusual transaction frequency

* 📊 **Risk Scoring System**

  * Categorizes transactions into risk levels
  * Calculates fraud probability using Python & SQL

* 📈 **Live Dashboard (Streamlit)**

  * Displays flagged transactions in real-time
  * Visualizes fraud patterns and trends
  * Includes geographical heatmaps for suspicious activity

---

## 🧠 Tech Stack

* **Programming:** Python
* **Database:** SQLite / PostgreSQL
* **Visualization:** Streamlit, Plotly
* **Data Processing:** Pandas, NumPy
* **Concepts:** Data Streaming, SQL Window Functions, Fraud Detection

---

## 🏗️ Project Architecture

```text
Data Generator (Python)
        ↓
Database (SQL)
        ↓
Fraud Detection Queries (SQL + Python)
        ↓
Streamlit Dashboard (Visualization)
```

---

## 📂 Folder Structure

```text
data_generator/      → Generates streaming transaction data  
database/            → Database schema & connection  
analytics/           → Fraud detection logic & risk scoring  
dashboard/           → Streamlit UI  
```

---

## 📊 Example Use Cases

* Detect fraudulent transactions in real-time
* Analyze suspicious activity patterns
* Monitor transaction behavior across locations

---

## 💡 Why This Project Matters

This project demonstrates:

* Real-time data processing
* Advanced SQL (window functions)
* Data pipeline architecture
* End-to-end analytics system

It reflects real-world systems used in financial fraud detection.

---
