# 🌍 Disaster Analytics Lakehouse & Real-Time Prediction Project

## 🔍 Overview

This project combines **batch analytics** and **real-time stream processing** to monitor, analyze, and predict natural disasters — with a special focus on wildfires. It integrates **historical datasets** with **real-time environmental data** and **satellite imagery**, feeding into a **modern data lakehouse** pipeline for deep analysis and live predictions. Insights are visualized using Superset and Streamlit dashboards.

---

## 🎯 Objectives

### ✅ Batch Processing

- Analyze historical disasters: **earthquakes, volcanoes, wildfires, tsunamis, landslides**.
- Identify patterns by **region**, **cause**, and **impact**.
- Serve optimized, curated data from a **governed Iceberg Lakehouse**.
- Build rich visual dashboards in **Apache Superset**.

### ✅ Real-Time Processing

- Predict **wildfire risks** using real-time environmental feeds (temperature, humidity, wind).
- Detect wildfires from **satellite imagery** using a **CNN model**.
- Stream, transform, and store live data using Kafka → Spark → Cassandra.
- Display real-time predictions and visual metrics in **Streamlit**.

---

## 🏗️ Modern Lakehouse & Streaming Architecture

### 🔹 Data Lakehouse (Batch)

- **MinIO** – Object storage for raw and curated data.
- **Apache Iceberg** – Table format enabling schema evolution and time travel.
- **Project Nessie** – Data versioning with `bronze → silver → gold` branching logic.
- **Dremio** – SQL engine connected to Iceberg tables for fast analytics and BI.
- **Superset** – Front-end dashboards for disaster trend analysis.

### 🔹 Streaming Pipeline (Real-Time)

- **Kafka** – Ingests environmental data from **OpenWeather API**.
- **Spark Streaming** – Processes real-time streams and applies transformations.
- **MongoDB** – Stores raw weather data.
- **Cassandra** – Stores cleaned and structured features for predictions.
- **Streamlit** – Dashboard that shows:
  - Wildfire **risk scores**
  - **Real-time environmental metrics**
  - Fire detection results from **CNN image classifier**

---

## 🛠️ Tools & Technologies

| Category                 | Tools / Libraries                                |
|--------------------------|--------------------------------------------------|
| Language                 | Python                                           |
| Batch Processing         | Apache Spark, MLflow, MinIO, Dremio, Superset   |
| Stream Processing        | Kafka, Spark Streaming, Streamlit               |
| Data Storage             | MongoDB (raw), Cassandra (transformed)          |
| Data Lakehouse Format    | Apache Iceberg                                   |
| Version Control (Data)   | Project Nessie                                   |
| Machine Learning         | Scikit-learn, TensorFlow               |
| API Integration          | OpenWeather API                                 |
| Orchestration            | Apache Airflow                                  |

---

## 🌐 Data Sources

### 📦 Historical (Batch)

- **Kaggle Datasets**:
  - Volcanoes  
  - Earthquakes  
  - Tsunamis  
  - Wildfires  
- **NOAA** (National Oceanic and Atmospheric Administration)
- **EM-DAT** (Emergency Events Database)

### 🔁 Real-Time (Stream)

- **OpenWeather API** – Real-time temperature, wind, and humidity.
- **Satellite Imagery** – Used to train and infer with a **CNN wildfire detection model**.

---

## 📈 Key Outcomes

- ✅ **End-to-end Lakehouse** setup with Dremio + Iceberg + MinIO + Nessie.
- ✅ **Real-time stream** classification and alerting system for wildfire detection.
- ✅ **Interactive Dashboards**: Superset (batch) + Streamlit (real-time).
- ✅ **ML Ops-ready**: Integrated tracking and versioning with MLflow.

---

## 👨‍💻 Author & Contact

**Ali Rahiqi**  
📧 Email: [ali123rahiqi@gmail.com](mailto:ali123rahiqi@gmail.com)  
If you have any questions regarding this project or are interested in contributing, please feel free to reach out via email.  