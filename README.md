# 🚀 Real-Time IoT Motor Monitoring — Azure Data Engineering

A real-time IoT Data Engineering project that simulates motor sensor telemetry and processes it through an Azure Lakehouse architecture using Azure IoT Hub, ADLS Gen2, Databricks Lakeflow Declarative Pipelines, Delta Lake, and Power BI.

The project demonstrates an end-to-end streaming data pipeline from IoT devices to business-ready Gold tables and a live monitoring dashboard.

---

## 🏗️ Architecture

```text
Motor Sensors / Simulator
          │
          ▼
     Azure IoT Hub
          │
          ▼
       ADLS Gen2
       Bronze Layer
          │
          ▼
Databricks Lakeflow
Declarative Pipelines
          │
          ▼
      Silver Layer
   Cleaned & Structured
          │
          ▼
Databricks Lakeflow
Declarative Pipelines
          │
          ▼
       Gold Layer
 ┌───────────────────────┐
 │ motor_current_status  │
 │ motor_metrics         │
 └───────────────────────┘
          │
          ▼
    Databricks SQL
     SQL Warehouse
          │
          ▼
        Power BI
   Motor Monitoring Dashboard
