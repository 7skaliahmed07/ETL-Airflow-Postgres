# Airflow ETL: Sales CSV → Postgres

## Overview

This project demonstrates a **basic batch ETL pipeline** using **Apache Airflow** and **PostgreSQL**:

- **Source**: `data/sample_sales.csv` (10 rows, some null values)  
- **Transform**:  
  - Drop rows with null `Quantity`  
  - Parse `Date` column  
  - Remove duplicates  
  - Add calculated column `Total = Price * Quantity`  
- **Load**: Use `psycopg2 COPY` to load into the `sales_cleaned` table  
- **Schedule**: Daily via Airflow  

---

## Project Structure

etl-airflow-postgres/
│
├── dags/ # Airflow DAG definitions
│ └── etl_dag.py # ETL DAG code
├── data/ # Sample CSV datasets
│ └── sample_sales.csv # Input CSV file
├── scripts/ # Python ETL scripts (optional)
│ └── etl.py # Extract, Transform, Load logic
├── docker-compose.yml # Docker Compose for Airflow + Postgres
├── Dockerfile # Custom Dockerfile for Airflow (if used)
├── requirements.txt # Python dependencies
└── README.md # Project documentation