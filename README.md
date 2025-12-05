# Airflow ETL: Sales CSV → Postgres


![image_alt](https://github.com/7skaliahmed07/ETL-Airflow-Postgres/blob/dd2e8c9e4ab0d17f15a41f8b344e689a76306ea4/ETL_pipeline.webp)




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

```
etl-airflow-postgres/
│
├── dags/ # Airflow DAG definitions
│ └── etl_dag.py
│
├── data/ # Sample CSV datasets
│ └── sample_sales.csv 
│
├── scripts/ 
│ └── etl.py 
│
├── docker-compose.yml 
├── Dockerfile 
├── requirements.txt 
└── README.md 
```


## **✅ Summary – Flow in Real Time**

1. **CSV arrives** (manual or automated)

2. **Airflow Scheduler triggers DAG** at a set time

3. **Python ETL function runs**: extract → clean → load

4. **Data is inserted into PostgreSQL**

5. **Airflow UI logs status** → optional, but useful for monitoring

6. **Next day**, DAG runs again with new CSV

   * **Option 1**: Drop table → keep only today’s data
   * **Option 2**: Append → keep historical data
