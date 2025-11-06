from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from io import StringIO

def df_to_csv_buffer(df):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep=',', na_rep='', date_format='%Y-%m-%d')
    buffer.seek(0)
    return buffer

def etl():
    # Load & clean
    df = pd.read_csv('/opt/airflow/data/sample_sales.csv')
    df = df.dropna(subset=['Quantity'])
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.drop_duplicates()
    df['Quantity'] = df['Quantity'].astype(int)
    df['Total'] = df['Price'] * df['Quantity']

    buffer = df_to_csv_buffer(df)

    # Connect
    conn = psycopg2.connect(
        host='postgres', database='airflow', user='airflow', password='airflow', port=5432
    )
    cur = conn.cursor()

    # Drop & create
    cur.execute("DROP TABLE IF EXISTS sales_cleaned")
    cur.execute("""
    CREATE TABLE sales_cleaned (
        Date DATE,
        Product TEXT,
        Quantity INTEGER,
        Price NUMERIC,
        Total NUMERIC
    )
    """)

    # Load
    cur.copy_expert("COPY sales_cleaned FROM STDIN WITH (FORMAT CSV, NULL '', DELIMITER ',')", buffer)
    conn.commit()

    cur.close()
    conn.close()

    print(f"Loaded {len(df)} rows into sales_cleaned")

with DAG('etl_batch_sales', start_date=datetime(2025, 11, 1), schedule='@daily', catchup=False) as dag:
    PythonOperator(task_id='etl_task', python_callable=etl)
