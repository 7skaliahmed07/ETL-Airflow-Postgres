# etl.py
import pandas as pd
import psycopg2
from io import StringIO

def df_to_csv_buffer(df):
    """
    Convert DataFrame to an in-memory CSV buffer suitable for psycopg2 COPY
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep=',', na_rep='', date_format='%Y-%m-%d')
    buffer.seek(0)
    return buffer

def etl():
    """
    Extract -> Transform -> Load pipeline:
    - Reads sales CSV
    - Cleans data (drop null Quantity, parse Date, remove duplicates)
    - Adds Total column
    - Loads data into PostgreSQL using psycopg2 COPY
    """
    # Extract & Transform
    df = pd.read_csv('/opt/airflow/data/sample_sales.csv')
    df = df.dropna(subset=['Quantity'])
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.drop_duplicates()
    df['Quantity'] = df['Quantity'].astype(int)
    df['Total'] = df['Price'] * df['Quantity']

    # Prepare CSV buffer
    buffer = df_to_csv_buffer(df)

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow',
        port=5432
    )
    cur = conn.cursor()

    # Drop existing table and create new
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

    # Load data into PostgreSQL using COPY
    cur.copy_expert(
        "COPY sales_cleaned FROM STDIN WITH (FORMAT CSV, NULL '', DELIMITER ',')",
        buffer
    )
    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(df)} rows into sales_cleaned")

# For testing standalone
if __name__ == "__main__":
    etl()
