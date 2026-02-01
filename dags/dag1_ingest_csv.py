from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import sys
import os

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/scripts')
from fetch_data import fetch_and_update_data
from db_config import get_db_config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15),
}


def fetch_latest_data(**context):
    """Download latest data from Kaggle. Falls back to existing CSV if download fails."""
    print("Starting data fetch...")

    # Check for existing CSV file
    csv_path = '/opt/airflow/dags/online_retail.csv'

    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        print(f"Using existing CSV file ({os.path.getsize(csv_path):,} bytes)")
    else:
        raise FileNotFoundError(
            "No CSV file found. Please download the Online Retail dataset from Kaggle "
            "and place it in the dags/ directory as 'online_retail.csv'"
        )


def create_staging_table(**context):
    """Create staging table from schema file"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    with open('/opt/airflow/schema/staging_schema.sql', 'r') as f:
        cur.execute(f.read())

    conn.commit()
    cur.close()
    conn.close()
    print("Staging table created")


def truncate_staging_table(**context):
    """Clear staging table before fresh load"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE staging_online_retail")
    conn.commit()
    cur.close()
    conn.close()
    print("Staging table truncated")


def load_csv_to_staging(**context):
    """Load CSV data into staging table"""
    csv_path = '/opt/airflow/dags/online_retail.csv'
    df = pd.read_csv(csv_path)

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
    df['CustomerID'] = df['CustomerID'].astype(str).replace('nan', None)

    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    records = []
    for _, row in df.iterrows():
        records.append((
            row['InvoiceNo'], row['StockCode'], row['Description'],
            row['Quantity'], row['InvoiceDate'], row['UnitPrice'],
            row['CustomerID'], row['Country']
        ))

    execute_values(
        cur,
        """
        INSERT INTO staging_online_retail
        (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
        VALUES %s
        """,
        records,
        page_size=1000
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(records)} rows into staging table")


with DAG(
    'dag1_ingest_csv',
    default_args=default_args,
    description='Fetch latest data and ingest CSV into staging table',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'ingestion'],
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_latest_data',
        python_callable=fetch_latest_data,
    )

    create_table = PythonOperator(
        task_id='create_staging_table',
        python_callable=create_staging_table,
    )

    truncate_table = PythonOperator(
        task_id='truncate_staging_table',
        python_callable=truncate_staging_table,
    )

    load_data = PythonOperator(
        task_id='load_csv_to_staging',
        python_callable=load_csv_to_staging,
    )

    fetch_data >> create_table >> truncate_table >> load_data
