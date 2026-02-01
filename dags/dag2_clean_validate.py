from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import sys

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/scripts')
from db_config import get_db_config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10),
}


def remove_nulls(**context):
    """Remove records with critical null values"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM staging_online_retail
        WHERE InvoiceNo IS NULL
           OR StockCode IS NULL
           OR Quantity IS NULL
           OR InvoiceDate IS NULL
           OR UnitPrice IS NULL
    """)

    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Removed {deleted} records with null critical values")


def remove_duplicates(**context):
    """Remove duplicate records"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM staging_online_retail
        WHERE ctid IN (
            SELECT ctid FROM (
                SELECT ctid,
                       ROW_NUMBER() OVER (
                           PARTITION BY InvoiceNo, StockCode, Quantity,
                                       InvoiceDate, UnitPrice,
                                       COALESCE(CustomerID, '')
                           ORDER BY ctid
                       ) as rn
                FROM staging_online_retail
            ) sub
            WHERE rn > 1
        )
    """)

    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Removed {deleted} duplicate records")


def remove_invalid_quantities(**context):
    """Remove records with zero quantities (keep negatives for returns)"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM staging_online_retail
        WHERE Quantity = 0
    """)

    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Removed {deleted} records with zero quantities")


def remove_invalid_prices(**context):
    """Remove records with zero or negative prices"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM staging_online_retail
        WHERE UnitPrice <= 0
    """)

    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Removed {deleted} records with invalid prices")


def validate_data_quality(**context):
    """Check data quality and fail if quality is too poor"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM staging_online_retail")
    total_records = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT InvoiceNo) FROM staging_online_retail")
    unique_invoices = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT StockCode) FROM staging_online_retail")
    unique_products = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT CustomerID) FROM staging_online_retail WHERE CustomerID IS NOT NULL")
    unique_customers = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"Data Quality Report:")
    print(f"  Total records: {total_records}")
    print(f"  Unique invoices: {unique_invoices}")
    print(f"  Unique products: {unique_products}")
    print(f"  Unique customers: {unique_customers}")

    MIN_RECORDS = 400000
    if total_records < MIN_RECORDS:
        raise ValueError(
            f"Data quality check failed: {total_records} records (expected >{MIN_RECORDS})"
        )


def log_data_quality():
    """Log data quality metrics for tracking over time"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM staging_online_retail")
    total_rows = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM staging_online_retail
        WHERE InvoiceNo IS NULL
           OR StockCode IS NULL
           OR Quantity IS NULL
           OR UnitPrice IS NULL
    """)
    rows_with_nulls = cur.fetchone()[0]

    status = 'PASS' if rows_with_nulls == 0 else 'WARNING'

    cur.execute("""
        INSERT INTO data_quality_log
        (dag_run_id, table_name, total_rows, rows_with_nulls, status)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        datetime.now().strftime('%Y%m%d_%H%M'),
        'staging_online_retail',
        total_rows,
        rows_with_nulls,
        status
    ))

    conn.commit()
    print(f"Data quality logged: {total_rows} rows, {rows_with_nulls} with nulls, Status: {status}")

    cur.close()
    conn.close()


with DAG(
    'dag2_clean_validate',
    default_args=default_args,
    description='Clean and validate staging data',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'cleaning'],
) as dag:

    clean_nulls = PythonOperator(
        task_id='remove_nulls',
        python_callable=remove_nulls,
    )

    clean_duplicates = PythonOperator(
        task_id='remove_duplicates',
        python_callable=remove_duplicates,
    )

    clean_quantities = PythonOperator(
        task_id='remove_zero_quantities',
        python_callable=remove_invalid_quantities,
    )

    clean_prices = PythonOperator(
        task_id='remove_invalid_prices',
        python_callable=remove_invalid_prices,
    )

    log_quality = PythonOperator(
        task_id='log_data_quality',
        python_callable=log_data_quality,
    )

    validate = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
    )

    clean_nulls >> clean_duplicates >> clean_quantities >> clean_prices >> log_quality >> validate
