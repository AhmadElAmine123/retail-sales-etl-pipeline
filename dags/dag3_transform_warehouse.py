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


def create_warehouse_tables():
    """Create dimension and fact tables from schema file"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    with open('/opt/airflow/schema/warehouse_schema.sql', 'r') as f:
        cur.execute(f.read())

    conn.commit()
    cur.close()
    conn.close()
    print("Warehouse tables created")


def load_dim_product(**context):
    """Load product dimension from staging"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO dim_product (stock_code, description)
        SELECT StockCode, MAX(Description) as description
        FROM staging_online_retail
        WHERE StockCode IS NOT NULL
        GROUP BY StockCode
        ON CONFLICT (stock_code)
        DO UPDATE SET description = EXCLUDED.description
    """)

    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {inserted} products into dim_product")


def load_dim_customer(**context):
    """Load customer dimension from staging"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO dim_customer (customer_id, country)
        VALUES ('UNKNOWN', 'UNKNOWN')
        ON CONFLICT (customer_id) DO NOTHING
    """)

    cur.execute("""
        INSERT INTO dim_customer (customer_id, country)
        SELECT CustomerID, MAX(Country) as country
        FROM staging_online_retail
        WHERE CustomerID IS NOT NULL
        GROUP BY CustomerID
        ON CONFLICT (customer_id)
        DO UPDATE SET country = EXCLUDED.country
    """)

    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {inserted} customers into dim_customer")


def load_dim_date(**context):
    """Load date dimension from staging"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO dim_date (date, year, month, day, quarter, day_of_week)
        SELECT DISTINCT
            DATE(InvoiceDate) as date,
            EXTRACT(YEAR FROM InvoiceDate) as year,
            EXTRACT(MONTH FROM InvoiceDate) as month,
            EXTRACT(DAY FROM InvoiceDate) as day,
            EXTRACT(QUARTER FROM InvoiceDate) as quarter,
            EXTRACT(DOW FROM InvoiceDate) as day_of_week
        FROM staging_online_retail
        WHERE InvoiceDate IS NOT NULL
        ON CONFLICT (date) DO NOTHING
    """)

    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {inserted} dates into dim_date")


def load_fact_sales(**context):
    """Load sales fact table from staging"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE fact_sales")

    cur.execute("""
        INSERT INTO fact_sales (
            invoice_no, product_key, customer_key, date_key,
            invoice_date, quantity, unit_price, total_amount
        )
        SELECT
            s.InvoiceNo,
            p.product_key,
            c.customer_key,
            d.date_key,
            s.InvoiceDate,
            s.Quantity,
            s.UnitPrice,
            s.Quantity * s.UnitPrice as total_amount
        FROM staging_online_retail s
        INNER JOIN dim_product p ON s.StockCode = p.stock_code
        INNER JOIN dim_customer c ON COALESCE(s.CustomerID, 'UNKNOWN') = c.customer_id
        INNER JOIN dim_date d ON DATE(s.InvoiceDate) = d.date
    """)

    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {inserted} sales records into fact_sales")


def validate_warehouse(**context):
    """Validate warehouse data load"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM dim_product")
    products = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM dim_customer")
    customers = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM dim_date")
    dates = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fact_sales")
    sales = cur.fetchone()[0]

    cur.execute("SELECT SUM(total_amount) FROM fact_sales")
    total_revenue = cur.fetchone()[0] or 0

    cur.close()
    conn.close()

    print(f"Warehouse Load Report:")
    print(f"  Products: {products}")
    print(f"  Customers: {customers}")
    print(f"  Dates: {dates}")
    print(f"  Sales records: {sales}")
    print(f"  Total revenue: ${total_revenue:,.2f}")


with DAG(
    'dag3_transform_warehouse',
    default_args=default_args,
    description='Transform staging to warehouse (dim + fact tables)',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'warehouse'],
) as dag:

    create_tables = PythonOperator(
        task_id='create_warehouse_tables',
        python_callable=create_warehouse_tables,
    )

    load_products = PythonOperator(
        task_id='load_dim_product',
        python_callable=load_dim_product,
    )

    load_customers = PythonOperator(
        task_id='load_dim_customer',
        python_callable=load_dim_customer,
    )

    load_dates = PythonOperator(
        task_id='load_dim_date',
        python_callable=load_dim_date,
    )

    load_facts = PythonOperator(
        task_id='load_fact_sales',
        python_callable=load_fact_sales,
    )

    validate = PythonOperator(
        task_id='validate_warehouse',
        python_callable=validate_warehouse,
    )

    create_tables >> [load_products, load_customers, load_dates]
    [load_products, load_customers, load_dates] >> load_facts >> validate
