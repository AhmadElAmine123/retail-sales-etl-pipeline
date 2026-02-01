from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
import sys
import psycopg2

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/scripts')
from metadata_tracker import create_metadata_tables, log_pipeline_run
from db_config import get_db_config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}


def log_pipeline_summary(**context):
    """Log pipeline summary to metadata tables"""
    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    try:
        create_metadata_tables(conn)

        cur.execute("SELECT COUNT(*) FROM fact_sales")
        sales_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM dim_product")
        product_count = cur.fetchone()[0]

        start_time = context['dag_run'].start_date
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60

        log_pipeline_run(
            conn,
            total_rows_processed=sales_count,
            total_rows_loaded=sales_count,
            duration_minutes=duration,
            status='completed',
            notes=f"Products: {product_count:,}"
        )

        print(f"Pipeline completed successfully")
        print(f"  Sales records: {sales_count:,}")
        print(f"  Products: {product_count:,}")
        print(f"  Duration: {duration:.2f} minutes")

    except Exception as e:
        print(f"Warning: Could not log metadata: {e}")
    finally:
        cur.close()
        conn.close()


with DAG(
    'master_pipeline',
    default_args=default_args,
    description='Master pipeline to orchestrate all ETL DAGs',
    schedule_interval='@daily',
    catchup=False,
    tags=['master', 'etl'],
) as dag:

    trigger_dag1 = TriggerDagRunOperator(
        task_id='trigger_fetch_and_ingest',
        trigger_dag_id='dag1_ingest_csv',
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_clean_validate',
        trigger_dag_id='dag2_clean_validate',
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_dag3 = TriggerDagRunOperator(
        task_id='trigger_transform_warehouse',
        trigger_dag_id='dag3_transform_warehouse',
        wait_for_completion=True,
        poke_interval=30,
    )

    log_summary = PythonOperator(
        task_id='log_pipeline_summary',
        python_callable=log_pipeline_summary,
    )

    trigger_dag1 >> trigger_dag2 >> trigger_dag3 >> log_summary
