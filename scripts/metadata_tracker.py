"""
Simple ETL Run Tracker (L1/L2 Level)

Basic helper functions to track pipeline runs and stage metrics.

Usage:
    from metadata_tracker import log_pipeline_run, log_stage_metric

    run_id = log_pipeline_run(conn, rows_processed=540000, rows_loaded=520000, duration_minutes=2.5)
    log_stage_metric(conn, run_id, 'remove_nulls', rows_before=540000, rows_after=535000)
"""

import psycopg2
from datetime import datetime
from typing import Optional


def create_metadata_tables(conn):
    """
    Create metadata tables if they don't exist.
    Call this once at the start of the pipeline.
    """
    cur = conn.cursor()
    try:
        with open('/opt/airflow/schema/metadata_schema.sql', 'r') as f:
            cur.execute(f.read())
        conn.commit()
        print("Metadata tables ready")
    except Exception as e:
        conn.rollback()
        print(f"Note: Metadata tables may already exist ({e})")
    finally:
        cur.close()


def log_pipeline_run(
    conn,
    total_rows_processed: int,
    total_rows_loaded: int,
    duration_minutes: float,
    status: str = 'completed',
    notes: Optional[str] = None
) -> int:
    """
    Log a complete pipeline run.

    Args:
        conn: Database connection
        total_rows_processed: Total rows processed in pipeline
        total_rows_loaded: Total rows loaded to warehouse
        duration_minutes: Pipeline duration in minutes
        status: 'completed' or 'failed'
        notes: Optional notes

    Returns:
        run_id: The ID of this pipeline run
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO pipeline_runs (
                total_rows_processed,
                total_rows_loaded,
                duration_minutes,
                status,
                notes
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING run_id
        """, (total_rows_processed, total_rows_loaded, duration_minutes, status, notes))

        run_id = cur.fetchone()[0]
        conn.commit()

        print(f"\n=== Pipeline Run Logged ===")
        print(f"Run ID: {run_id}")
        print(f"Rows Processed: {total_rows_processed:,}")
        print(f"Rows Loaded: {total_rows_loaded:,}")
        print(f"Duration: {duration_minutes:.2f} minutes")
        print(f"Status: {status}")

        return run_id

    except Exception as e:
        conn.rollback()
        print(f"Warning: Could not log pipeline run: {e}")
        return -1
    finally:
        cur.close()


def log_stage_metric(
    conn,
    run_id: int,
    stage_name: str,
    rows_before: int,
    rows_after: int,
    rows_removed: int = 0
):
    """
    Log metrics for a specific ETL stage.

    Args:
        conn: Database connection
        run_id: ID from log_pipeline_run()
        stage_name: Name of the stage (e.g., 'remove_nulls', 'load_facts')
        rows_before: Row count before this stage
        rows_after: Row count after this stage
        rows_removed: Rows removed in this stage
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO stage_metrics (
                run_id,
                stage_name,
                rows_before,
                rows_after,
                rows_removed
            )
            VALUES (%s, %s, %s, %s, %s)
        """, (run_id, stage_name, rows_before, rows_after, rows_removed))

        conn.commit()
        print(f"  {stage_name}: {rows_before:,} -> {rows_after:,} rows", end="")
        if rows_removed > 0:
            print(f" ({rows_removed:,} removed)")
        else:
            print()

    except Exception as e:
        conn.rollback()
        print(f"Warning: Could not log stage metric: {e}")
    finally:
        cur.close()
