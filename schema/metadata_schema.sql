CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL DEFAULT CURRENT_DATE,
    run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    pipeline_name VARCHAR(100) NOT NULL DEFAULT 'retail_etl_pipeline',
    status VARCHAR(20) NOT NULL DEFAULT 'completed',
    total_rows_processed INTEGER,
    total_rows_loaded INTEGER,
    duration_minutes DECIMAL(8,2),
    notes TEXT
);

CREATE TABLE IF NOT EXISTS stage_metrics (
    metric_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES pipeline_runs(run_id),
    stage_name VARCHAR(100) NOT NULL,
    rows_before INTEGER,
    rows_after INTEGER,
    rows_removed INTEGER DEFAULT 0,
    execution_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW recent_pipeline_runs AS
SELECT
    run_id,
    run_date,
    pipeline_name,
    status,
    total_rows_processed,
    total_rows_loaded,
    duration_minutes
FROM pipeline_runs
ORDER BY run_date DESC, run_timestamp DESC
LIMIT 10;
