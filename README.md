# Retail Sales ETL Pipeline

A production-grade ETL pipeline for processing online retail transaction data, built with Apache Airflow and PostgreSQL. This project demonstrates end-to-end data engineering practices including data ingestion, cleaning, validation, transformation, and warehousing with a star schema design.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              MASTER PIPELINE                                 │
│                         (Orchestrates all DAGs)                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          ▼                           ▼                           ▼
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│   DAG 1:        │         │   DAG 2:        │         │   DAG 3:        │
│   INGESTION     │────────▶│   CLEANING &    │────────▶│   TRANSFORM     │
│                 │         │   VALIDATION    │         │   TO WAREHOUSE  │
└─────────────────┘         └─────────────────┘         └─────────────────┘
        │                           │                           │
        ▼                           ▼                           ▼
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│ • Fetch CSV     │         │ • Remove nulls  │         │ • dim_product   │
│ • Create staging│         │ • Deduplicate   │         │ • dim_customer  │
│ • Load 541K     │         │ • Validate qty  │         │ • dim_date      │
│   records       │         │ • Validate price│         │ • fact_sales    │
│                 │         │ • Quality logs  │         │   (partitioned) │
└─────────────────┘         └─────────────────┘         └─────────────────┘
        │                           │                           │
        └───────────────────────────┴───────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │         POSTGRESQL            │
                    │    ┌─────────────────────┐    │
                    │    │   STAGING LAYER     │    │
                    │    │ staging_online_     │    │
                    │    │ retail              │    │
                    │    └─────────────────────┘    │
                    │              │                │
                    │              ▼                │
                    │    ┌─────────────────────┐    │
                    │    │   WAREHOUSE LAYER   │    │
                    │    │  ┌───────────────┐  │    │
                    │    │  │  DIMENSIONS   │  │    │
                    │    │  │ ┌───────────┐ │  │    │
                    │    │  │ │dim_product│ │  │    │
                    │    │  │ │dim_customer│ │ │    │
                    │    │  │ │dim_date   │ │  │    │
                    │    │  │ └───────────┘ │  │    │
                    │    │  └───────────────┘  │    │
                    │    │         │           │    │
                    │    │         ▼           │    │
                    │    │  ┌───────────────┐  │    │
                    │    │  │     FACT      │  │    │
                    │    │  │  fact_sales   │  │    │
                    │    │  │ (partitioned) │  │    │
                    │    │  └───────────────┘  │    │
                    │    └─────────────────────┘    │
                    └───────────────────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow 2.8.1 |
| Database | PostgreSQL 15 |
| Data Processing | Python, Pandas |
| Containerization | Docker, Docker Compose |
| Data Modeling | Star Schema (Kimball) |

## Features

- **Multi-stage ETL Pipeline**: Ingestion → Cleaning → Transformation
- **Star Schema Data Warehouse**: 3 dimensions + 1 fact table
- **Data Quality Framework**: Null checks, deduplication, validation thresholds
- **Table Partitioning**: Monthly partitions on fact table for query performance
- **Strategic Indexing**: Optimized for common query patterns
- **Metadata Tracking**: Pipeline execution logs and data quality metrics
- **Environment-based Configuration**: Secure credential management via environment variables

## Quick Start

### Prerequisites

- Docker and Docker Compose
- 4GB+ available RAM

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/retail-sales-etl.git
   cd retail-sales-etl
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your preferred credentials
   ```

3. **Download the dataset**

   Download the [Online Retail Dataset](https://www.kaggle.com/datasets/vijayuv/onlineretail) from Kaggle and place it in the `dags/` directory as `online_retail.csv`.

4. **Start the services**
   ```bash
   docker compose up -d
   ```

5. **Access Airflow UI**

   Open http://localhost:8080 (default credentials: `airflow`/`airflow`)

6. **Run the pipeline**

   Trigger the `master_pipeline` DAG from the Airflow UI.

## Project Structure

```
├── dags/                           # Airflow DAG definitions
│   ├── dag1_ingest_csv.py         # Data ingestion from CSV
│   ├── dag2_clean_validate.py     # Data cleaning & validation
│   ├── dag3_transform_warehouse.py # Star schema transformation
│   └── dag_master_pipeline.py     # Master orchestrator
├── schema/                         # SQL schema definitions
│   ├── staging_schema.sql         # Raw data staging table
│   ├── warehouse_schema.sql       # Star schema + partitions + indexes
│   └── metadata_schema.sql        # Pipeline tracking tables
├── scripts/                        # Python utility modules
│   ├── db_config.py               # Database configuration
│   ├── fetch_data.py              # Data download utilities
│   ├── metadata_tracker.py        # Pipeline logging
│   └── verify_ingestion.py        # Verification utilities
├── config/                         # Configuration files
│   └── data_source.json           # Data source metadata
├── docker-compose.yml             # Container orchestration
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment variables template
└── README.md
```

## Data Warehouse Schema

### Star Schema Design

**Dimensions:**
- `dim_product` - Product catalog (stock_code, description)
- `dim_customer` - Customer directory (customer_id, country)
- `dim_date` - Date attributes (year, month, day, quarter, day_of_week)

**Fact Table:**
- `fact_sales` - Transaction records with foreign keys to dimensions
  - Partitioned by month (2010-12 to 2011-12)
  - Indexed on all foreign keys and date fields

### Data Volume

| Table | Records |
|-------|---------|
| Raw Input | 541,909 |
| After Cleaning | 534,123 |
| dim_product | ~3,937 |
| dim_customer | ~4,372 |
| dim_date | ~305 |
| fact_sales | ~534,123 |

## Pipeline Stages

### Stage 1: Ingestion
- Loads CSV data into staging table
- Handles data type conversions
- Bulk insert with batch processing (1,000 records/batch)

### Stage 2: Cleaning & Validation
- Removes records with null critical values
- Deduplicates using window functions
- Removes invalid quantities (zero) and prices (<=0)
- Logs quality metrics to `data_quality_log`
- Fails pipeline if records < 400,000

### Stage 3: Transformation
- Creates/updates dimension tables with UPSERT pattern
- Loads fact table with calculated `total_amount`
- Parallel dimension loading, sequential fact loading
- Validates final warehouse state

## Verification

Connect to the warehouse and run verification queries:

```bash
docker exec -it warehouse psql -U retail_user -d retail_db
```

```sql
-- Check record counts
SELECT 'dim_product' as table_name, COUNT(*) as records FROM dim_product
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales;

-- Check total revenue
SELECT SUM(total_amount) as total_revenue FROM fact_sales;

-- View recent pipeline runs
SELECT * FROM recent_pipeline_runs;

-- View data quality logs
SELECT * FROM data_quality_log ORDER BY check_timestamp DESC LIMIT 5;
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_DB` | Warehouse database name | `retail_db` |
| `POSTGRES_USER` | Warehouse database user | `retail_user` |
| `POSTGRES_PASSWORD` | Warehouse database password | `retail_password` |
| `AIRFLOW_USERNAME` | Airflow admin username | `airflow` |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow admin password | `airflow` |

## Performance Optimizations

1. **Partitioning**: Monthly range partitions on `fact_sales` enable partition pruning
2. **Indexing**: Strategic indexes on foreign keys and date fields
3. **Bulk Operations**: Uses `execute_values()` with batch processing
4. **UPSERT Pattern**: `ON CONFLICT` clauses for idempotent dimension loads

## Future Enhancements

- Incremental loading instead of full refresh
- CeleryExecutor for horizontal scaling
- Slowly Changing Dimensions (SCD Type 2)
- Email/Slack notifications for failures
- Advanced data quality checks (anomaly detection)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

Ahmad - Data Engineer

---

*Built as a portfolio project demonstrating ETL pipeline design, data warehousing, and workflow orchestration.*
