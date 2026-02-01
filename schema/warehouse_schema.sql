-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    stock_code VARCHAR(50) UNIQUE NOT NULL,
    description VARCHAR(500)
);

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    country VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_country ON dim_customer(country);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    day_of_week INTEGER
);

CREATE INDEX IF NOT EXISTS idx_dim_date_year ON dim_date(year);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month);
CREATE INDEX IF NOT EXISTS idx_dim_date_quarter ON dim_date(quarter);

-- Fact Table with Monthly Partitioning
DROP TABLE IF EXISTS fact_sales CASCADE;

CREATE TABLE fact_sales (
    sale_key BIGSERIAL,
    invoice_no VARCHAR(50),
    product_key INTEGER NOT NULL,
    customer_key INTEGER,
    date_key INTEGER NOT NULL,
    invoice_date TIMESTAMP NOT NULL,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),

    CONSTRAINT fk_sales_product FOREIGN KEY (product_key)
        REFERENCES dim_product(product_key),
    CONSTRAINT fk_sales_customer FOREIGN KEY (customer_key)
        REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_sales_date FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key)
) PARTITION BY RANGE (invoice_date);

-- 2010 Partitions
CREATE TABLE fact_sales_2010_12 PARTITION OF fact_sales
    FOR VALUES FROM ('2010-12-01') TO ('2011-01-01');

-- 2011 Partitions
CREATE TABLE fact_sales_2011_01 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-01-01') TO ('2011-02-01');

CREATE TABLE fact_sales_2011_02 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-02-01') TO ('2011-03-01');

CREATE TABLE fact_sales_2011_03 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-03-01') TO ('2011-04-01');

CREATE TABLE fact_sales_2011_04 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-04-01') TO ('2011-05-01');

CREATE TABLE fact_sales_2011_05 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-05-01') TO ('2011-06-01');

CREATE TABLE fact_sales_2011_06 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-06-01') TO ('2011-07-01');

CREATE TABLE fact_sales_2011_07 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-07-01') TO ('2011-08-01');

CREATE TABLE fact_sales_2011_08 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-08-01') TO ('2011-09-01');

CREATE TABLE fact_sales_2011_09 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-09-01') TO ('2011-10-01');

CREATE TABLE fact_sales_2011_10 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-10-01') TO ('2011-11-01');

CREATE TABLE fact_sales_2011_11 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-11-01') TO ('2011-12-01');

CREATE TABLE fact_sales_2011_12 PARTITION OF fact_sales
    FOR VALUES FROM ('2011-12-01') TO ('2012-01-01');

CREATE TABLE fact_sales_default PARTITION OF fact_sales DEFAULT;

-- Indexes
CREATE INDEX idx_fact_sales_product_key ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer_key ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_date_key ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_invoice_no ON fact_sales(invoice_no);
CREATE INDEX idx_fact_sales_invoice_date ON fact_sales(invoice_date);
CREATE INDEX idx_fact_sales_date_product ON fact_sales(date_key, product_key);

-- Data Quality Monitoring

CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dag_run_id VARCHAR(100),
    table_name VARCHAR(100),
    total_rows INTEGER,
    rows_with_nulls INTEGER,
    rows_removed INTEGER,
    status VARCHAR(20),
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_dq_log_timestamp ON data_quality_log(check_timestamp);