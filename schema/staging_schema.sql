-- Staging table for raw online retail data
CREATE TABLE IF NOT EXISTS staging_online_retail (
    InvoiceNo VARCHAR(50),
    StockCode VARCHAR(50),
    Description VARCHAR(500),
    Quantity INTEGER,
    InvoiceDate TIMESTAMP,
    UnitPrice DECIMAL(10,2),
    CustomerID VARCHAR(50),
    Country VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);