#!/usr/bin/env python3
"""
Script to verify that data was correctly ingested into the database.
Checks staging tables, warehouse tables, and data quality.
"""
import psycopg2
from datetime import datetime
import sys

# Database configurations
DB_CONFIG = {
    'host': 'warehouse',
    'port': 5432,
    'database': 'retail_db',
    'user': 'retail_user',
    'password': 'retail_password'
}


def print_header(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def print_subheader(title):
    """Print a formatted subsection header."""
    print("\n" + "-" * 80)
    print(f" {title}")
    print("-" * 80)


def check_connection():
    """Test database connection."""
    print_header("DATABASE CONNECTION CHECK")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Successfully connected to database")

        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"✓ PostgreSQL version: {version.split(',')[0]}")

        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"✗ Connection failed: {str(e)}")
        return False


def check_staging_table():
    """Check staging table data."""
    print_header("STAGING TABLE CHECK")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'staging_online_retail'
            );
        """)
        exists = cur.fetchone()[0]

        if not exists:
            print("✗ Staging table 'staging_online_retail' does not exist")
            cur.close()
            conn.close()
            return False

        print("✓ Table 'staging_online_retail' exists")

        # Get row count
        cur.execute("SELECT COUNT(*) FROM staging_online_retail;")
        row_count = cur.fetchone()[0]
        print(f"✓ Total rows: {row_count:,}")

        if row_count == 0:
            print("⚠ WARNING: Table is empty!")
            cur.close()
            conn.close()
            return False

        # Get date range
        cur.execute("""
            SELECT
                MIN(InvoiceDate) as earliest,
                MAX(InvoiceDate) as latest
            FROM staging_online_retail
            WHERE InvoiceDate IS NOT NULL;
        """)
        date_range = cur.fetchone()
        if date_range[0]:
            print(f"✓ Date range: {date_range[0]} to {date_range[1]}")

        # Get sample statistics
        cur.execute("""
            SELECT
                COUNT(DISTINCT InvoiceNo) as invoices,
                COUNT(DISTINCT CustomerID) as customers,
                COUNT(DISTINCT StockCode) as products,
                COUNT(DISTINCT Country) as countries
            FROM staging_online_retail;
        """)
        stats = cur.fetchone()
        print(f"\n  Statistics:")
        print(f"  - Unique Invoices: {stats[0]:,}")
        print(f"  - Unique Customers: {stats[1]:,}")
        print(f"  - Unique Products: {stats[2]:,}")
        print(f"  - Countries: {stats[3]:,}")

        # Check for NULLs in critical columns
        cur.execute("""
            SELECT
                SUM(CASE WHEN InvoiceNo IS NULL THEN 1 ELSE 0 END) as null_invoices,
                SUM(CASE WHEN StockCode IS NULL THEN 1 ELSE 0 END) as null_stockcodes,
                SUM(CASE WHEN InvoiceDate IS NULL THEN 1 ELSE 0 END) as null_dates,
                SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END) as null_customers
            FROM staging_online_retail;
        """)
        nulls = cur.fetchone()
        print(f"\n  Data Quality:")
        print(f"  - NULL InvoiceNo: {nulls[0]:,}")
        print(f"  - NULL StockCode: {nulls[1]:,}")
        print(f"  - NULL InvoiceDate: {nulls[2]:,}")
        print(f"  - NULL CustomerID: {nulls[3]:,}")

        # Sample records
        print_subheader("Sample Records (First 3 rows)")
        cur.execute("""
            SELECT InvoiceNo, StockCode, Description, Quantity, UnitPrice, Country
            FROM staging_online_retail
            LIMIT 3;
        """)

        rows = cur.fetchall()
        for i, row in enumerate(rows, 1):
            print(f"\n  Row {i}:")
            print(f"    Invoice: {row[0]}, Stock: {row[1]}")
            print(f"    Description: {row[2][:50] if row[2] else 'None'}...")
            print(f"    Qty: {row[3]}, Price: ${row[4]}, Country: {row[5]}")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"✗ Error checking staging table: {str(e)}")
        return False


def check_warehouse_tables():
    """Check warehouse dimension and fact tables."""
    print_header("WAREHOUSE TABLES CHECK")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # List of expected warehouse tables
        tables = [
            'dim_customer',
            'dim_product',
            'dim_date',
            'dim_country',
            'fact_sales'
        ]

        all_exist = True
        table_stats = {}

        for table in tables:
            # Check existence
            cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = '{table}'
                );
            """)
            exists = cur.fetchone()[0]

            if exists:
                # Get row count
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                table_stats[table] = count
                print(f"✓ {table:20} - {count:,} rows")
            else:
                print(f"✗ {table:20} - NOT FOUND")
                all_exist = False

        if all_exist and table_stats.get('fact_sales', 0) > 0:
            # Additional fact table checks
            print_subheader("Fact Table Details")

            cur.execute("""
                SELECT
                    MIN(transaction_date) as earliest,
                    MAX(transaction_date) as latest,
                    SUM(quantity) as total_quantity,
                    SUM(total_amount) as total_revenue
                FROM fact_sales;
            """)
            facts = cur.fetchone()

            if facts[0]:
                print(f"  Date range: {facts[0]} to {facts[1]}")
                print(f"  Total quantity sold: {facts[2]:,}")
                print(f"  Total revenue: ${facts[3]:,.2f}")

            # Check data consistency
            print_subheader("Data Consistency Check")

            # Compare staging to fact table counts
            staging_count = table_stats.get('staging_online_retail', 0)
            fact_count = table_stats.get('fact_sales', 0)

            if staging_count > 0:
                cur.execute("SELECT COUNT(*) FROM staging_online_retail;")
                staging_count = cur.fetchone()[0]

                print(f"  Staging records: {staging_count:,}")
                print(f"  Fact records: {fact_count:,}")

                if abs(staging_count - fact_count) / staging_count < 0.05:  # Within 5%
                    print("  ✓ Record counts are consistent")
                else:
                    diff = abs(staging_count - fact_count)
                    pct = (diff / staging_count) * 100
                    print(f"  ⚠ Record count difference: {diff:,} ({pct:.1f}%)")
                    print(f"    This may be normal if data was cleaned/filtered")

        cur.close()
        conn.close()
        return all_exist

    except Exception as e:
        print(f"✗ Error checking warehouse tables: {str(e)}")
        return False


def check_recent_data():
    """Check if data is recent."""
    print_header("DATA FRESHNESS CHECK")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Check staging table
        cur.execute("""
            SELECT MAX(InvoiceDate) as latest_invoice
            FROM staging_online_retail
            WHERE InvoiceDate IS NOT NULL;
        """)
        latest = cur.fetchone()[0]

        if latest:
            print(f"  Latest invoice date in staging: {latest}")

            # Check if fact table exists and has data
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'fact_sales'
                );
            """)

            if cur.fetchone()[0]:
                cur.execute("""
                    SELECT MAX(transaction_date) as latest_fact
                    FROM fact_sales;
                """)
                latest_fact = cur.fetchone()[0]

                if latest_fact:
                    print(f"  Latest transaction date in warehouse: {latest_fact}")

                    if latest == latest_fact:
                        print("  ✓ Staging and warehouse are in sync")
                    else:
                        print("  ⚠ Staging and warehouse dates differ")
                        print("    (This may be normal if DAG 3 hasn't run yet)")
        else:
            print("  ⚠ No dates found in staging table")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"✗ Error checking data freshness: {str(e)}")
        return False


def run_data_quality_checks():
    """Run data quality checks."""
    print_header("DATA QUALITY CHECKS")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        issues = []

        # Check for negative quantities
        cur.execute("""
            SELECT COUNT(*)
            FROM staging_online_retail
            WHERE Quantity < 0;
        """)
        neg_qty = cur.fetchone()[0]
        if neg_qty > 0:
            issues.append(f"Found {neg_qty:,} records with negative quantities (returns/cancellations)")
            print(f"  ⚠ {neg_qty:,} negative quantity records (may be returns)")
        else:
            print(f"  ✓ No negative quantities")

        # Check for negative prices
        cur.execute("""
            SELECT COUNT(*)
            FROM staging_online_retail
            WHERE UnitPrice < 0;
        """)
        neg_price = cur.fetchone()[0]
        if neg_price > 0:
            issues.append(f"Found {neg_price:,} records with negative prices")
            print(f"  ✗ {neg_price:,} negative price records")
        else:
            print(f"  ✓ No negative prices")

        # Check for zero prices
        cur.execute("""
            SELECT COUNT(*)
            FROM staging_online_retail
            WHERE UnitPrice = 0;
        """)
        zero_price = cur.fetchone()[0]
        if zero_price > 0:
            print(f"  ⚠ {zero_price:,} records with zero price")
        else:
            print(f"  ✓ No zero prices")

        # Check for duplicates
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT InvoiceNo, StockCode, COUNT(*) as cnt
                FROM staging_online_retail
                GROUP BY InvoiceNo, StockCode
                HAVING COUNT(*) > 1
            ) duplicates;
        """)
        dup_count = cur.fetchone()[0]
        if dup_count > 0:
            print(f"  ⚠ {dup_count:,} potential duplicate records")
        else:
            print(f"  ✓ No obvious duplicates")

        cur.close()
        conn.close()

        return len(issues) == 0

    except Exception as e:
        print(f"✗ Error running quality checks: {str(e)}")
        return False


def print_summary(checks):
    """Print overall summary."""
    print_header("VERIFICATION SUMMARY")

    passed = sum(checks.values())
    total = len(checks)

    print(f"\nResults: {passed}/{total} checks passed\n")

    for check, result in checks.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status:8} - {check}")

    print("\n" + "=" * 80)

    if passed == total:
        print("\n🎉 ALL CHECKS PASSED! Data ingestion successful.\n")
        return 0
    elif passed >= total * 0.7:
        print("\n⚠ PARTIAL SUCCESS - Some checks failed but core data is present.\n")
        print("This may be normal if later pipeline stages haven't run yet.")
        print("Check Airflow logs for more details.\n")
        return 1
    else:
        print("\n✗ MULTIPLE FAILURES - Data ingestion may have issues.\n")
        print("Check Airflow logs and verify the pipeline ran successfully.\n")
        return 2


def main():
    """Main verification function."""
    print("=" * 80)
    print(" DATA INGESTION VERIFICATION TOOL")
    print(" " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 80)

    checks = {}

    # Run all checks
    checks["Database Connection"] = check_connection()

    if checks["Database Connection"]:
        checks["Staging Table"] = check_staging_table()
        checks["Warehouse Tables"] = check_warehouse_tables()
        checks["Data Freshness"] = check_recent_data()
        checks["Data Quality"] = run_data_quality_checks()
    else:
        print("\n✗ Cannot proceed - database connection failed")
        return 1

    # Print summary
    return print_summary(checks)


if __name__ == "__main__":
    sys.exit(main())
