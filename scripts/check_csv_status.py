#!/usr/bin/env python3
"""
Script to check the status of CSV files and display useful information.
Helps users understand which data files are available and when they were last updated.
"""
import os
import datetime
from pathlib import Path


def format_size(bytes_size):
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


def check_csv_file(filepath, description):
    """Check and display information about a CSV file."""
    if os.path.exists(filepath):
        stats = os.stat(filepath)
        size = stats.st_size
        mod_time = datetime.datetime.fromtimestamp(stats.st_mtime)
        age = datetime.datetime.now() - mod_time

        print(f"\n✓ {description}")
        print(f"  Path: {filepath}")
        print(f"  Size: {format_size(size)} ({size:,} bytes)")
        print(f"  Last Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Age: {age.days} days, {age.seconds // 3600} hours ago")

        if age.days > 7:
            print(f"  ⚠ WARNING: File is more than 7 days old")

        return True
    else:
        print(f"\n✗ {description}")
        print(f"  Path: {filepath}")
        print(f"  Status: NOT FOUND")
        return False


def main():
    """Main function to check CSV file status."""
    print("=" * 80)
    print("CSV FILES STATUS CHECK")
    print("=" * 80)

    dags_path = '/opt/airflow/dags'

    # Check main CSV file (used for ingestion)
    main_csv = os.path.join(dags_path, 'online_retail.csv')
    main_exists = check_csv_file(main_csv, "Main CSV (used for ingestion)")

    # Check backup/latest successful CSV
    backup_csv = os.path.join(dags_path, 'latest_successfully_online_retail.csv')
    backup_exists = check_csv_file(backup_csv, "Latest Successfully Downloaded CSV (backup)")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if main_exists and backup_exists:
        print("\n✓ Both CSV files are available")
        print("✓ Pipeline can run with fresh data")

        # Check if files are the same
        if os.path.exists(main_csv) and os.path.exists(backup_csv):
            main_size = os.path.getsize(main_csv)
            backup_size = os.path.getsize(backup_csv)
            main_mtime = os.path.getmtime(main_csv)
            backup_mtime = os.path.getmtime(backup_csv)

            if abs(main_mtime - backup_mtime) < 60:  # Within 1 minute
                print("✓ Files appear to be synchronized (same timestamp)")
            else:
                print("⚠ Warning: Main and backup files have different timestamps")
                print(f"  This might indicate a partial update or different data versions")

    elif main_exists and not backup_exists:
        print("\n⚠ Main CSV exists but no backup found")
        print("⚠ If download fails, pipeline will use the main CSV")
        print("ℹ Backup will be created on next successful download")

    elif not main_exists and backup_exists:
        print("\n⚠ Backup exists but main CSV is missing")
        print("⚠ This is unusual - main CSV may have been deleted")
        print("ℹ Consider copying backup to main: cp latest_successfully_online_retail.csv online_retail.csv")

    else:
        print("\n✗ NO CSV FILES FOUND!")
        print("✗ Pipeline cannot run without data")
        print("\nACTION REQUIRED:")
        print("1. Ensure Kaggle download URL is valid")
        print("2. Run the DAG to download data")
        print("3. Or manually place a CSV file at: " + main_csv)

    print("\n" + "=" * 80)
    print("HOW TO UPDATE DATA")
    print("=" * 80)
    print("\n1. Update Kaggle URL (if expired):")
    print("   docker exec -it airflow_webserver python /opt/airflow/scripts/update_data_url.py")
    print("\n2. Trigger DAG manually:")
    print("   Open http://localhost:8080 and trigger 'dag1_ingest_csv'")
    print("\n3. Or run master pipeline:")
    print("   Trigger 'master_pipeline' to run full ETL")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    main()
