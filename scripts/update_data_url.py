#!/usr/bin/env python3
"""
Script to update the Kaggle dataset URL in the configuration and DAG files.
This is useful when the signed URL expires and needs to be refreshed.
"""
import json
import re
from datetime import datetime
from pathlib import Path


def update_url_in_config(new_url: str, config_path: str = '/opt/airflow/config/data_source.json'):
    """
    Update the URL in the config file.

    Args:
        new_url: The new Kaggle dataset URL
        config_path: Path to the configuration file
    """
    config_file = Path(config_path)

    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)

        config['kaggle_dataset']['url'] = new_url
        config['kaggle_dataset']['last_updated'] = datetime.now().strftime('%Y-%m-%d')

        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)

        print(f"Updated config file: {config_path}")
    else:
        print(f"Config file not found: {config_path}")


def update_url_in_dag(new_url: str, dag_path: str = '/opt/airflow/dags/dag0_fetch_data.py'):
    """
    Update the URL in the DAG file.

    Args:
        new_url: The new Kaggle dataset URL
        dag_path: Path to the DAG file
    """
    dag_file = Path(dag_path)

    if dag_file.exists():
        with open(dag_file, 'r') as f:
            content = f.read()

        # Find and replace the URL assignment
        pattern = r'KAGGLE_DATASET_URL\s*=\s*\([^)]+\)'
        replacement = f'KAGGLE_DATASET_URL = "{new_url}"'

        updated_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

        with open(dag_file, 'w') as f:
            f.write(updated_content)

        print(f"Updated DAG file: {dag_path}")
    else:
        print(f"DAG file not found: {dag_path}")


def main():
    """Main function to update the URL in both config and DAG files."""
    print("=== Kaggle Dataset URL Updater ===")
    print("\nPaste the new Kaggle download URL below:")
    print("(You can get this from the Kaggle website by inspecting the download link)")

    new_url = input("\nNew URL: ").strip()

    if not new_url:
        print("Error: No URL provided")
        return

    if not new_url.startswith('http'):
        print("Error: Invalid URL format")
        return

    print("\nUpdating files...")

    try:
        update_url_in_config(new_url)
        update_url_in_dag(new_url)
        print("\n✓ Update completed successfully!")
        print("\nNext steps:")
        print("1. Restart Airflow scheduler if it's running")
        print("2. Test the data fetch DAG to ensure it works")
    except Exception as e:
        print(f"\nError during update: {str(e)}")


if __name__ == "__main__":
    main()
