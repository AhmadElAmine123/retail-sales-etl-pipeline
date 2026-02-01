"""
Database configuration module.
Reads credentials from environment variables for security.
"""
import os


def get_db_config():
    """
    Get database configuration from environment variables.
    Falls back to default values for local development.
    """
    return {
        'host': os.environ.get('WAREHOUSE_HOST', 'warehouse'),
        'port': int(os.environ.get('WAREHOUSE_PORT', 5432)),
        'database': os.environ.get('WAREHOUSE_DB', 'retail_db'),
        'user': os.environ.get('WAREHOUSE_USER', 'retail_user'),
        'password': os.environ.get('WAREHOUSE_PASSWORD', 'retail_password')
    }
