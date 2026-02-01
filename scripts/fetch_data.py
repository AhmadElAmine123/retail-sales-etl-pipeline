"""
Script to download and extract online retail dataset from Kaggle.
This script downloads the data as a zip file and extracts the CSV.
"""
import os
import subprocess
import zipfile
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_data(url: str, output_path: str) -> bool:
    """
    Download data from URL using curl.

    Args:
        url: The URL to download from
        output_path: Path where to save the downloaded file

    Returns:
        bool: True if download successful, False otherwise
    """
    try:
        logger.info(f"Downloading data from Kaggle...")

        # Use curl to download the file
        result = subprocess.run(
            ['curl', '-L', url, '-o', output_path],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        if result.returncode == 0:
            logger.info(f"Download successful: {output_path}")
            return True
        else:
            logger.error(f"Download failed: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("Download timeout after 5 minutes")
        return False
    except Exception as e:
        logger.error(f"Error downloading data: {str(e)}")
        return False


def extract_zip(zip_path: str, extract_to: str) -> bool:
    """
    Extract zip file to specified directory.

    Args:
        zip_path: Path to the zip file
        extract_to: Directory to extract files to

    Returns:
        bool: True if extraction successful, False otherwise
    """
    try:
        logger.info(f"Extracting zip file: {zip_path}")

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

        logger.info(f"Extraction successful to: {extract_to}")
        return True

    except zipfile.BadZipFile:
        logger.error(f"Invalid zip file: {zip_path}")
        return False
    except Exception as e:
        logger.error(f"Error extracting zip: {str(e)}")
        return False


def move_csv_to_target(source_dir: str, target_path: str, backup_path: str = None, csv_name: str = 'online_retail.csv') -> bool:
    """
    Find and move CSV file to target location, with optional backup.

    Args:
        source_dir: Directory containing extracted files
        target_path: Target path for the CSV file
        backup_path: Optional path to save a backup copy (for successful downloads)
        csv_name: Name of the CSV file to look for

    Returns:
        bool: True if file moved successfully, False otherwise
    """
    try:
        # Find the CSV file in the extracted directory
        source_dir_path = Path(source_dir)
        csv_files = list(source_dir_path.rglob('*.csv'))

        if not csv_files:
            logger.error(f"No CSV files found in {source_dir}")
            return False

        # Use the first CSV file found (or look for specific name)
        source_csv = None
        for csv_file in csv_files:
            if csv_name.lower() in csv_file.name.lower():
                source_csv = csv_file
                break

        if not source_csv:
            source_csv = csv_files[0]
            logger.warning(f"Specific CSV not found, using: {source_csv.name}")

        logger.info(f"Moving {source_csv} to {target_path}")

        # Create target directory if it doesn't exist
        target_path_obj = Path(target_path)
        target_path_obj.parent.mkdir(parents=True, exist_ok=True)

        # Import shutil for file operations
        import shutil

        # Copy the file to target location
        shutil.copy2(source_csv, target_path)
        logger.info(f"CSV file successfully copied to: {target_path}")

        # If backup path is provided, save a copy with timestamp/success marker
        if backup_path:
            backup_path_obj = Path(backup_path)
            backup_path_obj.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_csv, backup_path)
            logger.info(f"Backup copy saved to: {backup_path}")

        return True

    except Exception as e:
        logger.error(f"Error moving CSV file: {str(e)}")
        return False


def cleanup_temp_files(zip_path: str, extract_dir: str):
    """
    Clean up temporary files and directories.

    Args:
        zip_path: Path to the zip file
        extract_dir: Directory containing extracted files
    """
    try:
        # Remove zip file
        if os.path.exists(zip_path):
            os.remove(zip_path)
            logger.info(f"Removed temp zip file: {zip_path}")

        # Remove extraction directory
        if os.path.exists(extract_dir):
            import shutil
            shutil.rmtree(extract_dir)
            logger.info(f"Removed temp directory: {extract_dir}")

    except Exception as e:
        logger.warning(f"Error during cleanup: {str(e)}")


def fetch_and_update_data(
    kaggle_url: str,
    temp_zip_path: str = '/opt/airflow/data/temp_download.zip',
    temp_extract_dir: str = '/opt/airflow/data/temp_extract',
    target_csv_path: str = '/opt/airflow/dags/online_retail.csv',
    backup_csv_path: str = '/opt/airflow/dags/latest_successfully_online_retail.csv'
) -> dict:
    """
    Main function to download, extract, and update the data file.
    Returns detailed status to allow graceful fallback.

    Args:
        kaggle_url: URL to download data from
        temp_zip_path: Temporary path for downloaded zip
        temp_extract_dir: Temporary directory for extraction
        target_csv_path: Final location for the CSV file
        backup_csv_path: Path to save successful download as backup

    Returns:
        dict: Status dictionary with keys:
            - success (bool): Overall success
            - message (str): Status message
            - using_fallback (bool): Whether fallback CSV is being used
            - error (str): Error message if failed
    """
    try:
        logger.info("Starting data fetch process...")

        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(temp_zip_path), exist_ok=True)

        # Step 1: Download the zip file
        if not download_data(kaggle_url, temp_zip_path):
            logger.warning("Download failed - will attempt to use existing CSV file")
            cleanup_temp_files(temp_zip_path, temp_extract_dir)
            return {
                'success': False,
                'message': 'Download failed - URL may be expired',
                'using_fallback': True,
                'error': 'HTTP download failed or timed out'
            }

        # Step 2: Extract the zip file
        if not extract_zip(temp_zip_path, temp_extract_dir):
            logger.warning("Extraction failed - will attempt to use existing CSV file")
            cleanup_temp_files(temp_zip_path, temp_extract_dir)
            return {
                'success': False,
                'message': 'ZIP extraction failed - file may be corrupted',
                'using_fallback': True,
                'error': 'Invalid or corrupted ZIP file'
            }

        # Step 3: Move CSV to target location (with backup)
        if not move_csv_to_target(temp_extract_dir, target_csv_path, backup_csv_path):
            logger.warning("CSV move failed - will attempt to use existing CSV file")
            cleanup_temp_files(temp_zip_path, temp_extract_dir)
            return {
                'success': False,
                'message': 'Failed to save CSV file',
                'using_fallback': True,
                'error': 'File system error during CSV save'
            }

        # Step 4: Cleanup temporary files
        cleanup_temp_files(temp_zip_path, temp_extract_dir)

        logger.info("Data fetch process completed successfully!")
        logger.info(f"Latest successful download saved to: {backup_csv_path}")

        return {
            'success': True,
            'message': 'Fresh data downloaded and ready',
            'using_fallback': False,
            'error': None
        }

    except Exception as e:
        logger.error(f"Fatal error in fetch_and_update_data: {str(e)}")
        cleanup_temp_files(temp_zip_path, temp_extract_dir)
        return {
            'success': False,
            'message': f'Unexpected error: {str(e)}',
            'using_fallback': True,
            'error': str(e)
        }


if __name__ == "__main__":
    # For testing purposes
    test_url = "https://storage.googleapis.com/kaggle-data-sets/2827948/4877104/bundle/archive.zip"
    success = fetch_and_update_data(test_url)
    exit(0 if success else 1)
