import os
import logging
from kaggle.api.kaggle_api_extended import KaggleApi

# Set up logging
logging.basicConfig(
    filename='logs/download_data.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def download_data(dataset_name, download_path='data/'):
    """
    Download data from Kaggle and save it as a CSV file.

    Args:
        dataset_name (str): Name of the Kaggle dataset in the format 'username/dataset-name'.
        download_path (str): Directory to save the downloaded data.
    """
    try:
        # Check if the download directory exists; if not, create it
        if not os.path.exists(download_path):
            os.makedirs(download_path)
            logging.info(f"Created directory: {download_path}")

        # Set up Kaggle API credentials
        api = KaggleApi()
        api.authenticate()
        logging.info("Kaggle API authentication successful.")

        # Download data from Kaggle and extract it to the download_path
        logging.info(f"Downloading data from '{dataset_name}'...")
        api.dataset_download_files(dataset_name, path=download_path, unzip=True)
        logging.info("Data downloaded successfully.")

        # Look for CSV files after the download
        csv_files = [f for f in os.listdir(download_path) if f.endswith('.csv')]
        if not csv_files:
            logging.warning("No CSV files found after download.")
            return

        # Rename the first CSV file (if multiple files exist) to 'dataset.csv'
        original_csv = os.path.join(download_path, csv_files[0])
        new_file_path = os.path.join(download_path, 'dataset.csv')
        os.rename(original_csv, new_file_path)

        logging.info(f"CSV file saved to '{new_file_path}'!")

    except Exception as e:
        logging.error(f"Error while downloading data: {e}")
        print("An error occurred while downloading data.")

if __name__ == "__main__":
    # Replace 'username/dataset-name' with the name of the dataset you want to download
    dataset = 'ankitbansal06/retail-orders'
    download_data(dataset)
