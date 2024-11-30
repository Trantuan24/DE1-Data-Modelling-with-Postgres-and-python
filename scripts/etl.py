import os
import pandas as pd
import psycopg2
import logging
from dotenv import load_dotenv
from sql_queries import *

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def connect_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        logging.info("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        raise


def load_data(file_path):
    """Load data from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded data from {file_path}.")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def transform_data(df):
    """Transform and clean data."""
    try:
        # Convert and clean date fields
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        df.dropna(subset=['order_date'], inplace=True)

        # Extract date components
        df['year'] = df['order_date'].dt.year
        df['quarter'] = df['order_date'].dt.quarter
        df['month'] = df['order_date'].dt.month
        df['week'] = df['order_date'].dt.isocalendar().week
        df['day'] = df['order_date'].dt.day
        df['day_name'] = df['order_date'].dt.day_name()
        df['is_weekend'] = df['order_date'].dt.weekday >= 5

        logging.info("Data transformation complete.")
        return df
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise


def insert_data(cur, conn, query, data, bulk=False):
    """Insert data into the database."""
    try:
        if bulk:
            cur.executemany(query, data)
        else:
            for record in data:
                cur.execute(query, record)
        conn.commit()
        logging.info(f"Inserted {len(data)} records successfully.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting data: {e}")
        raise


def fetch_mapping(cur, query):
    """Fetch mapping from dimension tables."""
    try:
        cur.execute(query)
        return dict(cur.fetchall())
    except Exception as e:
        logging.error(f"Error fetching mapping: {e}")
        raise


def map_to_ids(cur, df):
    """Map data to dimension IDs."""
    try:
        mappings = {
            'ship_mode': fetch_mapping(cur, "SELECT ship_mode, ship_mode_id FROM dim_ship_mode;"),
            'segment': fetch_mapping(cur, "SELECT segment, segment_id FROM dim_segment;")
        }

        cur.execute("""
            SELECT postal_code, city, state, country, region, location_id
            FROM dim_location
        """)
        location_map = {
            (row[0], row[1], row[2], row[3], row[4]): row[5] for row in cur.fetchall()
        }

        # Map IDs
        df['ship_mode_id'] = df['ship_mode'].map(mappings['ship_mode'])
        df['segment_id'] = df['segment'].map(mappings['segment'])
        df['location_id'] = df.apply(
            lambda x: location_map.get((x['postal_code'], x['city'], x['state'], x['country'], x['region'])),
            axis=1
        )

        # Handle unmapped records
        for column in ['ship_mode_id', 'segment_id', 'location_id']:
            missing = df[df[column].isnull()]
            if not missing.empty:
                logging.warning(f"Unmapped records in column {column}:")
                logging.warning(missing[['postal_code', 'city', 'state', 'country', 'region']].drop_duplicates())

        # Drop unmapped records
        df.dropna(subset=['ship_mode_id', 'segment_id', 'location_id'], inplace=True)
        logging.info(f"Remaining records after mapping: {len(df)}.")
        return df
    except Exception as e:
        logging.error(f"Error mapping data to IDs: {e}")
        raise


def main():
    """Run the ETL process."""
    file_path = os.getenv("DATASET_PATH", "data/clean_dataset.csv")
    try:
        with connect_db() as conn:
            with conn.cursor() as cur:
                # Load and transform data
                df = transform_data(load_data(file_path))

                # Insert data into dimensions
                insert_data(cur, conn, dim_ship_mode_table_insert, df[['ship_mode']].drop_duplicates().values.tolist())
                insert_data(cur, conn, dim_segment_table_insert, df[['segment']].drop_duplicates().values.tolist())
                insert_data(cur, conn, dim_location_table_insert,
                            df[['postal_code', 'country', 'city', 'state', 'region']].drop_duplicates().values.tolist(), bulk=True)
                insert_data(cur, conn, dim_product_table_insert,
                            df[['product_id', 'category', 'sub_category']].drop_duplicates().values.tolist(), bulk=True)
                insert_data(cur, conn, dim_date_table_insert,
                            df[['order_date', 'year', 'quarter', 'month', 'week', 'day', 'day_name', 'is_weekend']].drop_duplicates().values.tolist(), bulk=True)

                # Map to IDs and insert into fact table
                df = map_to_ids(cur, df)
                fact_sales_data = df[['order_id', 'product_id', 'ship_mode_id', 'segment_id', 'location_id',
                                      'order_date', 'cost_price', 'list_price', 'quantity',
                                      'discount_percent', 'discount', 'sale_price', 'profit']].values.tolist()
                insert_data(cur, conn, fact_sales_table_insert, fact_sales_data, bulk=True)

        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")


if __name__ == "__main__":
    main()
