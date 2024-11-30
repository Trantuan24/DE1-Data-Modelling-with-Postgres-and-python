import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='logs/create_tables.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_database():
    """
    Create retail_orders database.

    Returns:
        cur (obj): Cursor object for the connection to the retail_orders database.
        conn (obj): Connection object to the retail_orders database.
    """
    try:
        # Kết nối tới database mặc định
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            dbname='postgres',
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # Tạo database retail_orders
        cur.execute("DROP DATABASE IF EXISTS retail_orders")
        cur.execute("CREATE DATABASE retail_orders WITH ENCODING 'utf8' TEMPLATE template0")
        logging.info("Database 'retail_orders' created successfully.")

        # Đóng kết nối tới database mặc định
        conn.close()

        # Kết nối tới database retail_orders
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            dbname='retail_orders',
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cur = conn.cursor()

        return cur, conn

    except Exception as e:
        logging.error(f"Error while creating the database: {e}")
        raise


def drop_tables(cur, conn):
    """
    Drop all tables in the retail_orders database.
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
        logging.info("All tables dropped successfully.")
    except Exception as e:
        logging.error(f"Error while dropping tables: {e}")
        raise


def create_tables(cur, conn):
    """
    Create all tables in the retail_orders database.
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        logging.info("All tables created successfully.")
    except Exception as e:
        logging.error(f"Error while creating tables: {e}")
        raise


def main():
    """
    Main function to set up the database and its tables.
    """
    try:
        cur, conn = create_database()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
        logging.info("Database setup completed successfully.")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")


if __name__ == "__main__":
    main()
