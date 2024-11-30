# DE1 - Data Modelling with Postgres and pandas

## Overview
This project focuses on building a data management system using `Postgres` and processing data with `Python`. The workflow includes downloading, cleaning, normalizing, and storing data in a database to support analysis. Data analysis is performed through SQL queries and reporting.

### Database
The database is optimized for analyzing retail sales data and is designed using a star schema. It includes the following tables:
* Fact table
    * fact_sales: Contains transactional data for retail sales, such as sales prices, profits, discounts, and quantities.
* Dimension tables
    * dim_ship_mode: Contains information about different shipping modes.
    * dim_segment: Details customer segments (e.g., consumer, corporate).
    * dim_location: Stores geographic details, including country, city, state, postal code, and region.
    * dim_product: Contains information on product categories, subcategories, and product IDs.
    * dim_date: Includes details about dates (e.g., year, month, day) to support temporal analysis.

### ETL pipeline
The ETL pipeline processes data from the retail orders dataset to populate the database.


## Usage
To create `retail_orders` database and its tables:
1. Download Kaggle API Key
2. Run `python scripts/download_data.py`
3. Open Jupyter Notebook and run the file `notebooks/analysis.ipynb`
4. Run `python scripts/create_tables.py`

To load data into the `retail_orders` database: Run `python3 scripts/etl.py`


## Project structure
data_modeling_with_postgres_and_python/
├── data/                           # Raw and cleaned datasets
│   ├── retail_orders.csv           # Raw dataset
│   └── clean_retail_orders.csv     # Cleaned and prepared dataset
├── notebooks/                      # Jupyter notebooks for analysis and reporting
│   └── analysis.ipynb              # Notebook for data cleaning and exploration
├── logs/                           # Log files for monitoring scripts
│   ├── create_tables.log           # Log for table creation script
│   ├── download_data.log           # Log for data download script
│   └── etl.log                     # Log for ETL pipeline
├── scripts/                        # Python scripts for ETL and database management
│   ├── create_tables.py            # Script to create database and tables
│   ├── download_data.py            # Script to download data from Kaggle or APIs
│   ├── sql_queries.py              # SQL queries for table creation and ETL
│   └── etl.py                      # ETL pipeline to load data into the database
├── sql/                            # SQL files for analysis
│   └── analysis_queries.sql        # SQL queries for data analysis
├── requirements.txt                # Python dependencies for the project
├── .env                            # Environment variables for database configuration
├── README.md                       # Project documentation
