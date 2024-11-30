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











