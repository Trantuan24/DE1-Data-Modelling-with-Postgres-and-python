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
