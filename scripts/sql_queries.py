# DROP TABLES
fact_sales_table_drop = "DROP TABLE IF EXISTS fact_sales"
dim_ship_mode_table_drop = "DROP TABLE IF EXISTS dim_ship_mode"
dim_segment_table_drop = "DROP TABLE IF EXISTS dim_segment"
dim_location_table_drop = "DROP TABLE IF EXISTS dim_location"
dim_product_table_drop = "DROP TABLE IF EXISTS dim_product"
dim_date_table_drop = "DROP TABLE IF EXISTS dim_date"

# CREATE TABLES
fact_sales_table_create = ("""
    CREATE TABLE fact_sales (
        fact_id SERIAL PRIMARY KEY,               
        order_id INT NOT NULL,                     
        product_id VARCHAR(20) NOT NULL REFERENCES dim_product(product_id), 
        ship_mode_id INT NOT NULL REFERENCES dim_ship_mode(ship_mode_id), 
        segment_id INT NOT NULL REFERENCES dim_segment(segment_id), 
        location_id INT NOT NULL REFERENCES dim_location(location_id), 
        order_date DATE NOT NULL REFERENCES dim_date(order_date), 
        cost_price NUMERIC(10, 2) NOT NULL,      
        list_price NUMERIC(10, 2) NOT NULL,      
        quantity INT NOT NULL,                   
        discount_percent NUMERIC(5, 2),         
        discount NUMERIC(10, 2),                
        sale_price NUMERIC(10, 2) NOT NULL,      
        profit NUMERIC(10, 2) NOT NULL
    );
""")

dim_ship_mode_table_create = ("""
    CREATE TABLE dim_ship_mode (
        ship_mode_id SERIAL PRIMARY KEY,
        ship_mode VARCHAR(50) NOT NULL UNIQUE
    );
""")

dim_segment_table_create = ("""
    CREATE TABLE dim_segment (
        segment_id SERIAL PRIMARY KEY,
        segment VARCHAR(50) NOT NULL UNIQUE
    );
""")

dim_location_table_create = ("""
    CREATE TABLE dim_location (
        location_id SERIAL PRIMARY KEY,
        postal_code INT,
        country VARCHAR(100) NOT NULL,
        city VARCHAR(100) NOT NULL,
        state VARCHAR(100),
        region VARCHAR(50)
    );
""")

dim_product_table_create = ("""
    CREATE TABLE dim_product (
        product_id VARCHAR(20) PRIMARY KEY,
        category VARCHAR(50) NOT NULL,
        sub_category VARCHAR(50) NOT NULL
    );
""")

dim_date_table_create = ("""
    CREATE TABLE dim_date (
        order_date DATE PRIMARY KEY,
        year INT NOT NULL,
        quarter INT NOT NULL,
        month INT NOT NULL,
        week INT NOT NULL,
        day INT NOT NULL,
        day_name VARCHAR(15) NOT NULL,
        is_weekend BOOLEAN NOT NULL
    );
""")

# INSERT RECORDS
fact_sales_table_insert = ("""
    INSERT INTO fact_sales (order_id, product_id, ship_mode_id, segment_id, location_id, order_date, 
                            cost_price, list_price, quantity, discount_percent, discount, 
                            sale_price, profit) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
""")

dim_ship_mode_table_insert = ("""
    INSERT INTO dim_ship_mode (ship_mode) 
        VALUES (%s) ON CONFLICT DO NOTHING;
""")

dim_segment_table_insert = ("""
    INSERT INTO dim_segment (segment) 
        VALUES (%s) ON CONFLICT DO NOTHING;
""")

dim_location_table_insert = ("""
    INSERT INTO dim_location (postal_code, country, city, state, region) 
        VALUES (%s, %s, %s, %s, %s);
""")

dim_product_table_insert = ("""
    INSERT INTO dim_product (product_id, category, sub_category) 
        VALUES (%s, %s, %s) ON CONFLICT (product_id) DO NOTHING;
""")

dim_date_table_insert = ("""
    INSERT INTO dim_date (order_date, year, quarter, month, week, day, day_name, is_weekend)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_date) DO NOTHING;
""")


# QUERY LISTS
create_table_queries = [
    dim_ship_mode_table_create,
    dim_segment_table_create,
    dim_location_table_create,
    dim_product_table_create,
    dim_date_table_create,
    fact_sales_table_create
]

drop_table_queries = [
    fact_sales_table_drop,
    dim_ship_mode_table_drop,
    dim_segment_table_drop,
    dim_location_table_drop,
    dim_product_table_drop,
    dim_date_table_drop
]
