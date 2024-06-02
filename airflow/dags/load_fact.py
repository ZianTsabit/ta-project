import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database connection parameters
source_params = {
    "dbname": "mydb",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5434"
}

dest_params = {
    "dbname": "mydb",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5435"
}

# Function to get surrogate key
def get_surrogate_key(cursor, dimension_table, natural_key_column, natural_key_value):
    dim_table = dimension_table.split('_')
    surrogate_key = f"sk_{dim_table[1]}"
    query = f"SELECT {surrogate_key} FROM {dimension_table} WHERE {natural_key_column} = %s"
    cursor.execute(query, (natural_key_value,))
    result = cursor.fetchone()
    return result[0] if result else None

# Function to extract data using lookup query
def extract_data(conn_params):
    lookup_query = """
    SELECT
        o.order_id,
        o.client_id,
        DATE(o.order_date) AS order_date,
        od.product_id,
        od.price_one,
        od.quantity,
        e.employee_id,
        oc.office_id
    FROM orders o
    JOIN orderdetails od USING (order_id)
    JOIN clients c USING (client_id)
    LEFT JOIN employees e ON c.sales_rep_employee_id = e.employee_id
    JOIN offices oc USING (office_id)
    """
    conn_str = f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
    engine = create_engine(conn_str)
    df = pd.read_sql_query(lookup_query, engine)
    return df

# Function to transform data and add surrogate keys
def transform_data(df, dest_params):
    dest_conn = psycopg2.connect(**dest_params)
    dest_cursor = dest_conn.cursor()

    logging.info("Transforming data and adding surrogate keys")

    df['sk_client'] = df.apply(lambda row: get_surrogate_key(dest_cursor, 'dim_client', 'client_id', row['client_id']), axis=1)
    df['sk_product'] = df.apply(lambda row: get_surrogate_key(dest_cursor, 'dim_product', 'product_id', row['product_id']), axis=1)
    df['sk_employee'] = df.apply(lambda row: get_surrogate_key(dest_cursor, 'dim_employee', 'employee_id', row['employee_id']), axis=1)
    df['sk_office'] = df.apply(lambda row: get_surrogate_key(dest_cursor, 'dim_office', 'office_id', row['office_id']), axis=1)
    df['sk_order_time'] = df.apply(lambda row: get_surrogate_key(dest_cursor, 'dim_time', 'date', row['order_date']), axis=1)

    dest_conn.close()
    return df

# Function to load data into the fact table
def load_data_to_fact_table(df, conn_params):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    logging.info("Loading data into fact table")

    for index, row in df.iterrows():
        sk_client = row['sk_client']
        sk_product = row['sk_product']
        sk_employee = row['sk_employee']
        sk_office = row['sk_office']
        sk_order_time = row['sk_order_time']
        
        # Validate surrogate keys before insertion
        if not all(isinstance(value, int) and -9223372036854775808 <= value <= 9223372036854775807 for value in [sk_client, sk_product, sk_employee, sk_office, sk_order_time]):
            logging.error(f"Invalid surrogate key value detected at index {index}: {sk_client}, {sk_product}, {sk_employee}, {sk_office}, {sk_order_time}")
            continue

        cursor.execute("""
        INSERT INTO fact_table (sk_client, sk_product, sk_employee, sk_office, sk_order_time, price_one, quantity)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            sk_client, sk_product, sk_employee, sk_office, sk_order_time, row['price_one'], row['quantity']
        ))

    conn.commit()
    cursor.close()
    conn.close()

def main():
    logging.info("Starting ETL process")

    extracted_data = extract_data(source_params)
    logging.info("Data extracted successfully")

    transformed_data = transform_data(extracted_data, dest_params)
    logging.info("Data transformed successfully")

    load_data_to_fact_table(transformed_data, dest_params)
    logging.info("Data loaded into fact table successfully")

if __name__ == '__main__':
    main()