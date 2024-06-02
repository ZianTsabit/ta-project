import pandas as pd
import psycopg2
from datetime import datetime

# Database connection parameters
conn_params = {
    "dbname": "mydb",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5435"
}

# Function to create a time dimension table
def create_time_dimension(start_date, end_date):
    # Generate date range
    dates = pd.date_range(start=start_date, end=end_date)
    
    # Create a DataFrame to hold the dimension data
    dim_time = pd.DataFrame({
        'date': dates,
        'year': dates.year,
        'quarter': dates.quarter,
        'month': dates.month,
        'day_of_month': dates.day,
        'week_of_year': dates.isocalendar().week,
        'day_of_week': dates.dayofweek,
        'day_name': dates.day_name(),
        'day_abbr': dates.day_name().str[:3],
        'month_name': dates.month_name(),
        'month_abbr': dates.month_name().str[:3],
        'date_text': dates.strftime('%Y-%m-%d'),
        'year_text': dates.strftime('%Y'),
        'quarter_name': dates.quarter.map(lambda x: f'Q{x}'),
        'day_of_month_text': dates.day.map(lambda x: f'{x:02d}'),
        'week_of_year_text': dates.isocalendar().week.map(lambda x: f'{x:02d}')
    })
    
    # Create surrogate key
    dim_time['sk_time'] = range(1, len(dim_time) + 1)
    
    return dim_time

# Function to load data into PostgreSQL
def load_data_to_postgres(df, table_name, conn_params):
    # Establish the connection
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    # Create the table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        sk_time SERIAL PRIMARY KEY,
        date DATE,
        year INTEGER,
        quarter INTEGER,
        month INTEGER,
        day_of_month INTEGER,
        week_of_year INTEGER,
        day_of_week INTEGER,
        day_name VARCHAR,
        day_abbr VARCHAR,
        date_text VARCHAR,
        year_text VARCHAR,
        quarter_name VARCHAR,
        month_name VARCHAR,
        month_abbr VARCHAR,
        day_of_month_text VARCHAR,
        week_of_year_text VARCHAR
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    # Insert data into the table
    for index, row in df.iterrows():
        cursor.execute(f"""
        INSERT INTO {table_name} (date, year, quarter, month, day_of_month, week_of_year, day_of_week,
                                  day_name, day_abbr, date_text, year_text, quarter_name, month_name, month_abbr,
                                  day_of_month_text, week_of_year_text)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['date'], row['year'], row['quarter'], row['month'], row['day_of_month'], row['week_of_year'], 
            row['day_of_week'], row['day_name'], row['day_abbr'], row['date_text'], row['year_text'], 
            row['quarter_name'], row['month_name'], row['month_abbr'], row['day_of_month_text'], 
            row['week_of_year_text']
        ))
    
    # Commit the transaction
    conn.commit()
    
    # Close the connection
    cursor.close()
    conn.close()

# Main ETL process
def main():
    # Define the date range
    start_date = '2023-01-01'
    end_date = '2023-12-31'
    
    # Create time dimension data
    dim_time_df = create_time_dimension(start_date, end_date)
    
    # Load data into PostgreSQL
    load_data_to_postgres(dim_time_df, 'dim_time', conn_params)

if __name__ == '__main__':
    main()