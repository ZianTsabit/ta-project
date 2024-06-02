import re
import json
import argparse
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def get_current_table_schema(cursor, table_name):
    cursor.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
    """)
    columns = cursor.fetchall()
    return {col[0]: col[1] for col in columns}

def alter_table_schema(cursor, table_name, new_fields):
    for field, field_type in new_fields.items():
        cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{field}" {field_type}')

def alter_table_schema_type(cursor, table_name, column_name, new_fields):
    cursor.execute(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" SET DATA TYPE {new_fields}')

def get_sql_type(value):
    if isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "DOUBLE PRECISION"
    elif isinstance(value, str):
        return "VARCHAR(255)"
    elif isinstance(value, dict):
        return "JSONB"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', str(value)):
        return "TIMESTAMP"
    else:
        return "TEXT"

def sanitize_column_name(name):
    return re.sub(r'\W|^(?=\d)', '_', name)

def generate_create_table_sql(table_name, fields):
    column_definitions = [f'"{sanitize_column_name(field)}" {field_type}' for field, field_type in fields.items()]
    columns_sql = ",\n    ".join(column_definitions)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns_sql}
    );
    """
    return create_table_sql

def handle_json_objects(cursor, table_name, data):
    fields = {}
    nested_tables = {}

    for key, value in data.items():
        if key == "_id":
            continue

        sanitized_key = sanitize_column_name(key)
        if isinstance(value, dict):
            nested_table_name = f"{table_name}_{sanitized_key}"
            nested_tables[sanitized_key] = nested_table_name
            handle_json_objects(cursor, nested_table_name, value)
            fields[sanitized_key] = "VARCHAR(255)"  # Foreign key reference
        else:
            fields[sanitized_key] = get_sql_type(value)

    create_table_sql = generate_create_table_sql(table_name, fields)
    cursor.execute(create_table_sql)

    return nested_tables

def generate_sql_from_cdc(cdc_message, cursor):
    cdc_data = json.loads(cdc_message)
    operation = cdc_data.get("op")
    after_data = json.loads(cdc_data.get("after")) if cdc_data.get("after") else None
    before_data = json.loads(cdc_data.get("before")) if cdc_data.get("before") else None
    update_description = cdc_data.get("updateDescription")
    table_name = cdc_data['source']['collection']

    if after_data:
        nested_tables = handle_json_objects(cursor, table_name, after_data)
        new_fields = {sanitize_column_name(k): get_sql_type(v) for k, v in after_data.items() if k != "_id" and sanitize_column_name(k) not in get_current_table_schema(cursor, table_name)}
        if new_fields:
            alter_table_schema(cursor, table_name, new_fields)

        if operation == "c":
            for key, nested_table in nested_tables.items():
                nested_data = after_data[key]
                nested_columns = ', '.join(f'"{sanitize_column_name(k)}"' for k in nested_data.keys())
                nested_values = ', '.join(f"'{value}'" if isinstance(value, str) else str(value) for value in nested_data.values())
                nested_sql = f'INSERT INTO "{nested_table}" ({nested_columns}) VALUES ({nested_values});'
                cursor.execute(nested_sql)

                if key in after_data:
                    key_reference = list(nested_data.keys())[0]
                    foreign_key = nested_data[key_reference]
                    foreign_key_type = get_sql_type(foreign_key)
                    after_data[key] = str(foreign_key)
                    alter_table_schema_type(cursor, table_name, key, foreign_key_type)

            columns = ', '.join(f'"{sanitize_column_name(k)}"' for k in after_data.keys() if k != "_id")
            values = ', '.join(f"'{value}'" if isinstance(value, str) else str(value) for k, value in after_data.items() if k != "_id")
            sql = f'INSERT INTO "{table_name}" ({columns}) VALUES ({values});'
    
    elif operation == "u":
        updated_fields = json.loads(update_description['updatedFields']) if update_description and update_description.get('updatedFields') else {}
        set_clause = ', '.join(f'"{sanitize_column_name(key)}" = \'{value}\'' if isinstance(value, str) else f'"{sanitize_column_name(key)}" = {value}' for key, value in updated_fields.items() if key != "_id")
        where_clause = f'"_id" = \'{before_data["_id"]["$oid"]}\'' if before_data and before_data.get('_id') else '1=1'
        sql = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause};'

    elif operation == "d":
        where_clause = f'"_id" = \'{before_data["_id"]["$oid"]}\'' if before_data and before_data.get('_id') else '1=1'
        sql = f'DELETE FROM "{table_name}" WHERE {where_clause};'

    else:
        sql = "-- Unsupported operation type"

    return sql

def process_row(row, lakehouse_host, lakehouse_port, lakehouse_db, lakehouse_user, lakehouse_password, mongo_collection):
    cdc_message = row.payload
    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(
            dbname=lakehouse_db,
            user=lakehouse_user,
            password=lakehouse_password,
            host=lakehouse_host,
            port=lakehouse_port
        )
        cursor = conn.cursor()

        sql_query = generate_sql_from_cdc(cdc_message, cursor)
        print(sql_query)
        cursor.execute(sql_query)
        conn.commit()

    except Exception as e:
        print(f"Error processing row: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Kafka CDC stream and apply changes to PostgreSQL Lakehouse.')
    parser.add_argument('--kafka-bootstrap-servers', type=str, required=True, help='Kafka bootstrap servers')
    parser.add_argument('--kafka-topic', type=str, required=True, help='Kafka topic to subscribe to')
    parser.add_argument('--lakehouse-host', type=str, required=True, help='Lakehouse PostgreSQL host')
    parser.add_argument('--lakehouse-port', type=int, required=True, help='Lakehouse PostgreSQL port')
    parser.add_argument('--lakehouse-db', type=str, required=True, help='Lakehouse PostgreSQL database name')
    parser.add_argument('--lakehouse-user', type=str, required=True, help='Lakehouse PostgreSQL user')
    parser.add_argument('--lakehouse-password', type=str, required=True, help='Lakehouse PostgreSQL password')

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("KafkaStreamProcessing") \
        .getOrCreate()

    schema = StructType([
        StructField("schema", StringType(), True),
        StructField("payload", StringType(), True)
    ])

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers) \
        .option("subscribe", args.kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df_json = df.selectExpr("CAST(value AS STRING)")

    parsed_df = df_json.withColumn("value_json", from_json(col("value"), schema)).select("value_json.*")

    mongo_collection = args.kafka_topic.split('.')[-1]
    query = parsed_df.writeStream.foreachBatch(lambda batch_df, batch_id: batch_df.foreach(lambda row: process_row(row, args.lakehouse_host, args.lakehouse_port, args.lakehouse_db, args.lakehouse_user, args.lakehouse_password, mongo_collection))).start()
    query.awaitTermination()