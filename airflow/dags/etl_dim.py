import psycopg2
from psycopg2.extras import execute_values

def get_table_columns(cursor, table_name, table_alias):
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '{table_name}';
    """)
    return {row[0]: f"{table_alias}.{row[0]} AS {table_alias}_{row[0]}" for row in cursor.fetchall()}

def get_foreign_keys(cursor, table_name):
    cursor.execute(f"""
        SELECT
            kcu.column_name AS local_column,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND kcu.table_name = '{table_name}';
    """)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def get_destination_columns(cursor, table_name):
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '{table_name}';
    """)
    return [row[0] for row in cursor.fetchall()]

def generate_select_query(tables, cursor):
    if not tables:
        return None, {}

    main_table = tables[0]
    main_alias = main_table[0]
    select_columns = get_table_columns(cursor, main_table, main_alias)
    select_parts = list(select_columns.values())
    column_map = {v.split(" AS ")[1]: k for k, v in select_columns.items()}
    from_part = f"FROM {main_table} AS {main_alias}"
    join_parts = []
    added_columns = set(select_columns.keys())

    for table in tables[1:]:
        table_alias = table[0]
        foreign_keys = get_foreign_keys(cursor, table)
        select_columns = get_table_columns(cursor, table, table_alias)

        for fk in foreign_keys:
            if fk['foreign_table'] == main_table:
                join_parts.append(f"LEFT JOIN {table} AS {table_alias} ON {main_alias}.{fk['foreign_column']} = {table_alias}.{fk['local_column']}")
                for col, alias in select_columns.items():
                    if col not in added_columns:
                        select_parts.append(alias)
                        column_map[alias.split(" AS ")[1]] = col
                        added_columns.add(col)

    query = f"SELECT DISTINCT {', '.join(select_parts)} {from_part} {' '.join(join_parts)}"
    return query, column_map

def execute_query_and_insert(source_cursor, dest_cursor, query, destination_table):
    try:
        source_cursor.execute(query)
    except psycopg2.errors.UndefinedColumn as e:
        print(f"Error executing query: {e}")
        print(f"Query: {query}")
        return

    rows = source_cursor.fetchall()
    if not rows:
        print(f"No rows found for query: {query}")
        return

    dest_columns = get_destination_columns(dest_cursor, destination_table)
    columns = [desc[0] for desc in source_cursor.description]
    
    filtered_columns = [col for col in columns if col.split('_', 1)[1] in dest_columns]

    if not filtered_columns:
        print(f"No matching columns found between source query and destination table {destination_table}.")
        return

    columns_str = ', '.join([col.split('_', 1)[1] for col in filtered_columns])
    values = [[row[columns.index(col)] for col in filtered_columns] for row in rows]

    insert_query = f"INSERT INTO {destination_table} ({columns_str}) VALUES %s"
    print(f"Insert query: {insert_query}")
    print(f"Values: {values[:2]}")
    execute_values(dest_cursor, insert_query, values)

def main(input_data, source_db_config, dest_db_config):
    
    source_conn = psycopg2.connect(**source_db_config)
    source_cursor = source_conn.cursor()

    dest_conn = psycopg2.connect(**dest_db_config)
    dest_cursor = dest_conn.cursor()

    for dimension, tables in input_data.items():
        query, column_map = generate_select_query(tables, source_cursor)
        if query:
            print(f"Generated query for {dimension}:\n{query}\n")
            execute_query_and_insert(source_cursor, dest_cursor, query, dimension)

    dest_conn.commit()
    source_conn.close()
    dest_conn.close()