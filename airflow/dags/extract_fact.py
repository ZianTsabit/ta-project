import psycopg2

def get_join_condition_from_db(conn, table1, table2):
    query = """
    SELECT
        kcu1.table_name AS table1,
        kcu1.column_name AS column1,
        kcu2.table_name AS table2,
        kcu2.column_name AS column2
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu1
            ON tc.constraint_name = kcu1.constraint_name
            AND tc.table_schema = kcu1.table_schema
        JOIN information_schema.referential_constraints AS rc
            ON tc.constraint_name = rc.constraint_name
        JOIN information_schema.key_column_usage AS kcu2
            ON rc.unique_constraint_name = kcu2.constraint_name
            AND rc.unique_constraint_schema = kcu2.table_schema
            AND kcu1.ordinal_position = kcu2.ordinal_position
    WHERE
        (kcu1.table_name = %s AND kcu2.table_name = %s)
        OR (kcu1.table_name = %s AND kcu2.table_name = %s);
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (table1, table2, table2, table1))
        result = cursor.fetchall()
        conditions = []
        for row in result:
            if row[0] == table1:
                conditions.append((row[1], row[3]))
            else:
                conditions.append((row[3], row[1]))
        return conditions

conn_params = {
    "dbname": "mydb",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5434"
}

conn = psycopg2.connect(**conn_params)

matching_result = {
    "fact_matching": {
        "fact_table": "orderdetails", 
        "match_table": ["quantity", "price_one"]
    }, 
    "dimension_table": {
        "dim_client": ["clients"], 
        "dim_employee": ["employees"], 
        "dim_office": ["offices"], 
        "dim_product": ["products"]
    }
}

select_columns = []
joins = []

base_table = "orders"
base_table_alias = "o"
fact_table = matching_result["fact_matching"]["fact_table"]
fact_table_alias = "od"

joins.append(f"FROM {base_table} {base_table_alias}")
joins.append(f"JOIN {fact_table} {fact_table_alias} USING (order_id)")

for column in matching_result["fact_matching"]["match_table"]:
    select_columns.append(f"{fact_table_alias}.{column}")

for dimension, tables in matching_result["dimension_table"].items():
    for table in tables:
        if table == "clients":
            joins.append(f"JOIN {table} c USING (client_id)")
        elif table == "employees":
            joins.append(f"LEFT JOIN {table} e ON c.sales_rep_employee_id = e.employee_id")
            select_columns.append("e.employee_id")
        elif table == "offices":
            joins.append(f"JOIN {table} oc USING (office_id)")
            select_columns.append("oc.office_id")

select_clause = f"""
SELECT
    {base_table_alias}.order_id
    ,{base_table_alias}.client_id
    ,DATE({base_table_alias}.order_date) AS order_date
    ,{fact_table_alias}.product_id
""" + "\n    ," + "\n    ,".join(select_columns)

join_clause = "\n".join(joins)

lookup_query = f"{select_clause}\n{join_clause}"

print(lookup_query)

conn.close()