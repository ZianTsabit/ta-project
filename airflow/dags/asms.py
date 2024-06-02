import pandas as pd
from sqlalchemy import create_engine, inspect

class TreeNode:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields
        self.children = []

def get_schema_from_db(connection_string):
    engine = create_engine(connection_string)
    inspector = inspect(engine)
    schema = {}
    foreign_keys = {}

    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        fields = [column['name'] for column in columns]
        schema[table_name] = fields
        fk_info = inspector.get_foreign_keys(table_name)
        if fk_info:
            foreign_keys[table_name] = [fk['referred_table'] for fk in fk_info]
            for fk in fk_info:
                if fk['referred_table'] not in foreign_keys:
                    foreign_keys[fk['referred_table']] = []
                foreign_keys[fk['referred_table']].append(table_name)

    return schema, foreign_keys

def build_schema_tree_from_fact_table(fact_table, foreign_keys, source_schema):
    root = TreeNode(fact_table, source_schema[fact_table])
    stack = [root]
    visited = set([fact_table])

    while stack:
        node = stack.pop()
        children_tables = foreign_keys.get(node.name, [])
        for child_table in children_tables:
            if child_table not in visited:
                child_node = TreeNode(child_table, source_schema[child_table])
                node.children.append(child_node)
                stack.append(child_node)
                visited.add(child_table)

    return root

def fact_matching(source_schema, target_fact_table):
    for table_name, fields in source_schema.items():
        if set(fields) & set(target_fact_table['fields']):
            common_column = list(set(fields) & set(target_fact_table['fields']))
            return table_name, common_column
    return None

def dimension_matching(root_node, target_dimensions, source_schema, foreign_keys):
    matching_results = {}
    used_tables = set()
    related_table_map = {dim['name']: [] for dim in target_dimensions}

    for dimension in target_dimensions:
        best_match = None
        max_score = 0
        for candidate_name, candidate_fields in traverse_tree(root_node, source_schema):
            if candidate_name in used_tables:
                continue

            score = len(set(candidate_fields) & set(dimension['fields']))
            if score > max_score:
                max_score = score
                best_match = candidate_name
        
        if best_match:
            matching_results[dimension['name']] = [best_match]
            used_tables.add(best_match)

            # Include related tables
            related_tables = get_related_tables(best_match, foreign_keys, used_tables)
            for table in related_tables:
                if table not in used_tables:
                    related_table_map[dimension['name']].append(table)
        else:
            matching_results[dimension['name']] = []

    # Ensure each dimension gets the best related tables
    for dimension, best_match in matching_results.items():
        if best_match:
            best_match = best_match[0]
            related_tables = related_table_map[dimension]
            for table in related_tables:
                if table not in used_tables:
                    matching_results[dimension].append(table)
                    used_tables.add(table)

    return matching_results

def get_related_tables(table_name, foreign_keys, used_tables):
    related_tables = set()
    
    # Check tables that the current table references
    for key, values in foreign_keys.items():
        if table_name in values and key not in used_tables:
            related_tables.add(key)
    
    # Check tables that reference the current table
    for value in foreign_keys.get(table_name, []):
        if value not in used_tables:
            related_tables.add(value)
    
    return related_tables

def traverse_tree(node, source_schema):
    yield (node.name, source_schema[node.name])
    for child in node.children:
        yield from traverse_tree(child, source_schema)

def print_tree(node, level=0):
    print('    ' * level + node.name)
    for child in node.children:
        print_tree(child, level + 1)

def automatic_schema_matching(source_db_connection, target_db_connection, target_fact_table_name, target_dimension_table_names):
    source_schema, foreign_keys = get_schema_from_db(source_db_connection)
    target_schema, _ = get_schema_from_db(target_db_connection)
    
    target_fact_table = {'name': target_fact_table_name, 'fields': target_schema[target_fact_table_name]}
    target_dimensions = [{'name': name, 'fields': target_schema[name]} for name in target_dimension_table_names]
    
    fact_table, common_column = fact_matching(source_schema, target_fact_table)

    if fact_table != '' and len(common_column) != 0 :
        schema_tree = build_schema_tree_from_fact_table(fact_table, foreign_keys, source_schema)
        matching_results = dimension_matching(schema_tree, target_dimensions, source_schema, foreign_keys)
        return fact_table, common_column, matching_results
    else:
        raise ValueError("Fact table could not be identified in the source schema")

try:
    source_db_connection = 'postgresql+psycopg2://user:password@localhost:5434/mydb'
    target_db_connection = 'postgresql+psycopg2://user:password@localhost:5435/mydb'

    target_fact_table_name = 'fact_table'

    target_dimension_table_names = ['dim_client', 'dim_employee', 'dim_office', 'dim_product']

    fact_table_matching, common_column, matching_results = automatic_schema_matching(
        source_db_connection, target_db_connection, target_fact_table_name, target_dimension_table_names
    )

    res = {}
    res['fact_matching'] = {}
    res['fact_matching']['fact_table'] = fact_table_matching
    res['fact_matching']['match_table'] = common_column
    dict_dim = {}
    
    for dimension, matches in matching_results.items():
        match_table = []
        if matches:
            for i in range(len(matches)):
                if matches[i] != fact_table_matching:
                    match_table.append(matches[i])

        dict_dim[dimension] = match_table
    
    res['dimension_table'] = dict_dim

    print(res)

except Exception as e:
    print(e)