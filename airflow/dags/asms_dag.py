import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from asms import automatic_schema_matching

# Example usage
source_db_connection = 'postgresql+psycopg2://user:password@postgres-lakehouse:5432/mydb'
target_db_connection = 'postgresql+psycopg2://user:password@postgres-warehouse:5432/mydb'

target_fact_table_name = 'fact_table'

target_dimension_table_names = ['dim_client', 'dim_employee', 'dim_office', 'dim_product']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def schema_matching():
    try:
        fact_table_matching, common_column, matching_results = automatic_schema_matching(
            source_db_connection, target_db_connection, target_fact_table_name, target_dimension_table_names
        )
        res = {}
        res['fact_table'] = {}
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

def create_dynamic_tasks():
    tasks = []
    results_file = '/tmp/schema_matching_results.json'
    if os.path.exists(results_file):
        with open(results_file) as f:
            results = json.load(f)
        
        for dimension, tables in results['dimension_tables'].items():
            task = PythonOperator(
                task_id=f'process_{dimension}',
                python_callable=lambda dimension=dimension, tables=tables: print(f'Processing {dimension} with tables {tables}'),
                dag=dag,
            )
            tasks.append(task)
    return tasks

with DAG(
    'data_warehousing_using_asms',
    default_args=default_args,
    description='A dynamic DAG for schema matching',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_schema_matching = PythonOperator(
        task_id='ASMS',
        python_callable=schema_matching,
    )

    dynamic_tasks = create_dynamic_tasks()

    run_schema_matching >> dynamic_tasks
    
    # run_schema_matching >> dynamic_tasks >> etl_dim >> etl_time >> etl_fact