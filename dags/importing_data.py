import csv
# Increase the maximum field size limit
csv.field_size_limit(10**8)
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2 import sql
from pathlib import Path

file_path = '../data/20181024_d1_0830_0900.csv'
# Define your PostgreSQL connection parameters
conn_params = {
    
            "host":"172.31.98.138",
            "database":"airflow_traffic_db",
            "port":"5432",
            "user":"postgres",
            "password":"password"
        }
import os

def split_list_into_parts(input_list, part_size):
    return [input_list[i:i + part_size] for i in range(0, len(input_list), part_size)]


def load_and_transform_data(**kwargs):
    
    # Get the current working directory
    current_directory = Path.cwd()

    # Print the current directory
    print("Current Directory:", current_directory)

    with open(file_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        i = 0
        # Read each row one by one
        for line_number, row in enumerate(csv_reader):
            # Process individual lines as needed
            if(i == 10):
                break
            if (i == 0):
                # create the column names for the fact and dimensional table
                columns = row[0]
                dimension_columns = [columns.split(";")[0]] + columns.split(";")[4:]
                dimension_columns = [column.strip() for column in dimension_columns]
                fact_columns = [column.strip() for column in columns.split(";")[:4]]
                # create fact and dimensional table
                fact_df = pd.DataFrame(columns=fact_columns)
                dimension_df = pd.DataFrame(columns=dimension_columns)          
            else:
                data = [elt  for elt in row[0].split(";") if elt != " "]
                track_id = data[0]
                
                fact_df.loc[len(fact_df)] = data[:4]
                
                # Split the list into parts containing 6 elements each
                split_parts = split_list_into_parts(data[4:], 6)

                for dimensions in split_parts:
                    dimensions.insert(0, track_id)
                    dimension_df.loc[len(dimension_df)] = dimensions
            i+=1
        fact_df.to_csv("../data/fact_df.csv", index=False)   
        dimension_df.to_csv("../data/dimension_df.csv", index=False)   

def check_db_connection(**kwargs):
    try:
        conn = psycopg2.connect(**conn_params
        )
        conn.close()
        return True
    except Exception as e:
        print(f"Database connection failed again: {e}")
        return False     

def create_tables(**kwargs):
    # Establish connection
    conn = psycopg2.connect(**conn_params
    )
    cursor = conn.cursor()

    # SQL statements to create tables
    create_fact_table = """
    CREATE TABLE IF NOT EXISTS fact_df (
        track_id INTEGER PRIMARY KEY,
        type VARCHAR(50),
        traveled_d FLOAT,
        avg_speed FLOAT
    )
    """

    create_dimensional_table = """
    CREATE TABLE IF NOT EXISTS dimensional_df (
        track_id INTEGER,
        lat FLOAT,
        lon FLOAT,
        speed FLOAT,
        lon_acc FLOAT,
        lat_acc FLOAT,
        time FLOAT,
        FOREIGN KEY (track_id) REFERENCES fact_df (track_id)
    )
    """

    # Execute the SQL commands
    cursor.execute(create_fact_table)
    cursor.execute(create_dimensional_table)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def insert_data(**kwargs):
    # Define the paths to your CSV files
    fact_csv_path = '../data/fact_df.csv'  # Change this to your actual CSV path
    dimensional_csv_path = '../data/dimension_df.csv'  # Change this to your actual CSV path


    # Read the CSV files into DataFrames
    fact_df = pd.read_csv(fact_csv_path)
    dimensional_df = pd.read_csv(dimensional_csv_path)

    # Connect to the database
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    # Insert data into fact_df
    for index, row in fact_df.iterrows():
        print(index)
        cursor.execute("""
            INSERT INTO fact_df (track_id, type, traveled_d, avg_speed) 
            VALUES (%s, %s, %s, %s)
        """, (row['track_id'], row['type'], row['traveled_d'], row['avg_speed']))

    # Insert data into dimensional_df
    for index, row in dimensional_df.iterrows():
        print(index)
        cursor.execute("""
            INSERT INTO dimensional_df (track_id, lat, lon, speed, lon_acc, lat_acc, time) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row['track_id']),
            float(row['lat']),
            float(row['lon']),
            float(row['speed']),
            float(row['lon_acc']),
            float(row['lat_acc']),
            float(row['time'])
        ))

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()



default_args = {
    'owner': 'alazar',
    'start_date': datetime(2024, 10, 23),
}

with DAG('csv_to_postgres_with_db_check', default_args=default_args, schedule_interval='@daily') as dag:

    # Task 1: Load CSV
    load_and_transform_data_task = PythonOperator(
        task_id='load_csv',
        python_callable=load_and_transform_data,
        provide_context=True
    )


    # Task 2: Check database connection
    check_db_task = PythonOperator(
        task_id='check_db_connection',
        python_callable=check_db_connection,
        provide_context=True
    )

    # Task 3: Create the necessary tables
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
        provide_context=True
    )

    # Task 3: insert the necessary data
    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
        provide_context=True
    )

    # Task to run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        # bash_command=f'''
        # cd ./dbt &&  # Navigate to your dbt project directory
        # dbt compile               # Compile and run DBT
        # dbt run                   # Run dbt
        # ''',
        bash_command="""
                    source "/mnt/c/Users/alaza/Desktop/New Programming/Airflow venv/venv/bin/activate" &&  # Activate the virtual environment
                    cd "/mnt/c/Users/alaza/Desktop/New Programming/Airflow venv/dbt" &&  # Navigate to your dbt project directory
                    pwd
                    dbt compile --profiles-dir "/mnt/c/Users/alaza/Desktop/New Programming/Airflow venv/dbt" &&  # Specify the correct profiles directory
                    dbt run --profiles-dir "/mnt/c/Users/alaza/Desktop/New Programming/Airflow venv/dbt"         # Run the dbt models with the correct profiles directory
                    """
    )

    # Define task dependencies
    load_and_transform_data_task >> check_db_task >> create_tables_task >> insert_data_task >>dbt_run