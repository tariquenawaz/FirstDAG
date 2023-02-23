# We'll start by importing the DAG object
from datetime import timedelta

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

import pandas as pd
import sqlite3
import os

# get dag directory path
dag_path = os.getcwd()


def transform_data():
    booking = pd.read_csv(f"{dag_path}/raw_data/booking.csv", low_memory=False)
    client = pd.read_csv(f"{dag_path}/raw_data/client.csv", low_memory=False)
    hotel = pd.read_csv(f"{dag_path}/raw_data/hotel.csv", low_memory=False)

    # merge booking with client
    data = pd.merge(booking, client, on='client_id')
    data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)

    # merge booking, client & hotel
    data = pd.merge(data, hotel, on='hotel_id')
    data.rename(columns={'name': 'hotel_name'}, inplace=True)

    # make date format consistent
    data.booking_date = pd.to_datetime(data.booking_date, infer_datetime_format=True)

    # make all cost in GBP currency
    data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
    data.currency.replace("EUR", "GBP", inplace=True)
    # The loc method is used to select rows of the data DataFrame where the currency column equals 'EUR'.
    # The second argument ['booking_cost'] specifies the columns to select, which in this case is just the 'booking_cost' column.

    # remove unnecessary columns
    data = data.drop('address', 1)

    # load processed data
    data.to_csv(f"{dag_path}/processed_data/processed_data.csv", index=False)


def load_data():
    conn = sqlite3.connect("/usr/local/airflow/db/datascience.db")
    c = conn.cursor()
    c.execute('''
                CREATE TABLE IF NOT EXISTS booking_record (
                    client_id INTEGER NOT NULL,
                    booking_date TEXT NOT NULL,
                    room_type TEXT(512) NOT NULL,
                    hotel_id INTEGER NOT NULL,
                    booking_cost NUMERIC,
                    currency TEXT,
                    age INTEGER,
                    client_name TEXT(512),
                    client_type TEXT(512),
                    hotel_name TEXT(512)
                );
             ''')
    records = pd.read_csv(f"{dag_path}/processed_data/processed_data.csv")
    # Extacting data from processed_data.csv to a dataframe named as records
    
    records.to_sql('booking_record', conn, if_exists='replace', index=False)
    
    # Here, to_sql() is a pandas method that allows you to write the contents of a dataframe to a SQL database table.
    # The first argument is the name of the table to write to (booking_record), the second argument is the database connection object (conn),
    # if_exists argument specifies what to do if the table already exists (here we replace it), and index=False specifies that we don't want to write the dataframe index as a column in the table.


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'booking_ingestion',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(days=1),   # will run everyday at midnigt
    catchup=False                          # if true DAG will run from past 5 days
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag,
)

# An operator is a class that represents a single task in a workflow.
# Operators are used to define what actions are taken when a task is executed,
# such as running a Python function, executing a SQL query, or transferring data between systems.

task_1 >> task_2
