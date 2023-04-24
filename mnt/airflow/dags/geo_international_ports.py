from haversine import haversine
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account
import os

from datetime import datetime, timedelta
from math import *
import json
import logging

# Set up logging
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Define callback functions
def on_success_task(dict):
    """Log a success message when a task succeeds."""
    logger.info('Task succeeded: %s', dict)

def on_failure_task(dict):
    """Log an error message when a task fails."""
    logger.error('Task failed: %s', dict)

# Set up default arguments for the DAG
default_args = {
    'owner': 'Airflow',
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
    'emails': ['sample@email.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': on_failure_task,
    'on_success_callback': on_success_task,
    'execution_time': timedelta(seconds=60)
}

# def haversine_distance(target_lat, target_long, port_lat, port_long):
#     """
#     Calculate the haversine distance between two points on Earth
#     given their latitudes and longitudes.

#     Args:
#         target_lat (float): Latitude of the target .
#         target_long (float): Longitude of the target port.
#         port_lat (float): Latitude of the port.
#         port_long (float): Longitude of the port.

#     Returns:
#         int: Distance in meters between the two points.
#     """
#     # Convert latitude and longitude to radians
#     lat_1, long_1, lat_2, long_2 = map(radians, [target_lat, target_long, port_lat, port_long])

#     # Haversine formula
#     diff_lat = lat_2 - lat_1
#     diff_long = long_2 - long_1
#     chord_length = sin(diff_lat/2)**2 + cos(lat_1) * cos(lat_2) * sin(diff_long/2)**2
#     angular_distance = 2 * asin(sqrt(chord_length))

#     # Radius of Earth in kilometers
#     earth_radius = 6371

#     # Calculate the distance in meters
#     distance_in_meters = int((earth_radius * angular_distance) * 1000)
#     return distance_in_meters

def fetch_bigquery_data(query, **kwargs):
    """
    Fetch data from BigQuery and save it to XCom.

    Args:
        query (str): The SQL query to run on BigQuery.
    """
    bq_hook = BigQueryHook()
    international_port_df = bq_hook.get_pandas_df(query, dialect='standard')
    results = international_port_df.to_json(orient='split')
    kwargs['ti'].xcom_push(key='query_results', value=results)

def write_output_files(output_df, dataset):
    """
    Write the output DataFrame to a CSV file.
    
    Args:
        output_df (dataframe): The DataFrame containing the output data.
        dataset (str): The name of the dataset, which will be used as the filename.
    """

    # Set the output path for the CSV files
    output_path = f'/opt/airflow/results'

    # Write the output DataFrame to a CSV file in the specified output path
    output_df.to_csv(f'{output_path}/{dataset}.csv', index=False)

def write_to_bigquery(output_df, dataset):
    """
    Write the output DataFrame to a BigQuery table.
    
    Args:
        output_df (dataframe): The DataFrame containing the output data.
        dataset (str): The name of the dataset, which will be used as the filename.
    """

    # Get the path to the service account JSON file from the environment variable
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

    # Create a credentials object from the JSON file
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Set the destination table in the format 'dataset_name.table_name'
    destination_table = f'foodpandas_result.{dataset}'

    # Write the output DataFrame to the BigQuery table
    to_gbq(output_df, destination_table, credentials.project_id, if_exists='replace', credentials=credentials)



def top_n_nearest_port(port_name, country, n, **kwargs):
    """
    Find the top N nearest ports to the specified port.

    Args:
        port_name (str): The name of the reference port.
        country (str): The country code of the reference port.
        n (int): The number of nearest ports to find.
    """
    data = kwargs['ti'].xcom_pull(key='query_results', task_ids='fetch_bigquery_data')
    data_dict = json.loads(data)
    sj_port_index = data_dict['columns'].index('port_name')
    sj_country_index = data_dict['columns'].index('country')
    sj_latitude_index = data_dict['columns'].index('port_latitude')
    sj_longitude_index = data_dict['columns'].index('port_longitude')

    # Iterate over the rows in the 'data' key
    for row in data_dict['data']:
        if row[sj_port_index] == port_name and row[sj_country_index] == country:
            sj_lat = row[sj_latitude_index]
            sj_long = row[sj_longitude_index]
            break

    data_df = pd.read_json(data, orient='split')
    exclude_sj_df = data_df[~((data_df['port_name'] == port_name) & (data_df['country'] == country))]

    # both methods generate the same result
    exclude_sj_df['distance_in_meters'] = exclude_sj_df.apply(lambda row: int(haversine((sj_lat, sj_long), (row['port_latitude'], row['port_longitude'])) * 1000), axis=1)
    # exclude_sj_df['distance_in_meters'] = exclude_sj_df.apply(lambda row: haversine_distance(sj_lat, sj_long, row['port_latitude'], row['port_longitude']), axis=1)

    exclude_sj_df.sort_values(by='distance_in_meters', inplace=True)
    top_n_nearest_port = exclude_sj_df[['port_name', 'distance_in_meters']].iloc[:n]
    dataset = '1_5_nearest_ports'

    # 2 output options: the outcomes avaliable in both table and results folder
    write_to_bigquery(top_n_nearest_port, dataset)
    write_output_files(top_n_nearest_port, dataset)


def largest_number_of_port(n, **kwargs):
    """
    Find the countries with the largest number of ports and cargo_wharf.

    Args:
        n (int): The number of countries to find.
    """
    data = kwargs['ti'].xcom_pull(key='query_results', task_ids='fetch_bigquery_data')
    data_df = pd.read_json(data, orient='split')
    with_wharf_df = data_df[data_df['cargo_wharf'] == True]
    port_count_by_country = with_wharf_df.groupby('country').size().reset_index(name='port_count')
    port_count_by_country.sort_values(by='port_count', ascending=False, inplace=True)
    largest_number_of_port = port_count_by_country[['country', 'port_count']].iloc[:n]
    dataset = '2_largest_number_of_port'

    # 2 output options: the outcomes avaliable in both bigquery table and results folder
    write_to_bigquery(largest_number_of_port, dataset)
    write_output_files(largest_number_of_port, dataset)

def nearest_port(target_lat, target_long, n, **kwargs):
    """
    Find the nearest ports with all needed provisions.

    Args:
        target_lat (float): The latitude of the target location.
        target_long (float): The longitude of the target location.
        n (int): The number of nearest ports to find.
    """
    data = kwargs['ti'].xcom_pull(key='query_results', task_ids='fetch_bigquery_data')
    data_df = pd.read_json(data, orient='split')
    port_with_all_needed = data_df[(data_df['provisions'] == True) & (data_df['water'] == True) & (data_df['fuel_oil'] == True) & (data_df['diesel'] == True)]

    # both methods generate the same result
    port_with_all_needed['distance_in_meters'] = port_with_all_needed.apply(lambda row: haversine((target_lat, target_long), (row['port_latitude'], row['port_longitude'])), axis=1)
    # port_with_all_needed['distance_in_meters'] = port_with_all_needed.apply(lambda row: haversine_distance(target_lat, target_long, row['port_latitude'], row['port_longitude']), axis=1)

    port_with_all_needed.sort_values(by='distance_in_meters', inplace=True)
    nearest_port = port_with_all_needed[['country', 'port_name', 'port_latitude', 'port_longitude']].iloc[:n]
    dataset = '3_nearest_port'

    # 2 outputs option: the outcomes avaliable in both bigquery table and results folder
    write_to_bigquery(nearest_port, dataset)
    write_output_files(nearest_port, dataset)


# Define the DAG
with DAG("geo_international_ports", start_date=datetime(2023, 4, 23), schedule_interval=timedelta(days=1), default_args=default_args, catchup=False) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_bigquery_data',
        python_callable=fetch_bigquery_data,
        op_args=['SELECT * FROM `bigquery-public-data.geo_international_ports.world_port_index`'],
        provide_context=True,
        dag=dag
    )

    top_n_nearest_port_task = PythonOperator(
        task_id='top_n_nearest_port',
        python_callable=top_n_nearest_port,
        op_args=['JURONG ISLAND', 'SG', 5],
        provide_context=True,
        dag=dag
    )

    largest_number_of_port_task = PythonOperator(
        task_id='largest_number_of_port',
        python_callable=largest_number_of_port,
        op_args=[1],
        provide_context=True,
        dag=dag
    )

    nearest_port_task = PythonOperator(
        task_id='nearest_port',
        python_callable=nearest_port,
        op_args=[32.610982, -38.706256, 1],
        provide_context=True,
        dag=dag
    )

    # Set task dependencies
    fetch_data_task >> top_n_nearest_port_task
    fetch_data_task >> largest_number_of_port_task
    fetch_data_task >> nearest_port_task

    