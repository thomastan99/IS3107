import os

import numpy as np
import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.oauth2 import service_account

def insert_data_into_BQ( coin_name):
    schema = [

                bigquery.SchemaField("priceUsd", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("time", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("date", "STRING", mode="NULLABLE"),

    ]

    client = bigquery.Client(location="asia-southeast1")
    table_id = f"crypto3107.coincap.{coin_name}"
    table_exists = False
    tables = client.list_tables('coincap')
    for table in tables:
        if table.table_id == coin_name:
            table_exists = True
            break

    # If the table does not exist, create it
    if not table_exists:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # API request

        print(f'Table {table.table_id} was created.')
    
    with open('data.json', 'rb') as f:
        job_config = bigquery.LoadJobConfig(source_format='NEWLINE_DELIMITED_JSON')
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()


