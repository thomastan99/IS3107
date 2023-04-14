import os

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

########################## ALL CREDENTIALS - GBQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"


########################## METHOD TO LOAD DATA INTO GBQ ##############################
def load_model_output_into_BQ(coin) :
    client = bigquery.Client()
    table_id = f"crypto3107.model_output.{coin}"
    job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.CSV, 
      skip_leading_rows=1, 
      autodetect=True,
      write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    with open(f"assets/model_output/model_output_{coin}.csv", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    
    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )