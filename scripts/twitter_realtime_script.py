import io
import json
import os
from io import StringIO

import requests
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

########################## ALL CREDENTIALS - REDDIT & BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

# email for disconnected stream
port = 465  
smtp_server = "smtp.gmail.com"
sender_email = "nus3107crypto@gmail.com"  
receiver_email = "nus3107crypto@gmail.com"  
password = "crypto12345"
message = """\
Subject: Twitter stream disconnected
Twitter stream has disconnected. Please reconnect."""

#OAuth explanation: https://developer.twitter.com/en/docs/tutorials/authenticating-with-twitter-api-for-enterprise/authentication-method-overview
cons_key = 'OrzYgeOiH3pAq1FzMj8ztxWKP' #INPUT CRED#
cons_secret = 'qPhRpzLu6nT2zbQGnW0ScDYYNVf4806cuzMae7EqD0AwZ9hFCM' #INPUT CRED#
acc_token = '130006594-9xx8NEPyImg7mTTYXvrXTPLu2Xxril614Mte7uWg' #INPUT CRED#
acc_secret = 'h9heI3YWGW0jtPiuLeI8EiCajmpX0H1Qg7RevgSnW07be' #INPUT CRED#
bear_token = "AAAAAAAAAAAAAAAAAAAAAAWMhAEAAAAAr9dE5XiFdQe1pWfLp7c65v3%2FuCM%3DSlCdglcfbIFnJLrLaKLy5NAJGXwKu2aRJ5JoMPGqQSoVXeCmpI" #INPUT CRED#

########################## GLOBAL VARIABLES ##############################
# rule setting for news and coin streaming
coins_news_rules = [
    {"value": "#bitcoin", "tag": "#bitcoin coin"},        
    {"value": "#ethereum", "tag": "#ethereum coin"},
    {"value": "#tether", "tag": "#tether coin"},
    {"value": "#crptomarket", "tag": "#crptomarket news"},        
    {"value": "#cryptocurrency", "tag": "#cryptocurrency news"},
    {"value": "#crypto", "tag": "#crypto news"},
    {"value": "#cryptonews", "tag": "#cryptonews news"},
    {"value": "#blockchain", "tag": "#blockchain news"}
]

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bear_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete, rules):
    # You can adjust the rules if needed
    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def insert_data_into_BQ(data_as_file) :
    schema = [
        bigquery.SchemaField("author_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "matching_rules",
            "RECORD",
            mode = "REPEATED",
            fields=[
                bigquery.SchemaField("id", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("tag", "STRING", mode="NULLABLE"),
            ],
        )
    ]

    client = bigquery.Client()
    table_id = "crypto3107.twitter.realtime_tweets"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    load_job = client.load_table_from_file(
        data_as_file,
        table_id,
        location="asia-southeast1",
        job_config=job_config,
    ) 

    try:
        load_job.result()
        print("Successfully uploaded data to BigQuery!")
    except BadRequest:
        for error in load_job.errors:
            print(error["message"])
    
    destination_table = client.get_table(table_id)

def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=author_id,created_at,text", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            data = json_response.get("data")
            matching_rules = json_response.get("matching_rules")
            
            # Setting structure of data to be inserted into BQ
            structured_response = [{
                'author_id': data.get('author_id'),
                'created_at': data.get('created_at'),
                'id': data.get('id'),
                'text': data.get('text'),
                'matching_rules': matching_rules
            }]
            
            # Formatting and manipulating data to a format that can be inserted into BQ
            streaming_json = StringIO(json.dumps(structured_response, indent=4, sort_keys=True))        
            streaming_result = [json.dumps(record) for record in json.load(streaming_json)] 
            streaming_formatted_json = '\n'.join(streaming_result)
            data_as_file = io.StringIO(streaming_formatted_json) # Put ndjson into file 
            
            # Insert into BQ
            insert_data_into_BQ(data_as_file)
            print(f"SUCCESSFULLY INSERTED TWITTER STREAMING RECORD ID {data.get('id')} INTO BQ")
            
def extract_twitter_realtime_coin_news_data():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete, coins_news_rules)
    get_stream(set)
    
