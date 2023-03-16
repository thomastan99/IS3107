import datetime
import json
import os
from io import StringIO
from datetime import datetime, timedelta
import requests
import ccxt
import time
import pandas as pd
import os
import numpy as np
import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.oauth2 import service_account
from helper_functions import insert_data_into_BQ, push_to_gbq

api_key =  "e70b1b8c-4edb-4628-a737-2d054da37c12"
coins_lst = ['bitcoin', 'ethereum', 'xrp'] #'xrp','cardano','polygon','dogecoin','solana','polkadot']
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"


def extract_coincap_api():
    for coin in coins_lst:
        push_to_gbq(coin)
extract_coincap_api()
