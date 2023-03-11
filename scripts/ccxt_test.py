import io
import json
import os
from io import StringIO
import datetime
import requests

from helper_functions import insert_data_into_BQ

api_key =  "e70b1b8c-4edb-4628-a737-2d054da37c12"
coins_lst = ['bitcoin', 'ethereum', 'tether'] #'xrp','cardano','polygon','dogecoin','solana','polkadot']
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

def extract_coincap_api():
    start_date = datetime.datetime.now() - datetime.timedelta(days=3*365)
    end_date = datetime.datetime.now()
    start_timestamp = int(start_date.timestamp() * 1000)
    end_timestamp = int(end_date.timestamp() * 1000)
    for coin in coins_lst:
        url = f"https://api.coincap.io/v2/assets/{coin}/history?interval=d1&start={start_timestamp}&end={end_timestamp}&apiKey={api_key}"
        results = requests.request("GET", url)
        coincap_json = json.loads(results.text)
        data = coincap_json['data'] 
        with open(f'assets/coincap_{coin}_data.json', 'w') as f:
            for d in data:
                json.dump(d, f)
                f.write('\n')
        insert_data_into_BQ(coin)


extract_coincap_api()
