import io
import json
import os
from io import StringIO

import requests

from scripts.helper_functions import insert_data_into_BQ

api_key =  "e70b1b8c-4edb-4628-a737-2d054da37c12"
coins_lst = ['bitcoin', 'ethereum', 'tether', 'xrp','cardano','polygon','dogecoin','solana','polkadot']
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

def extract_coincap_api():
    for coin in coins_lst:
        url = f"http://api.coincap.io/v2/assets/{coin}/history?interval=d1"
        results = requests.request("GET", url)
        coincap_json = json.loads(results.text)
        data = coincap_json['data'] 
        with open(f'assets/coincap_{coin}_data.json', 'w') as f:
            for d in data:
                json.dump(d, f)
                f.write('\n')
        insert_data_into_BQ(coin)
