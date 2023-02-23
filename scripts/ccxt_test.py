from helper_functions import insert_data_into_BQ
import requests
import json
import io
import json
import os
from io import StringIO
api_key =  "e70b1b8c-4edb-4628-a737-2d054da37c12"
coins_lst = ['bitcoin', 'ethereum', 'tether', 'xrp','cardano','polygon','dogecoin','solana','polkadot']
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

for coin in coins_lst:
    url = f"http://api.coincap.io/v2/assets/{coin}/history?interval=d1"
    results = requests.request("GET", url)
    coincap_json = json.loads(results.text)
    data = coincap_json['data'] 
    with open('data.json', 'w') as f:
        for d in data:
            json.dump(d, f)
            f.write('\n')
    insert_data_into_BQ(coin)
