import os

from helper_functions import push_to_gbq

api_key =  "e70b1b8c-4edb-4628-a737-2d054da37c12"
coins_lst = ['bitcoin', 'ethereum', 'xrp'] #'xrp','cardano','polygon','dogecoin','solana','polkadot']
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"


def extract_coincap_api():
    for coin in coins_lst:
        push_to_gbq(coin)
extract_coincap_api()
