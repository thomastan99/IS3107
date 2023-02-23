
import requests
api_key =  "e70b1b8c-4edb-4628-a737-2d054da37c12"
coins_lst = ['bitcoin', 'ethereum', 'tether', 'binance', 'xrp','cardano','polygon','dogecoin','solana','polkadot']
for coin in coins_lst:
    url = f"http://api.coincap.io/v2/assets/{coin}/history?interval=d1"
    results = requests.request("GET", url)
    print(results.text)