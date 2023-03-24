import requests
import pandas as pd
from datetime import datetime, timedelta

# Set the API endpoint and parameters
url = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/chart'
params = {
    'slug': 'bitcoin',
    'range': 'y_3',
    'interval': '1d',
    'quoteId': '1'
}

# Send a GET request and retrieve the data in JSON format
response = requests.get(url, params=params)
data = response.json()
print(data)

# Extract the market cap data and store it in a pandas DataFrame
market_cap = data['data']['chart']['quotes']
dates = [datetime.utcfromtimestamp(q['time']) for q in market_cap]
values = [q['quote']['1']['market_cap'] for q in market_cap]
df = pd.DataFrame({'Date': dates, 'Market Cap': values})

# Print the resulting DataFrame
print(df)
