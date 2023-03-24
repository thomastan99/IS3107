import pandas as pd
import yfinance as yf
import datetime

coin_list = ['BTC-USD', 'ETH-USD', 'XRP-USD']
start_date = datetime.date.today() - datetime.timedelta(days=3*365)
end_date = datetime.date.today()

def get_data(coin):
    data = yf.download(coin, start=start_date, end=end_date, interval="1d")
    data = data.drop('Adj Close', axis=1)
    data = data.reset_index(drop=False).rename(columns={'index': 'Date'})
    data = data.reset_index(drop=True)
    return data



print(get_data(coin_list[0])) 