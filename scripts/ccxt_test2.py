import ccxt
import time
import datetime
import pandas as pd

exchange = ccxt.binance()
timeframe = '1d'
start_timestamp = int((datetime.datetime.now() - datetime.timedelta(days=1095)).timestamp()) * 1000 # 3 years ago
end_timestamp = int(time.time()) * 1000 # current time
coin_list = ['BTC/USDT', 'ETH/USDT', 'USDT/USDT']

def get_data(coin):
  data = exchange.fetch_ohlcv(f'{coin}', timeframe, start_timestamp, end_timestamp)
  df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
  return df