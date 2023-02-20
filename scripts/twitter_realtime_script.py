import pymongo
import tweepy
from tweepy import OAuthHandler
from tweepy import Cursor 
from tweepy.streaming import Stream
from google.cloud import bigquery
from tweepy import Stream
import sys
import requests
import json
import smtplib, ssl
import os

########################## ALL CREDENTIALS - REDDIT & BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../cred.json"

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

# top 10 crypto coins hashtags
coins_dict = {
    '#bitcoin': '#bitcoin',
    '#ethereum': '#ethereum',
    '#tether': '#tether',
    '#binance': '#binance',
    '#binance': '#xrp',
    '#cardano': '#cardano',
    '#polygon': '#polygon',
    '#dogecoin': '#dogecoin',
    '#solana': '#solana',
    '#polkadot': '#polkadot'
}

# top 5 crypto news hashtags
news_dict = {
    '#crptomarket': '#crptomarket',
    '#cryptocurrency': '#cryptocurrency',
    '#crypto': '#crypto',
    '#cryptonews': '#cryptonews',
    '#blockchain': '#blockchain'
}


########################## CLASS TO HANDLE TWEETS STREAMING ##############################

class TweetsListener(Stream):
    def __init__(self, *args, **kwargs):
        self.bq_table=kwargs['table']
        kwargs.pop('table',None)
        super(TweetsListener, self).__init__(*args, **kwargs)
        self.count=0
        
    
    def on_data(self, data):

        if self.count >= 5: 
            sys.exit("Reached 20 tweets") 
                                                    
        try:
            tweet_data = json.loads(data)

            if 'RT @' not in tweet_data['text']:
            
                try:
                    full_tweet = tweet_data['extended_tweet']['full_text']
                except Exception as e:
                    full_tweet = tweet_data['text']
            
                ticker_list = []
                for ticker, name in coins_dict.items():
                    if ticker in full_tweet.lower():
                        ticker_list.append(name)
                              
                tweet_data_filtered = {
                    "data": [
                        {
                            "created_at": tweet_data['created_at'],
                            "ticker": ticker_list,
                            "publisher": tweet_data['user']['screen_name'],
                            "text": full_tweet
                        }
                    ]
                }

                self.count += 1

                if ticker_list != []:
                    tweet_db.insert_one(tweet_data_filtered["data"][0])
                    print(tweet_data_filtered["data"][0])
            
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            return True
  
    def on_error(self, status_code):
        if status_code == 420:
            #send email when on_data disconnects the stream
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, message)
            #returning False in on_data disconnects the stream
            return False

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["crypto3107"]
tweet_db = mydb["twitter_realtime"]

bigquery_client = bigquery.Client()
dataset_ref = bigquery_client.dataset('twitter') 
table_ref = dataset_ref.table('realtime_tweets')  
table = bigquery_client.get_table(table_ref)  

auth = OAuthHandler(cons_key, cons_secret)
auth.set_access_token(acc_token, acc_secret)
 
twitter_stream = Stream(auth, TweetsListener(table=table))
twitter_stream.filter(
    track=list(coins_dict.keys()),
    languages=['en']
) 