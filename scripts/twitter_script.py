import tweepy
from tweepy import OAuthHandler
from tweepy import Cursor 
import sys
import requests
import json
import os

########################## ALL CREDENTIALS - REDDIT & BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../cred.json"

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

########################## METHOD TO EXTRACT DATA FROM TWITTER ##############################

def get_twitter_client():

    try:
        consumer_key = cons_key
        consumer_secret = cons_secret
        access_token = acc_token
        access_secret = acc_secret
        bearer_token = bear_token
        
    except KeyError:
        sys.stderr.write("Twitter Environment Variable not Set\n")
        sys.exit(1)
        
    client = tweepy.Client(bearer_token=bearer_token, 
                        consumer_key=consumer_key, 
                        consumer_secret=consumer_secret, 
                        access_token=access_token, 
                        access_token_secret=access_secret, 
                        return_type = requests.Response,
                        wait_on_rate_limit=True)

    return client

########################## METHOD TO EXTRACT DATA FROM TWITTER ##############################

# get recent tweets via search query terms, return [10-100] results
def get_tweets_hashtags(query_dict): 
    client = get_twitter_client()
    twitter_data =[]
    for hashtag in query_dict.keys():
        # tweet_fields: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet 
        # Other types of fields: https://docs.tweepy.org/en/stable/expansions_and_fields.html#tweet-fields
        tweets = client.search_recent_tweets(query=hashtag, 
                                            tweet_fields = ['created_at','text', 'public_metrics'],
                                            expansions='entities.mentions.username',
                                            user_fields = 'public_metrics',
                                            max_results=100)
        tweets_dict = tweets.json() 
        twitter_data.append({'twitter': query_dict[hashtag], 'tweets_details': tweets_dict})
    
    tweets_json = json.dumps(twitter_data, indent=4)

    return tweets_json

########################## METHOD TO UPLOAD DATA TO BQ ##############################

########################## EXTRACT AND UPLOAD DATA ##############################

# Get JSON for latest 100 tweets for the 10 crypto coins hashtags
twitter_coins_json = get_tweets_hashtags(coins_dict)
print("TWITTER COIN DATA", twitter_coins_json)

# Get JSON for latest 100 tweets for the 5 crypto news hashtags 
twitter_news_json = get_tweets_hashtags(news_dict)
print("TWITTER NEWS DATA", twitter_news_json)

########################################################
# JSON Structure: 
# [{
#     "twitter": "#hashtag",
#     "tweets": {
#         "data" : [{
#             "created_at":"date, time",
#             "text": "text",
#             "public_metrics": {
#                 "retweet_count": "",
#                 "reply_count": "",
#                 "like_count": "",
#                 "quote_count": "",
#                 "impression_count":""
#             } 
#         }],
#         "includes": [{
#             "names": "",
#             "id": "",
#             "username":"",
#             "public_metrics": {
#                 "followers_count":"",
#                 "following_count":"",
#                 "tweet_count":"",
#                 "listed_count":""
#             }
#         }]
#     }
# }]
########################################################