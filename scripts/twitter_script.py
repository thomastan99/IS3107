import tweepy
from tweepy import OAuthHandler
from tweepy import Cursor 
import sys
import requests
import json


#OAuth explanation: https://developer.twitter.com/en/docs/tutorials/authenticating-with-twitter-api-for-enterprise/authentication-method-overview
cons_key = '' #INPUT CRED#
cons_secret = '' #INPUT CRED#
acc_token = '' #INPUT CRED#
acc_secret = '' #INPUT CRED#
bear_token = '' #INPUT CRED#

# top 5 crypto coins hashtags
coins_dict = {
    '#bitcoin': '#bitcoin',
    '#ethereum': '#ethereum',
    '#tether': '#tether',
    '#binance': '#binance',
    '#xrp': '#xrp'
}

# top 5 crypto news hashtags
news_dict = {
    '#crptomarket': '#crptomarket',
    '#cryptocurrency': '#cryptocurrency',
    '#crypto': '#crypto',
    '#cryptonews': '#cryptonews',
    '#blockchain': '#blockchain'
}

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
                                            max_results=10)
        tweets_dict = tweets.json() 
        twitter_data.append({'twitter': query_dict[hashtag], 'tweets_details': tweets_dict})
    
    tweets_json = json.dumps(twitter_data, indent=4)

    return tweets_json

# Get JSON for latest 10 posts for the 5 crypto coins tweets
twitter_coins_json = get_tweets_hashtags(coins_dict)
print(twitter_coins_json)

# Get JSON for latest 10 posts for the 5 crypto coins tweets
# twitter_news_json = get_tweets_hashtags(news_dict)
# print(twitter_news_json)


# TODO : insert into database
#############################
#############################

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
