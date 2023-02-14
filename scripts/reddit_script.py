import praw
import json

client_id = 'uwjwEuIgxWDsluEe4bgAew' 
client_secret = 'x9j8g9e5MGGKvxJqnCmfQUioxS3pKA' 
user_agent = 'crypto3107' 
reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)

# top 5 crypto coins subreddit
coins_dict = {
    'bitcoin': 'r/Bitcoin',
    'ethereum': 'r/ethereum',
    'tether': 'r/Tether',
    'binance': 'r/binance',
    'xrp': 'r/XRP'
}

# top 5 crypto news subreddit
# can use for justification: https://coinbound.io/best-crypto-subreddits/ 
news_dict = {
    'cryptocurrency': 'r/CryptoCurrency',
    'cryptomarkets': 'r/CryptoMarkets',
    'bitcoinbeginners': 'r/BitcoinBeginners',
    'cryptocurrencies': 'r/CryptoCurrencies',
    'crypto_general': 'r/Crypto_General'
}

def get_reddit_posts(subreddit_dict): 
    subreddit_data = []

    for subreddit in subreddit_dict.keys(): 
        hot_posts = reddit.subreddit(subreddit).hot(limit=10) # TODO: use hot() posts? remove pinned posts?
        subreddit_details = []

        for post in hot_posts:
            # post.created if need datetime of creation
            postDetails = {'title':post.title, 'text':post.selftext, 'author':str(post.author),'number_comments':post.num_comments,'number_upvotes':post.score}
            subreddit_details.append(postDetails)
        subreddit_data.append({'subreddit': subreddit_dict[subreddit], 'subreddit_details': subreddit_details})
    
    reddit_json = json.dumps(subreddit_data)
    return reddit_json

# Get JSON for top 10 posts for the 5 crypto coins
reddit_coins_json = get_reddit_posts(coins_dict)
print(reddit_coins_json)

# Get JSON for top 10 posts for the 5 crypto news
reddit_news_json = get_reddit_posts(news_dict)
print(reddit_news_json)

# TODO : insert into database
#############################
#############################


# JSON Structure: 
# [{
#     "subreddit": "r/subreddit",
#     "subreddit_details": [
#         {
#             "title": "title",
#             "text": "text",
#             "author": "author",
#             "number_comments": "number_comments",
#             "number_upvotes": "number_upvotes"
#         }
#     ]
# }]
