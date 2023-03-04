import json
import os
import smtplib
import ssl
import sys

import requests
import tweepy
from google.cloud import bigquery
from tweepy import Cursor, OAuthHandler, Stream
from tweepy.streaming import Stream

########################## ALL CREDENTIALS - REDDIT & BQ ##############################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./creds/cred.json"

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
# rule setting for news and coin streaming
coins_news_rules = [
    {"value": "#bitcoin", "tag": "#bitcoin coin"},        
    {"value": "#ethereum", "tag": "#ethereum coin"},
    {"value": "#tether", "tag": "#tether coin"},
    {"value": "#binance", "tag": "#binance coin"},
    {"value": "#xrp", "tag": "#xrp coin"},
    {"value": "#cardano", "tag": "#cardano coin"},
    {"value": "#polygon", "tag": "#polygon coin"},
    {"value": "#dogecoin", "tag": "#dogecoin coin"},
    {"value": "#polkadot", "tag": "#polkadot coin"},
    {"value": "#crptomarket", "tag": "#crptomarket news"},        
    {"value": "#cryptocurrency", "tag": "#cryptocurrency news"},
    {"value": "#crypto", "tag": "#crypto news"},
    {"value": "#cryptonews", "tag": "#cryptonews news"},
    {"value": "#blockchain", "tag": "#blockchain news"}
]

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bear_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete, rules):
    # You can adjust the rules if needed
    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            print(json.dumps(json_response, indent=4, sort_keys=True))


def extract_twitter_realtime_coin_news_data():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete, coins_news_rules)
    get_stream(set)
    
