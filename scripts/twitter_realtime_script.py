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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../creds/cred.json"

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

# top 10 crypto coins hashvalues
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

# top 5 crypto news hashvalues
news_dict = {
    '#crptomarket': '#crptomarket',
    '#cryptocurrency': '#cryptocurrency',
    '#crypto': '#crypto',
    '#cryptonews': '#cryptonews',
    '#blockchain': '#blockchain'
}

# rule setting for coin streaming
coin_rules = [
    {"value": "#bitcoin"},        
    {"value": "#ethereum"},
    {"value": "#tether"},
    {"value": "#binance"},
    {"value": "#xrp"},
    {"value": "#cardano"},
    {"value": "#polygon"},
    {"value": "#dogecoin"},
    {"value": "#polkadot"},
]

# rule setting for news streaming
news_rules = [
    {"value": "#crptomarket"},        
    {"value": "#cryptocurrency"},
    {"value": "#crypto"},
    {"value": "#cryptonews"},
    {"value": "#blockchain"}
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


def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete, coin_rules)
    get_stream(set)


if __name__ == "__main__":
    main()