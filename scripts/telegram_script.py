import json
from telethon import TelegramClient, events, sync

api_id = 123456 #INPUT CRED#
api_hash = '' #INPUT CRED#
client = TelegramClient('session_name', api_id, api_hash)
client.start()

# top 5 largest crypto telegram channels
crypto_dict = {
    'binanceexchange': '@binanceexchange',
    'CryptoWorldNews': '@CryptoWorldNews',
    'bitcoin_industry': '@bitcoin_industry',
    'crypto_mountains': '@crypto_mountains',
    'DeFimillion': '@DeFimillion'
}

def get_telegram_channel(channel_dict):
    channel_data = []

    for channel in channel_dict.keys(): 

        all_messages = []
        for message in client.get_messages(channel, limit=40):
            #https://docs.telethon.dev/en/stable/modules/custom.html#telethon.tl.custom.message.Message
            newMessage={'message': message.message, 'date': str(message.date), 'number_views': message.views, 'number_forwards': message.forwards}
            all_messages.append(newMessage)
        
        channel_data.append({'channel': channel_dict[channel], 'channel_messages': all_messages})

    telegram_json = json.dumps(channel_data)
    return telegram_json

# Get JSON for latest 20 messages for the 5 crypto channels
telegram_channel_json = get_telegram_channel(crypto_dict)
print("TELEGRAM COINS DATA", telegram_channel_json)


# TODO : insert into database
#############################
#############################


# JSON Structure: 
# [{
#     "channel": "@channel",
#     "channel_messages": [
#         {
#             "message": "message",
#             "date": "date", 
#             "number_views": "number_views", 
#             "number_forwards": "number_forwards" 
#         }
#     ]
# }]