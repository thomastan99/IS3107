import pandas as pd
from pytrends.request import TrendReq

pytrend = TrendReq()

# top 10 crypto coins search terms
# coins_dict = ['bitcoin', 'ethereum', 'tether', 'xrp', 'binance', 'cardano', 
# 'polygon', 'dogecoin', 'solana', 'polkadot']

# top 5 crypto news search terms
news_dict = {
    'nft',
    'cryptocurrency news',
    'buy cryptocurrency',
    'best cryptocurrency',
    'crypto exchange'
}

def get_search_metrics(query_dict): 
        
    pytrend.build_payload(kw_list=query_dict)

    #Referenced code: https://lazarinastoy.com/the-ultimate-guide-to-pytrends-google-trends-api-with-python/ 

    # Related Queries, returns a dictionary of dataframes
    related_queries = pytrend.related_queries()
    related_queries.values()

    #build lists dataframes
    top = list(related_queries.values())[0]['top']
    rising = list(related_queries.values())[0]['rising']

    #convert lists to dataframes

    dftop = pd.DataFrame(top)
    dfrising = pd.DataFrame(rising)

    #join two data frames
    joindfs = [dftop, dfrising]
    all_queries = pd.concat(joindfs, axis=1)

    #function to change duplicates
    cols=pd.Series(all_queries.columns)
    for dup in all_queries.columns[all_queries.columns.duplicated(keep=False)]: 
        cols[all_queries.columns.get_loc(dup)] = ([dup + '.' + str(d_idx) 
                                        if d_idx != 0 
                                        else dup 
                                        for d_idx in range(all_queries.columns.get_loc(dup).sum())]
                                        )
    all_queries.columns=cols

    #rename to proper names
    all_queries.rename({'query': 'top query', 'value': 'top query value', 'query.1': 'related query', 'value.1': 'related query value'}, axis=1, inplace=True) 

    #check your dataset
    all_queries.head(50)

    #save to json
    queries_json = all_queries.to_json()
    print("GOOGLE JSON QUERIES", queries_json)
    return queries_json

# # Get JSON of coins search terms matrics
# coins_json = get_search_metrics(coins_dict)
# print("GOOGLE COIN DATA", coins_json)

# # Get JSON of news search terms matrics
# news_json = get_search_metrics(news_dict)
# print("GOOGLE NEWS DATA", news_json)
