# import requests
# import pandas as pd


# appid = '1721470' # example appid
# cursor = '*' 

# reviews = []

# while True:
#     url = f'https://store.steampowered.com/appreviews/{appid}?json=1&cursor={cursor}'
#     response = requests.get(url)
#     data = response.json()
    
#     for review in data['reviews']:
#         reviews.append({
#             'app_id': appid, 
#             'name': data['query_summary']['app_name'],
#             'review': review['review']
#         })
        
#     cursor = data['cursor']
#     if not cursor:
#         break
        
# df = pd.DataFrame(reviews)
# print(df)
import requests
import pandas as pd

appid = '1721470'
url = f'https://store.steampowered.com/appreviews/{appid}?json=1'

response = requests.get(url)
data = response.json()

reviews_data = []
for review in data['reviews']:
    reviews_data.append({
        'appid': appid,
        'recommendationid': review['recommendationid'],
        'author_steamid': review['author']['steamid'],
        'review': review['review'],
        'voted_up': review['voted_up'],
        'votes_up': review['votes_up'],
        'votes_funny': review['votes_funny'],
        'weighted_vote_score': review['weighted_vote_score'],
        'steam_purchase': review['steam_purchase']
    
    })
    
df = pd.DataFrame(reviews_data)

print(df.head())