import pandas as pd
from sqlalchemy import create_engine
from steam import Steam
from decouple import config
import requests
import psycopg2

# # Connect to the PostgreSQL database
# engine = create_engine('postgresql://postgres:wassi@localhost:5432/wassi') 

# # Extract the game_data table into a Pandas DataFrame
# df = pd.read_sql_table('gamedata', engine)

# Print the DataFrame 
# print(df)


# KEY = config("STEAM_API_KEY")
# steam = Steam(KEY)

# print(steam.users.search_user("the12thchairman"))

#initialize all the important things that we need aka global variables 
cursor = '*'
appid = '1721470'
new_df = pd.DataFrame()
count = 0


#while loop 10 times 
while cursor and count <= 10:
    url = f'https://store.steampowered.com/appreviews/1721470?json=1&purchase_type=all&day_range=365&num_per_page=100&cursor={cursor}'
    response = requests.get(url)
    data = response.json()

    

    reviews_data = []
    for review in data['reviews']:
        reviews_data.append({
            'appid': appid,
            'recommendationid': review['recommendationid'],
            'steamid': review['author']['steamid'],
            'review': review['review'],
            'voted_up': review['voted_up'],
            'votes_up': review['votes_up'],
            'votes_funny': review['votes_funny'],
            'weighted_vote_score': review['weighted_vote_score'],
            'steam_purchase': review['steam_purchase'],
            'num_reviews': review['author']['num_reviews'],
            'num_games_owned': review['author']['num_games_owned'],
            'playtime_forever': review['author']['playtime_forever'],
            'playtime_last_two_weeks': review['author']['playtime_last_two_weeks'],
            'playtime_at_review': review['author']['playtime_at_review'],
            'last_played': review['author']['last_played'],
            'timestamp_created': review['timestamp_created'],
            'timestamp_updated': review['timestamp_updated'],
            'comment_count': review['comment_count'],
            'steam_purchase': review['steam_purchase'],
            'received_for_free': review['received_for_free'],
            'written_during_early_access': review['written_during_early_access'],
            'hidden_steam_china': review['hidden_in_steam_china'],
            'steam_china_location': review['steam_china_location']
        
        })
    
    dfr = pd.DataFrame(reviews_data)
    new_df = pd.concat([new_df, dfr], ignore_index=True)

    cursor = data.get('cursor')
    count += 1

    if cursor:
        url = f'https://store.steampowered.com/appreviews/1721470?json=1&purchase_type=all&day_range=365&num_per_page=100&cursor={cursor}'

print(new_df)


#create author and recs table 
## author table author_steamid, number of reviews, number of games owned, playtime forever, playtime last two weeks and playtime at review and last played 

## connecting to the wassi database 

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="wassi",
    user="postgres",
    password="wassi",
    host="localhost",
    port="5432"
)

engine = create_engine('postgresql://postgres:wassi@localhost:5432/wassi') 

print("Connected")

with conn.cursor() as cur:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS author (
    steamid VARCHAR(255) PRIMARY KEY,
    num_reviews INT,
    num_games_owned INT,
    playtime_forever INT,
    playtime_last_two_weeks INT,
    playtime_at_review INT, 
    last_played INT
    )
""")



with conn.cursor() as cur:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS recs (
    recommendationid VARCHAR(255) PRIMARY KEY,
    appid VARCHAR(255),
    voted_up BOOLEAN,
    votes_up INT,
    votes_funny INT,
    review TEXT,
    weighted_vote_score INT, 
    timestamp_created INT,
    timestamp_updated INT,
    comment_count INT,
    steam_purchase BOOLEAN,
    received_for_free BOOLEAN,
    written_during_early_access BOOLEAN,
    hidden_steam_china BOOLEAN,
    steam_china_location VARCHAR(255),
    steamid VARCHAR (255)
    )
"""
)

# Loop through the DataFrame and insert data into the tables
for index, row in new_df.iterrows():
    row['weighted_vote_score'] = int(float(row['weighted_vote_score']))
    # Insert data into the 'author' table
    author_data = {
        'steamid': row['steamid'],
        'num_reviews': row['num_reviews'],
        'num_games_owned': row['num_games_owned'],
        'playtime_forever': row['playtime_forever'],
        'playtime_last_two_weeks': row['playtime_last_two_weeks'],
        'playtime_at_review': row['playtime_at_review'],
        'last_played': row['last_played']
    }
    author_insert_query = """
        INSERT INTO author 
        (steamid, num_reviews, num_games_owned, playtime_forever, playtime_last_two_weeks, playtime_at_review, last_played) 
        VALUES (%(steamid)s, %(num_reviews)s, %(num_games_owned)s, %(playtime_forever)s, %(playtime_last_two_weeks)s, %(playtime_at_review)s, %(last_played)s)
        ON CONFLICT (steamid) DO UPDATE 
        SET num_reviews = EXCLUDED.num_reviews,
            num_games_owned = EXCLUDED.num_games_owned,
            playtime_forever = EXCLUDED.playtime_forever,
            playtime_last_two_weeks = EXCLUDED.playtime_last_two_weeks,
            playtime_at_review = EXCLUDED.playtime_at_review,
            last_played = EXCLUDED.last_played
    """
    with conn.cursor() as cur:
        cur.execute(author_insert_query, author_data)
    conn.commit()


    # Insert data into the 'recs' table
    recs_data = {
        'recommendationid': row['recommendationid'],
        'appid': row['appid'],
        'voted_up': row['voted_up'],
        'votes_up': row['votes_up'],
        'votes_funny': row['votes_funny'],
        'review': row['review'],
        'weighted_vote_score': row['weighted_vote_score'],
        'timestamp_created': row['timestamp_created'],
        'timestamp_updated': row['timestamp_updated'],
        'comment_count': row['comment_count'],
        'steam_purchase': row['steam_purchase'],
        'received_for_free': row['received_for_free'],
        'written_during_early_access': row['written_during_early_access'],
        'hidden_steam_china': row['hidden_steam_china'],
        'steam_china_location': row['steam_china_location'],
        'steamid': row['steamid']
    }

    recs_insert_query = """
        INSERT INTO recs 
        (recommendationid, appid, voted_up, votes_up, votes_funny, review, weighted_vote_score, timestamp_created, timestamp_updated, comment_count, steam_purchase, received_for_free, written_during_early_access, hidden_steam_china, steam_china_location, steamid) 
        VALUES (%(recommendationid)s, %(appid)s, %(voted_up)s, %(votes_up)s, %(votes_funny)s, %(review)s, %(weighted_vote_score)s, %(timestamp_created)s, %(timestamp_updated)s, %(comment_count)s, %(steam_purchase)s, %(received_for_free)s, %(written_during_early_access)s, %(hidden_steam_china)s, %(steam_china_location)s, %(steamid)s)
        ON CONFLICT (recommendationid) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(recs_insert_query, recs_data)
    conn.commit()

# Close the connection
conn.close()


# conn.commit()
# conn.close()