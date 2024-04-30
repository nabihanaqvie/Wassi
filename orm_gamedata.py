import requests
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define SQLAlchemy Base
Base = declarative_base()

# Define GameData ORM class
class GameData(Base):
    __tablename__ = 'gamedata'

    appid = Column(Integer, primary_key=True)
    name = Column(String(255))

# Make the API call
url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
response = requests.get(url)
data = response.json()

# Extract data
apps = data["applist"]["apps"]

# Connect to PostgreSQL database using SQLAlchemy
engine = create_engine('postgresql://postgres:wassiwassi@wassi.chac8sw84qup.us-east-1.rds.amazonaws.com:5432/wassi') 
Session = sessionmaker(bind=engine)
session = Session()

try:
    # Create table if not exists
    Base.metadata.create_all(engine)

    # Insert data into the table
    for app in apps:
        appid = app['appid']
        name = app['name']
        game_data = GameData(appid=appid, name=name)
        session.merge(game_data)  # Merge to handle duplicates

    session.commit()
    print("Data inserted successfully!")
except Exception as e:
    print("Error:", e)
    session.rollback()  # Rollback the transaction in case of error
finally:
    session.close()
