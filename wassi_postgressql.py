import requests
import psycopg2

# Make the API call
url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
response = requests.get(url)
data = response.json()

# Extract data
apps = data["applist"]["apps"]

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="wassiwassi",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

try:
    # Create table if not exists
    cur.execute('''CREATE TABLE IF NOT EXISTS gamedata
                (appid INT PRIMARY KEY, 
                name VARCHAR(255))''')
    conn.commit()

    # Insert data into the table
    for app in apps:
        appid = app['appid']
        name = app['name'].replace("'", "''")  # Escape single quotes
        try:
            cur.execute(f"INSERT INTO gamedata (appid, name) VALUES ({appid}, '{name}')")
        except psycopg2.IntegrityError:
            # Ignore duplicates
            conn.rollback()  # Rollback the transaction in case of error
            continue
    
    conn.commit()
    print("Data inserted successfully!")
except Exception as e:
    print("Error:", e)
    conn.rollback()  # Rollback the transaction in case of error
# Close the cursor and connection
cur.close()
conn.close()
