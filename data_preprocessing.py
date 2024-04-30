import psycopg2
import pandas as pd

# Connect to the AWS RDS instance
conn = psycopg2.connect(
    dbname='wassi',
    user='postgres',
    password='wassiwassi',
    host='wassi.chac8sw84qup.us-east-1.rds.amazonaws.com',
    port='5432'
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Example: Execute a query to fetch data from a table
cur.execute("SELECT * FROM gamedata")
rows = cur.fetchall()


# Close communication with the PostgreSQL database server
cur.close()
conn.close()

# Convert the fetched data into a Pandas DataFrame
df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

# Print the DataFrame
print(df)
