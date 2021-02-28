import sys

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

import scraper

# python3 initialize.py local '2020-01-01' '2021-02-26'

_, database_name, s, e = sys.argv

conn = psycopg2.connect(
   database=database_name, 
   user='postgres', 
   password='password', 
   host='127.0.0.1', 
   port= '5432'
)
conn.autocommit = True

cursor = conn.cursor()

cursor.execute("SELECT datname FROM pg_database;")
list_database = cursor.fetchall()
if (database_name,) in list_database:
    pass
else:
    cursor.execute(f"CREATE database {database_name};")

query = """

CREATE TABLE IF NOT EXISTS "cnms" (
    date date,
    symbol TEXT,
    short_volume real,
    short_exempt_volume real,
    total_volume real,
    B BOOLEAN,
    D BOOLEAN,
    N BOOLEAN,
    Q BOOLEAN
)
"""

cursor.execute(query)
cursor.execute("CREATE INDEX IF NOT EXISTS idx_cnms_date ON cnms(date);")

# conn.close()

l = []
for date in pd.date_range(s, e):
    df = scraper.main(date)
    l.append(df)
    
df = pd.concat(l)

engine = create_engine(f'postgresql://postgres:password@127.0.0.1:5432/{database_name}')
df.to_sql('cnms', engine, index=False, if_exists='append', method='multi', chunksize=10000)