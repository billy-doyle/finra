from io import StringIO

import pandas as pd
import numpy as np
import requests
import pandas_market_calendars as mcal
import psycopg2
from sqlalchemy import create_engine

engine = create_engine(f'postgresql://postgres:password@127.0.0.1:5432/local')

nyse = mcal.get_calendar('NYSE')

def retrieve(link):
    """
    http://regsho.finra.org/DailyShortSaleVolumeFileLayout.pdf
    """
    
    r = requests.get(link)
    if not r.ok:
        r.raise_for_status()
        
    buf = StringIO(r.content.decode())
    df = pd.read_table(buf, sep='|', skipfooter=1, engine='python')
    
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    df['B'] = df['Market'].str.contains('B')
    df['D'] = df['Market'].str.contains('D')
    df['N'] = df['Market'].str.contains('N')
    df['Q'] = df['Market'].str.contains('Q')
    
    df.drop(columns=['Market'], inplace=True)
    columns = {
        'Date': 'date',
        'Symbol': 'symbol',
        'ShortVolume': 'short_volume',
        'ShortExemptVolume': 'short_exempt_volume',
        'TotalVolume': 'total_volume',
        'B': 'b',
        'D': 'd',
        'N': 'n',
        'Q': 'q'
        }
    df.rename(columns=columns, inplace=True)
    
    return df

def upload(df):
    df.to_sql('cnms', engine, index=False, if_exists='append', method='multi')

def main(date):
    if len(nyse.valid_days(date, date)) != 0:
        sd = date.strftime("%Y%m%d")
        df = retrieve(f'http://regsho.finra.org/CNMSshvol{sd}.txt')
        upload(df)

        return df


if __name__ == '__main__':

    l = []
    date = pd.read_sql('select max(date) from cnms', engine)['max'][0]
    while pd.Timestamp(date) <= pd.Timestamp.today().date():
        # print(date)
        date = pd.Timestamp(date) + pd.Timedelta(days=1)
        l.append(date)

    for date in l:
        main(date)