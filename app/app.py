import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from flask import Flask, render_template, request

import plotter

app = Flask(__name__)
engine = create_engine('postgresql://postgres:password@127.0.0.1:5432/local')

@app.route('/')
def hiscores():

    date = request.args.get('date')
    if date is None:
        query = "(SELECT MAX(\"Date\")::date FROM cnms)"
        date = pd.read_sql(query, engine)['max'][0]
    
    query = f"""
    SELECT 
    "Date",
    "Symbol", 
    "ShortVolume"::decimal as "Short Volume",
    "TotalVolume"::decimal as "Total Volume",
    ROUND("ShortVolume"::decimal 
        / NULLIF("TotalVolume"::decimal, 0), 3) as "Volume Ratio"
    FROM cnms
    WHERE "Date" = '{date}'

    ORDER BY 2 desc
    LIMIT 25
    """

    pd.options.display.float_format = '{:,}'.format
    df = pd.read_sql(query, engine)
    return render_template('single_ticker.html',  table=df.to_html(index=False))


@app.route('/<ticker>')
def get_ticker(ticker):

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date is None or end_date is None:
        s = ''
    else:
        s = f"""AND "Date" BETWEEN '{start_date}' AND '{end_date}'"""

    # TODO: Sanitize for SQL injections
    query = f"""
    SELECT 
    "Date",
    "Symbol",
    "ShortVolume"::decimal as "Short Volume",
    "ShortExemptVolume"::decimal as "Short Exempt Volume",
    "TotalVolume"::decimal as "Total Volume",
    "B", "D", "N", "Q"

    FROM cnms 
    WHERE "Symbol" = '{ticker.upper()}' 
    {s} 
    ORDER BY "Date" DESC 
    """

    pd.options.display.float_format = '{:,}'.format
    df = pd.read_sql(query, engine)
    img = plotter.plot(df, ticker)

    return render_template('single_ticker.html',  table=df.to_html(index=False), ticker=ticker, img=img)

# /usr/lib/postgresql/12/bin/postgres -D /var/lib/postgresql/12/main

# if __name__ == '__main__':
app.run(debug=True)