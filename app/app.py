import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from flask import Flask, render_template, request

import plotter
from arg_factory import query_builder

app = Flask(__name__)
engine = create_engine('postgresql://postgres:password@127.0.0.1:5432/local')

@app.route('/')
def hiscores():

    date = request.args.get('date')
    if date is None:
        query = "(SELECT MAX(date) FROM cnms)"
        date = pd.read_sql(query, engine)['max'][0]
    
    query = f"""
    SELECT 
    date,
    symbol, 
    short_volume,
    total_volume,
    ROUND((short_volume / NULLIF(total_volume, 0))::numeric, 2) as "Volume Ratio"
    FROM cnms
    WHERE date = '{date}'

    ORDER BY 3 desc
    LIMIT 25
    """

    pd.options.display.float_format = '{:,}'.format
    df = pd.read_sql(query, engine)
    return render_template('single_ticker.html',  table=df.to_html(index=False))


@app.route('/<ticker>')
def get_ticker(ticker):

    query, col = query_builder(ticker, request.args)

    pd.options.display.float_format = '{:,}'.format
    df = pd.read_sql(query, engine)
    img = plotter.plot(df, ticker, col)

    return render_template('single_ticker.html',  table=df.to_html(index=False), ticker=ticker, img=img)

# /usr/lib/postgresql/12/bin/postgres -D /var/lib/postgresql/12/main

# if __name__ == '__main__':
app.run(debug=True)