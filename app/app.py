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
    
    query = """
    SELECT "Symbol", 
    SUM("ShortVolume") as ShortVolume,
    SUM("TotalVolume") as TotalVolume,
    SUM("ShortVolume")::decimal / NULLIF(SUM("TotalVolume")::decimal, 0) as VolumeRatio
    FROM cnms
    WHERE DATE_TRUNC('month', "Date")::date = DATE_TRUNC('month', current_timestamp)::date

    GROUP BY 1
    ORDER BY 2 desc
    LIMIT 25
    """

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
    SELECT * 
    FROM cnms 
    WHERE "Symbol" = '{ticker.upper()}' 
    {s} 
    ORDER BY "Date" DESC 
    """

    df = pd.read_sql(query, engine)
    img = plotter.plot(df, ticker)

    return render_template('single_ticker.html',  table=df.to_html(index=False), ticker=ticker, img=img)



# if __name__ == '__main__':
app.run(debug=True)