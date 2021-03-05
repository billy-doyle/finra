import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from flask import Flask, render_template, request, jsonify

import plotter
from arg_factory import query_builder

app = Flask(__name__)
engine = create_engine('postgresql://postgres:password@127.0.0.1:5432/local')

class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.route('/')
def hiscores():

    # TODO include in jsonschema too

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

    try:
        query, col = query_builder(ticker, request.args)
    except:
        raise InvalidUsage(f'Invalid arguments {request.args.to_dict()}', status_code=400)

    pd.options.display.float_format = '{:,}'.format
    df = pd.read_sql(query, engine)
    img = plotter.plot(df, ticker, col)

    return render_template('single_ticker.html',  table=df.to_html(index=False), ticker=ticker, img=img)

# /usr/lib/postgresql/12/bin/postgres -D /var/lib/postgresql/12/main

# if __name__ == '__main__':
app.run(debug=True)