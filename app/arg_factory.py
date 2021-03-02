import itertools
import jsonschema
import pandas as pd

def possible_combos(lst):
    l = []
    for i in range(1, len(lst)+1):
        for combo in itertools.permutations(lst, i):
            l.append(','.join(combo))
    return l

def check_json(d, columns, order_by):

    schema = {
        "type": "object",
        "properties": {
            "start_date": {
                "type": "string",
                "format": "date"
            },
            "end_date": {
                "type": "string",
                "format": "date"
            },
            "columns": {
                "type": "string",
                "enum": 
                # TODO: There must be a better way than enumerating all
                # possible permutations for strings
                    possible_combos(columns)
            },
            "order_by": {
                "type": "string",
                "enum": 
                    possible_combos(order_by)
            }
        }
    }

    jsonschema.validate(d, schema, format_checker=jsonschema.FormatChecker())

def query_builder(ticker, d):

    clst = ['short_volume', 'short_exempt_volume', 'total_volume']
    olst = ['date'] + clst

    check_json(d, clst, olst)

    start_date, end_date = d.get('start_date'), d.get('end_date')
    if start_date is not None and end_date is not None:
        period = f"AND date BETWEEN '{start_date}' AND '{end_date}'"
    if start_date is not None and end_date is None:
        period = f"AND date >= '{start_date}'"
    if start_date is None and end_date is not None:
        period = f"AND date <= '{end_date}'"
    else:
        period = ''

    columns = d.get('columns') if d.get('columns') is not None else ','.join(clst)
    order_by = d.get('order_by') if d.get('order_by') is not None else 'date'

    col = columns.split(',')[0]

    query = f"""
    SELECT date,
    symbol,
    {columns}

    FROM cnms

    WHERE symbol = '{ticker.upper()}'

    {period}

    ORDER BY {order_by} DESC

    """

    return query, col