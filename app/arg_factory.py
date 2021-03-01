import pandas as pd

# d = {
#     'start_date': '2021-02-01',
#     'end_date': '2021-02-26',
#     'columns': ['short_volume', 'short_exempt_volume', 'total_volume'],
#     'order_by': ['date']
# }

def query_builder(ticker, d):

    clst = ['short_volume', 'short_exempt_volume', 'total_volume']
    olst = ['date', 'short_volume', 'short_exempt_volume', 'total_volume']

    print(d)
    for k, v in d.items():
        if k in ['start_date', 'end_date']:
            try:
                pd.Timestamp(v)
            except:
                raise RuntimeError
        if k == 'columns':
            
            if v is None:
                d['columns'] = clst
            if not all([i in clst for i in v.split(',')]):
                raise RuntimeError
        if k == 'order_by':
            
            if v is None:
                d['order_by'] = 'date'
            if not all([i in olst for i in [v]]):
                raise RuntimeError

    start_date, end_date = d.get('start_date'), d.get('end_date')
    if start_date is not None and end_date is not None:
        period = f"AND date BETWEEN '{start_date}' AND '{end_date}'"
    else:
        period = ''

    columns = d.get('columns')
    if columns is None:
        columns = ','.join(clst)

    order_by = d.get('order_by')
    if order_by is None:
        order_by = 'date'

    columns = ','.join([columns])
    order_by = ','.join([order_by])

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