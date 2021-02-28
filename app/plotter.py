import io
import base64

from matplotlib import rcParams
from matplotlib.figure import Figure
import matplotlib.dates as mdates

import seaborn as sns
from pandas.plotting import register_matplotlib_converters

sns.set()
sns.set_style('whitegrid')
rcParams.update({'figure.autolayout': True})
register_matplotlib_converters()


def plot(df, ticker):

    fig = Figure()
    ax = fig.subplots()
    ax.title.set_text(f'{ticker.upper()}')
    ax.set_ylabel('Short Volume')
    ax.plot(df['Date'].tolist(), df['Short Volume'].tolist())
    ax.set_xticklabels(ax.get_xticks(), rotation=45)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    bfer = b''.join(buf)
    b2 = base64.b64encode(bfer)
    img=b2.decode('utf-8')

    return img