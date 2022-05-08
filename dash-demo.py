from dash import Dash, dcc, html, Input, Output
import plotly.express as px
from minio import Minio
import pandas as pd

MINIO_URL = 'datalake.website:9000'
BUCKET_NAME = 'test-bucket-3'
S3_FILE = 'GAZP_220401_220504.csv'
LOCAL_FILE = 'data/' + S3_FILE

client = Minio(MINIO_URL,
               access_key='tester-1',
               secret_key='testerpass',
               secure=False)
app = Dash(__name__)


def int_to_date(i):
    s = str(i)
    return f'{s[:4]}-{s[4:6]}-{s[6:]}'


client.fget_object(BUCKET_NAME, S3_FILE, LOCAL_FILE)
df = pd.read_csv(LOCAL_FILE, sep=';')
df['date'] = df['<DATE>'].map(int_to_date)

app.layout = html.Div([
    html.H1('Стоимость акций GAZP апрель 2022', style={'textAlign': 'center'}),
    dcc.Graph(id="time-series-chart"),
    html.P("Select stock:"),
    dcc.Dropdown(
        id="ticker",
        options=['<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>'],
        value="<OPEN>",
        clearable=False,
    ),
])


@app.callback(
    Output("time-series-chart", "figure"),
    Input("ticker", "value"))
def display_time_series(ticker):
    fig = px.line(df, x='date', y=ticker)
    return fig


app.run_server(debug=True)
