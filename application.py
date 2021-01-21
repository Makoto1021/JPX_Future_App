import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output
import pandas as pd
import os
import os
import datetime
from datetime import datetime as dt, timedelta, date
# from datetime import timedelta
from datetime import date
import plotly.graph_objects as go
import plotly.figure_factory as ff
import numpy as np
import boto3
from to_csv_on_s3 import to_csv_on_s3

# reading s3 bucket
client = boto3.client('s3')
path='s3://jpx-future-bucket/完成データ/先物2021-1-15.xlsx'

today = dt.today().replace(microsecond=0, second=0, minute=0, hour=0)
today = date(today.year, today.month, today.day)

dirname = os.getcwd()
# filename = '完成データ/' + '先物' + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".xlsx"
filename = '完成データ/先物2021-1-15.xlsx'
SAVED_DATA = os.path.join(dirname, filename)
# df = pd.read_excel(SAVED_DATA, header=1)
df = pd.read_excel(path, header=1)
# temp
# to_csv_on_s3(dataframe=df, path = 's3://jpx-future-bucket/元データ/', filename="test.csv")
df.to_csv('s3://jpx-future-bucket/元データ/test.csv')
# temp end

df_new = df.drop(["差引", "差引.1", "差引.2"], axis=1)
df_new = df_new[(df_new["売り買い"] != "総合計")&(df_new["売り買い"] != "取引参加者")]
x = ["第１限月　売り", "第１限月　買い", "第２限月　売り", "第２限月　買い", "第３限月　売り", "第３限月　買い",]
data_list = [{'x': x, 'y': np.delete(df_new[df_new["売り買い"]==i].values, 0), 'type': 'bar', 'name': i} for i in df_new["売り買い"].unique()]


app = dash.Dash(__name__)
application = app.server
app.title='Visualizing JPX Future Trading'

colors = {
    'background': '#111000',
    'text': '#7FDBFF'
}

data_bar = [go.Bar(name=i, x=x, y=np.delete(df_new[df_new["売り買い"]==i].values, 0)) for i in df_new["売り買い"].unique()]
fig = go.Figure(data=data_bar)

fig.update_layout(
    barmode='group', 
    plot_bgcolor= colors['background'], 
    paper_bgcolor= colors['background'], 
    font={'color': colors['text']},
    legend_title_text = "Institutions"
    )
fig.update_yaxes(title_text="JPY")

#######################
## necessary functions 
#######################
def has_seconds(a_string):
    a_string.split(".")[0]
    return "." in a_string.split(":")[2]

#######################
## app layout 
#######################
app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Nikkei 225 Futures',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='Created by DataZamurai', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    dcc.Graph(id="Graph1", figure=fig),
    
    html.Div(children='Table', style={
        'textAlign': 'center',
        'color': colors['text']
    }),
    
    html.Div([
        dcc.DatePickerSingle(
            id='my-date-picker-single',
            date=today
            ),
            html.Div(id='output-container-date-picker-single')
            ]),
    
    html.Div([dash_table.DataTable(
            id='jpx_future_table',
            columns=[{"name": i, "id": i}  for i in df.columns],
            data=df.to_dict('records'),
            style_cell=dict(textAlign='left'),
            style_header=dict(backgroundColor="paleturquoise"),
            style_data=dict(backgroundColor="lavender")
            )]
            )
    ]
)

@app.callback(
    Output('Graph1', 'figure'),
    Input('my-date-picker-single', 'date'))
def update_bar_chart(date_value):
    if date_value is not None:
        date_string = dt.strptime(date_value, "%Y-%m-%d")
        date_formatted = str(date_string.year) +"-"+ str(date_string.month) +"-"+ str(date_string.day)
        path = path='s3://jpx-future-bucket/完成データ/先物' + date_formatted + ".xlsx"
        df = pd.read_excel(path, header=1)
        df_new = df.drop(["差引", "差引.1", "差引.2"], axis=1)
        df_new = df_new[(df_new["売り買い"] != "総合計")&(df_new["売り買い"] != "取引参加者")]
        data_bar = [go.Bar(name=i, x=x, y=np.delete(df_new[df_new["売り買い"]==i].values, 0)) for i in df_new["売り買い"].unique()]
        fig = go.Figure(data=data_bar)
        fig.update_layout(
            barmode='group', 
            plot_bgcolor= colors['background'], 
            paper_bgcolor= colors['background'], 
            font={'color': colors['text']},
            legend_title_text = "Institutions"
        )
        fig.update_yaxes(title_text="JPY")
        return fig

if __name__ == '__main__':
    app.run_server(debug=True, host = '127.0.0.1', port=8080)