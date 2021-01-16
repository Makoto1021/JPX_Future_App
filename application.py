import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
import os
import os
from datetime import datetime as dt
from datetime import timedelta
import plotly.graph_objects as go
import plotly.figure_factory as ff
import numpy as np

day = dt.today() - timedelta(days=1)
dirname = os.getcwd()
filename = '完成データ/' + '先物' + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".xlsx"
SAVED_DATA = os.path.join(dirname, filename)
df = pd.read_excel(SAVED_DATA, header=1)

df_new = df.drop(["差引", "差引.1", "差引.2"], axis=1)
df_new = df_new[(df_new["売り買い"] != "総合計")&(df_new["売り買い"] != "取引参加者")]
x = ["第１限月　売り", "第１限月　買い", "第２限月　売り", "第２限月　買い", "第３限月　売り", "第３限月　買い",]
data_list = [{'x': x, 'y': np.delete(df_new[df_new["売り買い"]==i].values, 0), 'type': 'bar', 'name': i} for i in df_new["売り買い"].unique()]


app = dash.Dash(__name__)
application = app.server
app.title='Dash on AWS EB!'

colors = {
    'background': '#111000',
    'text': '#7FDBFF'
}

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

    dcc.Graph(
        id='Graph1',
        figure={
            'data': data_list,
            'layout': {
                'plot_bgcolor': colors['background'],
                'paper_bgcolor': colors['background'],
                'font': {
                    'color': colors['text']
                }
            }
        }
    ),
    html.Div(children='Table', style={
        'textAlign': 'center',
        'color': colors['text']
    }),
    
    html.Div([dash_table.DataTable(
        id='jpx_future_table',
        columns=[{"name": i, "id": i}  for i in df.columns],
        data=df.to_dict('records'),
        style_cell=dict(textAlign='left'),
        style_header=dict(backgroundColor="paleturquoise"),
        style_data=dict(backgroundColor="lavender")
    )
    ])
])

if __name__ == '__main__':
    app.run_server(debug=True, host = '127.0.0.1', port=8080)