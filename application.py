import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table
from dash.exceptions import PreventUpdate
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

# initial date
today = dt.today().replace(microsecond=0, second=0, minute=0, hour=0)
today = date(today.year, today.month, today.day)

# calling data from S3
client = boto3.client('s3')

path_hist_group_product='s3://jpx-future-bucket/final_data/historical_by_group_product.csv'
df_hist_group_product = pd.read_csv(path_hist_group_product)
groups = df_hist_group_product[df_hist_group_product['product']=='日経225（ラージ）']
data=[]
groups = df_hist_group_product.groupby(["group"])
for group, dataframe in groups:
    dataframe = dataframe.sort_values(by=['date'])
    trace = go.Scatter(x=dataframe['date'].tolist(), 
                       y=dataframe['buy'].tolist(),
                       # marker=dict(color=colors[len(data)]),
                       name=group)
    data.append(trace)

layout =  go.Layout(xaxis={'title': 'Date'},
                    yaxis={'title': 'JPY'},
                    margin={'l': 40, 'b': 40, 't': 50, 'r': 50},
                    hovermode='closest')

line_fig = go.Figure(data=data, layout=layout)  


# Layout instances
app = dash.Dash(__name__)
application = app.server
app.title='Visualizing JPX Future Trading'
app.css.append_css({'external_url': 'style.css'})
app.server.static_folder = 'static'

colors = {
    'background': '#111000',
    'text': '#7FDBFF'
}

# Elements to be declared in advance
def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H5("Manufacturing SPC Dashboard"),
                    html.H6("Process Control and Exception Reporting"),
                ],
            )
        ],
    )

button_group = dbc.ButtonGroup([
    dbc.Button("Nikkei 225 Large", id='btn-large'), 
    dbc.Button("Nikkei 225 Mini", id='btn-mini'), 
    dbc.Button("Nikkei 225 Sum", id='btn-nikkei-sum'), 
    dbc.Button("TOPIX", id='btn-topix'), 
    dbc.Button("Total", id='btn-total')],
    id="btn-chart-type", size="large"
)

dropdown = dcc.Dropdown(
        id='dropdown',
        options=[
            {'label': 'Nikkei 225 Large', 'value': 'large'},
            {'label': 'Nikkei 225 Mini', 'value': 'mini'},
            {'label': 'Nikkei 225 Sum', 'value': 'nikkei-sum'},
            {'label': 'TOPIX', 'value': 'topix'},
            {'label': 'Total', 'value': 'total'}
        ],
        value='large',
        clearable=False
    )

date_picker = dcc.DatePickerSingle(
    id='date-picker',
    date=today
    )

history_radio = dcc.RadioItems(
    id="history-radio",
    options=[
        {'label': 'BUY', 'value': 'buy'},
        {'label': 'SELL', 'value': 'sell'},
        {'label': 'DIFF', 'value': 'diff'}
    ],
    value='buy',
    labelStyle={'display': 'block'}
)

history_product_dropdown = dcc.Dropdown(
    id="history-product-dropdown",
    options=[
        {'label': 'Nikkei 225 Large', 'value': '日経225（ラージ）'},
        {'label': 'Nikkei 225 Mini', 'value': '日経225（ミニ）'},
        {'label': 'Nikkei 225 Sum', 'value': 'Nikkei sum'},
        {'label': 'TOPIX', 'value': 'TOPIX'},
        {'label': 'Total', 'value': 'total'}
    ],
    value='日経225（ラージ）',
    clearable=False
)

# Layout
app.layout = html.Div(
    id="big-app-container",
    style={'color': colors['background']},
    children=[
        build_banner(),
        dcc.Tabs(
            children=[
                dcc.Tab(label='By institutions', 
                children=[
                    # html.Div(button_group),
                    html.Div([
                        html.Div(dropdown, style={"width": "15%", "height":"40pts", 'display': 'inline-block'}),
                        html.P("on", style={"width": "5%", "height":"40pts", 'display': 'inline-block'}),
                        html.Div(date_picker, style={"width": "20%", "height":"40pts", 'display': 'inline-block'})
                        ]),

                    html.Div([
                        dcc.Graph(id='container-graph')
                        ]),

                    html.Div(children='Table', style={
                        'textAlign': 'center',
                        'color': colors['text']
                    }),

                    html.Div([
                        dash_table.DataTable(
                            id='institutions_table',
                            data=[],
                            style_cell=dict(textAlign='left'),
                            style_header=dict(backgroundColor="paleturquoise", fontWeight='bold'),
                            style_data=dict(backgroundColor="lavender"),
                            style_data_conditional=[
                                {
                                    'if': {'row_index': 'odd'},
                                    'backgroundColor': 'rgb(248, 248, 248)'
                                }
                            ]
                        )
                    ])
                ]),

            dcc.Tab(label='Historical data', children=[
                html.Div([
                    html.Div([dcc.Graph(id='historical-graph', figure=line_fig)], style={"width":"85%", 'display': 'inline-block'}),
                    html.Div([history_product_dropdown, history_radio], style={"width":"15%", 'display': 'inline-block'})
                    ]),
                html.Div([
                        dash_table.DataTable(
                            id='historical-table',
                            data=[],
                            style_cell=dict(textAlign='left'),
                            style_header=dict(backgroundColor="paleturquoise", fontWeight='bold'),
                            style_data=dict(backgroundColor="lavender"),
                            style_data_conditional=[
                                {
                                    'if': {'row_index': 'odd'},
                                    'backgroundColor': 'rgb(248, 248, 248)'
                                    }
                                ]
                            )
                    ])
            ])
        ])
    ]
)

@app.callback(
    Output('container-graph', 'figure'), 
    Output("institutions_table", "data"), 
    Output('institutions_table', 'columns'),
    Input('dropdown', 'value'),
    Input('date-picker', 'date')
    )
def update_figure_and_table(value, date):
    date_string = dt.strptime(date, "%Y-%m-%d")
    date_formatted = str(date_string.year) +"-"+ str(date_string.month) +"-"+ str(date_string.day)
    path='s3://jpx-future-bucket/final_data/df_total_final' + date_formatted + '.csv'
    try:
        df = pd.read_csv(path, header=0, index_col=0)
        print("data found")
    except:
        df = pd.DataFrame({"institutions_code":[np.nan], "buy":[np.nan], "sell":[np.nan], "product":["日経225（ラージ）"], "institutions_eng":[np.nan]})

    df_large = df[df['product']=="日経225（ラージ）"].sort_values("buy", ascending=False)
    df_mini = df[df['product']=="日経225（ミニ）"].sort_values("buy", ascending=False)
    df_nikkei_sum = df[df['product']=="Nikkei sum"].sort_values("buy", ascending=False)
    df_topix = df[df['product']=="TOPIX"].sort_values("buy", ascending=False)
    df_total = df[df['product']=="total"].sort_values("buy", ascending=False)

    dict = {'large':df_large, 'mini':df_mini, 'nikkei-sum':df_nikkei_sum, 'topix':df_topix, 'total':df_total}
    df_selected = dict[value]
    institutions = df_selected['institutions_eng'].values
    buy = df_selected['buy'].values
    sell = [-v for v in df_selected['sell'].values]

    fig = go.Figure(data=[
        go.Bar(name='BUY', x=institutions, y=buy),
        go.Bar(name='SELL', x=institutions, y=sell)
        ])
    fig.update_layout(
        transition_duration=80,
        plot_bgcolor= colors['background'], 
        paper_bgcolor= colors['background'], 
        font={'color': colors['text']}
        )
    return fig, df_selected.to_dict('records'), [{"name": i, "id": i}  for i in df_selected.columns]

"""
@app.callback([Output("institutions_table", "data"), Output('institutions_table', 'columns')],
              Input('dropdown', 'value'),
              Input('date-picker', 'date')
              )
def update_table(value, date):
    date_string = dt.strptime(date, "%Y-%m-%d")
    date_formatted = str(date_string.year) +"-"+ str(date_string.month) +"-"+ str(date_string.day)
    path='s3://jpx-future-bucket/final_data/df_total_final' + date_formatted + '.csv'
    try:
        df = pd.read_csv(path, header=0, index_col=0)
    except:
        df = pd.DataFrame({"institutions_code":[np.nan], "buy":[np.nan], "sell":[np.nan], "product":["日経225（ラージ）"], "institutions_eng":[np.nan]})

    df_large = df[df['product']=="日経225（ラージ）"].sort_values("buy", ascending=False)
    df_mini = df[df['product']=="日経225（ミニ）"].sort_values("buy", ascending=False)
    df_nikkei_sum = df[df['product']=="Nikkei sum"].sort_values("buy", ascending=False)
    df_topix = df[df['product']=="TOPIX"].sort_values("buy", ascending=False)
    df_total = df[df['product']=="total"].sort_values("buy", ascending=False)
    
    dict = {'large':df_large, 'mini':df_mini, 'nikkei-sum':df_nikkei_sum, 'topix':df_topix, 'total':df_total}
    df_selected = dict[value]
    institutions = df_selected['institutions_eng'].values
    buy = df_selected['buy'].values
    sell = [-v for v in df_selected['sell'].values]

    return df_selected.to_dict('records'), [{"name": i, "id": i}  for i in df_selected.columns]
"""
@app.callback(
    Output("historical-graph", 'figure'),
    Output("historical-table", "data"), 
    Output('historical-table', 'columns'),
    Input("history-product-dropdown", 'value'),
    Input("history-radio", 'value'))
def update_history_figure_and_table(value1, value2):
    print("v1", value1)
    print("v2", value2)
    data=[]
    df_prod = df_hist_group_product[df_hist_group_product['product']==value1]
    groups = df_prod.groupby(["group"])
    for group, dataframe in groups:
        dataframe = dataframe.sort_values(by=['date'])
        trace = go.Scatter(x=dataframe['date'].tolist(), 
                        y=dataframe[value2].tolist(),
                        # marker=dict(color=colors[len(data)]),
                        name=group)
        data.append(trace)

    line_fig = go.Figure(data=data, layout=layout)  
    line_fig.update_layout(
        xaxis={'title': 'Date'},
        yaxis={'title': 'JPY'},
        margin={'l': 40, 'b': 40, 't': 50, 'r': 50},
        hovermode='closest',
        transition_duration=80,
        plot_bgcolor= colors['background'], 
        paper_bgcolor= colors['background'], 
        font={'color': colors['text']}
        )

    return line_fig, df_prod.to_dict('records'), [{"name": i, "id": i}  for i in df_prod.columns]

if __name__ == '__main__':
    app.run_server(debug=True, host = '127.0.0.1', port=8080)# 