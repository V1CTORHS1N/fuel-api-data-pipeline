from MqttObj import MqttObj
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output, callback, dash_table
from dash.exceptions import PreventUpdate
import json
import datetime as dt

df = pd.DataFrame([], columns=['brand', 'code', 'name', 'address', 'fueltype', 'price', 'lastupdated', 'latitude',
                               'longitude', 'state'])
count = 0
selected = ""


def on_message(client, userdata, message):
    data = message.payload.decode("utf-8")
    data = json.loads(data)
    global df
    _df = pd.DataFrame.from_dict(data, orient='index').T
    df = pd.concat([df, _df], ignore_index=True)
    df.sort_values(by='name', inplace=True, ignore_index=True, ascending=True)


mqtt = MqttObj()
mqtt.subscribe("fuel-api/processed")
mqtt.client.on_message = on_message
mqtt.client.loop_start()

app = Dash(__name__)
app_server = app.server
app.layout = html.Div([
    html.Div([
        dcc.Graph(id='overview', figure=px.box(title='NSW Fuel Price Distribution')),
    ], id='inner-div1'),
    dcc.Dropdown(id='dropdown', clearable=False, placeholder='Select the Station Name'),
    html.Div([
        html.Div([
            dcc.Graph(id='graph', figure=px.line(title='Station Name')),
        ], className='column', style={'width': '100%', 'display': 'inline-block'}),
        html.Div([
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i, "type": "text"} for i in ['Fuel Type', 'Price', 'Date']],
                page_size=5,
                style_as_list_view=True
            )
        ], className='column', style={'width': '100%', 'display': 'inline-block'}),], id='inner-div2'),
    dcc.Interval(id='interval', interval=1000, n_intervals=0)
])


@callback(Output('overview', 'figure'),
          Input('interval', 'n_intervals')
          )
def update_overview(n_intervals):
    global df, count
    _df = df.copy()
    _df = _df[_df['lastupdated'].apply(lambda x: pd.to_datetime(x)) > (dt.datetime.now() - dt.timedelta(days=1))]
    if count == len(_df):
        raise PreventUpdate
    count = len(_df)
    fig = px.box(_df, x='fueltype', y='price', title='NSW 24 Hours Fuel Price Overview', color='fueltype')
    fig.update_layout(xaxis_title='Fuel Type', yaxis_title='Price', legend_title='Fuel Type')
    return fig


@callback(Output('dropdown', 'options'),
          Input('dropdown', 'search_value'),
          Input('interval', 'n_intervals')
          )
def update_dropdown(search_value, n_intervals):
    if not search_value:
        return [{'label': i, 'value': i} for i in df['name'].unique()]
    return [{'label': i, 'value': i} for i in df['name'].unique() if search_value.upper() in i.upper()]


@callback(Output('graph', 'figure'),
          Input('dropdown', 'value'),
          Input('interval', 'n_intervals')
          )
def update_graph(value, n_intervals):
    global df, selected
    if value is None:
        raise PreventUpdate
    data = df[df['name'] == value].sort_values(by=['fueltype', 'lastupdated'], ascending=[True, False])
    if selected == f'{value}_{len(data)}':
        raise PreventUpdate
    selected = f'{value}_{len(data)}'
    fig = px.line(data, x='lastupdated', y='price', color='fueltype', markers=True, title=value)
    fig.update_layout(xaxis_title='Date', yaxis_title='Price', legend_title='Fuel Type')
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1D", step="day", stepmode="backward"),
                dict(count=3, label="3D", step="day", stepmode="backward"),
                dict(count=7, label="7D", step="day", stepmode="backward"),
                dict(step="all", label="All")
            ])
        )
    )
    return fig


@callback(Output('table', 'data'),
          Input('dropdown', 'value'),
          Input('interval', 'n_intervals')
          )
def update_table(value, n_intervals):
    global df
    data = df[df['name'] == value]
    data = data.sort_values(by=['fueltype', 'lastupdated'], ascending=[True, False])
    data.rename(columns={'fueltype': 'Fuel Type', 'price': 'Price', 'lastupdated': 'Date'}, inplace=True)
    return data[['Fuel Type', 'Price', 'Date']].to_dict('records')


if __name__ == '__main__':
    app.run(debug=True, dev_tools_ui=False)
