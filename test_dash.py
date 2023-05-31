from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

# for testing purposes
from filter_stations import retreive_data, Interactive_maps, Filter
import json
# Authentication
with open('config.json') as f:
    conf = json.load(f)

apiKey = conf['apiKey']
apiSecret = conf['apiSecret']

fs = retreive_data(apiKey, apiSecret)

# all the data
stations = pd.read_csv('station_flags.csv', parse_dates=['Date']).set_index('Date')

# stations information as the metadata
metadata = pd.read_csv('stations_metadata.csv')

# get the precipitation data
pr_cols = [i for i in stations.columns if i.split('_')[-1] != 'clogFlags']
pr = stations[pr_cols]
# get the clog flag data
clog_cols = [i for i in stations.columns if i.split('_')[-1] == 'clogFlags']
clog = stations[clog_cols]

# get the proportion of clogged stations at a given time
clog_prop = clog.apply(pd.Series.value_counts, axis=1)

# fill null values
clog_prop = clog_prop.fillna(0)

# calculate the percentage of clogs per day x 100 and rounded to 2 decimal places
clog_prop_p = clog_prop.apply(lambda x: round(x/x.sum()*100, 2), axis=1)
# clog_prop_p = clog_prop.apply(lambda x: x/x.sum(), axis=1)

# the number of stations installed per day
met_code_date = metadata[['code', 'installationdate']]

app = Dash(__name__)


app.layout = html.Div([
    html.H4('Intensity of the clogs'),
    html.P("Select year:"),
    dcc.Dropdown(
        id="candidate",
        options=[2017, 2018, 2019, 2020, 2021, 2022],
        value="AMZN",
        clearable=False,
    ),
    dcc.Graph(id="graph"),

    html.H4('Clog Flag analysis'),
    dcc.Graph(id="time-series-chart"),
    html.P("Choose year:"),
    dcc.Dropdown(
        id="ticker",
        options=[2017, 2018, 2019, 2020, 2021, 2022],
        value="AMZN",
        clearable=False,
    ),

    html.H4('Precipitation analysis'),
    dcc.Graph(id="time-series-chart2"),
    html.P("Choose a station:"),
    dcc.Dropdown(
        id="ticker2",
        options=["AMZN", "FB", "NFLX"],
        value="AMZN",
        clearable=False,
    ),

    html.H4('Clog flag analysis'),
    dcc.Graph(id="time-series-chart3"),
    html.P("Choose a station:"),
    dcc.Dropdown(
        id="ticker3",
        options=["AMZN", "FB", "NFLX"],
        value="AMZN",
        clearable=False,
    ),
])


@app.callback(
    Output("time-series-chart", "figure"), 
    Input("ticker", "value"))
def display_time_series(ticker):
    print(ticker)
    df = px.data.stocks() # replace with your own data source
    fig = px.line(clog_prop_p.drop(0.0, axis=1),
              title='Percentage of clog flags per day',)
    return fig

@app.callback(
    Output("time-series-chart2", "figure"), 
    Input("ticker2", "value"))
def display_time_series(ticker2):
    print(ticker2)
    df = px.data.stocks() # replace with your own data source
    fig = px.line(pr[pr.columns[0:3]],
              title='Amount of precipitation per day',)
    return fig

@app.callback(
    Output("time-series-chart3", "figure"), 
    Input("ticker3", "value"))
def display_time_series(ticker3):
    print(ticker3)
    df = px.data.stocks() # replace with your own data source
    fig = px.line(clog[clog.columns[0:3]],
              title='clogFlag per day',)
    return fig

@app.callback(
    Output("graph", "figure"), 
    Input("candidate", "value"))
def loc(candidate):
    with open('african_countries.json') as af:
        afri = json.load(af)

    point = [-0.833111, 36.669667]

# Create a list of African countries and their respective population
    countries = afri['countries']
    population = [44200000, 100000, 22, 20, 25868000, 10575614, 2302878, 19034397, 10114505, 531239, 22709892, 4745185, 14497000, 806153, 47410000, 24294750, 956985, 91251839, 1358276, 4474690, 1093238, 91205855, 2025137, 2234858, 27499924, 10531273, 1890908, 49699862, 2074000, 4853516, 6678559, 19181144, 16745303, 18541980, 4077347, 1268316, 35126261, 29668834, 25068000, 18045729, 19194473, 11498000, 10139177, 8967541, 15854360, 5875414, 57398425, 12575714, 3024405, 44877000, 7714502, 11659174, 40006799, 16913261, 12894316]

# Create a data dictionary for Plotly
    data = dict(
    type='choropleth',
    locations=countries,
    locationmode='country names',
    z=population,
    colorscale='YlOrRd',
    text=countries,
    colorbar=dict(title='Percentage')
)

# Create a layout dictionary for Plotly
    layout = dict(
    title='Intensity of clog Flag 1',
    geo=dict(
        showframe=False,
        showcoastlines=False,
        projection_type='equirectangular'
    ),
    height=800,
    width=1200
)

# Create a figure using the data and layout dictionaries
    fig = go.Figure(data=[data], layout=layout)
    fig.update_layout(mapbox_zoom=17, mapbox_center = {"lat": point[0], "lon": point[1]})
    return fig
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)