from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import country_converter as coco
from collections import defaultdict

# for testing purposes
# from filter_stations import retreive_data, Interactive_maps, Filter
import json
# # Authentication
# with open('config.json') as f:
#     conf = json.load(f)

# apiKey = conf['apiKey']
# apiSecret = conf['apiSecret']

# fs = retreive_data(apiKey, apiSecret)

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
# TA00162_S000169_clogFlags

app.layout = html.Div([
    html.H4('Intensity of the clogs'),
    html.P("Select year:"),
    dcc.Dropdown(
        id="candidate",
        options=[2017, 2018, 2019, 2020, 2021, 2022],
        value=2022,
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

    html.H4('Clog flag analysis'),
    dcc.Graph(id="time-series-chart3"),
    html.P("Choose a station:"),
    dcc.Dropdown(
        id="ticker3",
        options=list(clog.columns),
        value="TA00001_S000007_clogFlags",
        clearable=False,
    ),

    html.H4('Precipitation analysis'),
    dcc.Graph(id="time-series-chart2"),
    html.P("Choose a station:"),
    dcc.Dropdown(
        id="ticker2",
        options=list(pr.columns),
        value='TA00001_S000007',
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
    fig = px.line(pr[ticker2],
              title='Amount of precipitation per day',)
    return fig

@app.callback(
    Output("time-series-chart3", "figure"), 
    Input("ticker3", "value"))
def display_time_series(ticker3):
    print(ticker3)
    # df = px.data.stocks() # replace with your own data source
    fig = px.line(clog[ticker3],
              title='Clog Flag per day',)
    return fig

@app.callback(
    Output("graph", "figure"), 
    Input("candidate", "value"))
def loc(candidate):
    with open('african_countries.json') as af:
        afri = json.load(af)
    
    # filter by the year selected
    clog_year = clog[clog.index.year == candidate]
    point = [-0.833111, 36.669667]

    # groupby country
    met2 = metadata.groupby(['location.countrycode', 'code']).count().index
    met3 = pd.DataFrame(data=[met2.get_level_values(0), met2.get_level_values(1)]).T

    country_dict = dict()

    for country in met3[0].unique():
        country_dict[country] = met3.loc[met3[0] == country][1].values

    
    val_key = defaultdict(list)

    # val_key = dict()
    for key, value in country_dict.items():
        for clog_f in clog_year.columns:
            if clog_f.split('_')[0] in value:
                # get the value counts for each key
                if len(clog_year[clog_f].value_counts().values) >= 2 and clog_year[clog_f].value_counts().index[1] != 2: # this shows it has a clog flag of 1
                    # get the number of stations with the clog flag
                    val_key[key].append(clog_f)

    # get the sum of the keys and replace with the number
    for key, value in val_key.items():
        # print(value)
        val_key[key] = len(set([i.split('_')[0] for i in value]))

    g_tr = dict(met3.groupby(0).count().reset_index().values)
    val_key = dict(val_key)

    for ky, vl in g_tr.items():
        # print(val_key[ky])
        try:
            # print(val_key[ky])
            val_key[ky] = round((val_key[ky]/g_tr[ky])*100, 2)
        except KeyError as e:
            # print(e.args[0])
            val_key[e.args[0]] = 0

    count_list_st = coco.convert(list(val_key.keys()), to='name_short')

    # for key, val in val_key.items():
    #     val_key[key] = round((val/tot)*100, 2)

# Create a list of African countries and their respective population
    countries = count_list_st
    population = list(val_key.values())

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


if __name__ == '__main__':
    app.run_server(debug=True)