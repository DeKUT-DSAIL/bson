import pandas as pd
import geopy
import plotly 
import plotly.express as px
from urllib.request import urlopen
import json
import numpy as np
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
import filter_stations as fs
import pycountry


# Load json file with apiKey
with open('config.json') as f:
    conf = json.load(f)

px.set_mapbox_access_token(conf['apiKey'])

stations = fs.getStationsInfo()
# print(stations.columns)

app = Dash(__name__)

app.layout = html.Div([
    html.H4('TAHMO Stations'),
    dcc.Dropdown(
        id="dropdown",
        options=[pycountry.countries.get(alpha_2=f'{i}').name for i in stations['location.countrycode'].unique().tolist()],
        # value=['sepal_length', 'sepal_width'],
        multi=True
    ),
    dcc.Input(
        id="search",
        type="text",
        placeholder="Search by station code..."
    ),
    dcc.Graph(
                id="graph",
                style={
                       'width': '100vw',
                       'height': '100vh'
                }
                ),
])


@app.callback(
    Output("graph", "figure"), 
    [Input("dropdown", "value"),
     Input("search", "value")])

def update_bar_chart(selected_value, search_value):
    if selected_value is None:
        df = stations
    else:
        df = stations[stations['location.countrycode'].isin(selected_value)]
    if search_value is not None:
        df = df[df['code'].str.contains(search_value)]
    fig = px.scatter_mapbox(
    df, 
    lat='location.latitude', 
    lon='location.longitude', 
    hover_name='code',
    title = 'TAHMO Stations',
    )
    # Add the rectangle shape layer
    fig.add_shape(
                    type='rect',
                    x0=-75.0,
                    y0=40.0,
                    x1=-74.0,
                    y1=41.0,
                    line=dict(color='red'),
                    fillcolor='rgba(255, 0, 0, 0.2)'
                )
    

    return fig

@app.callback(
    Output("output", "children"), 
    Input("graph", "relayoutData"))
def get_regions(relayoutData):
    if 'mapbox.zoom' in relayoutData:
        zoom_level = relayoutData['mapbox.zoom']
    else:
        zoom_level = None

    if 'mapbox.center' in relayoutData:
        center = relayoutData['mapbox.center']
    else:
        center = None

    if 'shapes' in relayoutData:
        shapes = relayoutData['shapes']
    else:
        shapes = []

    regions = []
    for shape in shapes:
        if shape['type'] == 'rect':
            x0, y0 = shape['x0'], shape['y0']
            x1, y1 = shape['x1'], shape['y1']
            region = {'x0': x0, 'y0': y0, 'x1': x1, 'y1': y1}
            regions.append(region)

    return regions


if __name__ == "__main__":
    app.run_server(debug=False)
